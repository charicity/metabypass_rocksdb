#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).
"""Normal mixed commands, zero phase, write placement and inherited safety."""

import copy
import json
from pathlib import Path
import sys
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
import node_normal as NORMAL
import node_tradeoff as TRADE
import test_tradeoff as FIXTURE


def protocol_events(capacity=1000, adaptive=True, unprotected=100):
    events = [{"event": "histogram_schema", "upper_bounds_us": list(range(1, 194))},
              {"event": "ready", "steady_time_us": 1}]
    stamp = 1
    for index, (phase, seconds) in enumerate(NORMAL.DURATIONS.items()):
        count = seconds * NORMAL.TARGET_RATE
        counters = {key: 0 for key in TRADE.node_diagnose.MIGRATION_COUNTERS}
        counters.update(protected_bytes=1000 if adaptive else 0,
                        unprotected_bytes=unprotected if adaptive else 0,
                        ssd_bytes=600 if adaptive else 0, reserved_bytes=0, pending_delete_bytes=0,
                        sampled_reads=(10, 20, 20)[index], migration_errors=0,
                        ssd_capacity_bytes=capacity, hdd_bytes=1000 if adaptive else 0)
        stamp += seconds * 1000000
        event = {"event": "phase_end", "phase": phase, "elapsed_us": seconds * 1000000,
                 "steady_time_us": stamp, "stats": counters, "ops": count, "ok": True,
                 "target_ops_per_sec": NORMAL.TARGET_RATE if seconds else 0,
                 "scheduled_ops": count, "unfinished_ops": 0, "late_ops": 0,
                 "late_fraction": 0 if seconds else None}
        for op, numerator in (("get", 90), ("put", 5), ("delete", 5)):
            n = count * numerator // 100
            hist = {"count": n, "buckets": [n] + [0] * 192, "max_us": 1 if n else 0}
            event[op] = {"count": n, "service": copy.deepcopy(hist), "response": copy.deepcopy(hist)}
        if count:
            events.append({**copy.deepcopy(event), "event": "window"})
        events.append(event)
    events.append({"event": "summary", "ok": True, "stats": copy.deepcopy(counters)})
    return events


class NormalTest(unittest.TestCase):
    def setUp(self):
        FIXTURE.TradeoffTest.setUp(self)

    def arguments(self, *extra):
        return FIXTURE.TradeoffTest.arguments(self, *extra)

    def runner(self, *extra):
        return NORMAL.NormalRunner(NORMAL.parse_args(self.arguments(*extra)))

    def fake_execute(self, runner, failures=(), mutate_disabled=False):
        calls, sources = [], []

        def execute(name, command, dirs, timeout=480):
            calls.append((name, command, timeout))
            if "--clone" in command:
                i = command.index("--clone")
                source = tuple(map(Path, command[i + 1:i + 3])); sources.append(source)
                TRADE.node_run.clone_dataset(zip(source, dirs))
                events = [{"event": "summary", "ok": name not in failures}]
            else:
                flags = dict(item[2:].split("=", 1) for item in command[1:])
                events = protocol_events(int(flags["metabypass_sst_capacity_bytes"]),
                                         flags["metabypass_sst_mode"] == "adaptive")
                if mutate_disabled and flags["metabypass_sst_mode"] == "disabled":
                    (dirs[0] / "index/000099.sst").write_bytes(b"new flushed SST")
            samples = runner.control / (name + ".samples.jsonl"); samples.write_text("")
            return {"name": name, "command": command, "events": events,
                    "valid": name not in failures, "oom_kill_delta": 0,
                    "elapsed_s": 1, "exit_code": int(name in failures), "timed_out": False,
                    "samples_path": str(samples), "exit_space": TRADE.node_run.space_snapshot(*dirs)}
        return execute, calls, sources

    def test_twenty_round_defaults_reverse_order_without_changing_frozen_catalog(self):
        old = copy.deepcopy((TRADE.POLICIES, TRADE.DURATIONS, TRADE.DEFAULT_STRATEGIES))
        args = NORMAL.parse_args(self.arguments())
        plan = NORMAL.trial_plan(args.strategies, args.repeats)
        self.assertEqual(20, len(plan))
        self.assertEqual(list(NORMAL.STRATEGIES), [r["strategy"] for r in plan[:10]])
        self.assertEqual(list(reversed(NORMAL.STRATEGIES)), [r["strategy"] for r in plan[10:]])
        self.assertTrue(all(r["workload"] == "mixed" and r["durations"] == {"warmup":60,"measured":180,"saturated":0} for r in plan))
        self.assertEqual((10800, "small", "0,1,2,3", True),
                         (args.deadline_seconds, args.profile, args.cpu_list, args.keep_data))
        self.assertEqual(old, (TRADE.POLICIES, TRADE.DURATIONS, TRADE.DEFAULT_STRATEGIES))
        self.assertEqual((90, 5, 5, 80, 25), tuple(NORMAL.WORKLOAD[k] for k in
                         ("get_percent","put_percent","delete_percent","hot_access_percent","hot_key_percent")))

    def test_ten_policy_parameters_and_native_rounding_include_conservative80(self):
        for baseline in (1, 199, 1001, 22670692):
            for name in NORMAL.STRATEGIES:
                with self.subTest(name=name, baseline=baseline):
                    c = NORMAL.policy_configuration(name, baseline)
                    expected = (1,1,0,0.0) if name.startswith("aggressive") else \
                               (2,8,30000,1.0) if name.startswith("conservative") else (2,3,10000,.25)
                    self.assertEqual(expected, tuple(c["parameters"][k] for k in
                                     ("promote_rounds","demote_rounds","min_residency_ms","replacement_margin")))
                    self.assertEqual(NORMAL.POLICY_VERSION, c["version"])
                    cap = c["capacity_bytes"]
                    self.assertEqual(cap - cap // 100 * 10 - cap % 100 * 10 // 100, c["effective_budget_bytes"])
                    self.assertEqual(baseline * int(name[-2:]) // 100 if name != "disabled_ssd" else 0, cap)
        self.assertEqual(72, NORMAL.policy_configuration("conservative80",10000)["effective_budget_percent"])

    def test_all_policy_flags_are_unique_and_zero_parameters_reach_fixed_only_command(self):
        runner = NORMAL.NormalRunner.__new__(NORMAL.NormalRunner)
        runner.args = NORMAL.parse_args(self.arguments()); runner.keys=TRADE.KEYS; runner.rate=NORMAL.TARGET_RATE
        for name in NORMAL.STRATEGIES:
            c=NORMAL.policy_configuration(name,10000); argv=runner.trial_command(self.seed,c,"mixed")
            pairs=[v[2:].split("=",1) for v in argv[1:]]; flags=dict(pairs)
            self.assertEqual(len(pairs),len(flags))
            for k,v in c["parameters"].items(): self.assertEqual(str(v),flags[TRADE.PARAMETER_FLAGS[k]])
            for k,v in {"metabypass_sst_target_ops_per_sec":"80","metabypass_sst_warmup_seconds":"60",
                        "metabypass_sst_measure_seconds":"180","metabypass_sst_saturated_seconds":"0",
                        "metabypass_sst_flush_interval_ms":"10000","metabypass_sst_cache_bytes":"65536",
                        "metabypass_queue_capacity":"2097152","num":"1000000","metabypass_sst_read_delay_us":"0",
                        "sync":"false"}.items():
                self.assertEqual(v,flags[k])

    def test_invalid_filters_paths_deadlines_and_output_reuse_are_rejected(self):
        for extra in (("--deadline-seconds","10801"),("--deadline-seconds","0"),("--repeats","3"),
                      ("--workloads","uniform"),("--workloads","mixed","mixed"),
                      ("--strategies","default80","default80"),("--space-interval","4")):
            with self.subTest(extra=extra),mock.patch("sys.stderr"),self.assertRaises(SystemExit):
                NORMAL.parse_args(self.arguments(*extra))
        (self.root / "report.json").write_text("existing evidence")
        with mock.patch("sys.stderr"),self.assertRaises(SystemExit): NORMAL.parse_args(self.arguments())

    def test_zero_saturated_phase_is_valid_but_coverage_and_cache_are_not_applicable(self):
        row={"name":"one","events":protocol_events(),"durations":dict(NORMAL.DURATIONS)}
        self.assertEqual([],TRADE.node_run.phase_validation(row,row["durations"]))
        runner=self.runner("--strategies","default80","--repeats","1")
        runner.collect_evidence(row,self.seed)
        self.assertTrue(row["mixed_placement_coverage"]["covered"])
        self.assertEqual(["measured"],row["formal_phases"])
        self.assertEqual("not_applicable",row["phases"]["saturated"]["status"])
        self.assertEqual(0,row["phases"]["saturated"]["ops"])
        self.assertIsNone(row["cache_gate"]["saturated"]["passed"])

    def test_unprotected_new_hot_sst_allows_positive_protected_cold_lower_bound(self):
        events=protocol_events(unprotected=100); result=NORMAL.mixed_coverage(events,TRADE.node_diagnose.phase_deltas(events))
        self.assertTrue(result["covered"])
        observed=result["phases"]["measured"]["observations"][0]
        self.assertEqual((100,600,400),(observed["unprotected_bytes"],observed["hot_bytes"],observed["protected_cold_lower_bound_bytes"]))
        self.assertEqual(0,result["phases"]["measured"]["actual_migrations"])

    def test_hot_backup_copies_and_unprotected_only_cold_do_not_prove_coverage(self):
        for changes in ({"protected_bytes":600,"unprotected_bytes":500},
                        {"sampled_reads":10},{"migration_errors":1},
                        {"ssd_bytes":-1},{"protected_bytes":None}):
            events=protocol_events()
            for e in events:
                if e.get("phase")=="measured": e["stats"].update(changes)
            self.assertFalse(NORMAL.mixed_coverage(events,{})["covered"],changes)

    def test_reserved_and_pending_charges_are_excluded_from_live_hot_lower_bound(self):
        events=protocol_events()
        for e in events:
            if e.get("phase")=="measured": e["stats"].update(ssd_bytes=1200,reserved_bytes=300,pending_delete_bytes=300)
        observed=NORMAL.mixed_coverage(events,{})["phases"]["measured"]["observations"][0]
        self.assertEqual(400,observed["protected_cold_lower_bound_bytes"])

    def test_all_twenty_trials_clone_seed_and_preserve_independent_inode_evidence(self):
        runner=self.runner(); execute,calls,sources=self.fake_execute(runner)
        with mock.patch.object(runner,"preflight"),mock.patch.object(runner,"execute",side_effect=execute): runner.run()
        self.assertEqual("complete",runner.report["status"])
        self.assertEqual(40,len(calls));self.assertEqual([self.seed]*20,sources)
        self.assertTrue(all(r["independent_inode_check"]["passed"] for r in runner.report["trials"]))
        self.assertTrue(all(r["valid"] for r in runner.report["trials"]))
        runner.finalize();self.assertTrue(runner.report["seed"]["final_immutable_check"])

    def test_ordinary_failure_preserves_row_and_continues_without_rate_or_duration_change(self):
        runner=self.runner("--strategies","default80","disabled_ssd","--repeats","1")
        execute,calls,_=self.fake_execute(runner,failures=("repeat-1-mixed-default80",))
        with mock.patch.object(runner,"preflight"),mock.patch.object(runner,"execute",side_effect=execute): runner.run()
        self.assertEqual(["failed","complete"],[r["status"] for r in runner.report["trials"]])
        self.assertEqual(4,len(calls));self.assertEqual(80,runner.rate)
        self.assertTrue(all(r["durations"]==NORMAL.DURATIONS for r in runner.report["trials"]))

    def test_early_child_oom_stops_even_without_runner_oom_delta(self):
        runner=self.runner("--strategies","default80","--repeats","1")
        row=runner.report["trials"][0]
        def fail(*args,**kwargs):
            (self.cgroup/"memory.oom_control").write_text("oom_kill 1\nunder_oom 0\n")
            raise RuntimeError("child exited before identity")
        with mock.patch.object(runner,"execute",side_effect=fail),self.assertRaises(TRADE.SafetyStop):
            runner.execute_checked(row,["/bin/true"],self.seed,480)
        self.assertEqual(1,row["independent_oom_checks"][0]["oom_kill_delta"])

    def test_occupied_cgroup_blocks_clone_before_execution(self):
        runner=self.runner("--strategies","default80","--repeats","1")
        (self.cgroup/"tasks").write_text("12345\n")
        with mock.patch.object(runner,"execute") as execute,self.assertRaises(TRADE.SafetyStop):
            runner.copy_round(runner.report["trials"][0])
        execute.assert_not_called()

    def test_runtime_identity_includes_new_entry_and_all_frozen_modules_and_detects_change(self):
        runner=self.runner("--strategies","default80","--repeats","1")
        sources=runner.report["driver_identity"]["source_sha256"]
        self.assertEqual({"node_normal.py","node_tradeoff.py","node_run.py","node_cache.py","node_diagnose.py","node_summary.py"},
                         {Path(p).name for p in sources})
        helper=self.root/"helper.py";helper.write_text("frozen")
        sources[str(helper)]=TRADE.node_run.digest(helper);helper.write_text("changed")
        with self.assertRaises(TRADE.SafetyStop):runner.check_identity()
        runner.finalize();self.assertTrue(runner.report["final_integrity_errors"])
        self.assertTrue(all(r["status"]=="not_run" for r in runner.report["trials"]))

    def test_disabled_mixed_can_flush_new_sst_without_claiming_tiered_placement(self):
        runner=self.runner("--strategies","disabled_ssd","--repeats","1")
        execute,_,_=self.fake_execute(runner,mutate_disabled=True)
        with mock.patch.object(runner,"preflight"),mock.patch.object(runner,"execute",side_effect=execute):runner.run()
        row=runner.report["trials"][0]
        self.assertEqual("complete",row["status"])
        self.assertNotEqual(row["initial_snapshot"]["local_tables"],row["closed_snapshot"]["local_tables"])
        self.assertIsNone(row["closed_snapshot"]["placement_sha256"])


if __name__ == "__main__":
    unittest.main()
