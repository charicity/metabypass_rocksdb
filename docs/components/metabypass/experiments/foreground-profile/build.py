# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).
from pathlib import Path
import subprocess,shlex,shutil
root=Path('/home/lj/metabypass_rocksdb');out=Path('/tmp/mb-foreground-profile');lib='/tmp/metabypass-release-build/librocksdb.a'
config=Path('/tmp/metabypass-release-build/make_config.mk').read_text();flags=shlex.split(next(x.split('=',1)[1] for x in config.splitlines() if x.startswith('PLATFORM_CXXFLAGS=')))
for variant in ['current','baseline','split-cv']:
 d=out/variant.replace('current','current-src');d.mkdir(exist_ok=True)
 for file in ['backup.cc','backup.h','native_files.cc','native_files.h','separated_storage.cc','separated_storage.h','metabypass_db.cc']:
  rel='utilities/metabypass/'+file;p=d/rel;p.parent.mkdir(parents=True,exist_ok=True)
  p.write_bytes(subprocess.check_output(['git','show','e7663cf90:'+rel],cwd=root) if variant=='baseline' else (root/rel).read_bytes())
 rel='include/rocksdb/utilities/metabypass.h';p=d/rel;p.parent.mkdir(parents=True,exist_ok=True)
 p.write_bytes(subprocess.check_output(['git','show','e7663cf90:'+rel],cwd=root) if variant=='baseline' else (root/rel).read_bytes())
 if variant=='split-cv':
  p=d/'utilities/metabypass/backup.h';s=p.read_text().replace('std::condition_variable cv_;','std::condition_variable cv_;\n  std::condition_variable validator_cv_;');p.write_text(s)
  p=d/'utilities/metabypass/backup.cc';s=p.read_text().replace('if (stats_.error.ok()) stats_.error = status;','if (stats_.error.ok()) stats_.error = status;\n  validator_cv_.notify_all();').replace('candidate_ = std::move(candidate);','candidate_ = std::move(candidate);\n      validator_cv_.notify_one();').replace('mirror_done_ = true;','mirror_done_ = true;\n  validator_cv_.notify_all();').replace('cv_.wait(lock,\n             [&] { return candidate_', 'validator_cv_.wait(lock,\n             [&] { return candidate_');p.write_text(s)
 objs=[]
 for file in ['backup','native_files','separated_storage','metabypass_db']:
  obj=d/(file+'.o');objs.append(str(obj));subprocess.run(['g++','-O2','-DNDEBUG','-fno-rtti',*flags,'-I'+str(d),'-I'+str(d/'include'),'-I'+str(root),'-I'+str(root/'include'),'-c',str(d/'utilities/metabypass'/(file+'.cc')),'-o',str(obj)],check=True)
 subprocess.run(['g++','-O2','-std=c++20','-I'+str(d/'include'),'-I'+str(root/'include'),str(out/'driver.cc'),*objs,lib,'-lpthread','-ldl','-lrt','-lsnappy','-lz','-lbz2','-llz4','-lzstd','-lnuma','-ltcmalloc','-o',str(out/(variant+'-driver'))],check=True)
 print('built',variant,flush=True)
