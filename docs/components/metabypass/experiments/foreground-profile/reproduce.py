# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).
import subprocess,tempfile,pathlib,shutil,json,re
out=[]
for trial in range(5):
 for version in (['baseline','async-incremental'] if trial%2==0 else ['async-incremental','baseline']):
  root=pathlib.Path(tempfile.mkdtemp(prefix='mb-prof-'))
  cmd=['/tmp/metabypass-scan-ablation-bin/'+version,'--metabypass_mode=write','--num=200000','--value_size=1024','--sync=false','--db='+str(root/'index'),'--metabypass_data_dir='+str(root/'data'),'--metabypass_backup_dir='+str(root/'backup')]
  p=subprocess.run(cmd,capture_output=True,text=True,timeout=60);assert p.returncode==0,p.stderr
  r=dict(trial=trial,version=version,stdout=p.stdout,metrics=dict((k,int(v))for k,v in re.findall(r'(\w+)=(\d+)',p.stdout)))
  out.append(r);print(version,r['metrics']['foreground_us'],flush=True);shutil.rmtree(root)
pathlib.Path('/tmp/mb-foreground-profile/reproduce.json').write_text(json.dumps(out,indent=2))
