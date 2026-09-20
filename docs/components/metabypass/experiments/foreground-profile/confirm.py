# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).
import subprocess,tempfile,pathlib,shutil,json,re,os
base=pathlib.Path('/tmp/mb-foreground-profile');out=[]
for phase,trials in [('plain',5),('hooks',3)]:
 for trial in range(trials):
  variants=['baseline','current','threshold'];variants=variants[trial%3:]+variants[:trial%3]
  for version in variants:
   root=pathlib.Path(tempfile.mkdtemp(prefix='mb-confirm-'));binary=base/(version+'-driver');cmd=[str(binary),str(root),'200000'];env=os.environ.copy()
   if phase=='hooks':env['LD_PRELOAD']=str(base/'hooks.so')
   p=subprocess.run(cmd,capture_output=True,text=True,timeout=60,env=env);assert p.returncode==0,(p.returncode,p.stdout,p.stderr)
   m={k:int(v)for k,v in re.findall(r'(\w+)=(\d+)',p.stdout)};r=dict(phase=phase,trial=trial,version=version,command=cmd,stdout=p.stdout,stderr=p.stderr,metrics=m)
   if phase=='hooks':
    offsets=re.findall(r'offset=([0-9a-f]+)',p.stderr)
    r['symbols']=subprocess.check_output(['addr2line','-Cf','-e',str(binary),*['0x'+x for x in offsets]],text=True)
   if phase=='plain' and trial==0:
    shutil.rmtree(root/'index');r['verification']=[]
    for mode in ['restore','verify']:
     c=['/tmp/metabypass-scan-ablation-bin/async-incremental','--metabypass_mode='+mode,'--num=200000','--value_size=1024','--db='+str(root/'index'),'--metabypass_data_dir='+str(root/'data'),'--metabypass_backup_dir='+str(root/'backup')]
     p=subprocess.run(c,capture_output=True,text=True,timeout=60);assert p.returncode==0,(p.stdout,p.stderr);r['verification'].append(dict(command=c,stdout=p.stdout))
   out.append(r);(base/'confirmation.json').write_text(json.dumps(out,indent=2));print(phase,version,m,flush=True);shutil.rmtree(root)
