# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).
import subprocess,tempfile,pathlib,shutil,json,re,os,statistics
base=pathlib.Path('/tmp/mb-foreground-profile');out=[]
for phase,trials in [('plain',5),('hooks',3),('ram',3)]:
 for trial in range(trials):
  variants=['baseline','current','split-cv'];variants=variants[trial%3:]+variants[:trial%3]
  for version in variants:
   root=pathlib.Path(tempfile.mkdtemp(prefix='mb-prof-',dir='/dev/shm' if phase=='ram' else '/tmp'))
   binary=base/(version+'-driver');cmd=[str(binary),str(root),'200000'];env=os.environ.copy()
   if phase=='hooks':env['LD_PRELOAD']=str(base/'hooks.so')
   p=subprocess.run(cmd,capture_output=True,text=True,timeout=60,env=env);assert p.returncode==0,(cmd,p.stdout,p.stderr)
   m={k:int(v)for k,v in re.findall(r'(\w+)=(\d+)',p.stdout)}
   r=dict(phase=phase,trial=trial,version=version,command=cmd,stdout=p.stdout,stderr=p.stderr,metrics=m)
   if phase=='hooks':
    offsets=re.findall(r'offset=([0-9a-f]+)',p.stderr)
    r['symbols']=subprocess.check_output(['addr2line','-Cf','-e',str(binary),*['0x'+x for x in offsets]],text=True)
   out.append(r);(base/'results.json').write_text(json.dumps(out,indent=2));print(phase,version,m,flush=True);shutil.rmtree(root)
for phase in ['plain','hooks','ram']:
 for version in ['baseline','current','split-cv']:
  rows=[r['metrics']for r in out if r['phase']==phase and r['version']==version]
  print(phase,version,{k:statistics.median(r[k]for r in rows)for k in rows[0]},flush=True)
