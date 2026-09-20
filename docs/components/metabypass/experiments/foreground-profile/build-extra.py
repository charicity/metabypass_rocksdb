# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).
from pathlib import Path
import subprocess,shlex,shutil
out=Path('/tmp/mb-foreground-profile');root=Path('/home/lj/metabypass_rocksdb')
config=Path('/tmp/metabypass-release-build/make_config.mk').read_text();flags=shlex.split(next(x.split('=',1)[1] for x in config.splitlines() if x.startswith('PLATFORM_CXXFLAGS=')))
def link(variant,d):
 subprocess.run(['g++','-O2','-std=c++20','-I'+str(d/'include'),'-I'+str(root/'include'),str(out/'driver.cc'),*[str(d/(f+'.o'))for f in ['backup','native_files','separated_storage','metabypass_db']],'/tmp/metabypass-release-build/librocksdb.a','-lpthread','-ldl','-lrt','-lsnappy','-lz','-lbz2','-llz4','-lzstd','-lnuma','-ltcmalloc','-o',str(out/(variant+'-driver'))],check=True)
link('baseline',out/'baseline')
d=out/'threshold';shutil.copytree(out/'split-cv',d,dirs_exist_ok=True)
p=d/'utilities/metabypass/backup.cc';s=p.read_text();start=s.index('void Backup::Finish');end=s.index('IOStatus Backup::NewWritableFile',start)
part=s[start:end].replace('cv_.notify_all();','if (!primary.ok() || stats_.queued_bytes >= options_.batch_bytes)\n    cv_.notify_all();');s=s[:start]+part+s[end:];p.write_text(s)
subprocess.run(['g++','-O2','-DNDEBUG','-fno-rtti',*flags,'-I'+str(d),'-I'+str(d/'include'),'-I'+str(root),'-I'+str(root/'include'),'-c',str(p),'-o',str(d/'backup.o')],check=True)
link('threshold',d)
