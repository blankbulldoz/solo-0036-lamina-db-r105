#!/usr/bin/env python3
"""Bounded production walkthroughs using only the public HTTP service."""
import argparse, concurrent.futures, json, os, pathlib, shutil, socket, subprocess, tempfile, time, urllib.request, urllib.error
WORKFLOWS=['catalog-create','txn-write','snapshot-read','index-build','query-plan','sql-execute','txn-conflict','restart','checkpoint','backup-restore','changes','vacuum','transaction-close','statistics','partial-publish','partial-recovery']
class Session:
 def __init__(self,binary,root,restore=None):
  self.binary=str(binary);self.root=pathlib.Path(root);self.restore=restore;self.process=None;self.log=None
 def start(self):
  with socket.socket() as sock:
   sock.bind(('127.0.0.1',0));port=sock.getsockname()[1]
  self.url=f'http://127.0.0.1:{port}/v1/lamina'
  args=[self.binary,'-data',str(self.root),'-listen',f'127.0.0.1:{port}']
  if self.restore:args+=['-restore',str(self.restore)];self.restore=None
  self.log=tempfile.TemporaryFile();self.process=subprocess.Popen(args,stdout=self.log,stderr=self.log)
  deadline=time.monotonic()+8
  while time.monotonic()<deadline:
   if self.process.poll() is not None:
    self.log.seek(0);raise RuntimeError(self.log.read().decode())
   try:self.call('GET','/health');return self
   except (OSError,urllib.error.URLError):time.sleep(.03)
  raise RuntimeError('service did not become ready')
 def stop(self):
  if self.process and self.process.poll() is None:
   self.process.terminate()
   try:self.process.wait(timeout=12)
   except subprocess.TimeoutExpired:self.process.kill();self.process.wait()
  if self.log:self.log.close();self.log=None
 def call(self,method,path,data=None,status=200,raw=False):
  encoded=None if data is None else json.dumps(data,ensure_ascii=False).encode()
  req=urllib.request.Request(self.url+path,data=encoded,method=method,headers={'Content-Type':'application/json'})
  try:
   with urllib.request.urlopen(req,timeout=12) as response:code=response.status;body=response.read()
  except urllib.error.HTTPError as e:code=e.code;body=e.read()
  if code!=status:raise RuntimeError(f'{method} {path}: expected {status}, got {code}: {body.decode()}')
  return body if raw else json.loads(body)
 def begin(self,readonly=False):return self.call('POST','/transactions',{'read_only':readonly},201)['id']
 def put(self,tx,row,status=200,insert=False):return self.call('POST',f'/transactions/{tx}/put',{'relation':'spectra','row':row,'insert_only':insert},status)
 def commit(self,tx,status=200):return self.call('POST',f'/transactions/{tx}/commit',status=status)
 def publish(self,tx,keys,dry=False,status=200):return self.call('POST',f'/transactions/{tx}/publish',{'selection':[{'relation':'spectra','key':k} for k in keys],'dry_run':dry},status)
 def refresh(self,tx,dry=False,status=200):return self.call('POST',f'/transactions/{tx}/refresh',{'dry_run':dry},status)
 def txstatus(self,tx):return self.call('GET',f'/transactions/{tx}')
 def query(self,**kwargs):return self.call('POST','/query',{'relation':'spectra',**kwargs})
 def stats(self):return self.call('GET','/stats')
def require(condition,message):
 if not condition:raise RuntimeError(message)
def tuple_(n,band=None,amplitude=None):return {'sample':n,'band':band or f'b{n}','amplitude':n*10 if amplitude is None else amplitude,'valid':True}
def seed(s):
 schema={'name':'spectra','primary':'sample','columns':[{'name':'sample','type':'int'},{'name':'band','type':'text'},{'name':'amplitude','type':'int','nullable':True},{'name':'valid','type':'bool'}]}
 s.call('POST','/relations',schema,201)
 tx=s.begin()
 for n in range(1,7):s.put(tx,tuple_(n))
 s.put(tx,{'sample':7,'band':'b7','amplitude':None,'valid':False})
 return s.commit(tx)['sequence']
def walkthrough(name,binary):
 with tempfile.TemporaryDirectory(prefix='lamina-flow-') as temp:
  temp=pathlib.Path(temp);s=Session(binary,temp/'source').start();other=None
  try:
   seed_seq=seed(s);observations={}
   if name=='catalog-create':
    before=s.stats()['sequence'];bad={'name':'invalid','primary':'missing','columns':[{'name':'value','type':'int'}]}
    s.call('POST','/relations',bad,400);require(s.stats()['sequence']==before,'invalid schema changed sequence')
    cat=s.call('GET','/catalog');require(len(cat['relations'])==1,'catalog mismatch');observations=cat
   elif name=='txn-write':
    tx=s.begin();s.put(tx,tuple_(8));s.put(tx,tuple_(9));require(s.query()['matched']==7,'uncommitted tuples leaked')
    result=s.commit(tx);require(result['writes']==2 and s.query()['matched']==9,'atomic commit failed')
    bad=s.begin();s.put(bad,{'sample':10,'band':'x','amplitude':'bad','valid':True},400)
    s.call('POST',f'/transactions/{bad}/rollback');observations=result
   elif name=='snapshot-read':
    old=s.begin();writer=s.begin();s.put(writer,tuple_(1,amplitude=555));s.commit(writer)
    q={'where':[{'column':'sample','op':'=','value':1}]}
    require(s.query(transaction=old,**q)['rows'][0]['amplitude']==10,'repeatable read violated')
    require(s.query(**q)['rows'][0]['amplitude']==555,'current snapshot stale')
    s.put(old,tuple_(8));require(s.query(transaction=old)['matched']==8,'read-own-write scan missing')
    require(s.query(at=seed_seq)['rows'][0]['amplitude']==10,'time travel failed')
    s.call('POST',f'/transactions/{old}/rollback');s.call('POST','/query',{'relation':'spectra','at':999},410);observations={'old':10,'current':555}
   elif name=='index-build':
    definition={'name':'by_band','relation':'spectra','column':'band','unique':True}
    s.call('POST','/indexes',definition,201)
    a=s.begin();b=s.begin();s.put(a,tuple_(8,band='shared'));s.put(b,tuple_(9,band='shared'));s.commit(a);s.commit(b,409)
    swap=s.begin();s.put(swap,tuple_(1,band='b2'));s.put(swap,tuple_(2,band='b1'));s.commit(swap)
    duplicate=s.begin();s.put(duplicate,tuple_(10,band='b3'));s.commit(duplicate,409)
    observations=s.call('GET','/catalog')
   elif name=='query-plan':
    s.call('POST','/indexes',{'name':'by_amplitude','relation':'spectra','column':'amplitude'},201)
    at=s.stats()['sequence'];tx=s.begin();s.put(tx,tuple_(1,amplitude=999));s.commit(tx)
    q={'where':[{'column':'amplitude','op':'<=','value':20}]}
    fast=s.query(**q);slow=s.query(force_scan=True,**q)
    require(fast['rows']==slow['rows'] and fast['plan']['access']=='index','index/scan mismatch')
    historical=s.query(at=at,**q);require(len(historical['rows'])==2,'historical index lost prior value')
    observations={'indexed':fast,'historical':historical}
   elif name=='sql-execute':
    sql=s.call('POST','/sql',{'sql':'SELECT sample, amplitude FROM spectra WHERE amplitude >= 30 AND valid = true ORDER BY amplitude DESC LIMIT 2;'})
    structured=s.query(columns=['sample','amplitude'],where=[{'column':'amplitude','op':'>=','value':30},{'column':'valid','op':'=','value':True}],order=[{'column':'amplitude','desc':True}],limit=2)
    require(sql['rows']==structured['rows'],'SQL and structured query diverged')
    count=s.call('POST','/sql',{'sql':'SELECT count(amplitude) FROM spectra;'})
    total=s.call('POST','/sql',{'sql':'SELECT sum(amplitude) FROM spectra;'})
    require(count['rows']==[{'count':6}] and total['rows']==[{'sum':210}],'aggregate semantics wrong')
    s.call('POST','/sql',{'sql':'SELECT * FROM spectra; SELECT * FROM spectra;'},400);observations={'projection':sql['rows'],'count':count['rows'],'sum':total['rows']}
   elif name=='txn-conflict':
    a=s.begin();b=s.begin();s.put(a,tuple_(1,amplitude=101));s.put(b,tuple_(1,amplitude=202));s.put(b,tuple_(8))
    with concurrent.futures.ThreadPoolExecutor(2) as pool:
     jobs=[pool.submit(s.commit,a),pool.submit(s.commit,b)]
     results=[]
     for job in jobs:
      try:results.append({'commit':job.result()})
      except RuntimeError as e:
       require('got 409' in str(e),'unexpected concurrent failure');results.append({'conflict':True})
    require(sum('conflict' in r for r in results)==1,'exactly one writer must lose')
    rows=s.query()['rows'];one=next(r for r in rows if r['sample']==1)
    require((one['amplitude']==202)==any(r['sample']==8 for r in rows),'conflicting commit partially published')
    s.commit(a,404);s.commit(b,404);observations={'results':results,'rows':rows}
   elif name=='restart':
    before=s.query()['rows'];s.stop()
    with (s.root/'journal.lmw').open('ab') as f:f.write(b'\x00\x00\x00')
    s.start();require(s.query()['rows']==before,'tail recovery changed committed data');require(s.stats()['recovered_tail_bytes']==3,'tail recovery not reported')
    s.stop();broken=temp/'broken';shutil.copytree(s.root,broken)
    data=bytearray((broken/'journal.lmw').read_bytes());data[-1]^=1;(broken/'journal.lmw').write_bytes(data)
    result=subprocess.run([str(binary),'-data',str(broken),'-listen','127.0.0.1:0'],capture_output=True,text=True,timeout=5)
    require(result.returncode!=0 and 'checksum' in result.stderr,'full frame corruption accepted')
    observations={'tail_bytes':3,'corruption':'rejected'}
   elif name=='checkpoint':
    at=s.call('POST','/checkpoint')['sequence'];tx=s.begin();s.put(tx,tuple_(8));s.commit(tx)
    before=s.query()['rows'];s.stop();s.start();require(s.query()['rows']==before,'checkpoint plus suffix recovery failed')
    locked=subprocess.run([str(binary),'-data',str(s.root),'-listen','127.0.0.1:0'],capture_output=True,text=True,timeout=5)
    require(locked.returncode!=0 and 'already open' in locked.stderr,'exclusive directory ownership missing');observations={'checkpoint':at,'head':s.stats()['sequence']}
   elif name=='backup-restore':
    backup=s.call('GET','/backup',raw=True);path=temp/'portable.lmi';path.write_bytes(backup)
    frozen=s.query()['rows'];tx=s.begin();s.put(tx,tuple_(8));s.commit(tx)
    other=Session(binary,temp/'restored',path).start();require(other.query()['rows']==frozen,'backup mutated with source')
    tx=other.begin();other.put(tx,tuple_(9));result=other.commit(tx);other.stop();other.start()
    require(other.query()['matched']==8,'restored journal base does not continue')
    other.call('GET','/changes?after=0',status=410)
    broken=json.loads(backup);broken['sha256']='0'*64;bad=temp/'bad.lmi';bad.write_text(json.dumps(broken))
    failed=subprocess.run([str(binary),'-restore',str(bad),'-data',str(temp/'badrestore')],capture_output=True,text=True,timeout=5)
    require(failed.returncode!=0 and not (temp/'badrestore').exists(),'invalid backup published');observations={'restored_commit':result,'original_sequence':seed_seq}
   elif name=='changes':
    tx=s.begin()
    for n in range(8,12):s.put(tx,tuple_(n))
    committed=s.commit(tx);page=s.call('GET',f'/changes?after={seed_seq}&limit=1')
    require(len(page['records'])==1 and len(page['records'][0]['writes'])==4 and page['cursor']==committed['sequence'],'change page split transaction')
    s.call('GET','/changes?after=999',status=400);observations=page
   elif name=='vacuum':
    old=s.begin();tx=s.begin();s.put(tx,tuple_(1,amplitude=99));s.commit(tx);requested=s.stats()['sequence']
    pinned=s.call('POST','/vacuum',{'horizon':requested});require(pinned['pinned'] and pinned['horizon']==seed_seq,'vacuum crossed active snapshot')
    require(s.query(transaction=old)['rows'][0]['amplitude']==10,'active snapshot lost')
    s.call('POST',f'/transactions/{old}/rollback');reclaimed=s.call('POST','/vacuum',{'horizon':requested})
    require(reclaimed['removed']>=1,'obsolete version not reclaimed');s.call('POST','/query',{'relation':'spectra','at':seed_seq},410)
    s.call('POST','/checkpoint');s.stop();s.start();require(s.query()['rows'][0]['amplitude']==99,'vacuum did not survive restart');observations={'pinned':pinned,'reclaimed':reclaimed}
   elif name=='transaction-close':
    tx=s.begin();s.put(tx,tuple_(8));s.call('POST',f'/transactions/{tx}/rollback');s.commit(tx,404)
    require(s.query()['matched']==7 and not s.stats()['active'],'rollback leaked state or pin')
    tx=s.begin(True);s.put(tx,tuple_(9),409);s.call('POST',f'/transactions/{tx}/rollback');observations={'rollback':'discarded','read_only':'enforced'}
   elif name=='statistics':
    s.call('POST','/indexes',{'name':'by_amp','relation':'spectra','column':'amplitude'},201)
    q=s.query(where=[{'column':'amplitude','op':'>=','value':40}],limit=2)
    require(q['matched']==3 and q['scanned']==3 and len(q['rows'])==2 and q['truncated'],'query fact meanings conflated')
    observations={'query':q,'stats':s.stats()}
   elif name=='partial-publish':
    s.call('POST','/indexes',{'name':'by_band','relation':'spectra','column':'band','unique':True},201)
    edit=s.begin()
    s.put(edit,tuple_(10,band='approved'));s.put(edit,tuple_(11,band='pending'))
    # an unapproved unique-value swap: two staged rows exchange band values
    s.put(edit,tuple_(1,band='b2'),insert=False);s.put(edit,tuple_(2,band='b1'),insert=False)
    before=s.stats()['sequence']
    rejected=s.publish(edit,[1],status=409)
    require(rejected['report']['selected'][0]['key']=='int:1' and rejected['report']['unique_violations'],
            'dependent swap half was not rejected with its violation report')
    require(s.stats()['sequence']==before and s.txstatus(edit)['remaining_writes']==4,
            'rejected partial publish consumed a sequence or mutated the edit')
    s.publish(edit,[],status=400);s.publish(edit,[99],status=400)
    published=s.publish(edit,[10]);require(published['sequence']==before+1 and published['writes']==1,
            'independent selection did not publish as one complete commit')
    require(s.txstatus(edit)['remaining_writes']==3 and s.txstatus(edit)['partial_publishes']==1,
            'edit context did not survive its partial publish')
    # pinned view keeps the old read basis and own remaining writes only
    pinned=s.query(transaction=edit);require(all(r['sample']!=10 for r in pinned['rows']),
            'pinned view leaked the already-published row through committed state')
    require(next(r for r in pinned['rows'] if r['sample']==1)['band']=='b2','pinned view lost own write')
    # head view shows the own publication plus other callers' commits, never conflated
    head=s.query(transaction=edit,view='head');require(any(r['sample']==10 for r in head['rows']),
            'head view hid the edit own publication')
    require(next(r for r in head['rows'] if r['sample']==1)['band']=='b1',
            'head view must show committed values, not the private overlay')
    external=s.begin();s.put(external,tuple_(20,band='external'));s.commit(external)
    require(all(r['sample']!=20 for r in s.query(transaction=edit)['rows']),
            'pinned view adopted another caller value as the edit basis')
    require(any(r['sample']==20 for r in s.query(transaction=edit,view='head')['rows']),
            'head view did not expose the intervening external commit')
    # interleaving conflict before any view conversion: both edits start from
    # the same snapshot, so the second first-committer-wins loser must be rejected
    rival=s.begin();clash=s.begin()
    s.put(rival,tuple_(3,band='rival'),insert=False);s.put(clash,tuple_(3,band='clash'),insert=False)
    s.commit(rival)
    fail=s.publish(clash,[3],status=409);require(fail['report']['conflicts'][0]['key']=='int:3',
            'interleaved commit did not produce a first-committer-wins conflict')
    require(s.txstatus(clash)['remaining_writes']==1,'failed publish discarded the retained edit')
    cost=s.refresh(clash,dry=True);require(cost['conflicts'][0]['key']=='int:3',
            'refresh cost did not disclose the conflict being accepted')
    s.refresh(clash);s.publish(clash,[3]);s.call('POST',f'/transactions/{clash}/rollback')
    # a commit landing after refresh is still a brand-new first-committer-wins
    late=s.begin();winner=s.begin()
    s.put(late,tuple_(4,band='late'),insert=False);s.put(winner,tuple_(4,band='winner'),insert=False)
    s.commit(winner)
    late_fail=s.publish(late,[4],status=409)
    require(late_fail['report']['conflicts'][0]['key']=='int:4','post-refresh conflict not enforced')
    s.call('POST',f'/transactions/{late}/rollback')
    # refresh cost is checkable before the conversion; vacuum stays pinned meanwhile
    dry=s.refresh(edit,dry=True);require(dry['advanced'] and s.txstatus(edit)['snapshot']==before,
            'refresh dry-run moved the snapshot')
    requested=s.stats()['sequence'];pinned_vac=s.call('POST','/vacuum',{'horizon':requested})
    require(pinned_vac['pinned'],'long edit stopped pinning retention before refresh')
    # the vacuum attempt is itself a durable record; refresh still lands on current head
    head=s.stats()['sequence']
    s.refresh(edit);require(s.txstatus(edit)['snapshot']==head,'refresh did not advance the view')
    reclaimed=s.call('POST','/vacuum',{'horizon':head});require(not reclaimed['pinned'],
            'refreshed edit still blocked version reclamation')
    s.call('POST','/query',{'relation':'spectra','at':before},410)
    # legacy one-shot commit still publishes the remaining staged rows atomically
    final=s.commit(edit);require(final['writes']==3,'one-shot commit no longer handles the remainder')
    s.commit(edit,404)
    observations={'rejected':rejected['report'],'published':published,'refresh':dry,'final':final,
                  'stats':s.stats()}
   elif name=='partial-recovery':
    s.call('POST','/indexes',{'name':'by_band','relation':'spectra','column':'band','unique':True},201)
    edit=s.begin()
    s.put(edit,tuple_(10,band='approved'));s.put(edit,tuple_(11,band='pending'))
    s.put(edit,tuple_(12,band='held'))
    first=s.publish(edit,[10]);second=s.publish(edit,[12])
    require(first['sequence']+1==second['sequence'],'partial publishes lost commit ordering')
    require(s.txstatus(edit)['published_sequences']==[first['sequence'],second['sequence']],
            'edit lineage did not record its durable publications')
    durable=s.query()['rows'];require(any(r['sample']==11 for r in s.query(transaction=edit)['rows']),
            'pinned view lost the still-private remainder')
    require(all(r['sample']!=11 for r in durable),'unpublished row became externally visible')
    # rejection keeps everything needed to retry, and writes nothing durable
    s.put(edit,tuple_(2,band='b1'),insert=False)  # duplicates committed key1 band b1
    seq_before=s.stats()['sequence']
    bad=s.publish(edit,[2],status=409);require(s.stats()['sequence']==seq_before,
            'rejected publish advanced the journal')
    require(s.txstatus(edit)['remaining_writes']==2,'rejected publish did not preserve the edit')
    s.call('POST',f'/transactions/{edit}/rollback')
    page=s.call('GET',f'/changes?after={seed_seq}&limit=100')
    partials=[r for r in page['records'] if r.get('edit')=='partial']
    require(len(partials)==2 and all(len(r['writes'])==1 for r in partials),
            'change feed did not preserve complete partial-commit boundaries')
    s.call('POST','/checkpoint')
    s.stop();s.start()
    rows=s.query()['rows']
    require(any(r['sample']==10 for r in rows) and any(r['sample']==12 for r in rows),
            'restart lost a truly published row')
    require(all(r['sample']!=11 for r in rows),'restart resurrected a never-published private row')
    again=s.call('GET',f'/changes?after={seed_seq}&limit=100')
    require([r['sequence'] for r in again['records'] if r.get('edit')=='partial']==
            [first['sequence'],second['sequence']],'restart changed the durable publish lineage')
    observations={'published_sequences':[first['sequence'],second['sequence']],
                  'rejected':bad['report'],'after_restart':rows,'changes':again}
   else:raise ValueError(name)
   return {'workflow':name,'status':'observed','observations':observations}
  finally:
   s.stop()
   if other:other.stop()
def main():
 parser=argparse.ArgumentParser();parser.add_argument('--workflow',choices=['all',*WORKFLOWS],default='all');args=parser.parse_args()
 project=pathlib.Path(__file__).resolve().parents[1];binary=project/'bin/laminad'
 if not binary.exists():raise SystemExit('build first: go build -o bin/laminad ./cmd/laminad')
 names=WORKFLOWS if args.workflow=='all' else [args.workflow]
 for name in names:print(json.dumps(walkthrough(name,binary),ensure_ascii=False),flush=True)
if __name__=='__main__':main()
