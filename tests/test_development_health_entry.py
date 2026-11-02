"""来源入口真实输入回放与独立评审，不发网络请求或正式消息。"""
import json
from pathlib import Path
from types import SimpleNamespace
import copy
import pytest
from app import development_health as entry
from app.development_policy import DevelopmentError

FIXTURE=json.loads((Path(__file__).parent/'fixtures/source_health_oct31_nov01.json').read_text())

class Client:
    repo='xiexie20211028-bot/robtaxi-digest-new'
    run_id='unit-current-run'
    budget=None
    def __init__(self):self.writes=[]
    def events(self):return []
    def append(self,row):self.writes.append(row)
    def api(self,path):return {'status':'ahead'}


def fake_sync(monkeypatch, source, trace):
    from app import health_loop_sync as sync
    run={'id':int(source['health']['run']['github_run_id']),'run_attempt':1,'head_sha':source['health']['run']['commit_sha'],
         'created_at':source['date']+'T06:00:00Z','status':'completed','event':'schedule','head_branch':'main'}
    monkeypatch.setattr(entry,'normal_runs',lambda c,w:[copy.deepcopy(run)] if w==entry.WORKFLOW else [])
    monkeypatch.setattr(entry,'source_inputs',lambda *a:(copy.deepcopy(source['health']),copy.deepcopy(source['report'])))
    class Metadata:
        def preflight(self,**kw):return {'complete':True,'incidents':{}}
    monkeypatch.setattr(sync,'GhMetadataClient',Metadata)
    def execute(args,guard):
        d=json.loads(Path(args.decision).read_text());trace.append(d)
        Path(args.out).write_text(json.dumps({'complete':True,'official_state':{'complete':True,'incidents':{}},
          'historical_only':False,'deferred_operations':[],'applied':[],'development_run_id':guard.run_id}))
    monkeypatch.setattr(sync,'sync_main',execute)
    return run


@pytest.mark.parametrize('source',FIXTURE['items'],ids=lambda r:r['date'])
def test_real_report_keeps_four_critical_without_review(monkeypatch,tmp_path,source):
    trace=[];fake_sync(monkeypatch,source,trace)
    c=Client();r=entry.sync_health(c,{},tmp_path)
    assert len(trace)==1
    critical=[a for a in trace[0]['actions'] if a['severity']=='critical' and a.get('source_id')]
    assert len(critical)==4
    assert trace[0]['business_review']['status']=='pending'
    assert r['source_sync']['status']=='complete' and r['complete'] is False and not c.writes


def test_default_shadow_never_writes_even_with_matched_review(monkeypatch,tmp_path):
    source=FIXTURE['items'][0];trace=[];run=fake_sync(monkeypatch,source,trace)
    review_run={**run,'id':99,'created_at':'2026-11-01T08:00:00Z'}
    monkeypatch.setattr(entry,'normal_runs',lambda c,w:[copy.deepcopy(run)] if w==entry.WORKFLOW else [review_run])
    order=[]
    def artifact(*args):
        order.append('review_download');assert trace,'必须先保存/判断来源，再读取业务复盘'
        return {'daily':{'date':source['date'],'valid_statistical_day':True}},'hash'
    monkeypatch.setattr(entry,'pages',lambda *a:[]);monkeypatch.setattr(entry,'artifact_json',artifact)
    c=Client();r=entry.sync_health(c,{},tmp_path)
    assert len(trace)==2 and trace[-1]['business_review']['review_run_id']=='99' and not c.writes


def test_ambiguous_review_never_attaches_to_wrong_production():
    c=Client();run={'id':1,'head_sha':'a','created_at':'2026-11-01T01:00:00Z','_date':'2026-11-01'}
    review=({'id':2,'head_sha':'a','run_attempt':1,'created_at':'2026-11-02T01:00:00Z'},{'daily':{'date':'2026-11-01'}})
    value,status=entry.review_binding(c,[review],run,'2026-11-01',[run,{**run,'id':3}])
    assert not value and status['status']=='unmatched'
    value,status=entry.review_binding(c,[review],run,'2026-10-31',[run])
    assert not value and status['status']=='pending'


def test_invalid_provenance_is_global_stop(monkeypatch):
    source=FIXTURE['items'][0];run={'id':1,'run_attempt':1,'head_sha':'a'*40}
    monkeypatch.setattr(entry,'pages',lambda *a:[])
    monkeypatch.setattr(entry,'artifact_json',lambda c,r,a,p,f:(source['health'],'hash') if f=='health_report.json' else (source['report'],'hash'))
    with pytest.raises(DevelopmentError,match='身份或摘要'):entry.source_inputs(Client(),run)


def test_apply_requires_registered_budget_and_trusted_main(tmp_path):
    with pytest.raises(DevelopmentError,match='租约'):entry.sync_health(Client(),{},tmp_path,apply=True)


def test_budget_stops_without_waiting_or_side_effects(monkeypatch,tmp_path):
    trace=[];fake_sync(monkeypatch,FIXTURE['items'][0],trace)
    c=Client();c.budget=SimpleNamespace(remaining=lambda:100)
    result=entry.sync_health(c,{},tmp_path)
    assert result['stopped']=='budget_finalization' and not trace and not c.writes


def test_missing_source_artifact_makes_same_day_review_ambiguous():
    run={'id':1,'head_sha':'a','created_at':'2026-11-01T01:00:00Z','_date':'2026-11-01'}
    review=({'id':2,'head_sha':'a','run_attempt':1,'created_at':'2026-11-01T08:00:00Z'},
            {'daily':{'date':'2026-11-01','valid_statistical_day':True}})
    value,status=entry.review_binding(Client(),[review],run,'2026-11-01',
        [run,{'id':3,'created_at':'2026-11-01T02:00:00Z'}])
    assert not value and status['status']=='unmatched'


def test_blocked_business_item_never_marks_review_consumed(monkeypatch,tmp_path):
    from app import health_loop_sync as sync
    source=FIXTURE['items'][0];trace=[];run=fake_sync(monkeypatch,source,trace)
    rr={**run,'id':99,'created_at':'2026-11-01T08:00:00Z'}
    monkeypatch.setattr(entry,'normal_runs',lambda c,w:[copy.deepcopy(run)] if w==entry.WORKFLOW else [rr])
    monkeypatch.setattr(entry,'pages',lambda *a:[])
    monkeypatch.setattr(entry,'artifact_json',lambda *a:({'daily':{'date':source['date'],
       'valid_statistical_day':True,'business_status':'empty_uncovered'}},'hash'))
    def execute(args,guard):
        d=json.loads(Path(args.decision).read_text());trace.append(d)
        Path(args.out).write_text(json.dumps({'complete':False,'source_complete':True,
           'official_state':{'complete':True,'incidents':{}},'deferred_operations':[],'applied':[],
           'item_results':[{'incident_key':'review:business_empty_uncovered','source_id':'','status':'blocked'}]}))
    monkeypatch.setattr(sync,'sync_main',execute)
    monkeypatch.setattr('app.development_cycle.assert_trusted_checkout',lambda c:None)
    c=Client();c.budget=SimpleNamespace(remaining=lambda:1000,check=lambda:None)
    r=entry.sync_health(c,{},tmp_path,apply=True)
    assert r['source_sync']['status']=='complete' and not r['complete'] and not c.writes
    assert trace[-1]['observations'][-1]['attempt']==1


def test_controller_public_sync_failure_calls_failed_finalizer(monkeypatch,tmp_path):
    from app import development_cycle as cycle, development_outcomes as outcomes, health_loop_sync as sync
    from contextlib import nullcontext
    import sys
    c=Client();c.policy=cycle.load_policy();c.budget=SimpleNamespace(remaining=lambda:1000,check=lambda:None)
    c.failures=[]
    monkeypatch.setattr(cycle,'ROOT',tmp_path)
    monkeypatch.setattr(cycle,'GitHub',lambda p:c)
    monkeypatch.setattr(cycle,'repository_lock',lambda *a:nullcontext())
    monkeypatch.setattr(cycle,'initialize_run',lambda *a:None)
    monkeypatch.setattr(cycle,'assert_trusted_checkout',lambda *a:None)
    def fail(*a,**kw):raise sync.SyncError('公共权限失败')
    monkeypatch.setattr(entry,'sync_health',fail)
    called=[]
    monkeypatch.setattr(outcomes,'finalize_run',lambda *a,**kw:called.append((a,kw)))
    monkeypatch.setattr(sys,'argv',['cycle','sync-health','--apply','--run-id',c.run_id])
    assert cycle.main()==1
    assert len(called)==1 and called[0][0]==(c,'failed')
    assert called[0][1]['error']=='公共权限失败'


def test_unrun_source_is_not_eligible_for_code_repair(monkeypatch,tmp_path):
    source=copy.deepcopy(FIXTURE['items'][0]);trace=[]
    source['report']['source_stats']=[]
    fake_sync(monkeypatch,source,trace)
    entry.sync_health(Client(),{},tmp_path)
    assert all(a['action']=='needs_approval' for a in trace[0]['actions'] if a.get('source_id'))
    assert all(not o['valid'] for o in trace[0]['observations'] if o.get('source_id'))


def test_source_only_apply_receipt_is_not_a_complete_heartbeat(monkeypatch,tmp_path):
    from app import health_loop_sync as sync
    from app.development_lease import validate_health_receipt
    from types import SimpleNamespace
    decision={'schema_version':sync.DECISION_SCHEMA,'run_id':'102','state_origin':'github_reconstructed',
        'run_evidence':{'event_name':'schedule','health_report_available':True},'changed_actions':[],
        'business_review':{'status':'pending'}}
    path=tmp_path/'decision.json';path.write_text(json.dumps(decision))
    class Metadata:
        def preflight(self,**kw):return {'schema_version':sync.STATE_SCHEMA,'complete':True,'incidents':{}}
        def persist_observations(self,d):return []
        def apply(self,ops):return []
    monkeypatch.setattr(sync,'GhMetadataClient',Metadata)
    args=SimpleNamespace(decision=str(path),mode='apply',out=str(tmp_path/'receipt.json'),state_out='',run_id='unit-current-run')
    sync.sync_main(args)
    receipt=json.loads(Path(args.out).read_text())
    assert receipt['source_complete'] and not receipt['complete'] and receipt['business_review']['status']=='pending'
    with pytest.raises(DevelopmentError,match='未完成'):validate_health_receipt(receipt,args.run_id)
