"""最终收口：历史身份、严格验收绑定、限定维护与真实业务回放。"""
import copy
import pytest
from app.development_history import adoption, record
from app.development_budget import pending_deliveries
from app.development_acceptance import binding_for, evaluate_offline
from app.development_policy import DevelopmentError
from app.development_production import reconcile_production
from app.development_cycle import load_policy
from test_development_history import HistoryClient, request


def offline_request(issue=83):
    r=request();r.update(issue=issue,remaining_scope=[])
    r['production']={**binding_for(issue),'checks':['原任务业务验收']}
    return r


def offline_client(issue=83):
    c=HistoryClient();c.pr['body']=f'Primary task: Fixes #{issue}'
    return c


def test_main_advance_and_evidence_refresh_do_not_repeat_adoption():
    c=HistoryClient();r=request();first=record(c,r)
    c.main_sha=lambda:'d'*40
    r['acceptance_evidence']=['刷新证据，但交付和验收不变']
    second=record(c,r)
    assert first['key']==second['key'] and first['at']==second['at']
    assert len(c.rows)==1 and second['facts']['main_sha']=='c'*40
    r['remaining_scope']=['进一步缩小剩余范围']
    third=record(c,r)
    assert third['key']!=first['key'] and third['delivery_id']==first['delivery_id']
    assert third['supersedes_adoptions']==[first['key']]


def test_two_sided_interruption_recovers_without_rewriting():
    class C(HistoryClient):
        def __init__(self):super().__init__();self.control=[];self.local=[];self.crash=True
        def events(self,issue=None):return self.local if issue else self.control
        def append(self,e,issue=None):
            if issue:self.local.append(e)
            elif self.crash:self.crash=False;raise OSError('断网')
            else:self.control.append(e)
    c=C()
    with pytest.raises(OSError):record(c,request())
    original=copy.deepcopy(c.local[0]);record(c,request())
    assert c.local==c.control==[original]


def test_actual_pr_diff_overrides_claimed_impact():
    c=HistoryClient();r=request();r['impact_keys']=['source:fake']
    assert adoption(c,r)['facts']['impact_keys']==['shared:production']
    c.files=['tests/test_contract.py'];assert adoption(c,r)['facts']['impact_keys']==[]


def test_old_verification_cannot_retire_new_revision_or_other_task():
    a={'event':'historical_delivery_adopted','issue':83,'key':'v1','facts':{'pr':87,'merge_sha':'sha','production':{'kind':'task'},'impact_keys':['shared:production']}}
    newer={**a,'key':'v2'}
    proof={'event':'historical_production_verified','issue':83,'merge_sha':'sha','adoption_key':'v1'}
    assert len(pending_deliveries([a,newer,proof]))==1
    proof['adoption_key']='v2';proof['issue']=84
    assert len(pending_deliveries([a,newer,proof]))==1
    proof['issue']=83
    assert pending_deliveries([a,newer,proof])==[]


def test_regular_deliveries_do_not_share_sha_only_verification():
    d={'event':'delivery','issue':1,'merge_sha':'sha','contract_digest':'one','production':{'kind':'task'}}
    proof={'event':'production_verified','issue':1,'merge_sha':'sha','contract_digest':'two'}
    assert pending_deliveries([d,proof])==[d]
    proof['contract_digest']='one'
    assert pending_deliveries([d,proof])==[]


def test_binding_cannot_be_swapped_between_tasks():
    c=offline_client();r=offline_request();r['production']['verifier']=binding_for(84)['verifier']
    with pytest.raises(DevelopmentError):adoption(c,r)
    with pytest.raises(DevelopmentError):binding_for(69)


def test_real_offline_behavior_and_unknown_noise():
    assert evaluate_offline(46)['acceptance_passed'] is True
    r=evaluate_offline(73)
    assert r['checks']['negative_controls'] is True
    assert r['checks']['shadow_noise_growth_at_most_10_percent'] is None
    assert r['acceptance_passed'] is None
    # 原任务各项业务条件均须通过，包括具体路线身份和敏感内容投影。
    trace=evaluate_offline(83)
    assert trace['checks']['route_identity'] is True and trace['acceptance_passed'] is True
    assert evaluate_offline(84)['acceptance_passed'] is True


def test_pilot_maintenance_requires_explicit_existing_history(monkeypatch):
    c=offline_client(84);c.policy=load_policy();c.budget=type('B',(),{'check':lambda s:None})()
    row={'number':84,'state':'OPEN','status':'待验证','type':'优化','labels':[],'body':'原验收','blockers':[]}
    c.task=lambda n:row;c.tasks=lambda:[row]
    event=record(c,offline_request(84))
    assert reconcile_production(c,c.policy,84)['results']==[]
    r=reconcile_production(c,c.policy,84,historical_maintenance=True)
    assert r['results'][0]['status']=='complete'
    assert c.policy['mode']=='pilot' and c.policy['pilot_issue']==69
    assert c.rows==[event]
    with pytest.raises(DevelopmentError):reconcile_production(c,c.policy,historical_maintenance=True)
    c.rows=[]
    with pytest.raises(DevelopmentError):reconcile_production(c,c.policy,84,historical_maintenance=True)


def test_missing_original_test_or_remaining_scope_never_closes():
    c=offline_client(84);r=offline_request(84);r['remaining_scope']=['未完成实现']
    event=record(c,r)
    row={'number':84,'state':'OPEN','status':'待验证','type':'优化','labels':[],'body':'原验收','blockers':[]}
    c.task=lambda n:row
    result=reconcile_production(c,c.policy,84,historical_maintenance=True)
    assert result['results'][0]['status']=='remaining_scope_or_dependency'


def test_failed_business_behavior_requeues_only_remaining_work(monkeypatch):
    from scripts import replay_golden_event as replay
    original = replay.replay_event
    def broken_route(event):
        report = original(event)
        for row in report['results'].values():
            row['route'] = 'golden_replay'
        return report
    # 保留旧缺陷注入，证明错误路线仍不能被专属验收器放行。
    monkeypatch.setattr(replay, 'replay_event', broken_route)
    c=offline_client(83);c.policy=load_policy();c.budget=type('B',(),{'check':lambda s:None})()
    row={'number':83,'state':'OPEN','status':'待验证','type':'优化','labels':[],'body':'原验收','blockers':[]}
    c.task=lambda n:row;c.label=lambda *a:None;c.set_project_status=lambda *a:None
    r=offline_request(83);r['remaining_scope']=['回放记录路线身份']
    event=record(c,r)
    monkeypatch.setattr('app.development_cycle.assert_trusted_checkout',lambda _: 'c'*40)
    result=reconcile_production(c,c.policy,83,apply=True,historical_maintenance=True)
    assert result['results'][0]['status']=='requeue'
    requeued=next(e for e in c.rows if e.get('event')=='historical_production_requeue')
    assert requeued['adoption_key']==event['key'] and requeued['remaining_scope']==['回放记录路线身份']
    assert requeued['offline_evidence']['checks']['route_identity'] is False
    assert not any(e.get('event')=='historical_production_verified' for e in c.rows)


def test_matching_historical_regression_chain_does_not_retire_other_revision():
    adoption={'event':'historical_delivery_adopted','issue':83,'key':'new','facts':{'merge_sha':'old','pr':87,'production':{'kind':'task'}}}
    repaired={'event':'delivery','issue':83,'merge_sha':'repair','production':{'kind':'task'},'supersedes':['old'],'contract_digest':'contract'}
    proof={'event':'production_verified','issue':83,'merge_sha':'repair','contract_digest':'contract'}
    rows=[adoption,repaired,proof,{'event':'historical_production_requeue','issue':83,'merge_sha':'old','adoption_key':'previous'}]
    assert len(pending_deliveries(rows))==1
    rows.append({'event':'historical_production_requeue','issue':83,'merge_sha':'old','adoption_key':'new'})
    assert pending_deliveries(rows)==[]


def test_superseded_revision_cannot_be_silently_restored():
    c=HistoryClient();a=request();record(c,a)
    b=copy.deepcopy(a);b['remaining_scope']=['新范围'];record(c,b)
    with pytest.raises(DevelopmentError,match='已被替代'):record(c,a)


def test_source_impact_uses_real_content_snapshots_and_renames():
    import base64,json
    c=HistoryClient();original=c.api;c.files=['sources.json'];c.pr['base']['sha']='e'*40
    before={'sources':[{'id':'one','url':'old'}]};after={'sources':[{'id':'one','url':'new'}]}
    def api(path):
        if '/contents/' in path:
            value=before if path.endswith('e'*40) else after
            return {'encoding':'base64','content':base64.b64encode(json.dumps(value).encode()).decode()}
        return original(path)
    c.api=api
    assert adoption(c,request())['facts']['impact_keys']==['source:one']
    def renamed(path):
        if '/files?' in path:return [{'filename':'tests/test_old.py','previous_filename':'app/production.py'}]
        return api(path)
    c.api=renamed
    assert adoption(c,request())['facts']['impact_keys']==['shared:production']


def test_shadow_noise_growth_and_missing_comparable_samples():
    from app.development_shadow_noise import compare_decisions
    labels = {str(k): True for k in range(12)}
    def rows(count):
        return [{"candidate_id": str(k), "title": str(k), "source_id": "a", "route": "legacy",
                 "kept": k < count} for k in range(12)]
    assert compare_decisions(rows(10), rows(11), labels)["passed"] is True
    assert compare_decisions(rows(10), rows(12), labels)["passed"] is False
    assert compare_decisions(rows(0), rows(1), labels)["passed"] is None
    assert compare_decisions(rows(10), rows(11)[1:], labels)["passed"] is None
    assert compare_decisions(rows(10), rows(11), {})["passed"] is None
    assert evaluate_offline(73, shadow=compare_decisions(rows(10), rows(12), labels))["acceptance_passed"] is False


@pytest.mark.parametrize("maintenance", [True, False])
def test_one_of_two_deliveries_cannot_close_task(monkeypatch, maintenance):
    from app import development_production as prod
    c = offline_client(83)
    task = {"number": 83, "state": "OPEN", "status": "待验证", "type": "优化", "labels": [], "blockers": []}
    c.task = lambda n: task
    c.budget = type("Budget", (), {"check": lambda self: None})()
    c.gh = lambda *args: pytest.fail("尚有未验收交付，不能关闭任务")
    monkeypatch.setattr("app.development_cycle.assert_trusted_checkout", lambda _: "d" * 40)
    first = record(c, offline_request(83))
    second_request = offline_request(83)
    second_request["pr"] += 1
    second = record(c, second_request)
    seen = []
    def verify(client, policy, task, e, **_):
        seen.append(e["key"])
        return {"status": "remaining_scope_or_dependency" if e["key"] == first["key"] else "complete"}
    monkeypatch.setattr(prod, "reconcile_historical", verify)
    result = prod.reconcile_production(c, c.policy, 83, apply=True, historical_maintenance=maintenance)
    assert seen == [first["key"], second["key"]]
    assert [r["status"] for r in result["results"]] == ["remaining_scope_or_dependency", "complete"]
