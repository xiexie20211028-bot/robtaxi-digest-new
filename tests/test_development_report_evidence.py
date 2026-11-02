"""缺输入、历史快照和人工交付的日报回归。"""
from datetime import timedelta
from copy import deepcopy
from app.development_outcomes import recover_incidents,render_outcome
from test_development_outcomes import build,NOW


def queues():
    return [{'event':'health_queue_updated','production_run_id':'100','attempt':1,'complete':True,
       'production_date':'2026-10-26','entries':[{'incident_key':'source:'+s+':empty','source_id':s,'severity':'critical'} for s in ('zoox','pony','lta','mobileye')]}]


def test_missing_decision_and_receipt_retain_four_last_known_critical():
    result=build(events=queues(),decision={},receipt={})
    assert result['health_severity']=='critical' and len(result['active_incidents'])==4
    assert result['inspection']=='incomplete' and result['health_evidence_status']=='last_known'
    text=render_outcome(result)
    assert '事故至少4项' in text and '当前事故：0项' not in text
    assert 'source:zoox:empty' in result['health_evidence_origins']


def test_missing_all_history_shows_unknown_count_not_zero():
    result=build(decision={},receipt={})
    assert not result['incident_count_known'] and '异常数量未知' in render_outcome(result)


def observation(result,run='101',attempt=1,valid=True):
    return {'schema_version':'robtaxi-health-observation-v1','incident_key':'source:zoox:empty',
       'source_id':'zoox','run_id':run,'attempt':attempt,'run_at':'2026-10-27','result':result,
       'valid':valid,'event_name':'schedule','health_report_available':True,
       'severity':'healthy' if result in ('healthy','unknown') else 'critical'}


def test_unknown_and_engineering_observation_status_cannot_clear_incident():
    official={'complete':True,'incidents':{'source:zoox:empty':{'lifecycle':'observing','observations':[
       observation('failed','100'),observation('unknown','101',valid=False)]}}}
    x=recover_incidents(queues(),official)
    assert x['incidents']['source:zoox:empty']['severity']=='critical'
    assert x['incidents']['source:zoox:empty']['lifecycle']=='open'
    official['incidents']['source:zoox:empty']['observations'].append(observation('healthy','102'))
    assert recover_incidents(queues(),official)['incidents']['source:zoox:empty']['lifecycle']=='observing'


def test_old_merges_restored_to_snapshot_are_not_progress():
    state={'tasks':[{'number':n,'awaiting_production':{'number':n+100,'mergeCommit':{'oid':'a'*40},
        'mergedAt':(NOW-timedelta(days=2)).isoformat()}} for n in (55,126,128,129,140)],'next_action':None}
    previous=build(now=NOW-timedelta(days=1),state={'tasks':[{'number':n} for n in (55,126,128,129,140)]})
    r=build(state=state,events=[{'event':'run_outcome','result':previous}])
    assert r['progress_issues']==[] and r['manual_deliveries']==[]


def test_same_day_approved_manual_merge_is_not_automatic_work():
    state={'tasks':[{'number':129,'manual_merge_verified':True,'awaiting_production':{'number':148,
       'mergeCommit':{'oid':'a'*40},'mergedAt':NOW.isoformat(),'url':'https://github.com/example/repo/pull/148'}}],
       'next_action':None}
    r=build(state=state)
    assert r['progress_issues']==r['merged_issues']==r['developed_issues']==[]
    assert r['manual_deliveries'][0]['type']=='manual' and r['manual_deliveries'][0]['pr']==148
    assert '人工/外部协助交付：#129' in render_outcome(r)


def test_business_review_pending_does_not_hide_completed_source_sync():
    r=build(decision={'run_id':'100','date_bj':'2026-10-27','incidents':{},'actions':[],
       'run_evidence':{'event_name':'schedule','health_report_available':True},'business_review':{'status':'pending'}})
    assert r['source_inspection']=='complete' and r['inspection']=='partial'
    assert '来源独立处理' in render_outcome(r)


def test_old_official_recovery_and_old_decision_cannot_clear_newer_critical():
    q=queues();q[0].update(production_run_id='104',production_date='2026-10-28')
    official={'complete':True,'incidents':{'source:zoox:empty':{'observations':[observation('healthy','102')]}}}
    recovered=recover_incidents(q,official)
    assert recovered['incidents']['source:zoox:empty']['severity']=='critical'
    r=build(events=q,state={'tasks':[],'health_context':recovered},
        decision={'run_id':'102','date_bj':'2026-10-27','run_evidence':{'event_name':'schedule','health_report_available':True},
          'incidents':{'source:zoox:empty':{'incident_key':'source:zoox:empty','severity':'healthy','lifecycle':'observing'}}})
    assert len(r['active_incidents'])==4 and r['health_severity']=='critical'


def test_new_attempt_can_recover_without_adding_an_observation():
    q=queues();q[0]['production_date']='2026-10-27'
    official={'complete':True,'incidents':{'source:zoox:empty':{'observations':[observation('healthy','100',2)]}}}
    assert recover_incidents(q,official)['incidents']['source:zoox:empty']['lifecycle']=='observing'


def test_source_only_receipt_keeps_business_inspection_partial():
    from app.development_policy import digest
    d={'run_id':'100','date_bj':'2026-10-27','run_evidence':{'event_name':'schedule','health_report_available':True},
       'business_review':{'status':'pending'}}
    receipt={'complete':False,'source_complete':True,'global_state_valid':True,'mode':'apply',
       'development_run_id':'current-run','decision_digest':digest(d),'official_state':{'complete':True}}
    r=build(decision=d,receipt=receipt)
    assert r['source_inspection']=='complete' and r['inspection']=='partial'
    assert r['health_evidence_status']=='current'
