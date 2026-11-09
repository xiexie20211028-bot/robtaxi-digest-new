"""生产来源证据的共同有效性检查。缺少证据时保留未知。"""


def usable_health_report(health):
    if not isinstance(health, dict):
        return False
    source = health.get('source_report')
    findings = health.get('findings')
    return (isinstance(source, dict) and source.get('available') is True
            and isinstance(findings, list)
            and all(isinstance(f, dict) and f.get('check_id') != 'self_check_internal_failure' for f in findings))


def matches_production(health, run, repo, report_hash):
    if not usable_health_report(health) or not report_hash:
        return False
    origin = health.get('run')
    return (isinstance(origin, dict) and str(origin.get('github_run_id')) == str(run['id'])
            and str(origin.get('github_run_attempt')) == str(run['run_attempt'])
            and origin.get('commit_sha') == run['head_sha'] and origin.get('repository') == repo
            and origin.get('event_name') == 'schedule' and health['source_report'].get('sha256') == report_hash)
