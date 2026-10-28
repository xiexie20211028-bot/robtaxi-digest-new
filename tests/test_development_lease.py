"""整轮入口的重启、并发和过期验证，不访问真实 GitHub。"""
from datetime import datetime, timedelta, timezone
import json
import multiprocessing

import pytest

from app import development_cycle as cycle
from app.development_lease import isolated_output, require_exclusive_run, validate_health_receipt
from app.development_policy import DevelopmentError
from app.development_runtime import GitHub, repository_lock
from app.health_loop_sync import GhMetadataClient, main as sync_main


def start(run_id="first-run", minutes=30):
    now = datetime.now(timezone.utc)
    return {"event": "run_started", "key": f"run:{run_id}", "run_id": run_id,
            "at": now.isoformat(), "deadline": (now + timedelta(minutes=minutes)).isoformat()}


def client_with_events(monkeypatch, tmp_path, events):
    client = GitHub(cycle.load_policy())
    monkeypatch.setattr(client, "events", lambda: events)
    monkeypatch.setattr(client, "append", lambda e: events.append(e))
    monkeypatch.setattr(cycle, "ROOT", tmp_path)
    monkeypatch.setattr(cycle, "assert_trusted_checkout", lambda _: "a" * 40)
    return client


def test_other_owner_and_ambiguous_history_fail_closed():
    now = datetime.now(timezone.utc)
    with pytest.raises(DevelopmentError, match="并发跳过"):
        require_exclusive_run([start()], "other-run", now)
    with pytest.raises(DevelopmentError, match="多个有效"):
        require_exclusive_run([start(), start("other-run")], "first-run", now)


def test_finish_releases_lease_but_old_context_cannot_resume(monkeypatch, tmp_path):
    events = []
    client = client_with_events(monkeypatch, tmp_path, events)
    cycle.initialize_run(client, client.policy, "first-run", start=True)
    cycle.finish_run(client, "waiting_production")
    with pytest.raises(DevelopmentError, match="已结束"):
        cycle.initialize_run(client, client.policy, "first-run", start=True)
    cycle.initialize_run(client, client.policy, "other-run", start=True)
    assert events[-1]["run_id"] == "other-run"


def test_expired_old_context_cannot_renew_and_new_run_records_recovery(monkeypatch, tmp_path):
    events = [start(minutes=-1)]
    client = client_with_events(monkeypatch, tmp_path, events)
    with pytest.raises(DevelopmentError, match="预算已到期"):
        cycle.initialize_run(client, client.policy, "first-run", start=True)
    assert len(events) == 1
    event = cycle.initialize_run(client, client.policy, "other-run", start=True)
    assert event["expired_predecessors"] == ["first-run"]


def test_formal_state_failure_does_not_start_or_write_cache(monkeypatch, tmp_path):
    client = client_with_events(monkeypatch, tmp_path, [])
    def unavailable():
        raise DevelopmentError("正式状态不可读取")
    monkeypatch.setattr(client, "events", unavailable)
    with pytest.raises(DevelopmentError):
        cycle.initialize_run(client, client.policy, "first-run", start=True)
    assert not (tmp_path / ".local").exists()


def test_output_isolation_rejects_shared_path_and_symlink(tmp_path):
    path = isolated_output(tmp_path, "first-run", None, "snapshot.json")
    assert "first-run" in path.parts
    with pytest.raises(DevelopmentError, match="独立目录"):
        isolated_output(tmp_path, "first-run", str(tmp_path / "snapshot.json"), "snapshot.json")
    path.parent.mkdir(parents=True)
    (path.parent / "escape").symlink_to(tmp_path, target_is_directory=True)
    with pytest.raises(DevelopmentError, match="独立目录"):
        isolated_output(tmp_path, "first-run", str(path.parent / "escape" / "shared"), "snapshot.json")


def test_health_apply_without_run_fails_before_any_github_request(monkeypatch, tmp_path):
    monkeypatch.delenv("CODEX_THREAD_ID", raising=False)
    monkeypatch.setattr("sys.argv", ["sync", "--decision", "missing.json", "--mode", "apply", "--out", str(tmp_path / "out")])
    monkeypatch.setattr(GhMetadataClient, "preflight", lambda **_: pytest.fail("不能访问GitHub"))
    assert sync_main() == 1


def test_health_request_checks_deadline_before_subprocess(monkeypatch):
    client = GhMetadataClient()
    def expired():
        raise DevelopmentError("预算已到期")
    client.run_guard = expired
    monkeypatch.setattr("app.health_loop_sync.subprocess.run", lambda *a, **k: pytest.fail("不能执行写入"))
    with pytest.raises(DevelopmentError, match="预算已到期"):
        client._run(["issue", "close", "55"])


def _contender(root, run_id, barrier, queue):
    """文件模拟正式事件存储；真实进程竞争同一仓库锁。"""
    cycle.ROOT = root
    cycle.assert_trusted_checkout = lambda _: "a" * 40
    ledger = root / "events.json"
    client = GitHub(cycle.load_policy())
    client.events = lambda: json.loads(ledger.read_text())
    def append(event):
        ledger.write_text(json.dumps([*client.events(), event]))
    client.append = append
    barrier.wait(timeout=10)
    try:
        with repository_lock(str(root)):
            cycle.initialize_run(client, client.policy, run_id, start=True)
        queue.put("started")
    except DevelopmentError:
        queue.put("skipped")


def test_old_successful_health_receipt_cannot_refresh_current_heartbeat():
    receipt = {"development_run_id": "yesterday-run", "complete": True, "mode": "apply", "official_state": {"complete": True}}
    with pytest.raises(DevelopmentError, match="不属于本次"):
        validate_health_receipt(receipt, "today-run")
    validate_health_receipt(receipt, "yesterday-run")


def test_three_processes_only_one_starts(tmp_path):
    (tmp_path / "events.json").write_text("[]")
    context = multiprocessing.get_context("fork")
    barrier, queue = context.Barrier(3), context.Queue()
    children = [context.Process(target=_contender, args=(tmp_path, f"run-{n:08}", barrier, queue)) for n in range(3)]
    for child in children:
        child.start()
    try:
        results = [queue.get(timeout=15) for _ in children]
        assert sorted(results) == ["skipped", "skipped", "started"]
        assert len(json.loads((tmp_path / "events.json").read_text())) == 1
    finally:
        for child in children:
            child.join(timeout=5)
            if child.is_alive():
                child.terminate()
