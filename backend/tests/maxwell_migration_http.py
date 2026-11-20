"""HTTP 层冒烟：通过真实 HTTP 接口跑一遍切换/回滚/幂等主链路（仅用标准库）。"""
from __future__ import annotations

import json
import urllib.request
from datetime import datetime

BASE = "http://127.0.0.1:8011/api/maxwell"
TS = int(datetime(2026, 11, 10, 9, 0, 0).timestamp())


def call(method: str, path: str, payload: dict | None = None):
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(
        BASE + path, data=data, method=method,
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode())


def main() -> None:
    # 旧版首成员
    _, r = call("POST", "/ingest", {
        "source_instance": "inst-http", "member": {
            "database": "db", "table": "t", "type": "insert", "ts": TS,
            "xid": "tx-h1", "binlog_file": "bin.000001", "binlog_pos": 11,
            "data": {"id": 1}}})
    assert r["transaction"]["member_count"] == 1 and r["member"]["envelope_version"] == "legacy"

    # 不完整事务发布应 409
    code, _ = call("POST", "/transactions/tx-h1/publish")
    assert code == 409, code

    # 切换
    _, r = call("POST", "/deployment/switch", {"target_version": "v2"})
    assert r["current_version"] == "v2" and r["inflight_transactions"] == ["tx-h1"]

    # 新版末成员 + 提交
    _, r = call("POST", "/ingest", {
        "source_instance": "inst-http", "member": {
            "database": "db", "table": "t", "type": "update", "ts": TS + 2,
            "xid": "tx-h1", "binlog_file": "bin.000001", "binlog_pos": 22,
            "data": {"id": 2}, "old": {"id": 1}}})
    assert r["transaction"]["complete"] is False
    _, r = call("POST", "/commit", {
        "source_instance": "inst-http", "tx_id": "tx-h1",
        "binlog_file": "bin.000001", "binlog_pos": 33,
        "commit_time": datetime(2026, 11, 10, 9, 0, 3).isoformat()})
    assert r["complete"] is True

    # 发布 + 消费（正确边界）
    _, r = call("POST", "/transactions/tx-h1/publish")
    batch_id = r["batch"]["batch_id"]
    assert r["batch"]["event_time"] == datetime(2026, 11, 10, 9, 0, 0).isoformat()
    _, r = call("POST", f"/batches/{batch_id}/consume", {
        "expected_instance": "inst-http",
        "expected_commit_position": "bin.000001:33"})
    assert r["result"] == "delivered", r

    # 错误边界拒收
    _, r = call("POST", f"/batches/{batch_id}/consume", {
        "expected_instance": "inst-http",
        "expected_commit_position": "bin.000001:99"})
    assert r["result"] == "rejected", r

    # 回滚 + 同位点重放 -> 幂等
    _, r = call("POST", "/rollback", {
        "source_instance": "inst-http",
        "replay_position": "bin.000001:11"})
    assert r["rollback"]["replay_start_position"] == "bin.000001:11"
    assert r["replay_results"][0]["result"] == "deduplicated", r

    # 目标核对
    _, r = call("GET", "/target-batches")
    assert r["total"] == 1 and r["items"][0]["delivered_count"] == 1
    _, r = call("GET", "/replay-manifest")
    assert r["total"] >= 3
    _, r = call("GET", "/cursor/inst-http")
    assert r["position_key"] == "bin.000001:33"
    _, r = call("GET", "/source-log")
    assert len(r["members"]) == 2 and len(r["commits"]) == 1

    # 旧版存量接口
    call("POST", "/legacy/envelope", {
        "source_instance": "inst-http",
        "envelope": {"database": "db", "table": "t", "type": "insert",
                     "ts": TS, "xid": "legacy-x", "data": {"id": 9}},
        "binlog_file": "bin.000001", "binlog_pos": 5})
    _, r = call("GET", "/legacy/envelopes")
    assert r["total"] == 1 and r["items"][0]["envelope_version"] == "legacy"

    print("HTTP 冒烟全部通过")


if __name__ == "__main__":
    main()
