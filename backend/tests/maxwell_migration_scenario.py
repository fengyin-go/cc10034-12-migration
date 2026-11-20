"""Maxwell 事务边界迁移端到端场景核对（服务级，无外部依赖）。

场景（对应迁移需求）：
  1. 两笔带真实提交标记的 Maxwell 变更事务（tx-1001、tx-1002）跨两版部署到达；
  2. 旧版适配进程接收 tx-1001 首成员后中止迁移并回滚；
  3. 末成员与真实提交标记在切换（新版）后到达；
  4. 回滚后从同一源位点继续消费；
  5. 核对：目标收到的批次数据、重放起点、事务交付次数（重复回放只交付一次）。

运行：.venv/bin/python tests/maxwell_migration_scenario.py
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta

from app.pipeline import pipeline_bus
from app.services import service_for

# 固定的源事件时间（过去时刻），用于验证不会被当前时间替代
BASE_TS = datetime(2026, 11, 10, 8, 0, 0)
INSTANCE = "mysql-source-A"
BINLOG = "mysql-bin.000117"


def check(cond: bool, label: str) -> None:
    status = "PASS" if cond else "FAIL"
    print(f"[{status}] {label}")
    if not cond:
        raise AssertionError(label)


def main() -> int:
    adapter = service_for("format_adapter")
    consumer = service_for("change_event")

    # ---------- 0. 旧版适配环境，先登记一条旧版存量信封 ----------
    check(pipeline_bus.adapter_deployment_version == "legacy", "初始为旧版适配环境")
    legacy_raw = {
        "database": "shop", "table": "orders", "type": "insert",
        "ts": int(BASE_TS.timestamp()), "xid": "legacy-9001",
        "position": f"{BINLOG}:100",
        "data": {"id": 9001, "amount": 12.5}, "old": None,
    }
    legacy_env = adapter.build_legacy_maxwell_envelope(legacy_raw)
    check(legacy_env["commit"] is True and legacy_env["envelope_version"] == "legacy",
          "旧信封复现旧版行为（固定 commit、无真实边界）")
    check(len(adapter.list_legacy_envelopes()) == 1, "旧版存量信封可从原接口读取")

    # ---------- 1. 旧版接收 tx-1001 首成员 ----------
    first = {
        "database": "shop", "table": "orders", "type": "insert",
        "ts": int(BASE_TS.timestamp()), "xid": "tx-1001",
        "binlog_file": BINLOG, "binlog_pos": 1024, "gtid": "uuid:1001",
        "data": {"id": 1, "status": "CREATED"}, "old": None,
    }
    adapter.ingest_maxwell_change(first, source_instance=INSTANCE)
    txn1 = pipeline_bus.get_maxwell_transaction("tx-1001")
    check(txn1 is not None and len(txn1.members) == 1, "旧版接收 tx-1001 首成员")
    check(txn1.received_versions == ["legacy"], "首成员登记为旧版接收")
    check(pipeline_bus.get_maxwell_transaction("tx-1001").commit_marker is None,
          "首成员到达时无真实提交标记，事务不完整")
    check(adapter.publish_transaction_batch("tx-1001") is None,
          "未真实提交/成员不完整时禁止发布批次")

    # ---------- 2. 迁移中止并回滚，再切换到新版部署 ----------
    rollback_info = pipeline_bus.rollback_deployment(INSTANCE)
    check(rollback_info["member_log_retained"] >= 1, "回滚后带位点成员的源日志保留")
    switch = adapter.switch_adapter_deployment("v2")
    check(switch == {"previous_version": "legacy", "current_version": "v2",
                     "inflight_transactions": ["tx-1001"]},
          "切换到输出版本，在途事务 tx-1001 被保留")

    # ---------- 3. 新版接收 tx-1001 末成员 + 真实提交标记 ----------
    last = {
        "database": "shop", "table": "orders", "type": "update",
        "ts": int((BASE_TS + timedelta(seconds=2)).timestamp()), "xid": "tx-1001",
        "binlog_file": BINLOG, "binlog_pos": 2048, "gtid": "uuid:1001",
        "data": {"id": 1, "status": "PAID"},
        "old": {"id": 1, "status": "CREATED"},
    }
    adapter.ingest_maxwell_change(last, source_instance=INSTANCE)
    adapter.ingest_maxwell_commit(
        tx_id="tx-1001", source_instance=INSTANCE, binlog_file=BINLOG,
        binlog_pos=2100, gtid="uuid:1001",
        commit_time=BASE_TS + timedelta(seconds=3))
    txn1 = pipeline_bus.get_maxwell_transaction("tx-1001")
    check(len(txn1.members) == 2, "末成员在新版到达并并入同一事务")
    check(txn1.received_versions == ["legacy", "v2"], "事务跨两版部署聚合")
    check(pipeline_bus.is_transaction_complete("tx-1001"), "真实提交且成员完整")

    batch1 = adapter.publish_transaction_batch("tx-1001")
    check(batch1 is not None, "tx-1001 发布 Maxwell 事件数据批次")
    check(batch1.member_count == 2 and len(batch1.members) == 2,
          "批次包含全部成员（成员与提交凭证一起发布）")
    check(batch1.start_position == f"{BINLOG}:1024", "批次起点为首成员源位点")
    check(batch1.end_position == f"{BINLOG}:2100", "批次下发边界为真实提交位点")
    check(batch1.commit_evidence["real_commit"] is True
          and batch1.commit_evidence["member_count"] == 2,
          "提交凭证由真实提交标记生成并可核对")
    check(batch1.event_time == BASE_TS.isoformat(),
          "批次事件时间来自源事件时间，未被当前时间代替")
    check(all(m["envelope_version"] == "v2" and m["commit"] is False
              for m in batch1.members), "成员信封为输出版本，提交真相归凭证")
    check(batch1.members[0]["event_time"] == BASE_TS.isoformat(),
          "成员信封保留源事件时间")

    # ---------- 4. 第二笔事务 tx-1002 完整走新版 ----------
    for pos, op, ts_off, data, old in [
        (3072, "insert", 10, {"id": 2, "status": "CREATED"}, None),
        (4096, "update", 12, {"id": 2, "status": "SHIPPED"},
         {"id": 2, "status": "CREATED"}),
    ]:
        adapter.ingest_maxwell_change({
            "database": "shop", "table": "orders", "type": op,
            "ts": int((BASE_TS + timedelta(seconds=ts_off)).timestamp()),
            "xid": "tx-1002", "binlog_file": BINLOG, "binlog_pos": pos,
            "gtid": "uuid:1002", "data": data, "old": old,
        }, source_instance=INSTANCE)
    adapter.ingest_maxwell_commit(
        tx_id="tx-1002", source_instance=INSTANCE, binlog_file=BINLOG,
        binlog_pos=4200, gtid="uuid:1002",
        commit_time=BASE_TS + timedelta(seconds=13))
    batch2 = adapter.publish_transaction_batch("tx-1002")
    check(batch2 is not None and batch2.member_count == 2
          and batch2.end_position == f"{BINLOG}:4200", "tx-1002 发布批次")

    # ---------- 5. 消费端边界校验后下发 tx-1001 ----------
    row = consumer.consume_maxwell_event_batch(
        batch1.batch_id, expected_instance=INSTANCE,
        expected_commit_position=f"{BINLOG}:2100")
    check(row["result"] == "delivered" and row["boundary_verified"] is True,
          "源实例+位点+提交证据校验通过，tx-1001 交付目标")

    # 错误边界必须被拒收
    bad = consumer.consume_maxwell_event_batch(
        batch2.batch_id, expected_instance="mysql-source-OTHER",
        expected_commit_position=f"{BINLOG}:4200")
    check(bad["result"] == "rejected" and not bad["boundary_verified"],
          "源实例不匹配的批次被拒收")
    bad2 = consumer.consume_maxwell_event_batch(
        batch2.batch_id, expected_instance=INSTANCE,
        expected_commit_position=f"{BINLOG}:9999")
    check(bad2["result"] == "rejected", "下发位点不匹配的批次被拒收")
    # 正确边界下发 tx-1002
    ok2 = consumer.consume_maxwell_event_batch(
        batch2.batch_id, expected_instance=INSTANCE,
        expected_commit_position=f"{BINLOG}:4200")
    check(ok2["result"] == "delivered", "tx-1002 校验通过并交付目标")

    # ---------- 6. 回滚后从同一源位点（1024）继续消费/重复回放 ----------
    cursor_before = pipeline_bus.get_source_cursor(INSTANCE)
    check(cursor_before["position_key"] == f"{BINLOG}:4200",
          "消费游标推进到最后确认的提交位点 4200")

    resume = consumer.rollback_and_resume(INSTANCE, replay_position=f"{BINLOG}:1024")
    check(resume["rollback"]["replay_start_position"] == f"{BINLOG}:1024",
          "回滚后重放起点为同一源位点 1024")
    replay_results = resume["replay_results"]
    check(len(replay_results) == 2, "从 1024 重放覆盖两笔事务")
    check(all(r.get("result") == "deduplicated" for r in replay_results),
          "重复回放的事务全部被幂等拦截，未重复交付")

    # 再显式回放一次，验证幂等稳定性
    again = adapter.replay_transaction(INSTANCE, BINLOG, 1024)
    check(all(r.get("result") == "deduplicated" for r in again),
          "再次重复回放仍不重复交付")

    # ---------- 7. 核对目标收到的批次数据与交付次数 ----------
    target = pipeline_bus.maxwell_target_batches
    check(len(target) == 2, f"目标只收到 2 个批次（实际 {len(target)}）")
    for batch in pipeline_bus.maxwell_batches:
        check(batch.delivered_count == 1,
              f"{batch.tx_id} 目标交付次数恰好为 1（实际 {batch.delivered_count}）")
        check(batch.delivery_attempts >= 2 and batch.replay_count >= 1,
              f"{batch.tx_id} 记录了多次回放尝试但只交付一次")

    # 回放清单完整回写
    manifest = consumer.get_replay_manifest()
    check(len(manifest) >= 6, f"回放清单记录全部下发/拒收/重放（{len(manifest)} 条）")
    check(any(m["result"] == "delivered" for m in manifest)
          and any(m["result"] == "rejected" for m in manifest)
          and any(m["result"] == "deduplicated" for m in manifest),
          "回放清单覆盖 delivered/rejected/deduplicated 三类结果")
    check(all(m.get("requested_replay_position") == f"{BINLOG}:1024"
              for m in manifest if m["result"] == "deduplicated"),
          "清单中的重放行携带正确的请求重放起点")

    # ---------- 8. 旧版存量与迁移成员仍可读取，源日志完整 ----------
    check(len(adapter.list_legacy_envelopes()) == 1, "旧版存量数据仍可从原接口读到")
    check(len(pipeline_bus.maxwell_member_log) == 4, "源日志保留全部 4 个带位点成员")
    check(len(pipeline_bus.maxwell_commit_log) == 2, "源日志保留 2 个真实提交标记")
    check(pipeline_bus.adapter_deployment_version == "legacy",
          "回滚后适配环境版本回到旧版")

    # ---------- 9. 同位点成员重放不产生重复成员 ----------
    before = len(pipeline_bus.maxwell_member_log)
    adapter.ingest_maxwell_change(dict(first), source_instance=INSTANCE)
    check(len(pipeline_bus.maxwell_member_log) == before,
          "同源位点重复成员被去重，重放不产生重复成员")

    print("\n全部核对通过：批次数据、重放起点、事务恰好一次交付均符合预期。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
