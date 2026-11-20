"""变更事件领域服务：消费管道中的 ChangeEventRecord，执行事件解析、分类和状态流转。

上游：binlog_parser 产出事件推入管道 → change_event 消费解析
下游：format_adapter 格式适配 → event_filter 过滤 → route_dispatcher 路由分发
"""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.pipeline import pipeline_bus


class ChangeEventService(BaseModuleService):
    MODULE = "change_event"
    ENTITY = "变更记录"
    OPERATION_TERM = "解析"
    LIST_FIELDS = ["事件ID", "来源表", "操作类型", "变更前镜像", "变更后镜像", "事务ID", "事件时间"]
    REQUIRED_FIELDS = ["事件ID", "来源表", "操作类型"]
    STATUS_ORDER = ["已捕获", "已解析", "已下发", "解析失败"]
    ACTION_RULES = {"解析事件": "已捕获", "下发事件": "已解析", "标记失败": "已解析"}
    NEGATIVE_ACTIONS = []

    def consume_pipeline_events(self, batch_size: int = 50) -> list[dict[str, Any]]:
        """从管道总线消费变更事件并分类汇总。

        消费 pipeline_bus 中 binlog_parser 产出的缓冲区事件，
        按操作类型 (INSERT/UPDATE/DELETE) 和来源表分组。
        """
        raw_events = pipeline_bus.consume_events(batch_size=batch_size)
        if not raw_events:
            return []
        by_op: dict[str, int] = {}
        by_table: dict[str, int] = {}
        entries: list[dict[str, Any]] = []
        for evt in raw_events:
            by_op[evt.operation] = by_op.get(evt.operation, 0) + 1
            by_table[evt.source_table] = by_table.get(evt.source_table, 0) + 1
            before_str = json.dumps(evt.before_image, ensure_ascii=False) if evt.before_image else "{}"
            after_str = json.dumps(evt.after_image, ensure_ascii=False) if evt.after_image else "{}"
            entries.append({
                "事件ID": evt.event_id,
                "来源表": f"{evt.source_db}.{evt.source_table}",
                "操作类型": evt.operation,
                "变更前镜像": before_str[:200],
                "变更后镜像": after_str[:200],
                "事务ID": evt.tx_id,
                "事件时间": evt.event_time.isoformat(),
                "raw_format": evt.raw_format,
                "binlog_pos": evt.binlog_pos,
            })
        pipeline_bus.write_audit(
            "事件消费", f"{len(entries)}条事件",
            f"消费管道事件 {len(entries)} 条，按业务类型分组完成",
            "change_event", "已解析"
        )
        return entries

    def classify_by_operation(self, events: list[dict[str, Any]]) -> dict[str, Any]:
        """对消费后的事件按操作类型分类统计。

        为下游 format_adapter 提供操作类型分布以决定格式适配策略。
        """
        ops: dict[str, int] = {"insert": 0, "update": 0, "delete": 0}
        tables: dict[str, int] = {}
        total_rows_affected = 0
        for evt in events:
            op = evt.get("操作类型", "").upper()
            if op == "INSERT":
                ops["insert"] += 1
            elif op == "UPDATE":
                ops["update"] += 1
            elif op == "DELETE":
                ops["delete"] += 1
            tbl = evt.get("来源表", "unknown")
            tables[tbl] = tables.get(tbl, 0) + 1
            total_rows_affected += 1
        return {
            "total_events": len(events),
            "by_operation": ops,
            "by_table": tables,
            "insert_ratio": round(ops["insert"] / max(len(events), 1) * 100, 1),
            "update_ratio": round(ops["update"] / max(len(events), 1) * 100, 1),
            "delete_ratio": round(ops["delete"] / max(len(events), 1) * 100, 1),
        }

    def delta_computation(self, event: dict[str, Any]) -> dict[str, Any]:
        """对变更前后镜像执行差异计算。

        计算 UPDATE 操作中哪些字段实际发生了变化，
        产出变更增量列表供下游 SCD 策略消费。
        """
        try:
            before = json.loads(event.get("变更前镜像", "{}"))
            after_raw = event.get("变更后镜像", "{}")
            after = json.loads(after_raw) if isinstance(after_raw, str) else after_raw
        except (json.JSONDecodeError, TypeError):
            return {"delta_fields": [], "delta_count": 0}
        changed = []
        for key in after:
            if key not in before or before[key] != after[key]:
                delta_val = {"field": key, "old": before.get(key), "new": after[key]}
                changed.append(delta_val)
        return {"delta_fields": changed, "delta_count": len(changed)}

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, msg = super().run_action(entry_id, action)
        if entry and action == "下发事件":
            pipeline_bus.write_audit(
                "事件下发", entry.get("来源表", ""),
                f"事件 {entry_id} 下发给下游消费者",
                "change_event", "已下发"
            )
        return entry, msg

    # ==================================================================
    # Maxwell 事件数据批次消费：边界校验 + 幂等下发 + 回放清单回写
    # ==================================================================

    def consume_maxwell_event_batch(
        self, batch_id: str, *, expected_instance: str | None = None,
        expected_commit_position: str | None = None,
    ) -> dict[str, Any]:
        """消费一个 Maxwell 事件数据批次。

        下发边界以批次携带的源实例、源位点和真实提交凭证为准校验；
        校验通过才下发目标，结果回写到回放清单。重复回放由总线幂等拦截，
        同一事务只向目标交付一次。
        """
        batch = next((b for b in pipeline_bus.maxwell_batches
                      if b.batch_id == batch_id), None)
        if batch is None:
            return {"batch_id": batch_id, "result": "not_found",
                    "boundary_verified": False}
        manifest_row = pipeline_bus.consume_maxwell_batch(
            batch,
            expected_instance=expected_instance,
            expected_position=expected_commit_position)
        if manifest_row.get("result") == "delivered":
            pipeline_bus.write_audit(
                "Maxwell批次下发", batch.tx_id,
                f"批次 {batch_id} 边界校验通过并交付目标，成员 {batch.member_count} 条",
                "change_event", "已下发")
        elif manifest_row.get("result") == "deduplicated":
            pipeline_bus.write_audit(
                "Maxwell批次去重", batch.tx_id,
                f"批次 {batch_id} 重复回放被幂等拦截，目标不重复交付",
                "change_event", "已下发")
        else:
            pipeline_bus.write_audit(
                "Maxwell批次拒收", batch.tx_id,
                f"批次 {batch_id} 边界校验失败：{manifest_row.get('boundary_reasons')}",
                "change_event", "解析失败")
        return manifest_row

    def consume_pending_maxwell_batches(self, source_instance: str | None = None
                                        ) -> list[dict[str, Any]]:
        """连续消费所有已发布但未交付的 Maxwell 批次（部署切换后的串接消费）。"""
        results: list[dict[str, Any]] = []
        for batch in pipeline_bus.maxwell_batches:
            if source_instance and batch.source_instance != source_instance:
                continue
            if batch.batch_id in pipeline_bus.maxwell_delivery_ledger:
                continue
            results.append(self.consume_maxwell_event_batch(
                batch.batch_id,
                expected_instance=batch.source_instance,
                expected_commit_position=batch.end_position))
        return results

    def get_replay_manifest(self) -> list[dict[str, Any]]:
        """回放清单：每次下发/重放的源位点、边界校验与交付结果。"""
        return list(pipeline_bus.replay_manifest)

    def get_replay_start_position(self, source_instance: str) -> dict[str, Any] | None:
        """回滚后可继续消费的重放起点（最后确认的同源位点）。"""
        return pipeline_bus.get_source_cursor(source_instance)

    def rollback_and_resume(self, source_instance: str, replay_position: str | None = None
                            ) -> dict[str, Any]:
        """部署失败中止迁移并回滚，再从同一源位点继续消费。

        源日志保留带位点成员与真实提交标记；回滚后重放由幂等台账保证
        已交付事务不会重复投递目标。
        """
        rollback = pipeline_bus.rollback_deployment(source_instance, replay_position)
        binlog_file, _, binlog_pos = (replay_position or "").rpartition(":")
        replayed: list[dict[str, Any]] = []
        if binlog_file and binlog_pos.isdigit():
            # replay_transaction 内部已串联发布 + 消费端边界校验 + 幂等下发，
            # 并在回放清单中回写请求的重放源位点。
            from app.services import service_for
            adapter = service_for("format_adapter")
            replayed = adapter.replay_transaction(
                source_instance, binlog_file, int(binlog_pos))
        return {"rollback": rollback, "replay_results": replayed,
                "cursor": pipeline_bus.get_source_cursor(source_instance)}
