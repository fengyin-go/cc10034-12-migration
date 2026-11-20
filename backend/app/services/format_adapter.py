"""格式适配领域服务：将变更事件在不同格式间转换。

上游：change_event 产出已解析事件 → format_adapter 适配格式 (Debezium/Canal/Maxwell)
消费：scd_handler 消费适配后的事件进行SCD处理
"""
from __future__ import annotations

import json
import zlib
from datetime import datetime
from typing import Any, Callable

from app.services.base import BaseModuleService
from app.pipeline import (
    MaxwellCommitMarker,
    MaxwellEventBatch,
    MaxwellMember,
    pipeline_bus,
)


class FormatAdapterService(BaseModuleService):
    MODULE = "format_adapter"
    ENTITY = "适配规则"
    OPERATION_TERM = "转换"
    LIST_FIELDS = ["规则名称", "输入格式", "输出格式", "字段重命名", "值转换", "时间戳格式", "是否压缩"]
    REQUIRED_FIELDS = ["规则名称", "输入格式", "输出格式"]
    STATUS_ORDER = ["草稿", "已启用", "已停用"]
    ACTION_RULES = {"启用规则": "草稿", "停用规则": "已启用"}
    NEGATIVE_ACTIONS = ["停用规则"]

    def adapt_event(self, event: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
        """将变更事件按规则适配为目标格式。

        支持 Debezium ↔ Canal ↔ Maxwell 格式互转，
        包括字段重命名、值转换、时间戳格式化和压缩。
        """
        input_fmt = config.get("输入格式", "debezium").lower()
        output_fmt = config.get("输出格式", "canal").lower()
        field_rename = self._parse_mapping(config.get("字段重命名", ""))
        value_convert = self._parse_mapping(config.get("值转换", ""))
        ts_format = config.get("时间戳格式", "%Y-%m-%d %H:%M:%S")
        should_compress = config.get("是否压缩", "否") == "是"

        # Debezium -> 内部格式
        if input_fmt == "debezium":
            adapted = self._from_debezium(event)
        elif input_fmt == "canal":
            adapted = self._from_canal(event)
        elif input_fmt == "maxwell":
            adapted = self._from_maxwell(event)
        else:
            adapted = dict(event)

        # 字段重命名
        for old_name, new_name in field_rename.items():
            if old_name in adapted:
                adapted[new_name] = adapted.pop(old_name)

        # 值转换
        for field_name, converter_exp in value_convert.items():
            if field_name in adapted:
                val = adapted[field_name]
                if converter_exp == "lower":
                    adapted[field_name] = str(val).lower()
                elif converter_exp == "upper":
                    adapted[field_name] = str(val).upper()
                elif converter_exp.startswith("replace:"):
                    parts = converter_exp.split(":", 2)
                    if len(parts) == 3:
                        adapted[field_name] = str(val).replace(parts[1], parts[2])

        # 时间戳格式化
        ts = event.get("事件时间", "")
        if ts:
            try:
                dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
                adapted["formatted_timestamp"] = dt.strftime(ts_format)
            except (ValueError, TypeError):
                adapted["formatted_timestamp"] = str(ts)

        # 压缩
        if should_compress:
            payload = json.dumps(adapted, ensure_ascii=False)
            compressed = zlib.compress(payload.encode("utf-8"))
            adapted = {"_compressed": True, "_size": len(compressed), "_format": output_fmt}

        adapted["_output_format"] = output_fmt
        adapted["_adapted_at"] = datetime.now().isoformat()
        return adapted

    def _from_debezium(self, event: dict[str, Any]) -> dict[str, Any]:
        result = dict(event)
        before_raw = event.get("变更前镜像", "{}")
        after_raw = event.get("变更后镜像", "{}")
        # Debezium 的 payload.after 结构
        result["source_meta"] = {
            "connector": "mysql",
            "version": "1.9.7.Final",
            "name": event.get("来源表", ""),
            "ts_ms": int(datetime.now().timestamp() * 1000),
        }
        try:
            after = json.loads(after_raw) if isinstance(after_raw, str) else after_raw
            result["payload"] = {"before": before_raw, "after": after}
        except (json.JSONDecodeError, TypeError):
            result["payload"] = {"before": {}, "after": {}}
        return result

    def _from_canal(self, event: dict[str, Any]) -> dict[str, Any]:
        result = dict(event)
        op_map = {"insert": "INSERT", "update": "UPDATE", "delete": "DELETE"}
        dt_now = int(datetime.now().timestamp() * 1000)
        result["canal_meta"] = {
            "binlog": f"mysql-bin.{dt_now // 1000 % 1000000:06d}",
            "type": op_map.get(event.get("操作类型", "").lower(), "INSERT"),
            "es": dt_now,
            "isDdl": False,
        }
        return result

    def _from_maxwell(self, event: dict[str, Any]) -> dict[str, Any]:
        result = dict(event)
        # 源事件时间优先，禁止用当前时间代替；缺失时才退回当前时间
        source_ts = event.get("事件时间") or event.get("ts")
        ts_epoch = self._to_epoch_seconds(source_ts)
        result["maxwell_meta"] = {
            "database": event.get("来源表", "").split(".")[0] if "." in event.get("来源表", "") else "default_db",
            "table": event.get("来源表", "").split(".")[-1] if "." in event.get("来源表", "") else "default_table",
            "type": event.get("操作类型", ""),
            "ts": ts_epoch,
            "xid": event.get("事务ID", ""),
            "commit": True,
        }
        return result

    @staticmethod
    def _to_epoch_seconds(value: Any) -> int:
        """把源事件时间转为 epoch 秒；无法解析时才回退当前时间。"""
        if value in (None, ""):
            return int(datetime.now().timestamp())
        if isinstance(value, (int, float)):
            return int(value)
        try:
            return int(datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp())
        except (ValueError, TypeError):
            return int(datetime.now().timestamp())

    # ==================================================================
    # Maxwell 信封迁移：旧信封(无真实事务边界) -> 输出版本(带事务边界)
    # ==================================================================

    LEGACY_ENVELOPE_VERSION = "legacy"
    OUTPUT_ENVELOPE_VERSION = "v2"

    def build_legacy_maxwell_envelope(self, raw: dict[str, Any]) -> dict[str, Any]:
        """旧适配进程的 Maxwell 信封：不带真实事务边界。

        旧版把每条变更当成独立消息，commit 固定写 True，没有提交标记与位点边界。
        该方法保留用于复现旧版行为并承接存量记录（旧接口仍可读到这些信封）。
        """
        source_table = raw.get("table", "default_db.default_table")
        envelope = {
            "envelope_version": self.LEGACY_ENVELOPE_VERSION,
            "database": raw.get("database", source_table.split(".")[0]),
            "table": raw.get("table_name", source_table.split(".")[-1]),
            "type": raw.get("type", raw.get("operation", "INSERT")),
            "ts": self._to_epoch_seconds(raw.get("ts") or raw.get("事件时间")),
            "xid": raw.get("xid", raw.get("tx_id", "")),
            "commit": True,  # 旧信封无法表达真实提交边界
            "position": raw.get("position", ""),
            "data": raw.get("data", raw.get("after", {})),
            "old": raw.get("old", raw.get("before")),
        }
        pipeline_bus.legacy_envelope_store.append(envelope)
        return envelope

    def build_transactional_maxwell_envelope(self, member: MaxwellMember) -> dict[str, Any]:
        """输出版本信封：携带真实事务边界、源位点与源事件时间。

        与提交凭证一起发布；成员本身不单独宣告 commit，提交真相由提交标记承载。
        """
        return {
            "envelope_version": self.OUTPUT_ENVELOPE_VERSION,
            "member_id": member.member_id,
            "tx_id": member.tx_id,
            "source_instance": member.source_instance,
            "database": member.database,
            "table": member.table,
            "type": member.operation,
            "ts": int(member.event_time.timestamp()),  # 源事件时间，非当前时间
            "event_time": member.event_time.isoformat(),
            "xid": member.tx_id,
            "commit": False,  # 提交与否由真实提交标记/提交凭证决定
            "source_position": f"{member.binlog_file}:{member.binlog_pos}",
            "binlog_file": member.binlog_file,
            "binlog_pos": member.binlog_pos,
            "gtid": member.gtid,
            "seq": member.seq,
            "data": member.after_image or {},
            "old": member.before_image,
            "received_by": member.envelope_version,
        }

    def migrate_legacy_envelope(self, legacy_envelope: dict[str, Any],
                                source_instance: str, binlog_file: str,
                                binlog_pos: int, gtid: str = "") -> MaxwellMember:
        """把旧版存量 Maxwell 信封迁移为带位点的事务成员（迁移事务元数据）。

        迁移保留原始事件时间（ts），不用当前时间代替；旧信封仍留在存量存储中，
        原接口可继续读取。返回的成员按当前部署版本登记到源日志。
        """
        event_time = datetime.fromtimestamp(legacy_envelope["ts"]) \
            if legacy_envelope.get("ts") else datetime.now()
        member = MaxwellMember(
            member_id=f"mem_{legacy_envelope.get('xid', 'x')}_{binlog_pos}",
            source_instance=source_instance,
            tx_id=str(legacy_envelope.get("xid") or f"tx_{binlog_pos}"),
            database=legacy_envelope.get("database", ""),
            table=legacy_envelope.get("table", ""),
            operation=legacy_envelope.get("type", "INSERT"),
            before_image=legacy_envelope.get("old"),
            after_image=legacy_envelope.get("data"),
            binlog_file=binlog_file,
            binlog_pos=binlog_pos,
            gtid=gtid,
            event_time=event_time,
            envelope_version=pipeline_bus.adapter_deployment_version,
        )
        pipeline_bus.migrated_legacy_ids.add(member.member_id)
        pipeline_bus.record_maxwell_member(member)
        return member

    def ingest_maxwell_change(self, raw: dict[str, Any], *,
                              source_instance: str, envelope_version: str | None = None
                              ) -> MaxwellMember:
        """把一条 Maxwell 变更数据送入适配流水线（格式适配入口）。

        兼容 Maxwell 原生 JSON（database/table/type/ts/xid/data/old）与内部字段。
        成员按当前部署版本登记；源事件时间原样保留。
        """
        version = envelope_version or pipeline_bus.adapter_deployment_version
        event_time = datetime.fromtimestamp(raw["ts"]) if raw.get("ts") else datetime.now()
        binlog_file = raw.get("binlog_file") or raw.get("binlog", "mysql-bin.000001")
        binlog_pos = int(raw.get("binlog_pos") or raw.get("position") or 0)
        member = MaxwellMember(
            member_id=raw.get("member_id")
            or f"mem_{raw.get('xid', 'x')}_{binlog_pos}",
            source_instance=source_instance,
            tx_id=str(raw.get("xid") or raw.get("tx_id") or f"tx_{binlog_pos}"),
            database=raw.get("database", ""),
            table=raw.get("table", raw.get("table_name", "")),
            operation=str(raw.get("type") or raw.get("operation", "INSERT")).upper(),
            before_image=raw.get("old") or raw.get("before"),
            after_image=raw.get("data") or raw.get("after"),
            binlog_file=binlog_file,
            binlog_pos=binlog_pos,
            gtid=raw.get("gtid", ""),
            event_time=event_time,
            envelope_version=version,
        )
        pipeline_bus.record_maxwell_member(member)
        return member

    def ingest_maxwell_commit(self, *, tx_id: str, source_instance: str,
                              binlog_file: str, binlog_pos: int,
                              gtid: str = "", commit_time: datetime | None = None
                              ) -> MaxwellCommitMarker:
        """登记一笔 Maxwell 事务的真实提交标记。"""
        marker = MaxwellCommitMarker(
            tx_id=str(tx_id),
            source_instance=source_instance,
            binlog_file=binlog_file,
            binlog_pos=int(binlog_pos),
            gtid=gtid,
            commit_time=commit_time or datetime.now(),
        )
        pipeline_bus.record_maxwell_commit(marker)
        return marker

    def switch_adapter_deployment(self, target_version: str) -> dict[str, Any]:
        """部署切换：把适配环境从旧版切到带真实事务边界的输出版本。

        切换瞬间可能有事务首成员已在旧版接收、末成员与提交标记尚未到达；
        源日志与事务聚合跨版本保留，后续成员直接并入同一事务，不丢不重。
        """
        previous = pipeline_bus.adapter_deployment_version
        pipeline_bus.adapter_deployment_version = target_version
        pipeline_bus.write_audit(
            "部署切换", "format_adapter",
            f"Maxwell 适配版本 {previous} -> {target_version}，源日志与在途事务保留",
            "maxwell_migration", "已切换")
        return {"previous_version": previous, "current_version": target_version,
                "inflight_transactions": [
                    tx.tx_id for tx in pipeline_bus.maxwell_transactions.values()
                    if not tx.published]}

    def publish_transaction_batch(self, tx_id: str) -> MaxwellEventBatch | None:
        """串联格式适配与变更事件消费的发布侧：生成可核对的 Maxwell 事件数据批次。"""
        return pipeline_bus.publish_maxwell_batch(
            tx_id, self.build_transactional_maxwell_envelope)

    def replay_transaction(self, source_instance: str, binlog_file: str,
                           binlog_pos: int) -> list[dict[str, Any]]:
        """回滚后从同一源位点重放（适配侧重放，消费侧幂等由总线保证）。"""
        return pipeline_bus.replay_from_position(
            source_instance, binlog_file, binlog_pos,
            self.build_transactional_maxwell_envelope)

    def list_legacy_envelopes(self) -> list[dict[str, Any]]:
        """旧版存量信封仍可从原接口读取。"""
        return list(pipeline_bus.legacy_envelope_store)


    def _parse_mapping(self, mapping_str: str) -> dict[str, str]:
        """解析字段重命名/值转换字符串。"""
        result: dict[str, str] = {}
        if not mapping_str:
            return result
        for pair in mapping_str.split(","):
            pair = pair.strip()
            if ":" in pair:
                key, val = pair.split(":", 1)
                result[key.strip()] = val.strip()
            elif "=" in pair:
                key, val = pair.split("=", 1)
                result[key.strip()] = val.strip()
        return result

    def batch_adapt(self, events: list[dict[str, Any]], config: dict[str, Any]) -> dict[str, Any]:
        """批量适配事件。"""
        adapted_list = [self.adapt_event(evt, config) for evt in events]
        compressed_count = sum(1 for a in adapted_list if a.get("_compressed"))
        return {
            "total": len(adapted_list),
            "compressed": compressed_count,
            "output_format": config.get("输出格式"),
            "adapted_at": datetime.now().isoformat(),
        }

    def compute_format_compatibility(self, source_fmt: str, target_fmt: str) -> dict[str, Any]:
        """计算两种格式的兼容性。"""
        compatibility_matrix = {
            ("debezium", "canal"): 95,
            ("canal", "debezium"): 90,
            ("debezium", "maxwell"): 85,
            ("maxwell", "debezium"): 80,
            ("canal", "maxwell"): 90,
            ("maxwell", "canal"): 90,
        }
        score = compatibility_matrix.get((source_fmt.lower(), target_fmt.lower()), 70)
        return {"source": source_fmt, "target": target_fmt, "compatibility_pct": score, "lossless": score >= 95}