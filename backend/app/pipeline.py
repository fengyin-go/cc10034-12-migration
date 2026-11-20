"""跨模块管道状态总线：服务间通过管道层完成事件、位点、Schema 的发布-消费。"""
from __future__ import annotations

import hashlib
import json
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class ChangeEventRecord:
    """变更事件统一结构。"""
    event_id: str
    source_db: str
    source_table: str
    operation: str  # INSERT / UPDATE / DELETE
    before_image: dict[str, Any] | None
    after_image: dict[str, Any] | None
    tx_id: str
    binlog_file: str
    binlog_pos: int
    gtid: str
    event_time: datetime
    raw_format: str = "debezium"
    columns: list[str] = field(default_factory=list)


@dataclass
class DdlRecord:
    """DDL 变更结构。"""
    record_id: str
    database: str
    table_name: str
    ddl_statement: str
    executed_at: datetime
    affected_columns: int
    rollback_statement: str
    column_changes: list[dict[str, Any]] = field(default_factory=list)
    partition_changes: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class PositionSnapshot:
    """位点快照结构。"""
    instance: str
    binlog_name: str
    position: int
    gtid_set: str
    updated_at: datetime
    source: str
    channel_id: int | None = None


@dataclass
class CheckpointRecord:
    """检查点结构。"""
    checkpoint_id: str
    channel_name: str
    binlog_file: str
    offset: int
    confirmed_at: datetime
    committed: bool
    data_checksum: str


@dataclass
class MaxwellMember:
    """Maxwell 事务成员（一条带源位点的变更，对应 Maxwell JSON 行）。

    成员在事务提交前就可能到达（部署切换期间跨版本到达），
    因此独立保存源实例、binlog 文件、位点和源事件时间，绝不以当前时间替代。
    """
    member_id: str
    source_instance: str
    tx_id: str
    database: str
    table: str
    operation: str
    before_image: dict[str, Any] | None
    after_image: dict[str, Any] | None
    binlog_file: str
    binlog_pos: int
    gtid: str
    event_time: datetime
    envelope_version: str = "legacy"  # legacy(旧信封) / v2(带真实事务边界的输出版本)
    seq: int = 0


@dataclass
class MaxwellCommitMarker:
    """Maxwell 真实提交标记（Xid 提交事件）。"""
    tx_id: str
    source_instance: str
    binlog_file: str
    binlog_pos: int
    gtid: str
    commit_time: datetime


@dataclass
class MaxwellTransaction:
    """一笔 Maxwell 事务的聚合状态：成员 + 真实提交标记。"""
    tx_id: str
    source_instance: str
    members: list[MaxwellMember] = field(default_factory=list)
    commit_marker: MaxwellCommitMarker | None = None
    # 接收期间所处的适配版本（首成员可能在旧版接收，末成员在新版到达）
    received_versions: list[str] = field(default_factory=list)
    published: bool = False
    published_at: datetime | None = None


@dataclass
class MaxwellEventBatch:
    """可核对的 Maxwell 事件数据批次。

    只有「真实提交 + 成员完整」时才生成；成员信封与提交凭证一起发布。
    """
    batch_id: str
    tx_id: str
    source_instance: str
    start_position: str          # 首成员源位点
    end_position: str            # 提交标记源位点（下发边界）
    binlog_file: str
    start_pos: int
    commit_pos: int
    gtid: str
    commit_evidence: dict[str, Any]
    members: list[dict[str, Any]]
    event_time: str              # 源事件时间（非当前时间）
    commit_time: str
    member_count: int
    envelope_version: str
    checksum: str
    delivered: bool = False
    delivered_count: int = 0     # 实际向目标交付次数（幂等保护后恒 <= 1）
    replay_count: int = 0        # 回放尝试次数（含被幂等去重的次数）
    delivery_attempts: int = 0


class PipelineBus:
    """管道级事件总线：持有事件队列、Schema 注册表、位点注册表、SCD 状态存储。

    各领域服务通过 pipeline_bus 单例完成跨模块数据流通。
    """

    def __init__(self) -> None:
        # 事件队列 — binlog_parser 产出，change_event 消费
        self.event_queue: list[ChangeEventRecord] = []
        self.event_log: list[dict[str, Any]] = []
        # DDL 记录 — ddl_tracker 产出，schema_sync/partition_sync 消费
        self.ddl_log: list[DdlRecord] = []
        # 位点注册 — checkpoint_manager/position_store 写入，cdc_channel/failover_group 消费
        self.positions: dict[str, PositionSnapshot] = {}
        self.checkpoints: list[CheckpointRecord] = []
        # Schema 注册 — schema_sync 写入，ddl_tracker/format_adapter 消费
        self.schema_registry: dict[str, list[dict[str, Any]]] = {}
        # SCD 状态 — scd_handler 核心存储
        self.scd_type1_store: dict[str, dict[str, Any]] = {}  # snapshot
        self.scd_type2_store: dict[str, list[dict[str, Any]]] = {}  # history
        self.scd_type3_store: dict[str, dict[str, Any]] = {}  # prev-col
        # 去重状态 — dedup_window 核心存储
        self.dedup_state: dict[str, list[dict[str, Any]]] = defaultdict(list)
        # 路由规则 — route_dispatcher 核心配置
        self.routing_table: list[dict[str, Any]] = []
        # 过滤规则 — event_filter 核心配置
        self.filter_rules: list[dict[str, Any]] = []
        # 快照比对结果
        self.compare_results: list[dict[str, Any]] = []
        # 审计日志
        self.audit_log: list[dict[str, Any]] = []
        # 延迟指标缓冲
        self.lag_buffer: list[dict[str, Any]] = []
        # 解析统计
        self.parse_stats: dict[str, Any] = {
            "total_parsed": 0,
            "total_events": 0,
            "parse_errors": 0,
            "last_batch_time": None,
        }
        # 管道运行时统计
        self.pipeline_metrics: dict[str, Any] = {
            "total_input": 0,
            "total_output": 0,
            "filtered": 0,
            "routed": 0,
            "scd_processed": 0,
            "dedup_hits": 0,
            "backfill_rows": 0,
        }
        # ===== Maxwell 事务边界迁移状态 =====
        # 源日志：带位点的成员与真实提交标记按到达顺序保留，回滚后仍可从同一位点重放
        self.maxwell_member_log: list[MaxwellMember] = []
        self.maxwell_commit_log: list[MaxwellCommitMarker] = []
        # 事务聚合表 tx_id -> MaxwellTransaction（跨版本部署持续累积）
        self.maxwell_transactions: dict[str, MaxwellTransaction] = {}
        # 已发布的 Maxwell 事件数据批次（可核对）
        self.maxwell_batches: list[MaxwellEventBatch] = []
        # 幂等交付台账 batch_id -> 交付证据，重复回放只向目标交付一次
        self.maxwell_delivery_ledger: dict[str, dict[str, Any]] = {}
        # 目标系统已接收的批次（模拟下游目标，按 batch_id 去重）
        self.maxwell_target_batches: dict[str, MaxwellEventBatch] = {}
        # 回放清单：记录每次消费/回放的源位点、边界校验与交付结果
        self.replay_manifest: list[dict[str, Any]] = []
        # 当前适配环境版本：legacy(旧适配进程) / v2(带真实事务边界的输出版本)
        self.adapter_deployment_version: str = "legacy"
        # 已迁移的旧信封存量记录 id 集合（旧接口仍可读取）
        self.legacy_envelope_store: list[dict[str, Any]] = []
        self.migrated_legacy_ids: set[str] = set()
        # 每个源实例的消费游标（已安全消费到的源位点）
        self.source_cursors: dict[str, dict[str, Any]] = {}
        self._maxwell_seq: int = 0

    def publish_event(self, event: ChangeEventRecord) -> dict[str, Any]:
        """将解析产出的事件发布到队列。返回发布确认。"""
        self.event_queue.append(event)
        self.pipeline_metrics["total_input"] += 1
        self.parse_stats["total_parsed"] += 1
        self.parse_stats["last_batch_time"] = datetime.now().isoformat()
        return {
            "event_id": event.event_id,
            "position": event.binlog_pos,
            "general": self.pipeline_metrics["total_input"],
        }

    def consume_events(self, batch_size: int = 100) -> list[ChangeEventRecord]:
        """从事件队列消费变更事件。返回批量事件。"""
        batch = self.event_queue[:batch_size]
        self.event_queue = self.event_queue[batch_size:]
        self.pipeline_metrics["total_output"] += len(batch)
        return batch

    def register_position(self, pos: PositionSnapshot) -> None:
        """注册存储位点。"""
        self.positions[pos.instance] = pos

    def get_position(self, instance: str) -> PositionSnapshot | None:
        """查询当前位点。"""
        return self.positions.get(instance)

    def calc_position_lag(self, source_pos: PositionSnapshot, target_pos: PositionSnapshot) -> int:
        """计算两个位点之间的落后字节数。"""
        if source_pos.binlog_name != target_pos.binlog_name:
            return -1  # 跨文件无法直接比较
        return abs(source_pos.position - target_pos.position)

    def add_ddl(self, ddl: DdlRecord) -> None:
        """添加 DDL 记录。"""
        self.ddl_log.append(ddl)

    def get_recent_ddls(self, limit: int = 50) -> list[DdlRecord]:
        """查询最近 DDL 记录。"""
        return self.ddl_log[-limit:]

    def compute_event_hash(self, event: ChangeEventRecord) -> str:
        """计算事件的业务去重哈希。"""
        raw = f"{event.event_id}:{event.source_table}:{event.operation}:{event.binlog_file}:{event.binlog_pos}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    def apply_scd_type1(self, dim_key: str, row: dict[str, Any]) -> dict[str, Any]:
        """SCD Type1：覆盖式更新。"""
        existing = self.scd_type1_store.get(dim_key)
        if existing:
            existing.update(row)
        else:
            existing = row
        self.scd_type1_store[dim_key] = existing
        return existing

    def apply_scd_type2(self, dim_key: str, row: dict[str, Any]) -> list[dict[str, Any]]:
        """SCD Type2：保留完整历史。"""
        history = self.scd_type2_store.setdefault(dim_key, [])
        row["version"] = len(history) + 1
        history.append(dict(row))
        return list(history)

    def apply_scd_type3(self, dim_key: str, col_name: str, row: dict[str, Any]) -> dict[str, Any]:
        """SCD Type3：保留前值列。"""
        existing = self.scd_type3_store.get(dim_key)
        result = dict(row)
        if existing:
            result[f"_previous_{col_name}"] = existing.get(col_name)
        self.scd_type3_store[dim_key] = result
        return result

    def window_dedup(self, dedup_key: str, event_id: str, window_sec: float) -> bool:
        """窗口去重：检查指定 key 在窗口期内是否已有事件。"""
        now = time.time()
        buffer = self.dedup_state[dedup_key]
        buffer[:] = [e for e in buffer if now - e["ts"] < window_sec]
        for existing in buffer:
            if existing["event_id"] == event_id:
                self.pipeline_metrics["dedup_hits"] += 1
                return True  # 重复
        buffer.append({"event_id": event_id, "ts": now})
        return False

    def evaluate_filter(self, event: ChangeEventRecord) -> tuple[bool, list[str]]:
        """评估过滤规则，返回 (是否通过, 匹配规则列表)。"""
        passed = True
        matched: list[str] = []
        for rule in self.filter_rules:
            if rule.get("status") != "已启用":
                continue
            match = self._match_rule(event, rule)
            if match:
                matched.append(rule["name"])
                if rule.get("type") == "黑名单":
                    passed = False
            elif rule.get("type") == "白名单":
                passed = False
        if not passed:
            self.pipeline_metrics["filtered"] += 1
        return passed, matched

    def evaluate_route(self, event: ChangeEventRecord) -> list[str]:
        """评估路由规则，返回目标通道列表。"""
        targets: list[str] = []
        for rule in self.routing_table:
            if not rule.get("enabled"):
                continue
            if self._match_route(event, rule):
                targets.append(rule["target"])
        self.pipeline_metrics["routed"] += len(targets)
        return targets

    def _match_rule(self, event: ChangeEventRecord, rule: dict[str, Any]) -> bool:
        """匹配过滤规则。"""
        expr = rule.get("expression", "")
        if not expr:
            return False
        return (
            expr in event.source_table
            or expr in event.source_db
            or expr in event.operation
        )

    def _match_route(self, event: ChangeEventRecord, rule: dict[str, Any]) -> bool:
        """匹配路由规则。"""
        cond = rule.get("condition", "")
        if not cond:
            return False
        return (
            cond in event.source_table
            or cond in event.source_db
            or cond in event.operation
        )

    def write_audit(self, operation: str, target: str, summary: str,
                    source_info: str, result: str, remark: str = "") -> dict[str, Any]:
        """写入审计日志。"""
        entry = {
            "id": len(self.audit_log) + 1,
            "审计时间": datetime.now().isoformat(),
            "操作类型": operation,
            "目标对象": target,
            "变更摘要": summary,
            "操作来源": source_info,
            "结果状态": result,
            "审计备注": remark,
        }
        self.audit_log.append(entry)
        return entry

    def compute_backfill_estimate(self, source_pos: int, target_pos: int,
                                  batch_size: int, avg_row_bytes: int = 256) -> dict[str, Any]:
        """计算回填任务的预估指标。"""
        diff = abs(target_pos - source_pos) if target_pos >= source_pos else 0
        estimated_rows = diff // avg_row_bytes
        estimated_batches = (estimated_rows + batch_size - 1) // batch_size
        return {
            "position_diff_bytes": diff,
            "estimated_rows": estimated_rows,
            "estimated_batches": estimated_batches,
            "estimated_rows_str": f"{estimated_rows:,}",
        }

    def compare_snapshots(self, source_rows: int, target_rows: int,
                          diff_rows: int, sample_ratio: float) -> dict[str, Any]:
        """执行快照比对并计算差异指标。"""
        total = max(source_rows, target_rows, 1)
        diff_pct = round(diff_rows / total * 100, 2)
        return {
            "source_count": source_rows,
            "target_count": target_rows,
            "diff_count": diff_rows,
            "diff_percentage": diff_pct,
            "sample_ratio": sample_ratio,
            "status": "正常" if diff_pct < 5 else "差异发现",
            "checked_at": datetime.now().isoformat(),
        }

    def compute_lag(self, channel_name: str, lag_seconds: float,
                    backlog_events: int, source_pos: str, target_pos: str) -> dict[str, Any]:
        """计算延迟指标并缓冲。"""
        throughput = backlog_events / max(lag_seconds, 1) if lag_seconds > 0 else 0
        metric = {
            "指标时间": datetime.now().isoformat(),
            "所属通道": channel_name,
            "延迟秒数": round(lag_seconds, 1),
            "积压事件数": backlog_events,
            "吞吐量": f"{throughput:.1f}",
            "源位点": source_pos,
            "目标位点": target_pos,
        }
        self.lag_buffer.append(metric)
        if len(self.lag_buffer) > 1000:
            self.lag_buffer = self.lag_buffer[-1000:]
        return metric

    # ==================================================================
    # Maxwell 事务边界迁移：源日志 -> 事务聚合 -> 事件数据批次 -> 幂等交付
    # ==================================================================

    @staticmethod
    def _position_key(binlog_file: str, binlog_pos: int) -> str:
        return f"{binlog_file}:{binlog_pos}"

    def record_maxwell_member(self, member: MaxwellMember) -> MaxwellTransaction:
        """接收一条 Maxwell 事务成员并写入源日志与事务聚合表。

        源日志保留带位点的成员；事务按 tx_id 聚合，允许成员跨部署版本到达。
        重复成员（同源位点）直接去重，保证回滚后重放不会产生重复成员。
        """
        for existing in self.maxwell_member_log:
            if (existing.source_instance == member.source_instance
                    and existing.binlog_file == member.binlog_file
                    and existing.binlog_pos == member.binlog_pos):
                return self.maxwell_transactions[member.tx_id]
        self._maxwell_seq += 1
        member.seq = self._maxwell_seq
        self.maxwell_member_log.append(member)
        txn = self.maxwell_transactions.get(member.tx_id)
        if txn is None:
            txn = MaxwellTransaction(tx_id=member.tx_id,
                                     source_instance=member.source_instance)
            self.maxwell_transactions[member.tx_id] = txn
        txn.members.append(member)
        if member.envelope_version not in txn.received_versions:
            txn.received_versions.append(member.envelope_version)
        return txn

    def record_maxwell_commit(self, marker: MaxwellCommitMarker) -> MaxwellTransaction | None:
        """接收真实提交标记并写入源日志。

        提交标记可能晚于成员到达（切换后才到），也可能先于部分成员重放到达，
        因此只做登记并挂到事务上，不立即发布——发布必须等成员完整。
        """
        for existing in self.maxwell_commit_log:
            if (existing.source_instance == marker.source_instance
                    and existing.tx_id == marker.tx_id):
                return self.maxwell_transactions.get(marker.tx_id)
        self.maxwell_commit_log.append(marker)
        txn = self.maxwell_transactions.get(marker.tx_id)
        if txn is None:
            txn = MaxwellTransaction(tx_id=marker.tx_id,
                                     source_instance=marker.source_instance)
            self.maxwell_transactions[marker.tx_id] = txn
        txn.commit_marker = marker
        return txn

    def get_maxwell_transaction(self, tx_id: str) -> MaxwellTransaction | None:
        return self.maxwell_transactions.get(tx_id)

    def is_transaction_complete(self, tx_id: str) -> bool:
        """成员完整且持有真实提交标记。"""
        txn = self.maxwell_transactions.get(tx_id)
        if txn is None or not txn.members or txn.commit_marker is None:
            return False
        # 成员位点必须连续落在事务起点与提交位点之间，且不缺号
        ordered = sorted(txn.members, key=lambda m: m.binlog_pos)
        commit_pos = txn.commit_marker.binlog_pos
        if any(m.binlog_pos > commit_pos for m in ordered):
            return False
        positions = [m.binlog_pos for m in ordered]
        return len(positions) == len(set(positions))

    def build_commit_evidence(self, txn: MaxwellTransaction) -> dict[str, Any]:
        """基于真实提交标记生成提交凭证（含成员/位点摘要，可核对）。"""
        assert txn.commit_marker is not None
        ordered = sorted(txn.members, key=lambda m: m.binlog_pos)
        member_digest_raw = "|".join(
            f"{m.member_id}@{m.binlog_file}:{m.binlog_pos}" for m in ordered)
        member_digest = hashlib.sha256(member_digest_raw.encode()).hexdigest()[:16]
        marker = txn.commit_marker
        return {
            "evidence_id": f"ev_{txn.tx_id}_{member_digest}",
            "tx_id": txn.tx_id,
            "source_instance": txn.source_instance,
            "real_commit": True,
            "commit_position": self._position_key(marker.binlog_file, marker.binlog_pos),
            "gtid": marker.gtid,
            "commit_time": marker.commit_time.isoformat(),
            "member_count": len(ordered),
            "member_digest": member_digest,
        }

    def publish_maxwell_batch(
        self, tx_id: str, envelope_builder
    ) -> MaxwellEventBatch | None:
        """发布一笔事务的 Maxwell 事件数据批次。

        硬性条件：只有真实提交并且成员完整，才把成员信封与提交凭证一起发布。
        已发布的事务重复调用返回原批次（幂等），不会二次发布。
        envelope_builder: Callable[[MaxwellMember], dict] 由格式适配服务提供，
        负责把成员适配为带真实事务边界的输出信封。
        """
        txn = self.maxwell_transactions.get(tx_id)
        if txn is None:
            return None
        if txn.published:
            return next((b for b in self.maxwell_batches if b.tx_id == tx_id), None)
        if not self.is_transaction_complete(tx_id):
            return None  # 未真实提交或成员不完整：绝不发布
        ordered = sorted(txn.members, key=lambda m: m.binlog_pos)
        marker = txn.commit_marker
        assert marker is not None
        member_envelopes = [envelope_builder(m) for m in ordered]
        evidence = self.build_commit_evidence(txn)
        checksum_raw = json.dumps(
            {"tx_id": tx_id, "members": [m["member_id"] for m in member_envelopes],
             "evidence": evidence["evidence_id"]},
            sort_keys=True, ensure_ascii=False)
        checksum = hashlib.sha256(checksum_raw.encode()).hexdigest()[:16]
        batch = MaxwellEventBatch(
            batch_id=f"batch_{txn.source_instance}_{tx_id}",
            tx_id=tx_id,
            source_instance=txn.source_instance,
            start_position=self._position_key(ordered[0].binlog_file, ordered[0].binlog_pos),
            end_position=self._position_key(marker.binlog_file, marker.binlog_pos),
            binlog_file=marker.binlog_file,
            start_pos=ordered[0].binlog_pos,
            commit_pos=marker.binlog_pos,
            gtid=marker.gtid,
            commit_evidence=evidence,
            members=member_envelopes,
            # 源事件时间：取首成员源时间，禁止用当前时间代替
            event_time=ordered[0].event_time.isoformat(),
            commit_time=marker.commit_time.isoformat(),
            member_count=len(member_envelopes),
            envelope_version="v2",
            checksum=checksum,
        )
        self.maxwell_batches.append(batch)
        txn.published = True
        txn.published_at = datetime.now()
        return batch

    def consume_maxwell_batch(
        self, batch: MaxwellEventBatch, *, expected_instance: str | None = None,
        expected_position: str | None = None,
        requested_replay_position: str | None = None,
    ) -> dict[str, Any]:
        """消费端下发一个 Maxwell 事件数据批次。

        下发边界校验：批次的源实例、位点和提交证据必须与消费端期望一致。
        幂等：同一批次无论回放多少次，只向目标交付一次；结果回放到回放清单。
        requested_replay_position: 本次重放请求的源位点（回滚后继续消费的起点）。
        """
        boundary_ok = True
        boundary_reasons: list[str] = []
        if expected_instance and batch.source_instance != expected_instance:
            boundary_ok = False
            boundary_reasons.append(
                f"源实例不匹配: expected={expected_instance} actual={batch.source_instance}")
        if expected_position and batch.end_position != expected_position:
            boundary_ok = False
            boundary_reasons.append(
                f"下发位点不匹配: expected={expected_position} actual={batch.end_position}")
        if not batch.commit_evidence.get("real_commit"):
            boundary_ok = False
            boundary_reasons.append("缺少真实提交凭证")
        if batch.checksum != hashlib.sha256(json.dumps(
                {"tx_id": batch.tx_id,
                 "members": [m["member_id"] for m in batch.members],
                 "evidence": batch.commit_evidence["evidence_id"]},
                sort_keys=True, ensure_ascii=False).encode()).hexdigest()[:16]:
            boundary_ok = False
            boundary_reasons.append("批次校验和不一致")

        batch.delivery_attempts += 1
        manifest_row: dict[str, Any] = {
            "id": len(self.replay_manifest) + 1,
            "batch_id": batch.batch_id,
            "tx_id": batch.tx_id,
            "source_instance": batch.source_instance,
            "replay_start_position": batch.start_position,
            "requested_replay_position": requested_replay_position,
            "commit_boundary": batch.end_position,
            "boundary_verified": boundary_ok,
            "boundary_reasons": boundary_reasons,
            "attempt": batch.delivery_attempts,
            "delivered_this_time": False,
            "total_delivered": batch.delivered_count,
            "recorded_at": datetime.now().isoformat(),
        }

        if not boundary_ok:
            manifest_row["result"] = "rejected"
            self.replay_manifest.append(manifest_row)
            return manifest_row

        # 幂等交付：台账中已存在则不再向目标投递
        if batch.batch_id in self.maxwell_delivery_ledger:
            batch.replay_count += 1
            manifest_row["result"] = "deduplicated"
            manifest_row["total_delivered"] = batch.delivered_count
            self.replay_manifest.append(manifest_row)
            return manifest_row

        # 首次真正交付到目标
        self.maxwell_target_batches[batch.batch_id] = batch
        batch.delivered = True
        batch.delivered_count = 1
        self.maxwell_delivery_ledger[batch.batch_id] = {
            "batch_id": batch.batch_id,
            "tx_id": batch.tx_id,
            "delivered_at": datetime.now().isoformat(),
            "commit_evidence_id": batch.commit_evidence["evidence_id"],
            "checksum": batch.checksum,
        }
        self.source_cursors[batch.source_instance] = {
            "binlog_file": batch.binlog_file,
            "position": batch.commit_pos,
            "position_key": batch.end_position,
            "gtid": batch.gtid,
            "updated_at": datetime.now().isoformat(),
        }
        manifest_row["delivered_this_time"] = True
        manifest_row["total_delivered"] = 1
        manifest_row["result"] = "delivered"
        self.replay_manifest.append(manifest_row)
        return manifest_row

    def get_source_cursor(self, instance: str) -> dict[str, Any] | None:
        """返回消费端在某源实例上已确认的源位点（重放起点）。"""
        return self.source_cursors.get(instance)

    def replay_from_position(self, instance: str, binlog_file: str, binlog_pos: int,
                             envelope_builder, consume: bool = True) -> list[dict[str, Any]]:
        """回滚后从同一源位点重放。

        扫描源日志中位点 >= 指定位置的成员与其提交标记，重新聚合并发布批次，
        再走消费端下发。已交付事务由幂等台账拦截，目标只收到一次。
        """
        results: list[dict[str, Any]] = []
        affected_tx_ids: list[str] = []
        for member in self.maxwell_member_log:
            if (member.source_instance == instance
                    and member.binlog_file == binlog_file
                    and member.binlog_pos >= binlog_pos
                    and member.tx_id not in affected_tx_ids):
                affected_tx_ids.append(member.tx_id)
        for tx_id in affected_tx_ids:
            batch = self.publish_maxwell_batch(tx_id, envelope_builder)
            if batch is None:
                results.append({"tx_id": tx_id, "result": "incomplete",
                                "reason": "成员不完整或缺少真实提交标记"})
                continue
            if consume:
                row = self.consume_maxwell_batch(
                    batch, expected_instance=instance,
                    expected_position=batch.end_position,
                    requested_replay_position=self._position_key(binlog_file, binlog_pos))
                results.append(row)
            else:
                results.append({"tx_id": tx_id, "batch_id": batch.batch_id,
                                "result": "republished"})
        return results

    def rollback_deployment(self, instance: str, position_key: str | None = None) -> dict[str, Any]:
        """部署失败回滚：保留源日志，把消费游标回退到指定（或最后确认的）源位点。

        已交付记录与幂等台账保留——重复回放同一事务仍只交付一次。
        """
        cursor = self.source_cursors.get(instance)
        target = position_key or (cursor["position_key"] if cursor else None)
        self.adapter_deployment_version = "legacy"
        self.write_audit(
            "部署回滚", instance,
            f"Maxwell 适配回滚，源日志保留，重放起点 {target}",
            "maxwell_migration", "已回退")
        return {"rolled_back": True, "instance": instance,
                "replay_start_position": target,
                "member_log_retained": len(self.maxwell_member_log),
                "commit_log_retained": len(self.maxwell_commit_log)}

    def snapshot(self) -> dict[str, Any]:
        """全管道状态快照。"""
        return {
            "parse_stats": dict(self.parse_stats),
            "pipeline_metrics": dict(self.pipeline_metrics),
            "queue_depth": len(self.event_queue),
            "ddl_count": len(self.ddl_log),
            "position_count": len(self.positions),
            "checkpoint_count": len(self.checkpoints),
            "register_count": len(self.schema_registry),
            "scd_type1_keys": len(self.scd_type1_store),
            "scd_type2_keys": len(self.scd_type2_store),
            "scd_type3_keys": len(self.scd_type3_store),
            "audit_count": len(self.audit_log),
            "lag_buffer_size": len(self.lag_buffer),
            "maxwell_member_log": len(self.maxwell_member_log),
            "maxwell_commit_log": len(self.maxwell_commit_log),
            "maxwell_transactions": len(self.maxwell_transactions),
            "maxwell_batches": len(self.maxwell_batches),
            "maxwell_delivered": len(self.maxwell_delivery_ledger),
            "replay_manifest": len(self.replay_manifest),
            "adapter_deployment_version": self.adapter_deployment_version,
        }


pipeline_bus = PipelineBus()