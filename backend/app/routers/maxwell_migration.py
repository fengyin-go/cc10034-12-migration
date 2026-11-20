"""Maxwell 事务边界迁移专用接口。

串联：Maxwell 变更接入 -> 格式适配 -> 事务聚合（成员 + 真实提交标记）
-> Maxwell 事件数据批次发布 -> 消费端边界校验与幂等下发 -> 回放清单回写。
与通用资源路由并列，不改变既有 18 个业务模块的接口契约。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.pipeline import MaxwellEventBatch, MaxwellMember, pipeline_bus
from app.services import service_for

router = APIRouter(prefix="/api/maxwell", tags=["Maxwell事务迁移"])


def _adapter():
    return service_for("format_adapter")


def _consumer():
    return service_for("change_event")


def _batch_to_dict(batch: MaxwellEventBatch) -> dict[str, Any]:
    return {
        "batch_id": batch.batch_id,
        "tx_id": batch.tx_id,
        "source_instance": batch.source_instance,
        "start_position": batch.start_position,
        "end_position": batch.end_position,
        "commit_pos": batch.commit_pos,
        "gtid": batch.gtid,
        "commit_evidence": batch.commit_evidence,
        "members": batch.members,
        "member_count": batch.member_count,
        "event_time": batch.event_time,
        "commit_time": batch.commit_time,
        "envelope_version": batch.envelope_version,
        "checksum": batch.checksum,
        "delivered": batch.delivered,
        "delivered_count": batch.delivered_count,
        "delivery_attempts": batch.delivery_attempts,
        "replay_count": batch.replay_count,
    }


def _member_to_dict(member: MaxwellMember) -> dict[str, Any]:
    return {
        "member_id": member.member_id,
        "source_instance": member.source_instance,
        "tx_id": member.tx_id,
        "database": member.database,
        "table": member.table,
        "operation": member.operation,
        "binlog_file": member.binlog_file,
        "binlog_pos": member.binlog_pos,
        "gtid": member.gtid,
        "event_time": member.event_time.isoformat(),
        "envelope_version": member.envelope_version,
        "seq": member.seq,
    }


class IngestPayload(BaseModel):
    source_instance: str
    member: dict[str, Any] = Field(default_factory=dict)
    envelope_version: str | None = None


class CommitPayload(BaseModel):
    source_instance: str
    tx_id: str
    binlog_file: str
    binlog_pos: int
    gtid: str = ""
    commit_time: str | None = None  # ISO 源提交时间；缺省由总线登记


class SwitchPayload(BaseModel):
    target_version: str = "v2"


class ConsumePayload(BaseModel):
    expected_instance: str | None = None
    expected_commit_position: str | None = None


class RollbackPayload(BaseModel):
    source_instance: str
    replay_position: str | None = None


class LegacyIngestPayload(BaseModel):
    source_instance: str
    envelope: dict[str, Any]
    binlog_file: str
    binlog_pos: int
    gtid: str = ""


@router.post("/ingest")
def ingest_change(payload: IngestPayload) -> dict[str, Any]:
    """把一条 Maxwell 变更数据送入适配流水线（带源位点的事务成员）。"""
    adapter = _adapter()
    member = adapter.ingest_maxwell_change(
        payload.member, source_instance=payload.source_instance,
        envelope_version=payload.envelope_version)
    txn = pipeline_bus.get_maxwell_transaction(member.tx_id)
    return {
        "ok": True,
        "member": _member_to_dict(member),
        "transaction": {
            "tx_id": txn.tx_id,
            "member_count": len(txn.members),
            "committed": txn.commit_marker is not None,
            "received_versions": txn.received_versions,
            "complete": pipeline_bus.is_transaction_complete(txn.tx_id),
        },
    }


@router.post("/commit")
def ingest_commit(payload: CommitPayload) -> dict[str, Any]:
    """登记一笔 Maxwell 事务的真实提交标记。"""
    commit_time = (
        datetime.fromisoformat(payload.commit_time.replace("Z", "+00:00"))
        if payload.commit_time else None)
    marker = _adapter().ingest_maxwell_commit(
        tx_id=payload.tx_id, source_instance=payload.source_instance,
        binlog_file=payload.binlog_file, binlog_pos=payload.binlog_pos,
        gtid=payload.gtid, commit_time=commit_time)
    txn = pipeline_bus.get_maxwell_transaction(marker.tx_id)
    return {
        "ok": True,
        "commit": {
            "tx_id": marker.tx_id, "source_instance": marker.source_instance,
            "position": f"{marker.binlog_file}:{marker.binlog_pos}",
            "commit_time": marker.commit_time.isoformat(),
        },
        "complete": txn is not None and pipeline_bus.is_transaction_complete(marker.tx_id),
        "member_count": len(txn.members) if txn else 0,
    }


@router.post("/legacy/envelope")
def ingest_legacy_envelope(payload: LegacyIngestPayload) -> dict[str, Any]:
    """登记旧版存量信封并迁移为带位点的事务成员（旧信封原接口仍可读）。"""
    adapter = _adapter()
    legacy = adapter.build_legacy_maxwell_envelope(payload.envelope)
    member = adapter.migrate_legacy_envelope(
        legacy, payload.source_instance, payload.binlog_file,
        payload.binlog_pos, payload.gtid)
    return {"ok": True, "legacy_envelope": legacy,
            "migrated_member": _member_to_dict(member)}


@router.get("/legacy/envelopes")
def list_legacy_envelopes() -> dict[str, Any]:
    envelopes = _adapter().list_legacy_envelopes()
    return {"items": envelopes, "total": len(envelopes)}


@router.post("/deployment/switch")
def switch_deployment(payload: SwitchPayload) -> dict[str, Any]:
    """切换适配环境版本（在途事务的源日志与聚合状态保留）。"""
    return {"ok": True, **_adapter().switch_adapter_deployment(payload.target_version)}


@router.get("/deployment")
def deployment_status() -> dict[str, Any]:
    return {"adapter_deployment_version": pipeline_bus.adapter_deployment_version}


@router.post("/transactions/{tx_id}/publish")
def publish_transaction(tx_id: str) -> dict[str, Any]:
    """发布 Maxwell 事件数据批次（仅当真实提交且成员完整）。"""
    batch = _adapter().publish_transaction_batch(tx_id)
    if batch is None:
        txn = pipeline_bus.get_maxwell_transaction(tx_id)
        reason = "事务不存在" if txn is None else (
            "缺少真实提交标记" if txn.commit_marker is None else "成员不完整")
        raise HTTPException(status_code=409, detail=f"批次不可发布：{reason}")
    return {"ok": True, "batch": _batch_to_dict(batch)}


@router.post("/batches/{batch_id}/consume")
def consume_batch(batch_id: str, payload: ConsumePayload) -> dict[str, Any]:
    """消费端按源实例、位点和提交证据校验下发边界并交付（幂等）。"""
    result = _consumer().consume_maxwell_event_batch(
        batch_id, expected_instance=payload.expected_instance,
        expected_commit_position=payload.expected_commit_position)
    return {"ok": result.get("result") == "delivered", **result}


@router.post("/rollback")
def rollback_and_resume(payload: RollbackPayload) -> dict[str, Any]:
    """中止迁移并回滚，再从同一源位点继续消费（重复事务只交付一次）。"""
    return {"ok": True,
            **_consumer().rollback_and_resume(payload.source_instance,
                                              payload.replay_position)}


@router.get("/batches")
def list_batches() -> dict[str, Any]:
    return {"items": [_batch_to_dict(b) for b in pipeline_bus.maxwell_batches],
            "total": len(pipeline_bus.maxwell_batches)}


@router.get("/batches/{batch_id}")
def get_batch(batch_id: str) -> dict[str, Any]:
    batch = next((b for b in pipeline_bus.maxwell_batches
                  if b.batch_id == batch_id), None)
    if batch is None:
        raise HTTPException(status_code=404, detail=f"批次 {batch_id} 不存在")
    return _batch_to_dict(batch)


@router.get("/transactions")
def list_transactions() -> dict[str, Any]:
    items = [
        {
            "tx_id": txn.tx_id,
            "source_instance": txn.source_instance,
            "member_count": len(txn.members),
            "committed": txn.commit_marker is not None,
            "complete": pipeline_bus.is_transaction_complete(txn.tx_id),
            "published": txn.published,
            "received_versions": txn.received_versions,
            "members": [_member_to_dict(m) for m in
                        sorted(txn.members, key=lambda m: m.binlog_pos)],
            "commit_position": (
                f"{txn.commit_marker.binlog_file}:{txn.commit_marker.binlog_pos}"
                if txn.commit_marker else None),
        }
        for txn in pipeline_bus.maxwell_transactions.values()
    ]
    return {"items": items, "total": len(items)}


@router.get("/source-log")
def source_log() -> dict[str, Any]:
    """源日志：带位点的成员和真实提交标记（回滚后仍保留，可重放）。"""
    return {
        "members": [_member_to_dict(m) for m in pipeline_bus.maxwell_member_log],
        "commits": [
            {"tx_id": m.tx_id, "source_instance": m.source_instance,
             "position": f"{m.binlog_file}:{m.binlog_pos}",
             "gtid": m.gtid, "commit_time": m.commit_time.isoformat()}
            for m in pipeline_bus.maxwell_commit_log],
    }


@router.get("/replay-manifest")
def replay_manifest() -> dict[str, Any]:
    return {"items": pipeline_bus.replay_manifest,
            "total": len(pipeline_bus.replay_manifest)}


@router.get("/cursor/{source_instance}")
def source_cursor(source_instance: str) -> dict[str, Any]:
    cursor = pipeline_bus.get_source_cursor(source_instance)
    if cursor is None:
        return {"source_instance": source_instance, "position": None}
    return {"source_instance": source_instance, **cursor}


@router.get("/target-batches")
def target_batches() -> dict[str, Any]:
    """目标系统实际收到的批次（核对交付次数）。"""
    return {"items": [_batch_to_dict(b)
                      for b in pipeline_bus.maxwell_target_batches.values()],
            "total": len(pipeline_bus.maxwell_target_batches)}
