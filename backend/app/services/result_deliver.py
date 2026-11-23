"""结果交付服务：抽取结果按交付格式向目标系统的分片导出与交付记录登记。

从版本快照读取产物，按指定格式和分片策略导出到目标系统。
交付前对快照文档做归属校验：只有能证明属于快照文档的记录才进入交付
分片；邻近编号文件与无法核对归属的材料写入归属交付明细，便于追查。
"""
from __future__ import annotations

import re
import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class ResultDeliverService(BaseModuleService):
    MODULE = "result_deliver"
    ENTITY = "交付任务"
    OPERATION_TERM = "导出"
    LIST_FIELDS = ["交付编号", "快照编号", "交付格式", "目标系统", "字段范围", "记录条数", "分片大小", "交付时间"]
    REQUIRED_FIELDS = ["交付编号", "快照编号", "交付格式"]
    STATUS_ORDER = ["草稿", "待导出", "导出中", "已交付"]
    ACTION_RULES = {"启动导出": "待导出", "确认交付": "已交付", "标记失败": "已交付"}
    NEGATIVE_ACTIONS = []

    SUPPORTED_FORMATS = {
        "jsonl": {"mime": "application/x-ndjson", "description": "每行一条JSON记录的文本格式"},
        "csv": {"mime": "text/csv", "description": "逗号分隔值表格格式"},
        "parquet": {"mime": "application/octet-stream", "description": "列式存储格式"},
        "xlsx": {"mime": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "description": "Excel表格格式"},
    }
# 数据操作：导出交付任务数据将版本快照产物分发到目标系统

    # 可交付产物类别与数据子目录的对应关系
    DELIVERABLE_SOURCES = [
        ("entity", "entities"),
        ("relation", "relations"),
        ("vector", "vectors"),
    ]

    def deliver_from_snapshot(self, snapshot_id: str) -> dict[str, Any]:
        """从版本快照导出抽取产物到目标系统，支持分片分批交付。

        只交付能证明属于快照文档的记录；无法核对归属的材料不进入交付
        分片，而是登记到归属交付明细。每次交付生成独立的交付编号与分片
        文件，经过更正的快照与初次交付各自对应。
        """
        # 查找快照记录（按实际归属核对，不凭文件遍历顺序）
        snapshot_data = self._find_snapshot(snapshot_id)

        if not snapshot_data:
            return {"error": f"版本快照 {snapshot_id} 不存在，请先创建版本快照"}

        doc_id = snapshot_data.get("文档编号", "")
        if not doc_id:
            return {"error": f"版本快照 {snapshot_id} 缺少文档编号，无法核对归属"}
        # 文档版本提供的可验证来源数据；旧快照可能没有，走记录级兼容核对
        manifest = snapshot_data.get("来源清单")
        fmt = "jsonl"
        target = "下游知识库"

        # 收集归属校验通过的可交付产物，并登记未能核对的材料
        deliverables, materials = self._collect_deliverables(doc_id, manifest)

        if not deliverables:
            return {"error": f"快照 {snapshot_id} 无可交付的抽取产物", "unverified_materials": materials}

        # 交付序号：同一快照多次交付（如快照更正后补发）各自对应独立交付
        checkpoint = self._read_checkpoint(f"deliver_{snapshot_id}") or {}
        delivery_seq = int(checkpoint.get("delivery_seq", 0)) + 1
        delivery_id = f"DELIVER-{snapshot_id}-{delivery_seq:03d}"

        shard_size = 500
        shards = []
# 数据产物：交付分片文件写入deliveries目录持久化文件
        batch = []
        for item in deliverables:
            batch.append(item)
            if len(batch) >= shard_size:
                shards.append(batch)
                batch = []
        if batch:
            shards.append(batch)

        # 写入交付产物文件（按交付序号独立命名，补发不与初次交付混写）
        output_files = []
        shard_names = []
        for i, shard in enumerate(shards):
            shard_name = f"{snapshot_id}_deliv-{delivery_seq:03d}_shard_{i+1:03d}.jsonl"
            output_path = self._data_dir("deliveries") / shard_name
            self._write_jsonl(output_path, shard)
            output_files.append(str(output_path))
            shard_names.append(shard_name)

        # 归属交付明细：登记本次交付关联的产物与未能核对的材料
        details_name = f"{snapshot_id}_deliv-{delivery_seq:03d}_details.jsonl"
        self._write_jsonl(self._data_dir("deliveries") / details_name, materials)

        pending_records = sum(int(m.get("待核条数", 0)) for m in materials)

        delivery = {
            "交付编号": delivery_id,
            "快照编号": snapshot_id,
            "交付序号": delivery_seq,
            "交付格式": fmt,
            "目标系统": target,
            "字段范围": f"实体+关系+三元组+向量",
            "记录条数": len(deliverables),
            "待核条数": pending_records,
            "分片大小": shard_size,
            "分片文件": shard_names,
            "明细文件": details_name,
            "交付时间": time.strftime("%Y-%m-%d %H:%M:%S"),
        }

        self._write_jsonl(
            self._data_dir("deliveries") / f"{snapshot_id}_delivery.jsonl",
            [delivery]
        )

        self._write_checkpoint(f"deliver_{snapshot_id}", {
            "snapshot_id": snapshot_id,
            "delivery_seq": delivery_seq,
            "delivery_id": delivery_id,
            "format": fmt,
            "target": target,
            "total_records": len(deliverables),
            "pending_records": pending_records,
            "shards": len(shards),
        })

        return {
            "snapshot_id": snapshot_id,
            "delivery_id": delivery_id,
            "delivery_seq": delivery_seq,
            "format": fmt,
            "target": target,
            "total_records": len(deliverables),
            "pending_records": pending_records,
            "shards": len(shards),
            "output_files": output_files,
            "details_file": details_name,
            "unverified_materials": [m for m in materials if m.get("状态") != "已确认"],
        }

    def get_delivery_receipt(self, snapshot_id: str, delivery_seq: int | None = None) -> dict[str, Any]:
        """重新打开指定快照的下载回执：返回交付回执与归属交付明细。

        交付回执按交付序号逐次登记，经过更正的快照与初次交付各自对应；
        不指定序号时返回最近一次交付。
        """
        ledger_path = self._data_dir("deliveries") / f"{snapshot_id}_delivery.jsonl"
        ledger = self._read_jsonl(ledger_path)
        if not ledger:
            return {"error": f"快照 {snapshot_id} 没有交付记录"}
        if delivery_seq is None:
            receipt = ledger[-1]
        else:
            receipt = next(
                (row for row in ledger if int(row.get("交付序号", 0)) == delivery_seq),
                None,
            )
            if receipt is None:
                return {"error": f"快照 {snapshot_id} 没有序号为 {delivery_seq} 的交付"}
        details_name = receipt.get("明细文件", "")
        details = self._read_jsonl(self._data_dir("deliveries") / details_name) if details_name else []
        return {
            "snapshot_id": snapshot_id,
            "deliveries": len(ledger),
            "receipt": receipt,
            "details": details,
        }

    def _find_snapshot(self, snapshot_id: str) -> dict[str, Any] | None:
        """按快照编号定位版本快照记录。

        归属核对不凭文件遍历顺序：优先取记录归属文件
        （{文档编号}_v{版本号}.jsonl）内的记录；同一快照经多次更正时
        取最新一条。发布副本不参与归属判定。
        """
        versions_dir = self._data_dir("versions")
        candidates: list[tuple[int, int, dict[str, Any]]] = []
        order = 0
        for vf in sorted(versions_dir.glob("*.jsonl")):
            if vf.stem.endswith("_published"):
                continue
            for snap in self._read_jsonl(vf):
                order += 1
                if snap.get("快照编号") != snapshot_id:
                    continue
                canonical_name = f"{snap.get('文档编号', '')}_v{snap.get('版本号', '')}.jsonl"
                at_home = 1 if vf.name == canonical_name else 0
                candidates.append((at_home, order, snap))
        if not candidates:
            return None
        candidates.sort(key=lambda item: (item[0], item[1]))
        return candidates[-1][2]

    def _collect_deliverables(
        self, doc_id: str, manifest: list[dict[str, Any]] | None = None
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """收集指定文档的可交付抽取产物，并逐条核对归属。

        返回 (可交付记录, 归属交付明细)。文件级归属要求文件名以
        "{doc_id}_" 开头——不同来源的历史文件可能共用一段前缀（如
        D-12 与 D-120），仅有共同前缀不属于本文档；记录级归属核对记录
        自带的文档/分块引用。旧快照缺来源清单时，仅交付能自证归属的
        记录，其余标记为待核材料。
        """
        confirmed: list[dict[str, Any]] = []
        materials: list[dict[str, Any]] = []
        manifest_by_path = {m.get("path"): m for m in (manifest or [])}

        for category, sub in self.DELIVERABLE_SOURCES:
            source_dir = self._data_dir(sub)
            for artifact in sorted(source_dir.glob(f"{doc_id}*.jsonl")):
                rel_path = f"{sub}/{artifact.name}"
                # 文件级归属：编号后必须紧跟分隔符，邻近编号越权排除
                if not artifact.name.startswith(f"{doc_id}_"):
                    materials.append({
                        "来源": rel_path,
                        "类别": category,
                        "状态": "越权排除",
                        "原因": f"文件编号与快照文档 {doc_id} 共用前缀但归属邻近编号，不纳入交付范围",
                        "确认条数": 0,
                        "待核条数": 0,
                        "待核记录": [],
                    })
                    continue

                # 清单级校验：在快照来源清单内且校验值一致，证明文件自快照后未变
                entry = manifest_by_path.get(rel_path)
                if entry is not None:
                    content = artifact.read_text(encoding="utf-8")
                    file_verified = self._compute_checksum(content) == entry.get("checksum")
                    note = "快照来源清单在册" if file_verified else "校验值与快照来源清单不一致，文件在快照后已变更，仅按记录级归属核对"
                elif manifest is not None:
                    file_verified = False
                    note = "文件不在快照来源清单内，仅按记录级归属核对"
                else:
                    file_verified = False
                    note = "旧快照缺少来源清单，仅按记录级归属核对"

                kept = 0
                pending_rows: list[dict[str, Any]] = []
                for row_no, record in enumerate(self._read_jsonl(artifact), start=1):
                    ownership = self._record_ownership(record, doc_id)
                    if ownership is False:
                        pending_rows.append({
                            "行号": row_no,
                            "记录标识": self._record_label(record),
                            "原因": "记录归属其他文档",
                        })
                        continue
                    if ownership is True or file_verified:
                        kept += 1
                        confirmed.append(self._deliverable_item(category, rel_path, record))
                    else:
                        pending_rows.append({
                            "行号": row_no,
                            "记录标识": self._record_label(record),
                            "原因": "缺少可核对的归属字段",
                        })

                if not pending_rows:
                    status = "已确认"
                elif kept:
                    status = "部分待核"
                else:
                    status = "待核"
                materials.append({
                    "来源": rel_path,
                    "类别": category,
                    "状态": status,
                    "原因": note,
                    "确认条数": kept,
                    "待核条数": len(pending_rows),
                    "待核记录": pending_rows,
                })

        return confirmed, materials

    @staticmethod
    def _record_ownership(record: dict[str, Any], doc_id: str) -> bool | None:
        """核对单条记录与文档的归属关系。

        返回 True 表示记录可自证属于本文档，False 表示记录明确归属其他
        文档，None 表示记录缺少可核对的归属字段。
        """
        owner = record.get("doc_id")
        if owner not in (None, ""):
            return str(owner) == doc_id
        chunk_id = str(record.get("chunk_id") or "")
        if chunk_id:
            match = re.match(r"^CHUNK-(.+)-(\d+)$", chunk_id)
            if match:
                return match.group(1) == doc_id
        return None

    @staticmethod
    def _record_label(record: dict[str, Any]) -> str:
        """提取记录的可追查标识，用于归属交付明细中标记待核材料。"""
        for key in ("entity_id", "relation_id", "chunk_id", "triple_id", "doc_id"):
            value = record.get(key)
            if value not in (None, ""):
                return str(value)
        return ""

    @staticmethod
    def _deliverable_item(category: str, source: str, record: dict[str, Any]) -> dict[str, Any]:
        """组装交付记录：向量只交付索引摘要，其余类别交付完整记录。"""
        if category == "vector":
            data = {
                "chunk_id": record.get("chunk_id"),
                "doc_id": record.get("doc_id"),
                "dimension": record.get("vector_dim"),
                "index": record.get("index"),
            }
        else:
            data = record
        return {"type": category, "source": source, "data": data}

    def list_formats(self) -> list[dict[str, Any]]:
        """列出所有支持的交付格式。"""
        return [
            {"format": k, "mime": v["mime"], "description": v["description"]}
            for k, v in self.SUPPORTED_FORMATS.items()
        ]
