from __future__ import annotations

import hashlib
import json
import sqlite3
from typing import Any

from app.core.clock import Clock, SystemClock, to_storage
from app.core.errors import ConflictError, ValidationError
from app.germplasm.accessions import AccessionService
from app.germplasm.inventory import InventoryService
from app.germplasm.repository import GermplasmRepository, record

# 差异代码：逐项记录清单、护照与实收之间的不吻合，供复核员分流。
SEVERE_DISCREPANCIES = {"source_unknown", "source_mismatch", "accession_exists"}


def _content_hash(rows: list[dict[str, Any]]) -> str:
    canonical = json.dumps(rows, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


class IntakeService:
    """到库批次：分批导入清单与实收、生成差异、复核分流并留下可追查的链路。"""

    def __init__(self, connection: sqlite3.Connection, clock: Clock | None = None) -> None:
        self.connection = connection
        self.clock = clock or SystemClock()
        self.repository = GermplasmRepository(connection)
        self.accessions = AccessionService(connection, self.clock)
        self.inventory = InventoryService(connection, self.clock)

    # ---------- 批次 ----------

    def create_batch(self, data: dict[str, Any]) -> dict[str, Any]:
        timestamp = to_storage(self.clock.now())
        try:
            cursor = self.connection.execute(
                "INSERT INTO intake_batches(batch_no,source_code,acquisition_type,harvest_year,"
                "weight_tolerance_percent,required_passport_fields_json,note,created_by,created_at,updated_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?)",
                (
                    data["batch_no"], data["source_code"], data.get("acquisition_type", "采集"),
                    data["harvest_year"], data.get("weight_tolerance_percent", 5),
                    json.dumps(data.get("required_passport_fields", []), ensure_ascii=False),
                    data.get("note", ""), data["created_by"], timestamp, timestamp,
                ),
            )
        except sqlite3.IntegrityError as exc:
            raise ConflictError("到库批次编号已经存在") from exc
        batch_id = int(cursor.lastrowid)
        self._event(batch_id, None, "batch_created", data["created_by"], {"batch_no": data["batch_no"]})
        return self.batch_detail(batch_id)

    def batch_detail(self, batch_id: int) -> dict[str, Any]:
        batch = self.repository.require_intake_batch(batch_id)
        source = self.repository.source_by_code(batch["source_code"])
        batch["source"] = source
        batch["items"] = self.repository.list_intake_items(batch_id)
        batch["imports"] = self.repository.list_intake_imports(batch_id)
        batch["events"] = self.repository.list_intake_events(batch_id)
        return batch

    def close_batch(self, batch_id: int, data: dict[str, Any]) -> dict[str, Any]:
        batch = self.repository.require_intake_batch(batch_id)
        if int(batch["version"]) != int(data["expected_version"]):
            raise ConflictError("到库批次版本冲突", context={"current_version": batch["version"]})
        if batch["status"] != "open":
            raise ConflictError("到库批次已经关闭")
        pending = int(self.connection.execute(
            "SELECT COUNT(*) FROM intake_items WHERE batch_id=? AND decision IS NULL", (batch_id,)
        ).fetchone()[0])
        if pending:
            raise ConflictError("仍有明细未复核，不能关闭批次", context={"pending_count": pending})
        timestamp = to_storage(self.clock.now())
        cursor = self.connection.execute(
            "UPDATE intake_batches SET status='completed',version=version+1,updated_at=? WHERE id=? AND version=?",
            (timestamp, batch_id, data["expected_version"]),
        )
        if cursor.rowcount != 1:
            raise ConflictError("到库批次版本冲突")
        self._event(batch_id, None, "closed", data["actor"], {"item_count": self._item_count(batch_id)})
        return self.batch_detail(batch_id)

    # ---------- 导入 ----------

    def import_manifest(self, batch_id: int, rows: list[dict[str, Any]], actor: str) -> dict[str, Any]:
        return self._import_rows(batch_id, "manifest", rows, actor)

    def import_receipts(self, batch_id: int, rows: list[dict[str, Any]], actor: str) -> dict[str, Any]:
        return self._import_rows(batch_id, "receipt", rows, actor)

    def _import_rows(
        self, batch_id: int, kind: str, rows: list[dict[str, Any]], actor: str
    ) -> dict[str, Any]:
        batch = self.repository.require_intake_batch(batch_id)
        if batch["status"] != "open":
            raise ConflictError("到库批次已经关闭，不能继续导入")
        content_hash = _content_hash(rows)
        existing = self.repository.intake_import_by_hash(batch_id, kind, content_hash)
        if existing:
            return {**existing["summary"], "import_id": existing["id"], "replayed": True}

        applied = 0
        skipped: list[dict[str, Any]] = []
        rejected: list[dict[str, Any]] = []
        for index, row in enumerate(rows, start=1):
            marker = f"intake_import_{kind}_{index}"
            self.connection.execute(f"SAVEPOINT {marker}")
            try:
                outcome = self._apply_row(batch, kind, row, actor)
                self.connection.execute(f"RELEASE SAVEPOINT {marker}")
            except Exception as exc:
                self.connection.execute(f"ROLLBACK TO SAVEPOINT {marker}")
                self.connection.execute(f"RELEASE SAVEPOINT {marker}")
                rejected.append({"row": index, "reason": str(exc)})
                continue
            if outcome == "applied":
                applied += 1
            else:
                skipped.append({"row": index, "accession_no": row.get("accession_no", ""), "reason": outcome})

        timestamp = to_storage(self.clock.now())
        summary = {
            "batch_id": batch_id,
            "kind": kind,
            "content_hash": content_hash,
            "row_count": len(rows),
            "applied_count": applied,
            "skipped": skipped,
            "rejected": rejected,
            "imported_by": actor,
            "created_at": timestamp,
        }
        try:
            cursor = self.connection.execute(
                "INSERT INTO intake_imports(batch_id,import_kind,content_hash,row_count,applied_count,summary_json,"
                "imported_by,created_at) VALUES(?,?,?,?,?,?,?,?)",
                (
                    batch_id, kind, content_hash, len(rows), applied,
                    json.dumps(summary, ensure_ascii=False, sort_keys=True), actor, timestamp,
                ),
            )
        except sqlite3.IntegrityError:
            existing = self.repository.intake_import_by_hash(batch_id, kind, content_hash)
            if existing is None:  # pragma: no cover - 唯一约束冲突后必然能读到
                raise
            return {**existing["summary"], "import_id": existing["id"], "replayed": True}
        self._touch_batch(batch_id)
        return {**summary, "import_id": int(cursor.lastrowid), "replayed": False}

    def _apply_row(self, batch: dict[str, Any], kind: str, row: dict[str, Any], actor: str) -> str:
        accession_no = str(row.get("accession_no") or "").strip().upper()
        if not accession_no:
            raise ValidationError("缺少资源号")
        if " " in accession_no:
            raise ValidationError("资源号不能包含空格")
        item = self.repository.intake_item_by_accession(int(batch["id"]), accession_no)
        if item and item["decision"]:
            return "该明细已完成复核，跳过"
        timestamp = to_storage(self.clock.now())
        if kind == "manifest":
            expected = row.get("expected_weight_grams")
            if expected is not None and float(expected) <= 0:
                raise ValidationError("清单重量必须为正数")
            fields = {
                "scientific_name": str(row.get("scientific_name") or "").strip(),
                "crop_name": str(row.get("crop_name") or "").strip(),
                "cultivar_name": str(row.get("cultivar_name") or "").strip(),
                "source_code": str(row.get("source_code") or "").strip().upper(),
                "expected_weight_grams": float(expected) if expected is not None else None,
                "permit_reference": (str(row["permit_reference"]).strip() if row.get("permit_reference") else None),
                "passport": row.get("passport") or {},
                "manifest_received": 1,
            }
        else:
            received = row.get("received_weight_grams")
            if received is None:
                raise ValidationError("实收记录缺少称重")
            if float(received) <= 0:
                raise ValidationError("实收重量必须为正数")
            fields = {"received_weight_grams": float(received), "receipt_received": 1}
        if item is None:
            columns = ["batch_id", "accession_no"]
            for key in fields:
                columns.append("passport_json" if key == "passport" else key)
            columns.extend(["created_at", "updated_at"])
            placeholders = ",".join("?" for _ in columns)
            values: list[Any] = [batch["id"], accession_no]
            for key, value in fields.items():
                values.append(json.dumps(value, ensure_ascii=False, sort_keys=True) if key == "passport" else value)
            values.extend([timestamp, timestamp])
            self.connection.execute(
                f"INSERT INTO intake_items({','.join(columns)}) VALUES({placeholders})", values
            )
            item_id = int(self.connection.execute("SELECT last_insert_rowid()").fetchone()[0])
        else:
            assignments = []
            values = []
            for key, value in fields.items():
                column = "passport_json" if key == "passport" else key
                assignments.append(f"{column}=?")
                values.append(json.dumps(value, ensure_ascii=False, sort_keys=True) if key == "passport" else value)
            values.extend([timestamp, item["id"]])
            self.connection.execute(
                f"UPDATE intake_items SET {','.join(assignments)},updated_at=? WHERE id=?", values
            )
            item_id = int(item["id"])
        discrepancies, suggested = self._reevaluate(batch, item_id)
        self._event(int(batch["id"]), item_id, f"{kind}_imported", actor, {
            "accession_no": accession_no, "discrepancies": discrepancies, "suggested_action": suggested,
        })
        return "applied"

    # ---------- 复核 ----------

    def decide_item(self, item_id: int, data: dict[str, Any]) -> dict[str, Any]:
        item = self.repository.require_intake_item(item_id)
        batch = self.repository.require_intake_batch(int(item["batch_id"]))
        self._ensure_decidable(batch, item, int(data["expected_version"]))
        decision = str(data["decision"])
        reason = str(data.get("reason") or "").strip()
        if decision in {"quarantine", "return"} and not reason:
            raise ValidationError("隔离或退回时必须填写决定理由")
        timestamp = to_storage(self.clock.now())
        accession_id: int | None = None
        lot_id: int | None = None
        hold_id: int | None = None
        if decision in {"accept", "quarantine"}:
            accession_id, lot_id, hold_id = self._register_material(batch, item, decision, reason, data["actor"])
        cursor = self.connection.execute(
            "UPDATE intake_items SET decision=?,decision_reason=?,decided_by=?,decided_at=?,"
            "accession_id=?,lot_id=?,hold_id=?,version=version+1,updated_at=? WHERE id=? AND version=?",
            (
                decision, reason, data["actor"], timestamp, accession_id, lot_id, hold_id,
                timestamp, item_id, data["expected_version"],
            ),
        )
        if cursor.rowcount != 1:
            raise ConflictError("到库明细版本冲突")
        self._event(int(batch["id"]), item_id, "decided", data["actor"], {
            "decision": decision, "reason": reason, "discrepancies": item["discrepancies"],
            "accession_id": accession_id, "lot_id": lot_id, "hold_id": hold_id,
        })
        self._touch_batch(int(batch["id"]))
        return self.repository.require_intake_item(item_id)

    def decide_batch(self, batch_id: int, data: dict[str, Any]) -> dict[str, Any]:
        batch = self.repository.require_intake_batch(batch_id)
        if batch["status"] != "open":
            raise ConflictError("到库批次已经关闭，不能复核")
        specs = data["items"]
        seen: set[int] = set()
        items: list[dict[str, Any]] = []
        for spec in specs:
            item_id = int(spec["item_id"])
            if item_id in seen:
                raise ValidationError("同一明细在批量复核中重复出现")
            seen.add(item_id)
            item = self.repository.require_intake_item(item_id)
            if int(item["batch_id"]) != batch_id:
                raise ValidationError("明细不属于该到库批次")
            items.append(item)
        # 先整体校验版本与状态，任何一项冲突都不写入，避免覆盖他人刚完成的复核。
        for item, spec in zip(items, specs):
            expected = int(spec["expected_version"])
            if int(item["version"]) != expected:
                raise ConflictError(
                    "批量复核与最新状态冲突，请刷新后重试",
                    context={"item_id": item["id"], "accession_no": item["accession_no"],
                             "current_version": item["version"]},
                )
            if item["decision"]:
                raise ConflictError(
                    "明细已被他人复核，批量决定未执行",
                    context={"item_id": item["id"], "accession_no": item["accession_no"],
                             "decision": item["decision"], "decided_by": item["decided_by"]},
                )
        decided = [
            self.decide_item(int(item["id"]), {
                "decision": data["decision"], "reason": data.get("reason", ""),
                "expected_version": int(spec["expected_version"]), "actor": data["actor"],
            })
            for item, spec in zip(items, specs)
        ]
        return {
            "batch_id": batch_id,
            "decision": data["decision"],
            "applied_count": len(decided),
            "items": decided,
        }

    def correct_item(self, item_id: int, data: dict[str, Any]) -> dict[str, Any]:
        item = self.repository.require_intake_item(item_id)
        batch = self.repository.require_intake_batch(int(item["batch_id"]))
        if batch["status"] != "open":
            raise ConflictError("到库批次已经关闭，不能修正")
        if int(item["version"]) != int(data["expected_version"]):
            raise ConflictError("到库明细版本冲突", context={"current_version": item["version"]})
        if item["decision"] != "return":
            raise ConflictError("只有被退回的明细可以修正后继续批次")
        allowed = {
            "scientific_name", "crop_name", "cultivar_name", "source_code",
            "expected_weight_grams", "received_weight_grams", "permit_reference", "passport",
        }
        changes = {key: value for key, value in data.items() if key in allowed and value is not None}
        if not changes:
            raise ValidationError("没有可修正的明细字段")
        for key in ("expected_weight_grams", "received_weight_grams"):
            if key in changes and float(changes[key]) <= 0:
                raise ValidationError("重量必须为正数")
        if "source_code" in changes:
            changes["source_code"] = str(changes["source_code"]).strip().upper()
        changed_fields = sorted(changes)
        # 补登清单类字段视为清单已到，补登实收重量视为实收已到
        manifest_fields = {
            "scientific_name", "crop_name", "cultivar_name", "source_code",
            "expected_weight_grams", "permit_reference", "passport",
        }
        if manifest_fields & changes.keys():
            changes["manifest_received"] = 1
        if "received_weight_grams" in changes:
            changes["receipt_received"] = 1
        timestamp = to_storage(self.clock.now())
        assignments = []
        values: list[Any] = []
        for key, value in changes.items():
            column = "passport_json" if key == "passport" else key
            assignments.append(f"{column}=?")
            values.append(json.dumps(value, ensure_ascii=False, sort_keys=True) if key == "passport" else value)
        values.extend([timestamp, item_id, data["expected_version"]])
        cursor = self.connection.execute(
            f"UPDATE intake_items SET {','.join(assignments)},decision=NULL,decision_reason='',decided_by=NULL,"
            "decided_at=NULL,version=version+1,updated_at=? WHERE id=? AND version=?",
            values,
        )
        if cursor.rowcount != 1:
            raise ConflictError("到库明细版本冲突")
        discrepancies, suggested = self._reevaluate(batch, item_id)
        self._event(int(batch["id"]), item_id, "corrected", data["actor"], {
            "changed_fields": changed_fields, "previous_reason": item["decision_reason"],
            "discrepancies": discrepancies, "suggested_action": suggested,
        })
        self._touch_batch(int(batch["id"]))
        return self.repository.require_intake_item(item_id)

    # ---------- 汇总与对账 ----------

    def batch_summary(self, batch_id: int) -> dict[str, Any]:
        batch = self.repository.require_intake_batch(batch_id)
        items = self.repository.list_intake_items(batch_id)
        counts = {"total": len(items), "pending": 0, "accepted": 0, "quarantined": 0, "returned": 0}
        summaries: list[dict[str, Any]] = []
        reconciled = True
        for item in items:
            if item["decision"] is None:
                counts["pending"] += 1
            elif item["decision"] == "accept":
                counts["accepted"] += 1
            elif item["decision"] == "quarantine":
                counts["quarantined"] += 1
            else:
                counts["returned"] += 1
            reconciliation = self._reconcile_item(item)
            if reconciliation["consistent"] is False:
                reconciled = False
            summaries.append({
                "item_id": item["id"],
                "accession_no": item["accession_no"],
                "decision": item["decision"],
                "decision_reason": item["decision_reason"],
                "decided_by": item["decided_by"],
                "discrepancies": item["discrepancies"],
                "accession_id": item["accession_id"],
                "lot_id": item["lot_id"],
                "hold_id": item["hold_id"],
                "reconciliation": reconciliation,
            })
        return {
            "batch": {key: batch[key] for key in (
                "id", "batch_no", "source_code", "status", "version", "created_by", "created_at", "updated_at"
            )},
            "counts": counts,
            "reconciled": reconciled,
            "items": summaries,
            "imports": self.repository.list_intake_imports(batch_id),
        }

    def item_detail(self, item_id: int) -> dict[str, Any]:
        item = self.repository.require_intake_item(item_id)
        item["events"] = self.repository.list_intake_events(int(item["batch_id"]), item_id)
        return item

    # ---------- 内部 ----------

    def _ensure_decidable(self, batch: dict[str, Any], item: dict[str, Any], expected_version: int) -> None:
        if batch["status"] != "open":
            raise ConflictError("到库批次已经关闭，不能复核")
        if int(item["version"]) != expected_version:
            raise ConflictError("到库明细版本冲突", context={"current_version": item["version"]})
        if item["decision"]:
            raise ConflictError(
                "该明细已完成复核",
                context={"decision": item["decision"], "decided_by": item["decided_by"]},
            )

    def _register_material(
        self, batch: dict[str, Any], item: dict[str, Any], decision: str, reason: str, actor: str
    ) -> tuple[int, int, int | None]:
        source = self.repository.source_by_code(batch["source_code"])
        if source is None:
            raise ConflictError("来源编码未登记，不能接收或隔离，请先登记来源")
        if self.repository.accession_by_number(item["accession_no"]):
            raise ConflictError("资源号已进入正式档案，不能重复接收",
                                context={"accession_no": item["accession_no"]})
        if not item["scientific_name"] or not item["crop_name"]:
            raise ValidationError("缺少学名或作物名称，请先退回并修正明细")
        weight = item["received_weight_grams"]
        if weight is None:
            weight = item["expected_weight_grams"]
        if weight is None or float(weight) <= 0:
            raise ValidationError("缺少有效重量，请先退回并修正明细")
        accession = self.accessions.create_accession({
            "accession_no": item["accession_no"],
            "scientific_name": item["scientific_name"],
            "crop_name": item["crop_name"],
            "cultivar_name": item["cultivar_name"],
            "source_id": source["id"],
            "acquisition_type": batch["acquisition_type"],
            "received_on": self.clock.now().date().isoformat(),
            "passport": item["passport"],
            "created_by": actor,
        })
        target = "accepted" if decision == "accept" else "quarantine"
        accession = self.accessions.transition(int(accession["id"]), {
            "target_status": target,
            "reason": reason or "到库复核接收",
            "expected_version": 1,
            "actor": actor,
        })
        lot = self.inventory.create_lot({
            "lot_no": f"LOT-{item['accession_no']}",
            "accession_id": accession["id"],
            "parent_lot_id": None,
            "harvest_year": batch["harvest_year"],
            "initial_weight_grams": float(weight),
            "moisture_percent": None,
            "treatment": "",
            "sealed_on": None,
            "created_by": actor,
        })
        hold_id: int | None = None
        if decision == "quarantine":
            hold = self.inventory.impose_hold({
                "lot_id": lot["id"], "hold_type": "检疫",
                "reason": reason or "到库复核隔离", "actor": actor,
            })
            hold_id = int(hold["id"])
        return int(accession["id"]), int(lot["id"]), hold_id

    def _reevaluate(self, batch: dict[str, Any], item_id: int) -> tuple[list[str], str]:
        item = self.repository.require_intake_item(item_id)
        discrepancies = self._compute_discrepancies(batch, item)
        suggested = self._suggest(discrepancies)
        timestamp = to_storage(self.clock.now())
        self.connection.execute(
            "UPDATE intake_items SET discrepancy_json=?,suggested_action=?,version=version+1,updated_at=? WHERE id=?",
            (json.dumps(discrepancies, ensure_ascii=False), suggested, timestamp, item_id),
        )
        return discrepancies, suggested

    def _compute_discrepancies(self, batch: dict[str, Any], item: dict[str, Any]) -> list[str]:
        issues: list[str] = []
        source = self.repository.source_by_code(batch["source_code"])
        if source is None:
            issues.append("source_unknown")
        manifest_received = bool(item.get("manifest_received"))
        if manifest_received:
            row_source = str(item.get("source_code") or "").strip().upper()
            if row_source and row_source != batch["source_code"]:
                issues.append("source_mismatch")
            if source and source.get("permit_reference"):
                row_permit = str(item.get("permit_reference") or "").strip()
                if not row_permit:
                    issues.append("permit_missing")
                elif row_permit != str(source["permit_reference"]).strip():
                    issues.append("permit_mismatch")
        existing = self.repository.accession_by_number(item["accession_no"])
        if existing and existing["id"] != item.get("accession_id"):
            issues.append("accession_exists")
        if not manifest_received:
            issues.append("manifest_missing")
        if not item.get("receipt_received"):
            issues.append("receipt_missing")
        expected = item.get("expected_weight_grams")
        received = item.get("received_weight_grams")
        if manifest_received and expected is None:
            issues.append("weight_missing")
        if expected is not None and received is not None:
            if float(expected) <= 0:
                issues.append("weight_invalid")
            else:
                deviation = abs(float(received) - float(expected)) / float(expected) * 100
                if deviation > float(batch["weight_tolerance_percent"]) + 1e-9:
                    issues.append("weight_out_of_tolerance")
        if manifest_received:
            passport = item.get("passport") or {}
            for field in batch.get("required_passport_fields") or []:
                value = passport.get(field)
                if value is None or (isinstance(value, str) and not value.strip()):
                    issues.append(f"passport_missing:{field}")
        return issues

    @staticmethod
    def _suggest(discrepancies: list[str]) -> str:
        if any(issue in SEVERE_DISCREPANCIES for issue in discrepancies):
            return "return"
        if discrepancies:
            return "quarantine"
        return "accept"

    def _reconcile_item(self, item: dict[str, Any]) -> dict[str, Any]:
        decision = item["decision"]
        if decision is None:
            return {"consistent": None, "accession_linked": None, "lot_linked": None, "hold_linked": None}
        if decision == "return":
            consistent = not (item["accession_id"] or item["lot_id"] or item["hold_id"])
            return {"consistent": consistent, "accession_linked": None, "lot_linked": None, "hold_linked": None}
        accession = record(self.connection.execute(
            "SELECT * FROM accessions WHERE id=?", (item["accession_id"],)
        ).fetchone()) if item["accession_id"] else None
        accession_linked = bool(accession) and accession["accession_no"] == item["accession_no"]
        lot = record(self.connection.execute(
            "SELECT * FROM seed_lots WHERE id=?", (item["lot_id"],)
        ).fetchone()) if item["lot_id"] else None
        lot_linked = bool(lot) and int(lot["accession_id"]) == int(item["accession_id"])
        result: dict[str, Any] = {
            "accession_linked": accession_linked,
            "accession_status": accession["status"] if accession else None,
            "lot_linked": lot_linked,
            "lot_status": lot["status"] if lot else None,
        }
        if decision == "quarantine":
            hold = record(self.connection.execute(
                "SELECT * FROM lot_holds WHERE id=?", (item["hold_id"],)
            ).fetchone()) if item["hold_id"] else None
            hold_linked = bool(hold) and int(hold["lot_id"]) == int(item["lot_id"])
            result["hold_linked"] = hold_linked
            result["hold_active"] = bool(hold) and hold["released_at"] is None
            result["consistent"] = accession_linked and lot_linked and hold_linked
        else:
            result["hold_linked"] = None
            result["consistent"] = accession_linked and lot_linked
        return result

    def _item_count(self, batch_id: int) -> int:
        return int(self.connection.execute(
            "SELECT COUNT(*) FROM intake_items WHERE batch_id=?", (batch_id,)
        ).fetchone()[0])

    def _touch_batch(self, batch_id: int) -> None:
        self.connection.execute(
            "UPDATE intake_batches SET version=version+1,updated_at=? WHERE id=?",
            (to_storage(self.clock.now()), batch_id),
        )

    def _event(
        self, batch_id: int, item_id: int | None, event_type: str, actor: str, detail: dict[str, Any]
    ) -> None:
        self.connection.execute(
            "INSERT INTO intake_events(batch_id,item_id,event_type,actor,detail_json,created_at) VALUES(?,?,?,?,?,?)",
            (
                batch_id, item_id, event_type, actor,
                json.dumps(detail, ensure_ascii=False, sort_keys=True), to_storage(self.clock.now()),
            ),
        )
