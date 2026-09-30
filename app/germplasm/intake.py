from __future__ import annotations

import json
import sqlite3
from typing import Any

from app.core.clock import Clock, SystemClock, to_storage
from app.core.errors import ConflictError, NotFoundError, ValidationError
from app.germplasm.accessions import AccessionService
from app.germplasm.inventory import InventoryService

ACCEPT = "accept"
QUARANTINE = "quarantine"
RETURN = "return"
DECISIONS = (ACCEPT, QUARANTINE, RETURN)

# 阻断直接接收的差异码
BLOCKING_DIFFS = {
    "received_missing",
    "manifest_missing",
    "source_not_found",
    "source_missing",
    "permit_missing",
    "weight_missing",
    "weight_out_of_tolerance",
    "passport_missing",
    "accession_no_exists",
}

TOP_LEVEL_PASSPORT_FIELDS = {"scientific_name", "crop_name", "cultivar_name"}


def _dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _loads(raw: str | None, default: Any) -> Any:
    try:
        return json.loads(raw) if raw else default
    except json.JSONDecodeError:
        return default


def _item(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    data = dict(row)
    data["diff_codes"] = _loads(data.pop("diff_codes_json"), [])
    data["diff_detail"] = _loads(data.pop("diff_detail_json"), {})
    data["expected_passport"] = _loads(data.pop("expected_passport_json"), {})
    data["actual_passport"] = _loads(data.pop("actual_passport_json"), {})
    return data


class IntakeService:
    """到库批次：分批导入清单/实收、生成差异、复核决定并落地正式档案。"""

    def __init__(self, connection: sqlite3.Connection, clock: Clock | None = None) -> None:
        self.connection = connection
        self.clock = clock or SystemClock()
        self.accessions = AccessionService(connection, clock)
        self.inventory = InventoryService(connection, clock)

    # ------------------------------------------------------------------ 批次

    def create_batch(self, data: dict[str, Any]) -> dict[str, Any]:
        timestamp = to_storage(self.clock.now())
        try:
            cursor = self.connection.execute(
                "INSERT INTO intake_batches(batch_no,title,weight_tolerance_percent,tolerance_grams,"
                "required_passport_fields_json,status,created_by,created_at,updated_at) "
                "VALUES(?,?,?,?,?,'open',?,?,?)",
                (
                    data["batch_no"], data.get("title", ""), float(data["weight_tolerance_percent"]),
                    float(data["tolerance_grams"]), _dumps(data.get("required_passport_fields", [])),
                    data["actor"], timestamp, timestamp,
                ),
            )
        except sqlite3.IntegrityError as exc:
            raise ConflictError("到库批次编号已经存在") from exc
        return self.require_batch(int(cursor.lastrowid))

    def require_batch(self, batch_id: int) -> dict[str, Any]:
        row = self.connection.execute("SELECT * FROM intake_batches WHERE id=?", (batch_id,)).fetchone()
        if row is None:
            raise NotFoundError("到库批次不存在")
        batch = dict(row)
        batch["required_passport_fields"] = json.loads(batch.pop("required_passport_fields_json") or "[]")
        return batch

    def batch_detail(self, batch_id: int) -> dict[str, Any]:
        batch = self.require_batch(batch_id)
        batch["items"] = [
            _item(row) or {}
            for row in self.connection.execute(
                "SELECT * FROM intake_items WHERE batch_id=? ORDER BY id", (batch_id,)
            ).fetchall()
        ]
        batch["imports"] = [
            self._import_row(row)
            for row in self.connection.execute(
                "SELECT * FROM intake_imports WHERE batch_id=? ORDER BY id", (batch_id,)
            ).fetchall()
        ]
        batch["decisions"] = [
            self._decision_row(row)
            for row in self.connection.execute(
                "SELECT * FROM intake_decisions WHERE batch_id=? ORDER BY id", (batch_id,)
            ).fetchall()
        ]
        batch["summary"] = self.compute_summary(batch_id)
        return batch

    @staticmethod
    def _decision_row(row: sqlite3.Row) -> dict[str, Any]:
        decision = dict(row)
        decision["detail"] = _loads(decision.pop("detail_json"), {})
        return decision

    def list_batches(self, *, status: str | None, limit: int, offset: int) -> tuple[list[dict], int]:
        where = ""
        params: list[Any] = []
        if status:
            where = " WHERE status=?"
            params.append(status)
        total = int(self.connection.execute(f"SELECT COUNT(*) FROM intake_batches{where}", params).fetchone()[0])
        params.extend([limit, offset])
        rows = self.connection.execute(
            f"SELECT * FROM intake_batches{where} ORDER BY id DESC LIMIT ? OFFSET ?", params
        ).fetchall()
        items: list[dict[str, Any]] = []
        for row in rows:
            batch = dict(row)
            batch["required_passport_fields"] = json.loads(batch.pop("required_passport_fields_json") or "[]")
            items.append(batch)
        return items, total

    def _touch_batch(self, batch_id: int) -> None:
        self.connection.execute(
            "UPDATE intake_batches SET version=version+1,updated_at=? WHERE id=?",
            (to_storage(self.clock.now()), batch_id),
        )

    # ------------------------------------------------------------------ 导入

    def import_manifest(self, batch_id: int, data: dict[str, Any]) -> dict[str, Any]:
        return self._import_rows(batch_id, data, kind="manifest")

    def import_received(self, batch_id: int, data: dict[str, Any]) -> dict[str, Any]:
        return self._import_rows(batch_id, data, kind="received")

    def _import_rows(self, batch_id: int, data: dict[str, Any], *, kind: str) -> dict[str, Any]:
        batch = self.require_batch(batch_id)
        key = data["idempotency_key"]
        actor = data["actor"]
        rows = data["rows"]
        payload_hash = _dumps({"kind": kind, "rows": rows})
        previous = self.connection.execute(
            "SELECT * FROM intake_imports WHERE batch_id=? AND idempotency_key=?", (batch_id, key)
        ).fetchone()
        if previous is not None:
            if previous["payload_hash"] != payload_hash:
                raise ConflictError("同一导入幂等键不能用于不同文件")
            summary = self._import_row(previous)
            summary["replayed_count"] = int(previous["total_rows"])
            summary["replayed"] = True
            return summary

        timestamp = to_storage(self.clock.now())
        inserted = updated = 0
        rejected: list[dict[str, Any]] = []
        seen: set[str] = set()
        touched: set[int] = set()

        for index, row in enumerate(rows, start=1):
            accession_no = row["accession_no"]
            if accession_no in seen:
                rejected.append({"row": index, "accession_no": accession_no, "reason": "同一文件中资源编号重复"})
                continue
            seen.add(accession_no)
            marker = f"intake_import_{kind}_{index}"
            self.connection.execute(f"SAVEPOINT {marker}")
            try:
                existing = self.connection.execute(
                    "SELECT * FROM intake_items WHERE batch_id=? AND accession_no=?",
                    (batch_id, accession_no),
                ).fetchone()
                if existing is not None and existing["status"] in {"accepted", "quarantined"}:
                    raise ConflictError("材料已经完成复核，不能再次导入覆盖")
                if existing is None:
                    item_id = self._insert_item(batch, row, kind, timestamp)
                    inserted += 1
                else:
                    item_id = self._merge_item(batch, existing, row, kind, timestamp)
                    updated += 1
                changed = self._recompute_diff(batch, item_id, actor, timestamp, reason="")
                if changed:
                    touched.add(item_id)
                self.connection.execute(f"RELEASE SAVEPOINT {marker}")
            except Exception as exc:
                self.connection.execute(f"ROLLBACK TO SAVEPOINT {marker}")
                self.connection.execute(f"RELEASE SAVEPOINT {marker}")
                rejected.append({"row": index, "accession_no": accession_no, "reason": str(exc)})

        summary = {
            "batch_id": batch_id,
            "import_kind": kind,
            "idempotency_key": key,
            "total_rows": len(rows),
            "inserted_count": inserted,
            "updated_count": updated,
            "replayed_count": 0,
            "rejected_count": len(rejected),
            "rejected": rejected,
            "diff_changed_item_ids": sorted(touched),
        }
        cursor = self.connection.execute(
            "INSERT INTO intake_imports(batch_id,import_kind,idempotency_key,payload_hash,total_rows,"
            "inserted_count,updated_count,replayed_count,rejected_count,summary_json,imported_by,created_at) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                batch_id, kind, key, payload_hash, len(rows), inserted, updated, 0, len(rejected),
                _dumps(summary), actor, timestamp,
            ),
        )
        summary["id"] = int(cursor.lastrowid)
        summary["replayed"] = False
        if inserted or updated or rejected:
            self._touch_batch(batch_id)
            if batch["status"] == "completed":
                self.connection.execute(
                    "UPDATE intake_batches SET status='open' WHERE id=?", (batch_id,)
                )
        return summary

    def _import_row(self, row: sqlite3.Row) -> dict[str, Any]:
        summary = json.loads(row["summary_json"] or "{}")
        summary.update({
            "id": row["id"],
            "batch_id": row["batch_id"],
            "import_kind": row["import_kind"],
            "idempotency_key": row["idempotency_key"],
            "total_rows": row["total_rows"],
            "inserted_count": row["inserted_count"],
            "updated_count": row["updated_count"],
            "replayed_count": row["replayed_count"],
            "rejected_count": row["rejected_count"],
            "imported_by": row["imported_by"],
            "created_at": row["created_at"],
        })
        summary.setdefault("rejected", [])
        return summary

    def _insert_item(self, batch: dict[str, Any], row: dict[str, Any], kind: str, timestamp: str) -> int:
        if kind == "manifest":
            params = (
                batch["id"], row["accession_no"], self._source_id(row.get("source_code")), row.get("source_code"),
                row.get("scientific_name", ""), row.get("crop_name", ""), row.get("cultivar_name", ""),
                row.get("acquisition_type", "采集"), self._date(row.get("collected_on")), row.get("permit_reference"),
                _dumps(row.get("passport", {})), "{}", row.get("expected_weight_grams"), None, None,
                1, 0,
            )
        else:
            params = (
                batch["id"], row["accession_no"], self._source_id(row.get("source_code")), row.get("source_code"),
                "", "", "", "采集", None, row.get("permit_reference"),
                "{}", _dumps(row.get("passport", {})), None, row.get("received_weight_grams"),
                self._date(row["received_on"]), 0, 1,
            )
        cursor = self.connection.execute(
            "INSERT INTO intake_items(batch_id,accession_no,source_id,source_code,scientific_name,crop_name,"
            "cultivar_name,acquisition_type,collected_on,permit_reference,expected_passport_json,"
            "actual_passport_json,expected_weight_grams,received_weight_grams,received_on,has_manifest,has_received,"
            "created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (*params, timestamp, timestamp),
        )
        return int(cursor.lastrowid)

    def _merge_item(
        self, batch: dict[str, Any], existing: sqlite3.Row, row: dict[str, Any], kind: str, timestamp: str
    ) -> int:
        item_id = int(existing["id"])
        if kind == "manifest":
            source_id = self._source_id(row.get("source_code"))
            self.connection.execute(
                "UPDATE intake_items SET source_id=?,source_code=?,scientific_name=?,crop_name=?,cultivar_name=?,"
                "acquisition_type=?,collected_on=?,permit_reference=?,expected_passport_json=?,"
                "expected_weight_grams=?,has_manifest=1,status=CASE WHEN status='returned' THEN 'pending' ELSE status END,"
                "item_version=item_version+1,updated_at=? WHERE id=?",
                (
                    source_id, row.get("source_code"), row.get("scientific_name", ""), row.get("crop_name", ""),
                    row.get("cultivar_name", ""), row.get("acquisition_type", "采集"),
                    self._date(row.get("collected_on")), row.get("permit_reference"),
                    _dumps(row.get("passport", {})), row.get("expected_weight_grams"), timestamp, item_id,
                ),
            )
        else:
            source_id = self._source_id(row.get("source_code"))
            resolved_source = source_id if source_id is not None else existing["source_id"]
            resolved_code = row.get("source_code") or existing["source_code"]
            permit = row.get("permit_reference") or existing["permit_reference"]
            self.connection.execute(
                "UPDATE intake_items SET source_id=?,source_code=?,permit_reference=COALESCE(?,permit_reference),"
                "actual_passport_json=?,received_weight_grams=?,received_on=?,has_received=1,"
                "status=CASE WHEN status='returned' THEN 'pending' ELSE status END,"
                "item_version=item_version+1,updated_at=? WHERE id=?",
                (
                    resolved_source, resolved_code, permit,
                    _dumps(row.get("passport", {})), row.get("received_weight_grams"),
                    self._date(row["received_on"]), timestamp, item_id,
                ),
            )
        return item_id

    def _source_id(self, source_code: str | None) -> int | None:
        if not source_code:
            return None
        row = self.connection.execute(
            "SELECT id FROM collection_sources WHERE source_code=?", (source_code,)
        ).fetchone()
        return int(row[0]) if row else None

    @staticmethod
    def _date(value: Any) -> str | None:
        if value is None or value == "":
            return None
        return str(value)

    # ------------------------------------------------------------------ 差异

    def _recompute_diff(
        self, batch: dict[str, Any], item_id: int, actor: str, timestamp: str, *, reason: str
    ) -> bool:
        """重新计算逐项差异，差异集合变化时写入快照并递增版本。返回是否发生变化。"""
        row = self.connection.execute("SELECT * FROM intake_items WHERE id=?", (item_id,)).fetchone()
        item = _item(row)
        assert item is not None
        codes: list[str] = []
        detail: dict[str, Any] = {}

        if not item["has_manifest"] and item["has_received"]:
            codes.append("manifest_missing")
        if item["has_manifest"] and not item["has_received"]:
            codes.append("received_missing")

        source_id = item["source_id"]
        if not source_id and item["source_code"]:
            codes.append("source_not_found")
            detail["source_code"] = item["source_code"]
        elif not source_id:
            codes.append("source_missing")
        else:
            source = self.connection.execute(
                "SELECT * FROM collection_sources WHERE id=?", (source_id,)
            ).fetchone()
            if source is not None and not item.get("permit_reference") and not source["permit_reference"]:
                if item["acquisition_type"] == "采集":
                    codes.append("permit_missing")

        existing = None
        if item["accession_id"] is None:
            existing = self.connection.execute(
                "SELECT id FROM accessions WHERE accession_no=?", (item["accession_no"],)
            ).fetchone()
            if existing is not None:
                codes.append("accession_no_exists")
                detail["existing_accession_id"] = int(existing[0])

        expected = item["expected_weight_grams"]
        received = item["received_weight_grams"]
        if item["has_manifest"] and item["has_received"]:
            if expected is None or received is None:
                codes.append("weight_missing")
            else:
                tolerance = max(float(batch["tolerance_grams"]), float(expected) * float(batch["weight_tolerance_percent"]) / 100)
                delta = round(float(received) - float(expected), 6)
                detail["expected_weight_grams"] = expected
                detail["received_weight_grams"] = received
                detail["delta_grams"] = delta
                detail["tolerance_grams"] = round(tolerance, 6)
                if abs(delta) > tolerance + 1e-9:
                    codes.append("weight_out_of_tolerance")

        passport = dict(item.get("expected_passport", {}))
        passport.update(item.get("actual_passport", {}))
        missing_fields: list[str] = []
        for field in batch.get("required_passport_fields", []):
            if field in TOP_LEVEL_PASSPORT_FIELDS:
                if not item.get(field):
                    missing_fields.append(field)
            elif passport.get(field) in (None, ""):
                missing_fields.append(field)
        if missing_fields:
            codes.append("passport_missing")
            detail["missing_fields"] = missing_fields

        previous_codes = json.loads(row["diff_codes_json"] or "[]")
        self.connection.execute(
            "UPDATE intake_items SET diff_codes_json=?,diff_detail_json=?,updated_at=? WHERE id=?",
            (_dumps(codes), _dumps(detail), timestamp, item_id),
        )
        if codes != previous_codes:
            self.connection.execute(
                "INSERT INTO intake_item_snapshots(item_id,diff_codes_json,diff_detail_json,item_version,reason,"
                "created_by,created_at) VALUES(?,?,?,?,?,?,?)",
                (item_id, _dumps(codes), _dumps(detail), item["item_version"], reason, actor, timestamp),
            )
            self.connection.execute(
                "UPDATE intake_items SET item_version=item_version+1,updated_at=? WHERE id=?",
                (timestamp, item_id),
            )
            return True
        return False

    # ------------------------------------------------------------------ 决定

    def decide_item(self, item_id: int, data: dict[str, Any]) -> dict[str, Any]:
        row = self.connection.execute("SELECT * FROM intake_items WHERE id=?", (item_id,)).fetchone()
        if row is None:
            raise NotFoundError("到库材料不存在")
        item = _item(row)
        assert item is not None
        batch = self.require_batch(int(row["batch_id"]))
        decision = data["decision"]
        if decision == RETURN and not data.get("reason", "").strip():
            raise ValidationError("退回材料必须填写决定理由")
        if int(row["item_version"]) != int(data["expected_version"]):
            raise ConflictError("材料版本冲突，已刷新到最新差异", context={"current_version": row["item_version"]})
        if row["status"] != "pending":
            raise ConflictError("该材料已经完成复核", context={"current_status": row["status"]})

        timestamp = to_storage(self.clock.now())
        applied = self._apply_decision(batch, item, decision, data.get("reason", ""), data["actor"], timestamp)
        self._log_decision(
            batch["id"], item_id, decision, "item", data.get("reason", ""), data["expected_version"],
            "applied", int(row["item_version"]), int(row["item_version"]) + 1,
            {"accession_id": applied.get("accession_id"), "seed_lot_id": applied.get("seed_lot_id")},
            data["actor"],
        )
        self._touch_batch(batch["id"])
        self._maybe_complete_batch(batch["id"], timestamp)
        return self.require_item(item_id)

    def decide_batch(self, batch_id: int, data: dict[str, Any]) -> dict[str, Any]:
        batch = self.require_batch(batch_id)
        if int(batch["version"]) != int(data["expected_version"]):
            raise ConflictError(
                "批次版本冲突，可能已有人完成部分复核或有新导入",
                context={"current_version": batch["version"]},
            )
        decision = data["decision"]
        reason = data.get("reason", "")
        if decision == RETURN and not reason.strip():
            raise ValidationError("批量退回必须填写决定理由")
        actor = data["actor"]
        timestamp = to_storage(self.clock.now())
        rows = self.connection.execute(
            "SELECT * FROM intake_items WHERE batch_id=? ORDER BY id", (batch_id,)
        ).fetchall()

        results: list[dict[str, Any]] = []
        applied_count = skipped_count = invalid_count = 0
        for row in rows:
            item = _item(row)
            assert item is not None
            detail: dict[str, Any] = {}
            if row["status"] != "pending":
                result = "already_decided"
                skipped_count += 1
            elif decision == ACCEPT and set(item["diff_codes"]) & BLOCKING_DIFFS:
                result = "invalid"
                invalid_count += 1
                detail = {"diff_codes": item["diff_codes"]}
            else:
                marker = f"intake_batch_decision_{row['id']}"
                self.connection.execute(f"SAVEPOINT {marker}")
                try:
                    applied = self._apply_decision(batch, item, decision, reason, actor, timestamp)
                    self.connection.execute(f"RELEASE SAVEPOINT {marker}")
                    result = "applied"
                    applied_count += 1
                    detail = {
                        "accession_id": applied.get("accession_id"),
                        "seed_lot_id": applied.get("seed_lot_id"),
                        "hold_id": applied.get("hold_id"),
                    }
                except Exception as exc:
                    self.connection.execute(f"ROLLBACK TO SAVEPOINT {marker}")
                    self.connection.execute(f"RELEASE SAVEPOINT {marker}")
                    result = "invalid"
                    invalid_count += 1
                    detail = {"error": str(exc)}
            self._log_decision(
                batch_id, int(row["id"]), decision, "batch", reason, data["expected_version"], result,
                int(row["item_version"]), int(row["item_version"]) + (1 if result == "applied" else 0),
                detail, actor,
            )
            results.append({"item_id": int(row["id"]), "accession_no": row["accession_no"], "result": result})

        self._touch_batch(batch_id)
        self._maybe_complete_batch(batch_id, timestamp)
        summary = self.compute_summary(batch_id)
        summary.update({
            "decision": decision,
            "applied_count": applied_count,
            "skipped_decided_count": skipped_count,
            "invalid_count": invalid_count,
            "results": results,
        })
        return summary

    def _apply_decision(
        self,
        batch: dict[str, Any],
        item: dict[str, Any],
        decision: str,
        reason: str,
        actor: str,
        timestamp: str,
    ) -> dict[str, Any]:
        item_id = int(item["id"])
        if decision == ACCEPT and set(item["diff_codes"]) & BLOCKING_DIFFS:
            raise ValidationError(
                "存在未解决差异的材料不能直接接收，请先修正、隔离或退回",
                context={"diff_codes": item["diff_codes"]},
            )
        if decision in {ACCEPT, QUARANTINE}:
            problems: list[str] = []
            if not item.get("source_id"):
                problems.append("来源未登记")
            if not item.get("scientific_name"):
                problems.append("缺少学名")
            if not item.get("crop_name"):
                problems.append("缺少作物名称")
            if not item.get("received_weight_grams"):
                problems.append("缺少实收重量")
            if not item.get("received_on"):
                problems.append("缺少到库日期")
            if problems:
                raise ValidationError("材料不具备建立正式档案的条件", context={"problems": problems})
        accession_id: int | None = None
        lot_id: int | None = None
        hold_id: int | None = None

        if decision in {ACCEPT, QUARANTINE}:
            passport = dict(item.get("expected_passport", {}))
            passport.update(item.get("actual_passport", {}))
            accession = self.accessions.create_accession({
                "accession_no": item["accession_no"],
                "scientific_name": item["scientific_name"],
                "crop_name": item["crop_name"],
                "cultivar_name": item.get("cultivar_name", ""),
                "source_id": item["source_id"],
                "acquisition_type": item["acquisition_type"],
                "received_on": item["received_on"],
                "passport": passport,
                "created_by": actor,
            })
            accession_id = int(accession["id"])
            target = "accepted" if decision == ACCEPT else "quarantine"
            self.accessions.transition(accession_id, {
                "target_status": target,
                "reason": reason or ("到库批量接收" if decision == ACCEPT else "到库材料需要隔离"),
                "expected_version": 1,
                "actor": actor,
            })
            year_source = item.get("collected_on") or item.get("received_on") or timestamp[:4]
            lot = self.inventory.create_lot({
                "lot_no": f"IL-{batch['batch_no']}-{item_id}"[:60],
                "accession_id": accession_id,
                "parent_lot_id": None,
                "harvest_year": int(str(year_source)[:4]),
                "initial_weight_grams": float(item["received_weight_grams"]),
                "moisture_percent": None,
                "treatment": "到库接收",
                "sealed_on": item.get("received_on"),
                "created_by": actor,
            })
            lot_id = int(lot["id"])
            if decision == QUARANTINE:
                hold = self.inventory.impose_hold({
                    "lot_id": lot_id,
                    "hold_type": "检疫",
                    "reason": reason or "到库材料存在差异，先隔离检疫",
                    "actor": actor,
                })
                hold_id = int(hold["id"])

        new_status = {"accept": "accepted", "quarantine": "quarantined", "return": "returned"}[decision]
        updated = self.connection.execute(
            "UPDATE intake_items SET status=?,decided_by=?,decided_at=?,decision_reason=?,"
            "accession_id=COALESCE(accession_id,?),seed_lot_id=COALESCE(seed_lot_id,?),"
            "item_version=item_version+1,updated_at=? WHERE id=? AND status='pending' AND item_version=?",
            (new_status, actor, timestamp, reason, accession_id, lot_id, timestamp, item_id, item["item_version"]),
        )
        if updated.rowcount != 1:
            raise ConflictError("材料刚被其他人完成复核，本次决定未覆盖")
        return {"accession_id": accession_id, "seed_lot_id": lot_id, "hold_id": hold_id}

    def _log_decision(
        self,
        batch_id: int,
        item_id: int | None,
        decision: str,
        scope: str,
        reason: str,
        expected_version: int | None,
        result: str,
        before_version: int | None,
        after_version: int | None,
        detail: dict[str, Any],
        actor: str,
    ) -> None:
        self.connection.execute(
            "INSERT INTO intake_decisions(batch_id,item_id,decision,scope,reason,expected_version,result,"
            "before_version,after_version,detail_json,decided_by,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                batch_id, item_id, decision, scope, reason, expected_version, result, before_version,
                after_version, _dumps(detail), actor, to_storage(self.clock.now()),
            ),
        )

    def _maybe_complete_batch(self, batch_id: int, timestamp: str) -> None:
        pending = int(self.connection.execute(
            "SELECT COUNT(*) FROM intake_items WHERE batch_id=? AND status='pending'", (batch_id,)
        ).fetchone()[0])
        if pending == 0:
            self.connection.execute(
                "UPDATE intake_batches SET status='completed',updated_at=? WHERE id=? AND status='open'",
                (timestamp, batch_id),
            )
        else:
            self.connection.execute(
                "UPDATE intake_batches SET status='open',updated_at=? WHERE id=? AND status='completed'",
                (timestamp, batch_id),
            )

    # ------------------------------------------------------------------ 修正

    def correct_item(self, item_id: int, data: dict[str, Any]) -> dict[str, Any]:
        row = self.connection.execute("SELECT * FROM intake_items WHERE id=?", (item_id,)).fetchone()
        if row is None:
            raise NotFoundError("到库材料不存在")
        if int(row["item_version"]) != int(data["expected_version"]):
            raise ConflictError("材料版本冲突，请刷新后再修正", context={"current_version": row["item_version"]})
        if row["status"] != "returned":
            raise ConflictError("只有已退回的材料可以修正后继续原批次", context={"current_status": row["status"]})

        timestamp = to_storage(self.clock.now())
        columns: list[str] = []
        params: list[Any] = []
        field_map = {
            "accession_no": "accession_no",
            "scientific_name": "scientific_name",
            "crop_name": "crop_name",
            "cultivar_name": "cultivar_name",
            "acquisition_type": "acquisition_type",
            "collected_on": "collected_on",
            "permit_reference": "permit_reference",
            "expected_weight_grams": "expected_weight_grams",
            "received_weight_grams": "received_weight_grams",
            "received_on": "received_on",
        }
        for key, column in field_map.items():
            if data.get(key) is not None:
                columns.append(f"{column}=?")
                params.append(self._date(data[key]) if column in {"collected_on", "received_on"} else data[key])
        if data.get("source_code") is not None:
            source = self._source_id(data["source_code"])
            columns.extend(["source_code=?", "source_id=?"])
            params.extend([data["source_code"], source])
        if data.get("expected_passport") is not None:
            columns.append("expected_passport_json=?")
            params.append(_dumps(data["expected_passport"]))
        if data.get("actual_passport") is not None:
            columns.append("actual_passport_json=?")
            params.append(_dumps(data["actual_passport"]))
        if not columns:
            raise ValidationError("修正请求至少要包含一个需要修改的字段")
        columns.append("status='pending'")
        params.extend([timestamp, item_id])
        try:
            self.connection.execute(
                f"UPDATE intake_items SET {','.join(columns)},item_version=item_version+1,updated_at=? WHERE id=?",
                params,
            )
        except sqlite3.IntegrityError as exc:
            raise ConflictError("修正后的资源编号与批次内其他材料重复") from exc
        batch = self.require_batch(int(row["batch_id"]))
        self._recompute_diff(batch, item_id, data["actor"], timestamp, reason=data.get("reason", ""))
        self.connection.execute(
            "UPDATE intake_batches SET status='open',version=version+1,updated_at=? WHERE id=?",
            (timestamp, batch["id"]),
        )
        return self.require_item(item_id)

    def require_item(self, item_id: int) -> dict[str, Any]:
        item = _item(self.connection.execute("SELECT * FROM intake_items WHERE id=?", (item_id,)).fetchone())
        if item is None:
            raise NotFoundError("到库材料不存在")
        item["snapshots"] = [
            dict(snapshot)
            for snapshot in self.connection.execute(
                "SELECT id,diff_codes_json,diff_detail_json,item_version,reason,created_by,created_at "
                "FROM intake_item_snapshots WHERE item_id=? ORDER BY id", (item_id,)
            ).fetchall()
        ]
        for snapshot in item["snapshots"]:
            snapshot["diff_codes"] = json.loads(snapshot.pop("diff_codes_json") or "[]")
            snapshot["diff_detail"] = json.loads(snapshot.pop("diff_detail_json") or "{}")
        return item

    # ------------------------------------------------------------------ 对账

    def compute_summary(self, batch_id: int) -> dict[str, Any]:
        batch = self.require_batch(batch_id)
        rows = self.connection.execute(
            "SELECT * FROM intake_items WHERE batch_id=? ORDER BY id", (batch_id,)
        ).fetchall()
        counts = {"pending": 0, "accepted": 0, "quarantined": 0, "returned": 0}
        with_diffs = 0
        linkage: list[dict[str, Any]] = []
        created_accession_ids: list[int] = []
        created_lot_ids: list[int] = []
        hold_ids: list[int] = []

        for row in rows:
            counts[row["status"]] += 1
            codes = json.loads(row["diff_codes_json"] or "[]")
            if codes:
                with_diffs += 1
            entry: dict[str, Any] = {
                "item_id": row["id"],
                "accession_no": row["accession_no"],
                "status": row["status"],
                "diff_codes": codes,
                "accession_id": row["accession_id"],
                "seed_lot_id": row["seed_lot_id"],
                "issues": [],
            }
            if row["status"] in {"accepted", "quarantined"}:
                accession = self.connection.execute(
                    "SELECT * FROM accessions WHERE id=?", (row["accession_id"],)
                ).fetchone()
                lot = self.connection.execute(
                    "SELECT * FROM seed_lots WHERE id=?", (row["seed_lot_id"],)
                ).fetchone()
                if accession is None:
                    entry["issues"].append("正式资源缺失")
                else:
                    created_accession_ids.append(int(accession["id"]))
                    expected_status = "accepted" if row["status"] == "accepted" else "quarantine"
                    if accession["status"] != expected_status:
                        entry["issues"].append(f"资源状态为 {accession['status']}，应为 {expected_status}")
                if lot is None:
                    entry["issues"].append("种子批次缺失")
                else:
                    created_lot_ids.append(int(lot["id"]))
                    if abs(float(lot["initial_weight_grams"]) - float(row["received_weight_grams"] or 0)) > 1e-6:
                        entry["issues"].append("种子批次重量与实收重量不一致")
                if row["status"] == "quarantined":
                    holds = self.connection.execute(
                        "SELECT * FROM lot_holds WHERE lot_id=? AND released_at IS NULL", (row["seed_lot_id"],)
                    ).fetchall()
                    if not holds:
                        entry["issues"].append("缺少未解除的隔离冻结记录")
                    else:
                        entry["hold_ids"] = [int(hold["id"]) for hold in holds]
                        hold_ids.extend(entry["hold_ids"])
            if row["status"] == "returned" and (row["accession_id"] is not None or row["seed_lot_id"] is not None):
                entry["issues"].append("退回材料不应关联正式档案")
            entry["matches"] = not entry["issues"]
            linkage.append(entry)

        decided_total = counts["accepted"] + counts["quarantined"] + counts["returned"]
        return {
            "batch_id": batch_id,
            "batch_no": batch["batch_no"],
            "batch_status": batch["status"],
            "batch_version": batch["version"],
            "total_items": len(rows),
            "pending_count": counts["pending"],
            "accepted_count": counts["accepted"],
            "quarantined_count": counts["quarantined"],
            "returned_count": counts["returned"],
            "decided_count": decided_total,
            "items_with_diffs": with_diffs,
            "created_accession_ids": created_accession_ids,
            "created_seed_lot_ids": created_lot_ids,
            "active_hold_ids": hold_ids,
            "linkage": linkage,
            "fully_linked": all(item["matches"] for item in linkage),
        }
