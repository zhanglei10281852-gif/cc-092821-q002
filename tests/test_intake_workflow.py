from __future__ import annotations

import pytest

from app.core.errors import ConflictError, ValidationError
from app.database import transaction
from app.germplasm.service import GermplasmService


def create_intake_batch(service: GermplasmService, suffix: str = "001") -> tuple[dict, dict]:
    source = service.accessions.create_source({
        "source_code": f"SRC-IN-{suffix}", "provider_name": "合作单位", "country_code": "CN",
        "locality": "山地采集点", "collected_on": "2026-09-01", "permit_reference": "PERMIT-1",
        "restrictions": {},
    })
    batch = service.intake.create_batch({
        "batch_no": f"INB-{suffix}", "source_code": f"SRC-IN-{suffix}",
        "acquisition_type": "采集", "harvest_year": 2026,
        "weight_tolerance_percent": 5.0, "required_passport_fields": ["collector"],
        "note": "本周到库", "created_by": "登记员",
    })
    return source, batch


def manifest_rows() -> list[dict]:
    base = {"scientific_name": "Oryza sativa", "crop_name": "水稻", "cultivar_name": "地方材料"}
    return [
        {**base, "accession_no": "IN-001", "expected_weight_grams": 100,
         "permit_reference": "PERMIT-1", "passport": {"collector": "张三"}},
        {**base, "accession_no": "IN-002", "expected_weight_grams": 100,
         "permit_reference": "PERMIT-1", "passport": {"collector": "张三"}},
        {**base, "accession_no": "IN-003", "expected_weight_grams": 100, "passport": {}},
        {**base, "accession_no": "IN-004", "expected_weight_grams": 100, "source_code": "OTHER-SRC",
         "permit_reference": "PERMIT-1", "passport": {"collector": "李四"}},
    ]


def receipt_rows() -> list[dict]:
    return [
        {"accession_no": "IN-001", "received_weight_grams": 102},
        {"accession_no": "IN-002", "received_weight_grams": 120},
        {"accession_no": "IN-003", "received_weight_grams": 100},
        {"accession_no": "IN-009", "received_weight_grams": 50},
    ]


def import_both(service: GermplasmService, batch: dict) -> dict:
    service.intake.import_manifest(batch["id"], manifest_rows(), "登记员")
    service.intake.import_receipts(batch["id"], receipt_rows(), "登记员")
    return service.intake.batch_detail(batch["id"])


def items_by_no(batch_detail: dict) -> dict[str, dict]:
    return {item["accession_no"]: item for item in batch_detail["items"]}


def test_import_generates_discrepancies_and_suggestions(client):
    with transaction(immediate=True) as connection:
        service = GermplasmService(connection)
        _, batch = create_intake_batch(service)
        detail = import_both(service, batch)
        items = items_by_no(detail)
        assert len(items) == 5
        assert items["IN-001"]["discrepancies"] == []
        assert items["IN-001"]["suggested_action"] == "accept"
        assert items["IN-002"]["discrepancies"] == ["weight_out_of_tolerance"]
        assert items["IN-002"]["suggested_action"] == "quarantine"
        assert items["IN-003"]["discrepancies"] == ["permit_missing", "passport_missing:collector"]
        assert items["IN-003"]["suggested_action"] == "quarantine"
        assert items["IN-004"]["discrepancies"] == ["source_mismatch", "receipt_missing"]
        assert items["IN-004"]["suggested_action"] == "return"
        assert items["IN-009"]["discrepancies"] == ["manifest_missing"]
        assert items["IN-009"]["suggested_action"] == "quarantine"


def test_reimport_is_idempotent_and_never_duplicates(client):
    with transaction(immediate=True) as connection:
        service = GermplasmService(connection)
        _, batch = create_intake_batch(service)
        first = service.intake.import_manifest(batch["id"], manifest_rows(), "登记员")
        assert first["replayed"] is False and first["applied_count"] == 4
        second = service.intake.import_manifest(batch["id"], manifest_rows(), "登记员")
        assert second["replayed"] is True
        assert second["import_id"] == first["import_id"]
        service.intake.import_receipts(batch["id"], receipt_rows(), "登记员")
        replay = service.intake.import_receipts(batch["id"], receipt_rows(), "登记员")
        assert replay["replayed"] is True
        detail = service.intake.batch_detail(batch["id"])
        assert len(detail["items"]) == 5
        assert len(detail["imports"]) == 2
        # 部分重叠的新文件只更新既有明细，不复制档案
        partial = service.intake.import_manifest(batch["id"], manifest_rows()[:1], "登记员")
        assert partial["replayed"] is False and partial["applied_count"] == 1
        assert len(service.intake.batch_detail(batch["id"])["items"]) == 5


def test_import_rejects_bad_rows_but_keeps_batch_resumable(client):
    with transaction(immediate=True) as connection:
        service = GermplasmService(connection)
        _, batch = create_intake_batch(service)
        result = service.intake.import_manifest(batch["id"], [
            {"accession_no": "IN-101", "expected_weight_grams": 10},
            {"accession_no": "", "expected_weight_grams": 10},
            {"accession_no": "IN-102", "expected_weight_grams": -3},
        ], "登记员")
        assert result["applied_count"] == 1
        assert len(result["rejected"]) == 2
        detail = service.intake.batch_detail(batch["id"])
        assert [item["accession_no"] for item in detail["items"]] == ["IN-101"]


def test_decisions_create_formal_records_and_summary_reconciles(client):
    with transaction(immediate=True) as connection:
        service = GermplasmService(connection)
        _, batch = create_intake_batch(service)
        detail = import_both(service, batch)
        items = items_by_no(detail)
        accepted = service.intake.decide_item(items["IN-001"]["id"], {
            "decision": "accept", "reason": "完全一致", "expected_version": items["IN-001"]["version"],
            "actor": "复核员甲",
        })
        assert accepted["accession_id"] and accepted["lot_id"] and accepted["hold_id"] is None
        quarantined = service.intake.decide_item(items["IN-002"]["id"], {
            "decision": "quarantine", "reason": "重量偏差超出容差，待复称", "expected_version": items["IN-002"]["version"],
            "actor": "复核员甲",
        })
        assert quarantined["hold_id"] is not None
        returned = service.intake.decide_item(items["IN-004"]["id"], {
            "decision": "return", "reason": "来源编码与批次不符，退回合作单位",
            "expected_version": items["IN-004"]["version"], "actor": "复核员乙",
        })
        assert returned["accession_id"] is None and returned["lot_id"] is None
        accession = service.repository.require_accession(accepted["accession_id"])
        assert accession["accession_no"] == "IN-001" and accession["status"] == "accepted"
        lot = service.repository.require_lot(accepted["lot_id"])
        assert lot["initial_weight_grams"] == 102 and lot["accession_id"] == accepted["accession_id"]
        held_accession = service.repository.require_accession(quarantined["accession_id"])
        assert held_accession["status"] == "quarantine"
        holds = service.repository.active_holds(quarantined["lot_id"])
        assert [hold["id"] for hold in holds] == [quarantined["hold_id"]]
        assert holds[0]["hold_type"] == "检疫"
        summary = service.intake.batch_summary(batch["id"])
        assert summary["counts"] == {"total": 5, "pending": 2, "accepted": 1, "quarantined": 1, "returned": 1}
        assert summary["reconciled"] is True
        by_no = {item["accession_no"]: item for item in summary["items"]}
        assert by_no["IN-001"]["reconciliation"]["consistent"] is True
        assert by_no["IN-001"]["reconciliation"]["accession_linked"] is True
        assert by_no["IN-002"]["reconciliation"]["hold_linked"] is True
        assert by_no["IN-002"]["reconciliation"]["hold_active"] is True
        assert by_no["IN-004"]["reconciliation"]["consistent"] is True
        # 事件流保留操作者、理由与差异快照
        events = service.intake.item_detail(items["IN-002"]["id"])["events"]
        decided = [event for event in events if event["event_type"] == "decided"]
        assert decided[0]["actor"] == "复核员甲"
        assert decided[0]["detail"]["reason"] == "重量偏差超出容差，待复称"
        assert decided[0]["detail"]["discrepancies"] == ["weight_out_of_tolerance"]


def test_batch_decision_applies_and_conflict_never_overwrites(client):
    with transaction(immediate=True) as connection:
        service = GermplasmService(connection)
        _, batch = create_intake_batch(service, "002")
        detail = import_both(service, batch)
        items = items_by_no(detail)
        # 复核员甲先单独接收 IN-001
        service.intake.decide_item(items["IN-001"]["id"], {
            "decision": "accept", "reason": "", "expected_version": items["IN-001"]["version"],
            "actor": "复核员甲",
        })
        # 复核员乙拿着旧版本号发起整批接收：任何一项冲突都不写入
        with pytest.raises(ConflictError):
            service.intake.decide_batch(batch["id"], {
                "decision": "accept", "reason": "整批接收", "actor": "复核员乙",
                "items": [
                    {"item_id": items["IN-001"]["id"], "expected_version": items["IN-001"]["version"]},
                    {"item_id": items["IN-009"]["id"], "expected_version": items["IN-009"]["version"]},
                ],
            })
        after = items_by_no(service.intake.batch_detail(batch["id"]))
        assert after["IN-001"]["decision"] == "accept"
        assert after["IN-001"]["decided_by"] == "复核员甲"
        assert after["IN-009"]["decision"] is None
        assert service.repository.accession_by_number("IN-009") is None
        # 已复核明细即便版本号被猜到也不能被批量决定覆盖
        with pytest.raises(ConflictError):
            service.intake.decide_batch(batch["id"], {
                "decision": "return", "reason": "整批退回", "actor": "复核员乙",
                "items": [{"item_id": after["IN-001"]["id"], "expected_version": after["IN-001"]["version"]}],
            })
        assert items_by_no(service.intake.batch_detail(batch["id"]))["IN-001"]["decision"] == "accept"
        # 版本一致的批量决定对剩余明细生效（IN-009 只有实收没有清单，无法登记，需退回补正）
        pending = [after["IN-002"], after["IN-003"]]
        result = service.intake.decide_batch(batch["id"], {
            "decision": "quarantine", "reason": "等待补充材料", "actor": "复核员乙",
            "items": [{"item_id": item["id"], "expected_version": item["version"]} for item in pending],
        })
        assert result["applied_count"] == len(pending)
        summary = service.intake.batch_summary(batch["id"])
        assert summary["counts"]["quarantined"] == len(pending)
        # 无清单无学名的实收材料不能隔离登记，只能退回
        lone_receipt = items_by_no(service.intake.batch_detail(batch["id"]))["IN-009"]
        with pytest.raises(ValidationError):
            service.intake.decide_item(lone_receipt["id"], {
                "decision": "quarantine", "reason": "先隔离", "expected_version": lone_receipt["version"],
                "actor": "复核员乙",
            })


def test_correction_of_returned_item_continues_same_batch(client):
    with transaction(immediate=True) as connection:
        service = GermplasmService(connection)
        _, batch = create_intake_batch(service, "003")
        detail = import_both(service, batch)
        items = items_by_no(detail)
        target = items["IN-003"]
        assert "permit_missing" in target["discrepancies"]
        returned = service.intake.decide_item(target["id"], {
            "decision": "return", "reason": "缺少采集许可与采集人，退回补正",
            "expected_version": target["version"], "actor": "复核员甲",
        })
        corrected = service.intake.correct_item(returned["id"], {
            "permit_reference": "PERMIT-1", "passport": {"collector": "王五"},
            "expected_version": returned["version"], "actor": "登记员",
        })
        assert corrected["decision"] is None
        assert corrected["discrepancies"] == []
        assert corrected["suggested_action"] == "accept"
        accepted = service.intake.decide_item(corrected["id"], {
            "decision": "accept", "reason": "补正后一致", "expected_version": corrected["version"],
            "actor": "复核员甲",
        })
        assert accepted["accession_id"] is not None
        summary = service.intake.batch_summary(batch["id"])
        assert summary["items"][2]["reconciliation"]["consistent"] is True
        # 未退回的明细不允许走修正通道
        pending = items_by_no(service.intake.batch_detail(batch["id"]))["IN-001"]
        with pytest.raises(ConflictError):
            service.intake.correct_item(pending["id"], {
                "crop_name": "稻", "expected_version": pending["version"], "actor": "登记员",
            })


def test_correction_of_receipt_only_item_supplies_manifest(client):
    with transaction(immediate=True) as connection:
        service = GermplasmService(connection)
        _, batch = create_intake_batch(service, "007")
        detail = import_both(service, batch)
        items = items_by_no(detail)
        lone = items["IN-009"]
        assert lone["discrepancies"] == ["manifest_missing"]
        returned = service.intake.decide_item(lone["id"], {
            "decision": "return", "reason": "实收无清单，退回补登", "expected_version": lone["version"],
            "actor": "复核员甲",
        })
        corrected = service.intake.correct_item(returned["id"], {
            "scientific_name": "Oryza sativa", "crop_name": "水稻",
            "expected_weight_grams": 50, "permit_reference": "PERMIT-1",
            "passport": {"collector": "张三"}, "expected_version": returned["version"], "actor": "登记员",
        })
        assert corrected["decision"] is None
        assert corrected["discrepancies"] == []
        assert corrected["suggested_action"] == "accept"
        accepted = service.intake.decide_item(corrected["id"], {
            "decision": "accept", "reason": "清单补齐", "expected_version": corrected["version"],
            "actor": "复核员甲",
        })
        assert accepted["accession_id"] is not None
        summary = service.intake.batch_summary(batch["id"])
        target = [item for item in summary["items"] if item["accession_no"] == "IN-009"][0]
        assert target["reconciliation"]["consistent"] is True


def test_close_requires_all_items_decided_and_blocks_further_writes(client):
    with transaction(immediate=True) as connection:
        service = GermplasmService(connection)
        _, batch = create_intake_batch(service, "004")
        detail = import_both(service, batch)
        with pytest.raises(ConflictError):
            service.intake.close_batch(batch["id"], {"expected_version": 1, "actor": "复核员甲"})
        items = items_by_no(detail)
        for item in items.values():
            service.intake.decide_item(item["id"], {
                "decision": "return", "reason": "整批退回重验", "expected_version": item["version"],
                "actor": "复核员甲",
            })
        current = service.repository.require_intake_batch(batch["id"])
        with pytest.raises(ConflictError):
            service.intake.close_batch(batch["id"], {"expected_version": 1, "actor": "复核员甲"})
        closed = service.intake.close_batch(batch["id"], {
            "expected_version": current["version"], "actor": "复核员甲",
        })
        assert closed["status"] == "completed"
        with pytest.raises(ConflictError):
            service.intake.import_manifest(batch["id"], manifest_rows(), "登记员")
        with pytest.raises(ConflictError):
            service.intake.decide_item(items["IN-001"]["id"], {
                "decision": "accept", "reason": "", "expected_version": 99, "actor": "复核员甲",
            })


def test_accepting_duplicate_accession_number_is_blocked(client):
    with transaction(immediate=True) as connection:
        service = GermplasmService(connection)
        source, batch = create_intake_batch(service, "005")
        service.accessions.create_accession({
            "accession_no": "IN-001", "scientific_name": "Oryza sativa", "crop_name": "水稻",
            "cultivar_name": "", "source_id": source["id"], "acquisition_type": "采集",
            "received_on": "2026-09-10", "passport": {}, "created_by": "登记员",
        })
        detail = import_both(service, batch)
        items = items_by_no(detail)
        assert "accession_exists" in items["IN-001"]["discrepancies"]
        assert items["IN-001"]["suggested_action"] == "return"
        with pytest.raises(ConflictError):
            service.intake.decide_item(items["IN-001"]["id"], {
                "decision": "accept", "reason": "", "expected_version": items["IN-001"]["version"],
                "actor": "复核员甲",
            })


def test_decision_requires_reason_and_valid_weight(client):
    with transaction(immediate=True) as connection:
        service = GermplasmService(connection)
        _, batch = create_intake_batch(service, "006")
        detail = import_both(service, batch)
        items = items_by_no(detail)
        with pytest.raises(ValidationError):
            service.intake.decide_item(items["IN-001"]["id"], {
                "decision": "quarantine", "reason": "", "expected_version": items["IN-001"]["version"],
                "actor": "复核员甲",
            })
        # 清单与实收都缺少重量时不能接收或隔离，必须先退回补正
        service.intake.import_manifest(batch["id"], [{
            "accession_no": "IN-020", "scientific_name": "Oryza sativa", "crop_name": "水稻",
            "permit_reference": "PERMIT-1", "passport": {"collector": "张三"},
        }], "登记员")
        weightless = items_by_no(service.intake.batch_detail(batch["id"]))["IN-020"]
        with pytest.raises(ValidationError):
            service.intake.decide_item(weightless["id"], {
                "decision": "accept", "reason": "", "expected_version": weightless["version"],
                "actor": "复核员甲",
            })
        # IN-004 只有清单没有实收，接收前必须能确定重量；清单重量可用时允许接收
        weightless = items["IN-004"]
        accepted = service.intake.decide_item(weightless["id"], {
            "decision": "accept", "reason": "按清单重量先行接收",
            "expected_version": weightless["version"], "actor": "复核员甲",
        })
        lot = service.repository.require_lot(accepted["lot_id"])
        assert lot["initial_weight_grams"] == 100
