from __future__ import annotations

import threading

import pytest

from app.core.errors import ConflictError, ValidationError
from app.database import get_connection, transaction
from app.germplasm.intake_schemas import (
    BatchDecision,
    IntakeBatchCreate,
    ItemCorrection,
    ItemDecision,
    ManifestImport,
    ReceivedImport,
)
from app.germplasm.service import GermplasmService


def make_source(service: GermplasmService, code: str, *, permit: str | None = "P-001") -> dict:
    return service.accessions.create_source({
        "source_code": code, "provider_name": "合作采集队", "country_code": "CN",
        "locality": "河谷试验站", "collected_on": "2026-08-01", "permit_reference": permit,
        "restrictions": {},
    })


def make_batch(service: GermplasmService, batch_no: str = "IB-001", **options) -> dict:
    payload = {
        "batch_no": batch_no, "title": "本周到库", "weight_tolerance_percent": 10,
        "tolerance_grams": 5, "required_passport_fields": ["scientific_name", "crop_name", "origin"],
        "actor": "登记员",
    }
    payload.update(options)
    return service.intake.create_batch(IntakeBatchCreate(**payload).model_dump(mode="json"))


def manifest_payload(*rows: dict, key: str = "manifest-001", actor: str = "登记员") -> dict:
    return ManifestImport(idempotency_key=key, rows=rows, actor=actor).model_dump(mode="json")


def received_payload(*rows: dict, key: str = "received-001", actor: str = "登记员") -> dict:
    return ReceivedImport(idempotency_key=key, rows=rows, actor=actor).model_dump(mode="json")


def base_row(accession_no: str, **overrides) -> dict:
    row = {
        "accession_no": accession_no, "source_code": "SRC-1",
        "scientific_name": "Oryza sativa", "crop_name": "水稻", "cultivar_name": "地方材料",
        "acquisition_type": "采集", "collected_on": "2026-08-01", "permit_reference": "P-001",
        "expected_weight_grams": 100, "passport": {"origin": "河谷试验站"},
    }
    row.update(overrides)
    return row


def recv_row(accession_no: str, weight: float, **overrides) -> dict:
    row = {
        "accession_no": accession_no, "received_weight_grams": weight, "received_on": "2026-09-20",
        "source_code": "SRC-1", "permit_reference": "P-001", "passport": {"origin": "河谷试验站"},
    }
    row.update(overrides)
    return row


def test_import_generates_item_level_diffs_and_is_idempotent(client):
    with transaction(immediate=True) as connection:
        service = GermplasmService(connection)
        make_source(service, "SRC-1")
        make_source(service, "SRC-2", permit=None)
        batch = make_batch(service)

        summary = service.intake.import_manifest(batch["id"], manifest_payload(
            base_row("IB-ACC-1"),
            base_row("IB-ACC-2", passport={}, expected_weight_grams=100),
            base_row("IB-ACC-3", source_code="SRC-2", permit_reference=None),
            base_row("IB-ACC-4", source_code="SRC-GHOST"),
        ))
        assert summary["inserted_count"] == 4
        assert summary["rejected_count"] == 0

        # 实收：2 号超重，4 号沿用未知来源
        received = service.intake.import_received(batch["id"], received_payload(
            recv_row("IB-ACC-1", 105),
            recv_row("IB-ACC-2", 120, passport={}),
            recv_row("IB-ACC-3", 100, source_code="SRC-2", permit_reference=None),
            recv_row("IB-ACC-4", 100, source_code="SRC-GHOST", permit_reference=None),
        ))
        assert received["updated_count"] == 4
        assert received["rejected_count"] == 0

        detail = service.intake.batch_detail(batch["id"])
        by_no = {item["accession_no"]: item for item in detail["items"]}
        assert by_no["IB-ACC-1"]["diff_codes"] == []
        assert set(by_no["IB-ACC-2"]["diff_codes"]) == {"weight_out_of_tolerance", "passport_missing"}
        assert by_no["IB-ACC-2"]["diff_detail"]["delta_grams"] == 20
        assert by_no["IB-ACC-2"]["diff_detail"]["missing_fields"] == ["origin"]
        assert "permit_missing" in by_no["IB-ACC-3"]["diff_codes"]
        assert by_no["IB-ACC-4"]["diff_codes"] == ["source_not_found"]

        # 重复上传同一文件：幂等回放，不新增材料
        replay = service.intake.import_manifest(batch["id"], manifest_payload(
            base_row("IB-ACC-1"), base_row("IB-ACC-2", passport={}, expected_weight_grams=100),
            base_row("IB-ACC-3", source_code="SRC-2", permit_reference=None),
            base_row("IB-ACC-4", source_code="SRC-GHOST"),
        ))
        assert replay["replayed"] is True
        assert replay["replayed_count"] == 4
        item_count = connection.execute(
            "SELECT COUNT(*) FROM intake_items WHERE batch_id=?", (batch["id"],)
        ).fetchone()[0]
        assert item_count == 4

        # 同键不同内容必须拒绝
        with pytest.raises(ConflictError):
            service.intake.import_manifest(batch["id"], manifest_payload(base_row("IB-ACC-9"), key="manifest-001"))

        # 文件内重复行进入拒绝清单，不影响其他行
        dup = service.intake.import_manifest(batch["id"], manifest_payload(
            base_row("IB-ACC-5"), base_row("IB-ACC-5"), key="manifest-002"
        ))
        assert dup["inserted_count"] == 1
        assert dup["rejected_count"] == 1
        assert dup["rejected"][0]["reason"] == "同一文件中资源编号重复"


def test_item_decisions_create_accessions_lots_and_holds(client):
    with transaction(immediate=True) as connection:
        service = GermplasmService(connection)
        make_source(service, "SRC-1")
        batch = make_batch(service)
        service.intake.import_manifest(batch["id"], manifest_payload(
            base_row("IB-OK-1"),
            base_row("IB-BAD-2", passport={}),
        ))
        service.intake.import_received(batch["id"], received_payload(
            recv_row("IB-OK-1", 100),
            recv_row("IB-BAD-2", 130, passport={}),
        ))
        items = {item["accession_no"]: item for item in service.intake.batch_detail(batch["id"])["items"]}

        # 有阻断差异的材料不能直接接收
        with pytest.raises(ValidationError):
            service.intake.decide_item(items["IB-BAD-2"]["id"], ItemDecision(
                decision="accept", expected_version=items["IB-BAD-2"]["item_version"], actor="审核员"
            ).model_dump(mode="json"))

        accepted = service.intake.decide_item(items["IB-OK-1"]["id"], ItemDecision(
            decision="accept", reason="清单与实收一致",
            expected_version=items["IB-OK-1"]["item_version"], actor="审核员",
        ).model_dump(mode="json"))
        assert accepted["status"] == "accepted"
        accession = service.repository.require_accession(accepted["accession_id"])
        assert accession["status"] == "accepted"
        lot = service.repository.require_lot(accepted["seed_lot_id"])
        assert lot["initial_weight_grams"] == 100
        assert lot["status"] == "pending"

        quarantined = service.intake.decide_item(items["IB-BAD-2"]["id"], ItemDecision(
            decision="quarantine", reason="重量超差且护照不全",
            expected_version=items["IB-BAD-2"]["item_version"], actor="审核员",
        ).model_dump(mode="json"))
        assert quarantined["status"] == "quarantined"
        q_accession = service.repository.require_accession(quarantined["accession_id"])
        assert q_accession["status"] == "quarantine"
        holds = service.repository.active_holds(quarantined["seed_lot_id"])
        assert len(holds) == 1 and holds[0]["hold_type"] == "检疫"

        # 旧版本决定不能覆盖已完成的复核（即使另一名复核员再次提交）
        with pytest.raises(ConflictError):
            service.intake.decide_item(items["IB-OK-1"]["id"], ItemDecision(
                decision="return", reason="事后改判退回",
                expected_version=items["IB-OK-1"]["item_version"], actor="另一名审核员",
            ).model_dump(mode="json"))


def test_batch_decision_skips_conflicts_and_reports_per_item(client):
    with transaction(immediate=True) as connection:
        service = GermplasmService(connection)
        make_source(service, "SRC-1")
        batch = make_batch(service, "IB-BULK-1")
        service.intake.import_manifest(batch["id"], manifest_payload(
            base_row("IB-B-1"), base_row("IB-B-2"), base_row("IB-B-3", passport={}),
            key="manifest-bulk",
        ))
        service.intake.import_received(batch["id"], received_payload(
            recv_row("IB-B-1", 100), recv_row("IB-B-2", 100), recv_row("IB-B-3", 100, passport={}),
            key="received-bulk",
        ))
        items = {item["accession_no"]: item for item in service.intake.batch_detail(batch["id"])["items"]}

        # 先单项隔离第三项，批次版本因此前进
        service.intake.decide_item(items["IB-B-3"]["id"], ItemDecision(
            decision="quarantine", reason="护照待补",
            expected_version=items["IB-B-3"]["item_version"], actor="审核员甲",
        ).model_dump(mode="json"))

        # 旧批次版本做整批接收必须失败
        with pytest.raises(ConflictError):
            service.intake.decide_batch(batch["id"], BatchDecision(
                decision="accept", expected_version=1, actor="审核员乙"
            ).model_dump(mode="json"))

        current_version = service.intake.require_batch(batch["id"])["version"]
        result = service.intake.decide_batch(batch["id"], BatchDecision(
            decision="accept", expected_version=current_version, actor="审核员乙"
        ).model_dump(mode="json"))
        assert result["applied_count"] == 2
        assert result["skipped_decided_count"] == 1
        by_no = {row["accession_no"]: row["result"] for row in result["results"]}
        assert by_no["IB-B-1"] == "applied"
        assert by_no["IB-B-2"] == "applied"
        assert by_no["IB-B-3"] == "already_decided"

        # 全部有结论后批次自动完成
        refreshed = service.intake.require_batch(batch["id"])
        assert refreshed["status"] == "completed"


def test_returned_item_can_be_corrected_and_resumed(client):
    with transaction(immediate=True) as connection:
        service = GermplasmService(connection)
        make_source(service, "SRC-1")
        make_source(service, "SRC-2", permit=None)
        batch = make_batch(service, "IB-FIX-1")
        service.intake.import_manifest(batch["id"], manifest_payload(
            base_row("IB-F-1", source_code="SRC-2", permit_reference=None), key="manifest-fix"
        ))
        service.intake.import_received(batch["id"], received_payload(
            recv_row("IB-F-1", 100, source_code="SRC-2", permit_reference=None), key="received-fix"
        ))
        item = service.intake.batch_detail(batch["id"])["items"][0]
        assert "permit_missing" in item["diff_codes"]

        returned = service.intake.decide_item(item["id"], ItemDecision(
            decision="return", reason="采集许可缺失，先退回",
            expected_version=item["item_version"], actor="审核员",
        ).model_dump(mode="json"))
        assert returned["status"] == "returned"
        assert returned["accession_id"] is None

        def fix(permit, expected_version):
            return ItemCorrection(
                permit_reference=permit, expected_version=expected_version,
                reason="合作单位补来采集许可", actor="登记员",
            ).model_dump(mode="json", exclude_unset=True)

        # 退回决定不改变版本，旧版本修正仍应被拒绝
        with pytest.raises(ConflictError):
            service.intake.correct_item(item["id"], fix("P-777", item["item_version"]))

        corrected = service.intake.correct_item(item["id"], fix("P-777", returned["item_version"]))
        assert corrected["status"] == "pending"
        assert corrected["diff_codes"] == []
        assert corrected["permit_reference"] == "P-777"

        accepted = service.intake.decide_item(corrected["id"], ItemDecision(
            decision="accept", reason="许可补齐后接收",
            expected_version=corrected["item_version"], actor="审核员",
        ).model_dump(mode="json"))
        assert accepted["status"] == "accepted"


def test_summary_reconciles_with_accessions_lots_and_holds(client):
    with transaction(immediate=True) as connection:
        service = GermplasmService(connection)
        make_source(service, "SRC-1")
        batch = make_batch(service, "IB-SUM-1")
        service.intake.import_manifest(batch["id"], manifest_payload(
            base_row("IB-S-1"), base_row("IB-S-2", passport={}),
            base_row("IB-S-3", source_code="SRC-GHOST"), key="manifest-sum",
        ))
        service.intake.import_received(batch["id"], received_payload(
            recv_row("IB-S-1", 100), recv_row("IB-S-2", 100, passport={}),
            recv_row("IB-S-3", 100, source_code="SRC-GHOST", permit_reference=None), key="received-sum",
        ))
        items = {item["accession_no"]: item for item in service.intake.batch_detail(batch["id"])["items"]}
        service.intake.decide_item(items["IB-S-1"]["id"], ItemDecision(
            decision="accept", reason="合格", expected_version=items["IB-S-1"]["item_version"], actor="审核员",
        ).model_dump(mode="json"))
        service.intake.decide_item(items["IB-S-2"]["id"], ItemDecision(
            decision="quarantine", reason="隔离", expected_version=items["IB-S-2"]["item_version"], actor="审核员",
        ).model_dump(mode="json"))
        service.intake.decide_item(items["IB-S-3"]["id"], ItemDecision(
            decision="return", reason="来源不明", expected_version=items["IB-S-3"]["item_version"], actor="审核员",
        ).model_dump(mode="json"))

        summary = service.intake.compute_summary(batch["id"])
        assert summary["total_items"] == 3
        assert summary["accepted_count"] == 1
        assert summary["quarantined_count"] == 1
        assert summary["returned_count"] == 1
        assert len(summary["created_accession_ids"]) == 2
        assert len(summary["created_seed_lot_ids"]) == 2
        assert len(summary["active_hold_ids"]) == 1
        assert summary["fully_linked"] is True
        for linkage in summary["linkage"]:
            assert linkage["issues"] == []

        # 导入摘要、差异快照、操作者、决定理由都可追溯
        detail = service.intake.batch_detail(batch["id"])
        assert {entry["import_kind"] for entry in detail["imports"]} == {"manifest", "received"}
        assert detail["imports"][0]["imported_by"] == "登记员"
        reasons = {decision["decision"]: decision["reason"] for decision in detail["decisions"] if decision["scope"] == "item"}
        assert reasons["return"] == "来源不明"
        # 差异快照保留操作者与当时版本
        snapshot_actors = {
            row[0]
            for row in connection.execute("SELECT DISTINCT created_by FROM intake_item_snapshots").fetchall()
        }
        assert snapshot_actors == {"登记员"}
        versioned = connection.execute(
            "SELECT COUNT(*) FROM intake_item_snapshots WHERE item_version >= 1"
        ).fetchone()[0]
        assert versioned >= 3


def test_concurrent_item_decisions_cannot_overwrite_each_other(client):
    with transaction(immediate=True) as connection:
        service = GermplasmService(connection)
        make_source(service, "SRC-1")
        batch = make_batch(service, "IB-LOCK-1")
        service.intake.import_manifest(batch["id"], manifest_payload(base_row("IB-L-1"), key="manifest-lock"))
        service.intake.import_received(batch["id"], received_payload(recv_row("IB-L-1", 100), key="received-lock"))
        item = service.intake.batch_detail(batch["id"])["items"][0]
        item_id = item["id"]
        expected_version = item["item_version"]

    outcomes: list[str] = []
    barrier = threading.Barrier(2)

    def reviewer(name: str) -> None:
        local_connection = get_connection()
        try:
            barrier.wait()
            with transaction(immediate=True):
                GermplasmService(local_connection).intake.decide_item(item_id, ItemDecision(
                    decision="quarantine", reason=f"{name}要求隔离",
                    expected_version=expected_version, actor=name,
                ).model_dump(mode="json"))
            outcomes.append(f"{name}:applied")
        except ConflictError:
            outcomes.append(f"{name}:conflict")

    first = threading.Thread(target=reviewer, args=("复核员甲",))
    second = threading.Thread(target=reviewer, args=("复核员乙",))
    first.start()
    second.start()
    first.join()
    second.join()

    assert sorted(result.split(":")[1] for result in outcomes) == ["applied", "conflict"]
    winner = next(result.split(":")[0] for result in outcomes if result.endswith(":applied"))
    with transaction(immediate=True) as connection:
        final = GermplasmService(connection).intake.require_item(item_id)
        assert final["status"] == "quarantined"
        assert final["decided_by"] == winner


def test_existing_accession_number_is_flagged(client):
    with transaction(immediate=True) as connection:
        service = GermplasmService(connection)
        source = make_source(service, "SRC-1")
        service.accessions.create_accession({
            "accession_no": "DUP-001", "scientific_name": "Zea mays", "crop_name": "玉米",
            "source_id": source["id"], "acquisition_type": "采集", "received_on": "2026-09-01",
            "passport": {}, "created_by": "登记员",
        })
        batch = make_batch(service, "IB-DUP-1")
        service.intake.import_manifest(batch["id"], manifest_payload(
            base_row("DUP-001"), key="manifest-dup"
        ))
        service.intake.import_received(batch["id"], received_payload(
            recv_row("DUP-001", 100), key="received-dup"
        ))
        item = service.intake.batch_detail(batch["id"])["items"][0]
        assert "accession_no_exists" in item["diff_codes"]
        with pytest.raises(ValidationError):
            service.intake.decide_item(item["id"], ItemDecision(
                decision="accept", expected_version=item["item_version"], actor="审核员"
            ).model_dump(mode="json"))
