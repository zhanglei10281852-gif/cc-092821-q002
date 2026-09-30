from __future__ import annotations


def test_http_intake_and_inventory_flow(client, admin):
    headers = admin["headers"]
    source = client.post("/api/germplasm/sources", headers=headers, json={
        "source_code": "HTTP-SRC-1", "provider_name": "合作站", "country_code": "CN", "locality": "北方站",
        "restrictions": {},
    })
    assert source.status_code == 201, source.text
    accession = client.post("/api/germplasm/accessions", headers=headers, json={
        "accession_no": "HTTP-ACC-1", "scientific_name": "Triticum aestivum", "crop_name": "小麦",
        "cultivar_name": "地方材料", "source_id": source.json()["id"], "acquisition_type": "交换",
        "received_on": "2026-09-20", "passport": {}, "created_by": "登记员",
    })
    assert accession.status_code == 201, accession.text
    accepted = client.post(f"/api/germplasm/accessions/{accession.json()['id']}/transition", headers=headers, json={
        "target_status": "accepted", "reason": "手续齐全", "expected_version": 1, "actor": "审核员",
    })
    assert accepted.status_code == 200, accepted.text
    location = client.post("/api/germplasm/locations", headers=headers, json={
        "location_code": "HTTP-L1", "facility": "中期库", "room": "一室", "rack": "A", "shelf": "1",
        "capacity_grams": 3000, "temperature_c": 4, "humidity_percent": 35,
    })
    assert location.status_code == 201, location.text
    lot = client.post("/api/germplasm/lots", headers=headers, json={
        "lot_no": "HTTP-LOT-1", "accession_id": accepted.json()["id"], "harvest_year": 2025,
        "initial_weight_grams": 800, "moisture_percent": 8, "treatment": "清选", "created_by": "登记员",
    })
    assert lot.status_code == 201, lot.text
    placed = client.post("/api/germplasm/placements", headers=headers, json={
        "lot_id": lot.json()["id"], "location_id": location.json()["id"], "weight_grams": 800,
        "container_code": "HTTP-BOX-1", "idempotency_key": "http-place-0001", "actor": "保管员",
    })
    assert placed.status_code == 201, placed.text
    detail = client.get(f"/api/germplasm/lots/{lot.json()['id']}", headers=headers)
    assert detail.status_code == 200
    assert detail.json()["status"] == "stored"
    assert detail.json()["placements"][0]["container_code"] == "HTTP-BOX-1"


def test_api_rejects_unauthenticated_business_request(client):
    response = client.get("/api/germplasm/dashboard")
    assert response.status_code == 401


def test_http_intake_batch_flow(client, admin):
    headers = admin["headers"]
    source = client.post("/api/germplasm/sources", headers=headers, json={
        "source_code": "HTTP-IN-SRC", "provider_name": "合作单位", "country_code": "CN",
        "permit_reference": "PERMIT-9", "restrictions": {},
    })
    assert source.status_code == 201, source.text
    batch = client.post("/api/germplasm/intake-batches", headers=headers, json={
        "batch_no": "HTTP-INB-1", "source_code": "HTTP-IN-SRC", "acquisition_type": "采集",
        "harvest_year": 2026, "weight_tolerance_percent": 5, "required_passport_fields": ["collector"],
        "created_by": "登记员",
    })
    assert batch.status_code == 201, batch.text
    batch_id = batch.json()["id"]
    manifest = client.post(f"/api/germplasm/intake-batches/{batch_id}/imports/manifest", headers=headers, json={
        "imported_by": "登记员",
        "rows": [
            {"accession_no": "HTTP-IN-1", "scientific_name": "Triticum aestivum", "crop_name": "小麦",
             "expected_weight_grams": 200, "permit_reference": "PERMIT-9", "passport": {"collector": "张三"}},
            {"accession_no": "HTTP-IN-2", "scientific_name": "Triticum aestivum", "crop_name": "小麦",
             "expected_weight_grams": 200, "permit_reference": "PERMIT-9", "passport": {}},
        ],
    })
    assert manifest.status_code == 201, manifest.text
    assert manifest.json()["applied_count"] == 2 and manifest.json()["replayed"] is False
    replay = client.post(f"/api/germplasm/intake-batches/{batch_id}/imports/manifest", headers=headers, json={
        "imported_by": "登记员",
        "rows": [
            {"accession_no": "HTTP-IN-1", "scientific_name": "Triticum aestivum", "crop_name": "小麦",
             "expected_weight_grams": 200, "permit_reference": "PERMIT-9", "passport": {"collector": "张三"}},
            {"accession_no": "HTTP-IN-2", "scientific_name": "Triticum aestivum", "crop_name": "小麦",
             "expected_weight_grams": 200, "permit_reference": "PERMIT-9", "passport": {}},
        ],
    })
    assert replay.status_code == 201 and replay.json()["replayed"] is True
    receipts = client.post(f"/api/germplasm/intake-batches/{batch_id}/imports/receipts", headers=headers, json={
        "imported_by": "登记员",
        "rows": [
            {"accession_no": "HTTP-IN-1", "received_weight_grams": 198},
            {"accession_no": "HTTP-IN-2", "received_weight_grams": 260},
        ],
    })
    assert receipts.status_code == 201, receipts.text
    detail = client.get(f"/api/germplasm/intake-batches/{batch_id}", headers=headers)
    items = {item["accession_no"]: item for item in detail.json()["items"]}
    assert items["HTTP-IN-1"]["suggested_action"] == "accept"
    assert items["HTTP-IN-2"]["discrepancies"] == ["weight_out_of_tolerance", "passport_missing:collector"]
    decided = client.post(f"/api/germplasm/intake-items/{items['HTTP-IN-1']['id']}/decision", headers=headers, json={
        "decision": "accept", "reason": "", "expected_version": items["HTTP-IN-1"]["version"], "actor": "复核员",
    })
    assert decided.status_code == 200, decided.text
    assert decided.json()["accession_id"] and decided.json()["lot_id"]
    quarantined = client.post(
        f"/api/germplasm/intake-items/{items['HTTP-IN-2']['id']}/decision", headers=headers, json={
            "decision": "quarantine", "reason": "重量偏差且缺采集人",
            "expected_version": items["HTTP-IN-2"]["version"], "actor": "复核员",
        })
    assert quarantined.status_code == 200, quarantined.text
    assert quarantined.json()["hold_id"] is not None
    summary = client.get(f"/api/germplasm/intake-batches/{batch_id}/summary", headers=headers)
    assert summary.status_code == 200
    body = summary.json()
    assert body["counts"] == {"total": 2, "pending": 0, "accepted": 1, "quarantined": 1, "returned": 0}
    assert body["reconciled"] is True
    assert all(item["reconciliation"]["consistent"] for item in body["items"])
    closed = client.post(f"/api/germplasm/intake-batches/{batch_id}/close", headers=headers, json={
        "expected_version": summary.json()["batch"]["version"], "actor": "复核员",
    })
    assert closed.status_code == 200, closed.text
    assert closed.json()["status"] == "completed"


def test_intake_review_requires_review_permission(client, admin):
    headers = admin["headers"]
    created = client.post("/api/users", headers=headers, json={
        "username": "intake.clerk", "password": "Clerk!23456", "display_name": "登记员甲",
        "role_codes": ["registrar"],
    })
    assert created.status_code == 201, created.text
    login = client.post("/api/auth/login", json={
        "username": "intake.clerk", "password": "Clerk!23456", "client_label": "tests",
    })
    clerk_headers = {"Authorization": f"Bearer {login.json()['token']}"}
    source = client.post("/api/germplasm/sources", headers=headers, json={
        "source_code": "HTTP-PERM-SRC", "provider_name": "合作单位", "country_code": "CN", "restrictions": {},
    })
    assert source.status_code == 201
    batch = client.post("/api/germplasm/intake-batches", headers=clerk_headers, json={
        "batch_no": "HTTP-PERM-1", "source_code": "HTTP-PERM-SRC", "harvest_year": 2026,
        "created_by": "登记员甲",
    })
    assert batch.status_code == 201, batch.text
    batch_id = batch.json()["id"]
    imported = client.post(f"/api/germplasm/intake-batches/{batch_id}/imports/manifest", headers=clerk_headers, json={
        "imported_by": "登记员甲",
        "rows": [{"accession_no": "HTTP-PERM-ACC", "scientific_name": "Oryza sativa", "crop_name": "水稻",
                  "expected_weight_grams": 50}],
    })
    assert imported.status_code == 201, imported.text
    item = client.get(f"/api/germplasm/intake-batches/{batch_id}", headers=clerk_headers).json()["items"][0]
    denied = client.post(f"/api/germplasm/intake-items/{item['id']}/decision", headers=clerk_headers, json={
        "decision": "accept", "reason": "", "expected_version": item["version"], "actor": "登记员甲",
    })
    assert denied.status_code == 403


def test_api_validation_error_has_structured_body(client, admin):
    response = client.post("/api/germplasm/locations", headers=admin["headers"], json={
        "location_code": "BAD", "facility": "库", "room": "一室", "rack": "A", "shelf": "1",
        "capacity_grams": -1, "temperature_c": 4, "humidity_percent": 35,
    })
    assert response.status_code == 422
    assert response.json()["detail"]
