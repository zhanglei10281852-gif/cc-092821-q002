from __future__ import annotations


def _login(client, username: str, password: str = "Passw0rd!!") -> dict:
    response = client.post("/api/auth/login", json={
        "username": username, "password": password, "client_label": "tests"
    })
    assert response.status_code == 200, response.text
    return {"Authorization": f"Bearer {response.json()['token']}"}


def _make_user(client, admin_headers: dict, username: str, role: str) -> dict:
    response = client.post("/api/users", headers=admin_headers, json={
        "username": username, "password": "Passw0rd!!", "display_name": username,
        "role_codes": [role],
    })
    assert response.status_code == 201, response.text
    return _login(client, username)


def _manifest_rows() -> list[dict]:
    return [
        {
            "accession_no": "HTTP-IB-1", "source_code": "HTTP-IS-1",
            "scientific_name": "Oryza sativa", "crop_name": "水稻", "cultivar_name": "地方材料",
            "acquisition_type": "采集", "collected_on": "2026-08-01", "permit_reference": "P-WEB-1",
            "expected_weight_grams": 100, "passport": {"origin": "河谷"},
        },
        {
            "accession_no": "HTTP-IB-2", "source_code": "HTTP-IS-1",
            "scientific_name": "Oryza sativa", "crop_name": "水稻",
            "acquisition_type": "采集", "collected_on": "2026-08-01", "permit_reference": "P-WEB-1",
            "expected_weight_grams": 100, "passport": {},
        },
    ]


def _received_rows() -> list[dict]:
    return [
        {
            "accession_no": "HTTP-IB-1", "received_weight_grams": 100, "received_on": "2026-09-20",
            "source_code": "HTTP-IS-1", "permit_reference": "P-WEB-1", "passport": {"origin": "河谷"},
        },
        {
            "accession_no": "HTTP-IB-2", "received_weight_grams": 140, "received_on": "2026-09-20",
            "source_code": "HTTP-IS-1", "permit_reference": "P-WEB-1", "passport": {},
        },
    ]


def test_intake_batch_http_flow_and_role_separation(client, admin):
    admin_headers = admin["headers"]
    registrar = _make_user(client, admin_headers, "registrar1", "registrar")
    curator = _make_user(client, admin_headers, "curator1", "curator")

    source = client.post("/api/germplasm/sources", headers=admin_headers, json={
        "source_code": "HTTP-IS-1", "provider_name": "合作站", "country_code": "CN",
        "permit_reference": "P-WEB-1",
    })
    assert source.status_code == 201, source.text

    # 复核员不能建批次/导入
    denied = client.post("/api/germplasm/intake/batches", headers=curator, json={
        "batch_no": "HTTP-IB-B1", "actor": "登记员",
    })
    assert denied.status_code == 403

    batch = client.post("/api/germplasm/intake/batches", headers=registrar, json={
        "batch_no": "HTTP-IB-B1", "title": "本周一批", "weight_tolerance_percent": 10,
        "tolerance_grams": 5, "required_passport_fields": ["scientific_name", "crop_name", "origin"],
        "actor": "登记员",
    })
    assert batch.status_code == 201, batch.text
    batch_id = batch.json()["id"]

    manifest = client.post(f"/api/germplasm/intake/batches/{batch_id}/manifest", headers=registrar, json={
        "idempotency_key": "web-manifest-0001", "rows": _manifest_rows(), "actor": "登记员",
    })
    assert manifest.status_code == 200, manifest.text
    assert manifest.json()["inserted_count"] == 2

    received = client.post(f"/api/germplasm/intake/batches/{batch_id}/received", headers=registrar, json={
        "idempotency_key": "web-received-0001", "rows": _received_rows(), "actor": "登记员",
    })
    assert received.status_code == 200, received.text
    assert received.json()["updated_count"] == 2

    # 登记员不能做复核决定
    batch_detail = client.get(f"/api/germplasm/intake/batches/{batch_id}", headers=curator).json()
    by_no = {item["accession_no"]: item for item in batch_detail["items"]}
    forbidden = client.post(
        f"/api/germplasm/intake/items/{by_no['HTTP-IB-1']['id']}/decision",
        headers=registrar,
        json={"decision": "accept", "expected_version": by_no["HTTP-IB-1"]["item_version"], "actor": "登记员"},
    )
    assert forbidden.status_code == 403

    # 第二项有差异，整批接收跳过它；第一项落地正式资源和种子批次
    bulk = client.post(f"/api/germplasm/intake/batches/{batch_id}/decision", headers=curator, json={
        "decision": "accept", "expected_version": batch_detail["version"], "actor": "复核员",
    })
    assert bulk.status_code == 200, bulk.text
    assert bulk.json()["applied_count"] == 1
    assert bulk.json()["invalid_count"] == 1

    # 第二项隔离：资源进入 quarantine 并挂检疫冻结
    quarantine = client.post(
        f"/api/germplasm/intake/items/{by_no['HTTP-IB-2']['id']}/decision",
        headers=curator,
        json={
            "decision": "quarantine", "reason": "实收超差且护照缺项",
            "expected_version": by_no["HTTP-IB-2"]["item_version"], "actor": "复核员",
        },
    )
    assert quarantine.status_code == 200, quarantine.text
    seed_lot_id = quarantine.json()["seed_lot_id"]
    lot = client.get(f"/api/germplasm/lots/{seed_lot_id}", headers=curator)
    assert lot.status_code == 200
    assert lot.json()["holds"][0]["hold_type"] == "检疫"

    # 批次汇总必须与正式资源、种子批次、隔离记录逐项对上
    summary = client.get(f"/api/germplasm/intake/batches/{batch_id}/summary", headers=curator)
    assert summary.status_code == 200
    body = summary.json()
    assert body["batch_status"] == "completed"
    assert body["accepted_count"] == 1
    assert body["quarantined_count"] == 1
    assert body["fully_linked"] is True
    assert len(body["created_accession_ids"]) == 2
    assert len(body["active_hold_ids"]) == 1

    # 未认证请求被拒绝
    assert client.get(f"/api/germplasm/intake/batches/{batch_id}/summary").status_code == 401
