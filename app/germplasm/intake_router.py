from __future__ import annotations

from fastapi import APIRouter, Depends, Query

from app.api.dependencies import current_principal
from app.core.security import Principal
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

router = APIRouter(prefix="/api/germplasm/intake", tags=["到库批次"])


def _service() -> GermplasmService:
    return GermplasmService(get_connection())


@router.post("/batches", status_code=201)
def create_intake_batch(data: IntakeBatchCreate, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("intake.import")
    with transaction(immediate=True) as connection:
        return GermplasmService(connection).intake.create_batch(data.model_dump(mode="json"))


@router.get("/batches")
def list_intake_batches(
    status: str | None = Query(default=None, pattern="^(open|completed)$"),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    principal: Principal = Depends(current_principal),
) -> dict:
    principal.require("intake.read")
    items, total = _service().intake.list_batches(status=status, limit=limit, offset=offset)
    return {"items": items, "total": total, "limit": limit, "offset": offset}


@router.get("/batches/{batch_id}")
def intake_batch_detail(batch_id: int, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("intake.read")
    return _service().intake.batch_detail(batch_id)


@router.get("/batches/{batch_id}/summary")
def intake_batch_summary(batch_id: int, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("intake.read")
    return _service().intake.compute_summary(batch_id)


@router.post("/batches/{batch_id}/manifest")
def import_manifest(batch_id: int, data: ManifestImport, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("intake.import")
    with transaction(immediate=True) as connection:
        return GermplasmService(connection).intake.import_manifest(batch_id, data.model_dump(mode="json"))


@router.post("/batches/{batch_id}/received")
def import_received(batch_id: int, data: ReceivedImport, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("intake.import")
    with transaction(immediate=True) as connection:
        return GermplasmService(connection).intake.import_received(batch_id, data.model_dump(mode="json"))


@router.post("/items/{item_id}/decision")
def decide_intake_item(item_id: int, data: ItemDecision, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("intake.review")
    with transaction(immediate=True) as connection:
        return GermplasmService(connection).intake.decide_item(item_id, data.model_dump(mode="json"))


@router.post("/batches/{batch_id}/decision")
def decide_intake_batch(batch_id: int, data: BatchDecision, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("intake.review")
    with transaction(immediate=True) as connection:
        return GermplasmService(connection).intake.decide_batch(batch_id, data.model_dump(mode="json"))


@router.patch("/items/{item_id}")
def correct_intake_item(item_id: int, data: ItemCorrection, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("intake.import")
    with transaction(immediate=True) as connection:
        return GermplasmService(connection).intake.correct_item(item_id, data.model_dump(mode="json", exclude_unset=True))
