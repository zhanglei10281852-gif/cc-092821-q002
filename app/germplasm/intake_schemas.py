from __future__ import annotations

from datetime import date
from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator

ACQUISITION_TYPES = {"采集", "引进", "交换", "捐赠", "育种"}


def _normalize_code(value: str) -> str:
    normalized = value.strip().upper()
    if not normalized.replace("-", "").replace("_", "").isalnum():
        raise ValueError("编码只能包含字母、数字、连字符和下划线")
    return normalized


class IntakeBatchCreate(BaseModel):
    batch_no: str = Field(min_length=3, max_length=40)
    title: str = Field(default="", max_length=200)
    weight_tolerance_percent: float = Field(default=5, ge=0, le=100)
    tolerance_grams: float = Field(default=1, ge=0, le=100_000)
    required_passport_fields: list[str] = Field(default_factory=lambda: ["scientific_name", "crop_name"])
    actor: str = Field(min_length=1, max_length=100)

    @field_validator("batch_no")
    @classmethod
    def normalize_batch_no(cls, value: str) -> str:
        return _normalize_code(value)

    @field_validator("title", "actor")
    @classmethod
    def strip_text(cls, value: str) -> str:
        return value.strip()

    @field_validator("required_passport_fields")
    @classmethod
    def normalize_required_fields(cls, value: list[str]) -> list[str]:
        cleaned: list[str] = []
        for raw in value:
            field = raw.strip()
            if field and field not in cleaned:
                cleaned.append(field)
        if len(cleaned) > 30:
            raise ValueError("必填护照字段不能超过 30 个")
        return cleaned


class ManifestRow(BaseModel):
    line_no: int | None = Field(default=None, ge=1)
    accession_no: str = Field(min_length=3, max_length=50)
    source_code: str | None = Field(default=None, max_length=40)
    scientific_name: str = Field(default="", max_length=200)
    crop_name: str = Field(default="", max_length=100)
    cultivar_name: str = Field(default="", max_length=150)
    acquisition_type: str = Field(default="采集")
    collected_on: date | None = None
    permit_reference: str | None = Field(default=None, max_length=100)
    expected_weight_grams: float | None = Field(default=None, gt=0, le=10_000_000)
    passport: dict[str, Any] = Field(default_factory=dict)

    @field_validator("accession_no")
    @classmethod
    def normalize_accession_no(cls, value: str) -> str:
        normalized = value.strip().upper()
        if " " in normalized:
            raise ValueError("资源编号不能包含空格")
        return normalized

    @field_validator("source_code")
    @classmethod
    def normalize_source_code(cls, value: str | None) -> str | None:
        return _normalize_code(value) if value and value.strip() else None

    @field_validator("acquisition_type")
    @classmethod
    def validate_acquisition(cls, value: str) -> str:
        if value not in ACQUISITION_TYPES:
            raise ValueError("引种方式必须是 采集/引进/交换/捐赠/育种 之一")
        return value

    @field_validator("scientific_name", "crop_name", "cultivar_name")
    @classmethod
    def strip_text(cls, value: str) -> str:
        return value.strip()


class ReceivedRow(BaseModel):
    line_no: int | None = Field(default=None, ge=1)
    accession_no: str = Field(min_length=3, max_length=50)
    received_weight_grams: float | None = Field(default=None, gt=0, le=10_000_000)
    received_on: date
    source_code: str | None = Field(default=None, max_length=40)
    permit_reference: str | None = Field(default=None, max_length=100)
    passport: dict[str, Any] = Field(default_factory=dict)

    @field_validator("accession_no")
    @classmethod
    def normalize_accession_no(cls, value: str) -> str:
        normalized = value.strip().upper()
        if " " in normalized:
            raise ValueError("资源编号不能包含空格")
        return normalized

    @field_validator("source_code")
    @classmethod
    def normalize_source_code(cls, value: str | None) -> str | None:
        return _normalize_code(value) if value and value.strip() else None


class ManifestImport(BaseModel):
    idempotency_key: str = Field(min_length=8, max_length=100)
    rows: list[ManifestRow] = Field(min_length=1, max_length=1000)
    actor: str = Field(min_length=1, max_length=100)


class ReceivedImport(BaseModel):
    idempotency_key: str = Field(min_length=8, max_length=100)
    rows: list[ReceivedRow] = Field(min_length=1, max_length=1000)
    actor: str = Field(min_length=1, max_length=100)


class ItemCorrection(BaseModel):
    expected_version: int = Field(gt=0)
    reason: str = Field(min_length=2, max_length=500)
    actor: str = Field(min_length=1, max_length=100)
    accession_no: str | None = Field(default=None, min_length=3, max_length=50)
    source_code: str | None = Field(default=None, max_length=40)
    scientific_name: str | None = Field(default=None, max_length=200)
    crop_name: str | None = Field(default=None, max_length=100)
    cultivar_name: str | None = Field(default=None, max_length=150)
    acquisition_type: str | None = None
    collected_on: date | None = None
    permit_reference: str | None = Field(default=None, max_length=100)
    expected_weight_grams: float | None = Field(default=None, gt=0, le=10_000_000)
    expected_passport: dict[str, Any] | None = None
    received_weight_grams: float | None = Field(default=None, gt=0, le=10_000_000)
    received_on: date | None = None
    actual_passport: dict[str, Any] | None = None

    @field_validator("accession_no")
    @classmethod
    def normalize_accession_no(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip().upper()
        if " " in normalized:
            raise ValueError("资源编号不能包含空格")
        return normalized

    @field_validator("source_code")
    @classmethod
    def normalize_source_code(cls, value: str | None) -> str | None:
        return _normalize_code(value) if value and value.strip() else None

    @field_validator("acquisition_type")
    @classmethod
    def validate_acquisition(cls, value: str | None) -> str | None:
        if value is not None and value not in ACQUISITION_TYPES:
            raise ValueError("引种方式必须是 采集/引进/交换/捐赠/育种 之一")
        return value

    @model_validator(mode="after")
    def require_change(self) -> "ItemCorrection":
        change_fields = {
            "accession_no", "source_code", "scientific_name", "crop_name", "cultivar_name",
            "acquisition_type", "collected_on", "permit_reference", "expected_weight_grams",
            "expected_passport", "received_weight_grams", "received_on", "actual_passport",
        }
        if not any(getattr(self, field) is not None for field in change_fields):
            raise ValueError("修正请求至少要包含一个需要修改的字段")
        return self


class ItemDecision(BaseModel):
    decision: str = Field(pattern="^(accept|quarantine|return)$")
    reason: str = Field(default="", max_length=500)
    expected_version: int = Field(gt=0)
    actor: str = Field(min_length=1, max_length=100)


class BatchDecision(BaseModel):
    decision: str = Field(pattern="^(accept|quarantine|return)$")
    reason: str = Field(default="", max_length=500)
    expected_version: int = Field(gt=0)
    actor: str = Field(min_length=1, max_length=100)
