"""企业基本信息处理服务"""
from __future__ import annotations

from decimal import Decimal, InvalidOperation
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.constants import company_basic_info as constants_company_basic_info
from app.db.database import commit_or_rollback
from app.models import company_basic_info as models_company_basic_info
from app.schemas.common import ErrorCode
from app.utils.exception import ServiceException
from app.utils.xlsx_reader import read_sheet_as_dicts


# ========== 公共入口函数 ==========


def upsert_company_basic_info_records(
    db: Session,
    source_file_path: str | Path,
):
    """将附件1中的公司基础信息幂等写入 company_basic_info"""
    rows = _load_company_basic_info_rows(source_file_path)

    inserted_count = 0
    updated_count = 0
    for raw_row in rows:
        payload = _build_company_basic_info_payload(raw_row, Path(source_file_path).name)
        stock_code = str(payload["stock_code"])

        existing = db.execute(
            select(models_company_basic_info.CompanyBasicInfo).where(
                models_company_basic_info.CompanyBasicInfo.stock_code == stock_code
            )
        ).scalar_one_or_none()

        if existing is None:
            db.add(models_company_basic_info.CompanyBasicInfo(**payload))
            inserted_count += 1
            continue

        for field, value in payload.items():
            setattr(existing, field, value)
        updated_count += 1

    commit_or_rollback(db)
    return {
        "total": len(rows),
        "inserted": inserted_count,
        "updated": updated_count,
    }


# ========== 辅助函数 ==========


def _normalize_registered_capital_to_yuan(raw_value: str | None):
    """将附件1中的注册资本文本换算为元"""
    if raw_value is None:
        return None

    text = str(raw_value).strip().replace(",", "")
    if not text:
        return None

    units = {
        "亿": Decimal("100000000"),
        "万": Decimal("10000"),
        "元": Decimal("1"),
    }
    for unit, factor in units.items():
        if text.endswith(unit):
            number_part = text[: -len(unit)].strip()
            try:
                return Decimal(number_part) * factor
            except (InvalidOperation, ValueError) as exc:
                raise ServiceException(ErrorCode.PARAM_ERROR, f"无法解析注册资本：{raw_value}") from exc

    try:
        return Decimal(text)
    except (InvalidOperation, ValueError) as exc:
        raise ServiceException(ErrorCode.PARAM_ERROR, f"无法解析注册资本：{raw_value}") from exc


def _load_company_basic_info_rows(source_file_path: str | Path):
    """直接从附件1读取公司基础信息行"""
    rows = read_sheet_as_dicts(source_file_path, constants_company_basic_info.ATTACHMENT1_SHEET_NAME)
    if not rows:
        raise ServiceException(ErrorCode.PARAM_ERROR, "附件1-基本信息表为空")
    return rows


def _build_company_basic_info_payload(
    raw_row: dict[str, str],
    source_file_name: str,
):
    """将附件1原始行映射为 ORM 入库载荷"""
    payload = {
        target_field: raw_row.get(source_field, "").strip()
        for source_field, target_field in constants_company_basic_info.ATTACHMENT1_FIELD_MAP.items()
    }

    missing_fields = [
        source_field
        for source_field in ("序号", "股票代码", "A股简称", "公司名称", "上市交易所")
        if not raw_row.get(source_field, "").strip()
    ]
    if missing_fields:
        raise ServiceException(ErrorCode.PARAM_ERROR, f"附件1存在缺失关键字段：{', '.join(missing_fields)}")

    listed_exchange = str(payload["listed_exchange"]).strip()
    payload["stock_code"] = models_company_basic_info.normalize_company_stock_code(
        str(payload["stock_code"])
    )
    payload["exchange"] = models_company_basic_info.normalize_exchange_code(
        listed_exchange
    )
    payload["registered_capital_yuan"] = _normalize_registered_capital_to_yuan(
        str(payload["registered_capital_raw"]).strip() or None
    )
    payload["source_file_name"] = source_file_name

    for field in ("employee_count", "management_count", "source_row_no"):
        value = str(payload[field]).strip()
        payload[field] = int(value) if value else None

    return payload
