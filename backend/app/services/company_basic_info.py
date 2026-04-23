from __future__ import annotations

from decimal import Decimal, InvalidOperation
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import company_basic_info as models_company_basic_info
from app.utils.xlsx_reader import read_sheet_as_dicts


ATTACHMENT1_SHEET_NAME = "基本信息表"
ATTACHMENT1_FIELD_MAP = {
    "序号": "source_row_no",
    "股票代码": "stock_code",
    "A股简称": "stock_abbr",
    "公司名称": "company_name",
    "英文名称": "english_name",
    "所属证监会行业": "csrc_industry",
    "上市交易所": "listed_exchange",
    "证券类别": "security_category",
    "注册区域": "registered_region",
    "注册资本": "registered_capital_raw",
    "雇员人数": "employee_count",
    "管理人员人数": "management_count",
}


def normalize_registered_capital_to_yuan(raw_value: str | None) -> Decimal | None:
    """将附件1中的注册资本文本换算为元。"""
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
                raise ValueError(f"无法解析注册资本：{raw_value}") from exc

    try:
        return Decimal(text)
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"无法解析注册资本：{raw_value}") from exc


def load_company_basic_info_rows(source_file_path: str | Path) -> list[dict[str, str]]:
    """直接从附件1读取公司基础信息行。"""
    rows = read_sheet_as_dicts(source_file_path, ATTACHMENT1_SHEET_NAME)
    if not rows:
        raise ValueError("附件1-基本信息表为空")
    return rows


def build_company_basic_info_payload(
    raw_row: dict[str, str],
    source_file_name: str,
) -> dict[str, object]:
    """将附件1原始行映射为 ORM 入库载荷。"""
    payload = {
        target_field: raw_row.get(source_field, "").strip()
        for source_field, target_field in ATTACHMENT1_FIELD_MAP.items()
    }

    missing_fields = [
        source_field
        for source_field in ("序号", "股票代码", "A股简称", "公司名称", "上市交易所")
        if not raw_row.get(source_field, "").strip()
    ]
    if missing_fields:
        raise ValueError(f"附件1存在缺失关键字段：{', '.join(missing_fields)}")

    listed_exchange = str(payload["listed_exchange"]).strip()
    payload["stock_code"] = models_company_basic_info.normalize_company_stock_code(
        str(payload["stock_code"])
    )
    payload["exchange"] = models_company_basic_info.normalize_exchange_code(
        listed_exchange
    )
    payload["registered_capital_yuan"] = normalize_registered_capital_to_yuan(
        str(payload["registered_capital_raw"]).strip() or None
    )
    payload["source_file_name"] = source_file_name

    for field in ("employee_count", "management_count", "source_row_no"):
        value = str(payload[field]).strip()
        payload[field] = int(value) if value else None

    return payload


def upsert_company_basic_info_records(
    db: Session,
    source_file_path: str | Path,
) -> dict[str, int]:
    """将附件1中的公司基础信息幂等写入 company_basic_info。"""
    source_path = Path(source_file_path)
    rows = load_company_basic_info_rows(source_path)

    inserted_count = 0
    updated_count = 0
    for raw_row in rows:
        payload = build_company_basic_info_payload(raw_row, source_path.name)
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

    db.commit()
    return {
        "total": len(rows),
        "inserted": inserted_count,
        "updated": updated_count,
    }
