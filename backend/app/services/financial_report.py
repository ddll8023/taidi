"""财报元数据解析与入库服务"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime

from pypdf import PdfReader
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.constants import financial_report as constants_financial_report
from app.core.config import settings
from app.db.database import commit_or_rollback
from app.models import company_basic_info as models_company_basic_info
from app.models import financial_report as models_financial_report
from app.schemas import financial_report as schemas_financial_report
from app.schemas.common import ErrorCode
from app.utils.exception import ServiceException
from app.utils.logger_config import setup_logger

logger = setup_logger(__name__)


SSE_FILE_NAME_PATTERN = re.compile(
    r"^(?P<stock_code>\d{6})_(?P<report_date>\d{8})_[A-Za-z0-9]+(?:\s-\s[0-9a-fA-F]{32})?\.pdf$"
)
SZSE_FILE_NAME_PATTERN = re.compile(
    r"^(?P<stock_abbr>.+?)[：:](?P<report_body>.+?)(?:\s-\s[0-9a-fA-F]{32})?\.pdf$"
)
SZSE_REPORT_BODY_PATTERN = re.compile(
    r"^(?:(?P<company_prefix>[一-龥A-Za-z0-9（）()·\-\s]+?))?"
    r"(?P<report_year>\d{4})\s*年\s*"
    r"(?P<report_label>第一季度报告|一季度报告|半年度报告\s*摘\s*要|半年度报告|第三季度报告|三季度报告|年度报告\s*摘\s*要|年度报告)"
    r"(?P<full_text>\s*全文)?"
    r"(?P<suffixes>(?:\s*[（(](?:更正前|更正后|更新前|更新后|英文版|\d+)[)）])*)$"
)
REPORT_TITLE_PATTERN = re.compile(
    r"(?P<title>(?P<company_name>[一-龥A-Za-z0-9（）()·\-\s]+?)\s*(?P<report_year>20\d{2})\s*年\s*(?P<report_label>第一季度报告|一季度报告|半年度报告\s*摘\s*要|半年度报告|第三季度报告|三季度报告|年度报告\s*摘\s*要|年度报告))"
)
PDF_STOCK_CODE_PATTERNS = (
    re.compile(
        r"(?:证券代码|股票代码|公司代码|Stock\s+Code)\s*[:：]?\s*(?P<stock_code>\d{6})",
        flags=re.IGNORECASE,
    ),
)
PDF_PRIMARY_STOCK_ABBR_PATTERNS = (
    re.compile(
        r"(?:证券简称|股票简称|公司简称)(?:\s*[:：]\s*|\s+)(?P<stock_abbr>[一-龥A-Za-z0-9·()（） \t　]{2,30}?)"
        r"(?=(?:\s+(?:股票代码|证券代码|公司代码|Stock\s+Code)\b)|[\r\n]|$)",
        flags=re.IGNORECASE,
    ),
)
PDF_FALLBACK_STOCK_ABBR_PATTERNS = (
    re.compile(
        r"(?:公司的中文简称(?:\s*[（(]如有[)）])?)(?:\s*[:：]\s*|\s+)(?P<stock_abbr>[一-龥A-Za-z0-9·()（） \t　]{2,30})"
        r"(?=[\r\n]|$)",
        flags=re.IGNORECASE,
    ),
    re.compile(
        r"(?:Company\s+Abbreviation\s+in\s+Chinese(?:\s*[（(]\s*If\s+any\s*[)）])?)(?:\s*[:：]\s*|\s+)(?P<stock_abbr>[一-龥A-Za-z0-9·()（） \t　]{2,30})"
        r"(?=[\r\n]|$)",
        flags=re.IGNORECASE,
    ),
)
STOCK_ABBR_HEADER_KEYWORDS = (
    "股票代码",
    "证券代码",
    "公司代码",
    "变更前股票简称",
    "股票上市交易所",
    "上市交易所",
    "股票种类",
)
EXPLICIT_DATE_PATTERN = re.compile(
    r"(?P<year>20\d{2})\s*[年/-]\s*(?P<month>\d{1,2})\s*[月/-]\s*(?P<day>\d{1,2})\s*日?"
)


@dataclass(frozen=True)
class ResolvedFinancialReportMetadata:
    stock_code: str
    stock_abbr: str
    exchange: str
    report_year: int
    report_period: str
    report_type: str
    report_label: str
    report_title: str
    report_date: date | None


# ========== 公共入口函数 ==========


def resolve_financial_report_metadata(
    db: Session,
    source_file_name: str,
    file_path: str,
):
    """解析并合并文件名和PDF内容中的财报身份元数据"""
    normalized_file_name = _normalize_file_name(source_file_name)
    preview_text = _extract_pdf_preview_text(file_path)
    title_meta = _parse_report_title_meta(preview_text)
    pdf_security_meta = _parse_pdf_security_meta(preview_text)

    filename_report_date: date | None = None
    filename_stock_code: str | None = None
    filename_stock_abbr: str | None = None
    filename_report_year: int | None = None
    filename_report_period: str | None = None
    filename_report_type: str | None = None
    filename_report_label: str | None = None
    filename_exchange: str | None = None

    sse_match = SSE_FILE_NAME_PATTERN.match(normalized_file_name)
    if sse_match is not None:
        filename_stock_code = sse_match.group("stock_code")
        filename_report_date = _parse_report_date_token(sse_match.group("report_date"))
        filename_exchange = "SH"
    else:
        szse_file_match = SZSE_FILE_NAME_PATTERN.match(normalized_file_name)
        if szse_file_match is not None:
            filename_stock_abbr = szse_file_match.group("stock_abbr").strip()

        szse_meta = _parse_szse_file_name_meta(normalized_file_name)
        if szse_meta is not None:
            filename_stock_abbr = str(szse_meta["stock_abbr"])
            filename_report_year = int(szse_meta["report_year"])
            filename_report_period = str(szse_meta["report_period"])
            filename_report_type = str(szse_meta["report_type"])
            filename_report_label = str(szse_meta["report_label"])

    report_year = _require_report_field(
        "report_year",
        _merge_field(
            "report_year",
            filename_report_year,
            title_meta["report_year"] if title_meta is not None else None,
        ),
    )
    report_period = _require_report_field(
        "report_period",
        _merge_field(
            "report_period",
            filename_report_period,
            title_meta["report_period"] if title_meta is not None else None,
        ),
    )
    report_type = _require_report_field(
        "report_type",
        _merge_field(
            "report_type",
            filename_report_type,
            title_meta["report_type"] if title_meta is not None else None,
        ),
    )
    report_label = _require_report_field(
        "report_label",
        _merge_field(
            "report_label",
            filename_report_label,
            title_meta["report_label"] if title_meta is not None else None,
        ),
    )
    stock_code = _merge_field(
        "stock_code", filename_stock_code, pdf_security_meta["stock_code"]
    )
    stock_abbr = _merge_field(
        "stock_abbr", filename_stock_abbr, pdf_security_meta["stock_abbr"]
    )
    if stock_code is None and stock_abbr is None:
        raise ServiceException(
            ErrorCode.PARAM_ERROR,
            "未能从文件名或 PDF 元数据中解析股票代码或股票简称",
        )

    if stock_code is not None:
        try:
            company = _load_company_by_stock_code(db, str(stock_code))
        except ServiceException as exc:
            if exc.code != ErrorCode.DATA_NOT_FOUND:
                raise

            inferred_exchange = filename_exchange or _infer_exchange_from_stock_code(
                str(stock_code)
            )
            normalized_stock_abbr = _normalize_stock_abbr_value(
                stock_abbr if stock_abbr is not None else None
            )
            if inferred_exchange is None or not normalized_stock_abbr:
                raise

            company_name = ""
            if title_meta is not None:
                company_name = _normalize_stock_abbr_value(
                    str(title_meta.get("company_name") or "")
                )
            if not company_name:
                company_name = normalized_stock_abbr

            company = _create_pdf_derived_company(
                db,
                stock_code=str(stock_code),
                stock_abbr=normalized_stock_abbr,
                company_name=company_name,
                exchange=inferred_exchange,
                source_file_name=source_file_name,
            )
        if stock_abbr is not None and stock_abbr != company.stock_abbr:
            if _is_equivalent_stock_abbr(stock_abbr, company.stock_abbr):
                logger.warning(
                    f"财报文件中的股票简称 {stock_abbr} 与主数据 {company.stock_abbr} 仅存在格式差异，按一致处理"
                )
            else:
                logger.warning(
                    f"财报文件中的股票简称 {stock_abbr} 与附件1主数据 {company.stock_abbr} 不一致，但 stock_code={stock_code} 一致，继续处理"
                )
    else:
        company = _load_company_by_stock_abbr(db, str(stock_abbr))
        stock_code = company.stock_code

    final_exchange = filename_exchange or company.exchange
    if final_exchange != company.exchange:
        raise ServiceException(
            ErrorCode.PARAM_ERROR,
            f"财报来源交易所 {final_exchange} 与附件1主数据 {company.exchange} 不一致",
        )

    report_date = filename_report_date or _parse_explicit_date_from_text(preview_text)
    report_title = ""
    if title_meta is not None:
        report_title = str(title_meta["report_title"]).strip()
    if not report_title:
        report_title = f"{company.company_name} {report_year}年{report_label}"

    return ResolvedFinancialReportMetadata(
        stock_code=company.stock_code,
        stock_abbr=company.stock_abbr,
        exchange=company.exchange,
        report_year=int(report_year),
        report_period=models_financial_report.normalize_report_period(
            str(report_period)
        ),
        report_type=str(report_type),
        report_label=str(report_label),
        report_title=report_title,
        report_date=report_date,
    )


def upsert_financial_report_from_source(
    db: Session,
    source_file_name: str,
    file_path: str,
    structured_json_path: str | None = None,
):
    """根据PDF源文件信息新建或更新财报主表"""
    metadata = resolve_financial_report_metadata(db, source_file_name, file_path)
    entity = db.execute(
        select(models_financial_report.FinancialReport).where(
            models_financial_report.FinancialReport.stock_code == metadata.stock_code,
            models_financial_report.FinancialReport.report_year == metadata.report_year,
            models_financial_report.FinancialReport.report_period
            == metadata.report_period,
            models_financial_report.FinancialReport.report_type == metadata.report_type,
        )
    ).scalar_one_or_none()

    if entity is None:
        entity = models_financial_report.FinancialReport(
            stock_code=metadata.stock_code,
            stock_abbr=metadata.stock_abbr,
            exchange=metadata.exchange,
            report_year=metadata.report_year,
            report_period=metadata.report_period,
            report_type=metadata.report_type,
            report_label=metadata.report_label,
            report_title=metadata.report_title,
            report_date=metadata.report_date,
            period_sort_key=models_financial_report.get_period_sort_key(
                metadata.report_period
            ),
            source_priority=models_financial_report.get_report_source_priority(
                metadata.report_type
            ),
            source_file_name=source_file_name,
            storage_path=file_path,
            structured_json_path=structured_json_path,
            parse_status=schemas_financial_report.ParseStatus.PENDING,
            review_status=schemas_financial_report.ReviewStatus.PENDING,
            validate_status=schemas_financial_report.ValidateStatus.PENDING,
            validate_message=None,
            import_status=schemas_financial_report.ImportStatus.SUCCESS,
            vector_status=schemas_financial_report.VectorStatus.PENDING,
            vector_model=settings.EMBEDDING_MODEL,
            vector_dim=settings.EMBEDDING_DIM,
            vector_version=_get_vector_version(),
            vector_error_message=None,
            vectorized_at=None,
        )
        db.add(entity)
        commit_or_rollback(db)
        db.refresh(entity)
        return schemas_financial_report.FinancialReportResponse.model_validate(entity)

    entity.stock_abbr = metadata.stock_abbr
    entity.exchange = metadata.exchange
    entity.report_label = metadata.report_label
    entity.report_title = metadata.report_title
    if metadata.report_date is not None or entity.report_date is None:
        entity.report_date = metadata.report_date
    entity.period_sort_key = models_financial_report.get_period_sort_key(
        metadata.report_period
    )
    entity.source_priority = models_financial_report.get_report_source_priority(
        metadata.report_type
    )
    entity.source_file_name = source_file_name
    entity.storage_path = file_path
    entity.structured_json_path = structured_json_path
    entity.parse_status = schemas_financial_report.ParseStatus.PENDING
    entity.review_status = schemas_financial_report.ReviewStatus.PENDING
    entity.validate_status = schemas_financial_report.ValidateStatus.PENDING
    entity.validate_message = None
    entity.import_status = schemas_financial_report.ImportStatus.SUCCESS
    entity.vector_status = schemas_financial_report.VectorStatus.PENDING
    entity.vector_model = settings.EMBEDDING_MODEL
    entity.vector_dim = settings.EMBEDDING_DIM
    entity.vector_version = _get_vector_version()
    entity.vector_error_message = None
    entity.vectorized_at = None
    commit_or_rollback(db)
    db.refresh(entity)
    return schemas_financial_report.FinancialReportResponse.model_validate(entity)


def validate_structured_report_identity(
    data: dict[str, list],
    financial_report: models_financial_report.FinancialReport,
):
    """校验结构化数据中每条记录的身份字段与财报主表一致"""
    for records in data.values():
        if not records:
            continue

        for record in records:
            if record is None:
                continue
            if not isinstance(record, dict):
                raise ServiceException(
                    ErrorCode.PARAM_ERROR,
                    "结构化结果中的记录必须是对象",
                )

            raw_stock_code = record.get("stock_code")
            if raw_stock_code not in (None, ""):
                normalized_code = (
                    models_company_basic_info.normalize_company_stock_code(
                        str(raw_stock_code)
                    )
                )
                if normalized_code != financial_report.stock_code:
                    raise ServiceException(
                        ErrorCode.PARAM_ERROR,
                        "结构化结果中的 stock_code 与财报主表身份不一致",
                    )

            raw_stock_abbr = record.get("stock_abbr")
            if raw_stock_abbr not in (None, ""):
                normalized_abbr = str(raw_stock_abbr).strip()
                if normalized_abbr != financial_report.stock_abbr:
                    raise ServiceException(
                        ErrorCode.PARAM_ERROR,
                        "结构化结果中的 stock_abbr 与财报主表身份不一致",
                    )

            raw_report_year = record.get("report_year")
            if raw_report_year not in (None, ""):
                normalized_year = int(str(raw_report_year).strip())
                if normalized_year != financial_report.report_year:
                    raise ServiceException(
                        ErrorCode.PARAM_ERROR,
                        "结构化结果中的 report_year 与财报主表身份不一致",
                    )

            raw_report_period = record.get("report_period")
            if raw_report_period not in (None, ""):
                normalized_period = models_financial_report.normalize_report_period(
                    str(raw_report_period)
                )
                if normalized_period != financial_report.report_period:
                    raise ServiceException(
                        ErrorCode.PARAM_ERROR,
                        "结构化结果中的 report_period 与财报主表身份不一致",
                    )

            raw_report_type = record.get("report_type")
            if raw_report_type not in (None, ""):
                normalized_type = models_financial_report.normalize_report_type(
                    str(raw_report_type)
                )
                if normalized_type != financial_report.report_type:
                    raise ServiceException(
                        ErrorCode.PARAM_ERROR,
                        "结构化结果中的 report_type 与财报主表身份不一致",
                    )


def build_report_fact_identity_payload(
    financial_report: models_financial_report.FinancialReport,
):
    """构建财报身份标识字典，用于向下游传递"""
    return {
        "stock_code": financial_report.stock_code,
        "stock_abbr": financial_report.stock_abbr,
        "report_year": financial_report.report_year,
        "report_period": financial_report.report_period,
        "report_type": financial_report.report_type,
    }


"""辅助函数"""


def _get_vector_version():
    """生成向量模型版本标识字符串"""
    return (
        f"{settings.EMBEDDING_MODEL}:"
        f"{settings.EMBEDDING_DIM}:"
        f"{settings.CHUNK_SIZE}:"
        f"{settings.CHUNK_OVERLAP}"
    )


def _normalize_file_name(source_file_name: str):
    """规范化文件名并校验非空"""
    normalized = str(source_file_name).strip()
    if not normalized:
        raise ServiceException(ErrorCode.PARAM_ERROR, "上传文件名不能为空")
    return normalized


def _normalize_preview_text(text: str):
    """规范化PDF预读文本，合并空白和换行"""
    normalized = text.replace("\r", "\n")
    normalized = re.sub(r"[ \t]+", " ", normalized)
    normalized = re.sub(r"\n+", "\n", normalized)
    return normalized.strip()


def _normalize_name_token(raw_value: str | None):
    """将名称令牌统一格式化，去除空白并统一括号"""
    if raw_value is None:
        return ""

    normalized = str(raw_value).strip()
    normalized = normalized.replace("（", "(").replace("）", ")")
    normalized = re.sub(r"[\s　]+", "", normalized)
    return normalized


def _normalize_stock_abbr_value(raw_value: str | None):
    """规范化股票简称，去除前缀修饰和多余空白"""
    if raw_value is None:
        return ""

    normalized = str(raw_value).strip()
    normalized = re.sub(r"^[（(]\s*如有\s*[)）]\s*", "", normalized)
    normalized = re.sub(r"^[（(]\s*if\s+any\s*[)）]\s*", "", normalized, flags=re.IGNORECASE)
    normalized = re.sub(r"[ \t　]+", " ", normalized)
    return normalized


def _is_valid_stock_abbr_candidate(value: str):
    """判断股票简称候选值是否为真实简称，过滤 PDF 表格表头误命中"""
    normalized = _normalize_name_token(value)
    if not normalized:
        return False

    return not any(
        _normalize_name_token(keyword) in normalized
        for keyword in STOCK_ABBR_HEADER_KEYWORDS
    )


def _is_equivalent_stock_abbr(
    preferred: object | None,
    fallback: object | None,
):
    """判断两个股票简称在归一化后是否等价"""
    if preferred in (None, "") or fallback in (None, ""):
        return False
    return _normalize_name_token(str(preferred)) == _normalize_name_token(str(fallback))


def _extract_pdf_preview_text(file_path: str, max_pages: int = 12):
    """从PDF文件提取前N页预读文本"""
    try:
        reader = PdfReader(file_path)
    except Exception as exc:
        raise ServiceException(
            ErrorCode.PARAM_ERROR,
            "无法读取财报 PDF 内容，不能建立财报主表身份",
        ) from exc

    texts: list[str] = []
    for page in reader.pages[:max_pages]:
        page_text = page.extract_text() or ""
        if page_text.strip():
            texts.append(page_text)

    preview_text = _normalize_preview_text("\n".join(texts))
    if not preview_text:
        raise ServiceException(
            ErrorCode.PARAM_ERROR,
            "财报 PDF 首页未提取到有效文本，不能建立财报主表身份",
        )
    return preview_text


def _parse_report_date_token(raw_value: str):
    """将YYYYMMDD格式字符串解析为日期对象"""
    return datetime.strptime(raw_value, "%Y%m%d").date()


def _parse_explicit_date_from_text(preview_text: str):
    """从PDF预读文本中提取唯一的显式日期"""
    dates: set[date] = set()
    for match in EXPLICIT_DATE_PATTERN.finditer(preview_text):
        try:
            dates.add(
                date(
                    year=int(match.group("year")),
                    month=int(match.group("month")),
                    day=int(match.group("day")),
                )
            )
        except ValueError:
            continue

    if len(dates) == 1:
        return next(iter(dates))
    return None


def _parse_report_title_meta(preview_text: str):
    """从PDF预读文本中解析报告标题元数据"""
    match = REPORT_TITLE_PATTERN.search(preview_text)
    if match is None:
        return None

    normalized_report_label = re.sub(r"\s+", "", match.group("report_label"))
    report_period, report_type, report_label = constants_financial_report.REPORT_LABEL_TO_META[
        normalized_report_label
    ]
    return {
        "report_title": " ".join(str(match.group("title")).split()),
        "company_name": " ".join(str(match.group("company_name")).split()),
        "report_year": int(match.group("report_year")),
        "report_period": report_period,
        "report_type": report_type,
        "report_label": report_label,
    }


def _parse_szse_file_name_meta(
    normalized_file_name: str,
):
    """解析深交所格式文件名中的报告元数据"""
    match = SZSE_FILE_NAME_PATTERN.match(normalized_file_name)
    if match is None:
        return None

    stock_abbr = match.group("stock_abbr").strip()
    report_body = match.group("report_body").strip()
    body_match = SZSE_REPORT_BODY_PATTERN.match(report_body)
    if body_match is None:
        return None

    company_prefix = (body_match.group("company_prefix") or "").strip()
    if company_prefix and (
        _normalize_name_token(company_prefix) != _normalize_name_token(stock_abbr)
    ):
        return None

    normalized_report_label = re.sub(r"\s+", "", body_match.group("report_label"))
    report_period, report_type, report_label = constants_financial_report.REPORT_LABEL_TO_META[
        normalized_report_label
    ]
    return {
        "stock_abbr": stock_abbr,
        "report_year": int(body_match.group("report_year")),
        "report_period": report_period,
        "report_type": report_type,
        "report_label": report_label,
    }


def _extract_unique_pattern_value(
    *,
    preview_text: str,
    patterns: tuple[re.Pattern[str], ...],
    group_name: str,
    display_name: str,
    normalizer,
    dedupe_key_builder,
    candidate_filter=None,
):
    """从文本中通过正则模式提取唯一值，冲突时报错"""
    unique_values: dict[str, str] = {}
    for pattern in patterns:
        for match in pattern.finditer(preview_text):
            raw_value = match.group(group_name)
            normalized_value = normalizer(raw_value)
            if not normalized_value:
                continue
            if candidate_filter is not None and not candidate_filter(normalized_value):
                continue

            dedupe_key = dedupe_key_builder(normalized_value)
            existing_value = unique_values.get(dedupe_key)
            if existing_value is None or len(normalized_value) > len(existing_value):
                unique_values[dedupe_key] = normalized_value

    resolved_values = list(unique_values.values())
    if not resolved_values:
        return None
    if len(resolved_values) > 1:
        raise ServiceException(
            ErrorCode.PARAM_ERROR,
            f"PDF 预读内容中解析出多个不同的{display_name}，无法唯一确定财报身份：{resolved_values}",
        )
    return resolved_values[0]


def _parse_pdf_security_meta(preview_text: str):
    """从PDF预读文本中解析证券代码和简称"""
    primary_stock_abbr = _extract_unique_pattern_value(
        preview_text=preview_text,
        patterns=PDF_PRIMARY_STOCK_ABBR_PATTERNS,
        group_name="stock_abbr",
        display_name="股票简称",
        normalizer=_normalize_stock_abbr_value,
        dedupe_key_builder=_normalize_name_token,
        candidate_filter=_is_valid_stock_abbr_candidate,
    )
    fallback_stock_abbr = None
    if primary_stock_abbr is None:
        fallback_stock_abbr = _extract_unique_pattern_value(
            preview_text=preview_text,
            patterns=PDF_FALLBACK_STOCK_ABBR_PATTERNS,
            group_name="stock_abbr",
            display_name="股票简称",
            normalizer=_normalize_stock_abbr_value,
            dedupe_key_builder=_normalize_name_token,
            candidate_filter=_is_valid_stock_abbr_candidate,
        )

    return {
        "stock_code": _extract_unique_pattern_value(
            preview_text=preview_text,
            patterns=PDF_STOCK_CODE_PATTERNS,
            group_name="stock_code",
            display_name="股票代码",
            normalizer=models_company_basic_info.normalize_company_stock_code,
            dedupe_key_builder=lambda value: value,
        ),
        "stock_abbr": primary_stock_abbr or fallback_stock_abbr,
    }


def _merge_field(
    field_name: str,
    preferred: object | None,
    fallback: object | None,
):
    """合并文件名与PDF内容中同一字段的值，冲突时报错"""
    if preferred is None:
        return fallback
    if fallback is None:
        return preferred
    if field_name == "stock_abbr" and _is_equivalent_stock_abbr(preferred, fallback):
        return preferred
    if preferred != fallback:
        raise ServiceException(
            ErrorCode.PARAM_ERROR,
            f"文件名与 PDF 内容解析出的 {field_name} 不一致：{preferred} != {fallback}",
        )
    return preferred


def _require_report_field(field_name: str, value: object | None):
    """校验报告必要字段非空，为空时抛出异常"""
    field_label_map = {
        "report_year": "报告年份",
        "report_period": "报告期间",
        "report_type": "报告类型",
        "report_label": "报告标签",
    }
    if value in (None, ""):
        raise ServiceException(
            ErrorCode.PARAM_ERROR,
            f"未能唯一确定财报{field_label_map.get(field_name, field_name)}",
        )
    return value


def _load_company_by_stock_code(
    db: Session, stock_code: str
):
    """根据股票代码查询公司主数据"""
    normalized_code = models_company_basic_info.normalize_company_stock_code(stock_code)
    company = db.execute(
        select(models_company_basic_info.CompanyBasicInfo).where(
            models_company_basic_info.CompanyBasicInfo.stock_code == normalized_code
        )
    ).scalar_one_or_none()
    if company is None:
        raise ServiceException(
            ErrorCode.DATA_NOT_FOUND,
            f"company_basic_info 中不存在股票代码 {normalized_code}，请先导入附件1公司信息",
        )
    return company


def _load_company_by_stock_abbr(
    db: Session, stock_abbr: str
):
    """根据股票简称查询公司主数据，支持模糊匹配"""
    normalized_abbr = _normalize_stock_abbr_value(stock_abbr)
    if not normalized_abbr:
        raise ServiceException(ErrorCode.PARAM_ERROR, "股票简称不能为空")

    companies = (
        db.execute(
            select(models_company_basic_info.CompanyBasicInfo).where(
                models_company_basic_info.CompanyBasicInfo.stock_abbr == normalized_abbr
            )
        )
        .scalars()
        .all()
    )
    if not companies:
        normalized_key = _normalize_name_token(normalized_abbr)
        companies = [
            company
            for company in db.execute(
                select(models_company_basic_info.CompanyBasicInfo)
            ).scalars()
            if _normalize_name_token(company.stock_abbr) == normalized_key
        ]
    if not companies:
        raise ServiceException(
            ErrorCode.DATA_NOT_FOUND,
            f"company_basic_info 中不存在股票简称 {normalized_abbr}，请先导入附件1公司信息",
        )
    if len(companies) > 1:
        raise ServiceException(
            ErrorCode.PARAM_ERROR,
            f"company_basic_info 中股票简称 {normalized_abbr} 对应多家公司，无法唯一确定财报归属",
        )
    return companies[0]


def _infer_exchange_from_stock_code(stock_code: str):
    """根据股票代码前缀推断所属交易所"""
    normalized_code = models_company_basic_info.normalize_company_stock_code(stock_code)
    if normalized_code.startswith(("600", "601", "603", "605", "688", "689", "900")):
        return "SH"
    if normalized_code.startswith(
        ("000", "001", "002", "003", "300", "301", "200")
    ):
        return "SZ"
    if normalized_code.startswith(("400", "430", "800", "830", "831", "832", "833")):
        return "BJ"
    return None


def _create_pdf_derived_company(
    db: Session,
    *,
    stock_code: str,
    stock_abbr: str,
    company_name: str,
    exchange: str,
    source_file_name: str,
):
    """根据PDF解析的身份信息补建公司最小主数据"""
    normalized_code = models_company_basic_info.normalize_company_stock_code(stock_code)
    normalized_abbr = _normalize_stock_abbr_value(stock_abbr)
    normalized_company_name = _normalize_stock_abbr_value(company_name) or normalized_abbr
    listed_exchange = constants_financial_report.DERIVED_LISTED_EXCHANGE_MAP[exchange]
    entity = models_company_basic_info.CompanyBasicInfo(
        stock_code=normalized_code,
        stock_abbr=normalized_abbr,
        company_name=normalized_company_name,
        listed_exchange=listed_exchange,
        exchange=exchange,
        source_row_no=900000000 + int(normalized_code),
        source_file_name=source_file_name,
    )
    db.add(entity)
    db.flush()
    logger.warning(
        f"company_basic_info 缺少股票代码 {normalized_code}，已根据 PDF 身份信息补建最小主数据"
    )
    return entity
