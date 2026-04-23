from sqlalchemy import (
    Boolean,
    CHAR,
    CheckConstraint,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    func,
    text,
)
from sqlalchemy.orm import validates

from app.db.database import Base
from app.models.company_basic_info import normalize_company_stock_code
from app.models.financial_report import (
    ALLOWED_REPORT_PERIODS,
    ALLOWED_REPORT_TYPES,
    normalize_report_period,
    normalize_report_type,
)


VALIDATION_STAGE_FILE_ARCHIVE = "FILE_ARCHIVE"
VALIDATION_STAGE_REPORT_IDENTITY = "REPORT_IDENTITY"
VALIDATION_STAGE_STRUCTURED_EXTRACT = "STRUCTURED_EXTRACT"
VALIDATION_STAGE_STRUCTURED_VALIDATE = "STRUCTURED_VALIDATE"
VALIDATION_STAGE_FACT_PERSIST = "FACT_PERSIST"
ALLOWED_VALIDATION_STAGES = (
    VALIDATION_STAGE_FILE_ARCHIVE,
    VALIDATION_STAGE_REPORT_IDENTITY,
    VALIDATION_STAGE_STRUCTURED_EXTRACT,
    VALIDATION_STAGE_STRUCTURED_VALIDATE,
    VALIDATION_STAGE_FACT_PERSIST,
)

VALIDATION_CHECK_TYPE_FILE_RULE = "FILE_RULE"
VALIDATION_CHECK_TYPE_PDF_METADATA = "PDF_METADATA"
VALIDATION_CHECK_TYPE_COMPANY_MATCH = "COMPANY_MATCH"
VALIDATION_CHECK_TYPE_STRUCT_SCHEMA = "STRUCT_SCHEMA"
VALIDATION_CHECK_TYPE_STRUCT_VALUE = "STRUCT_VALUE"
VALIDATION_CHECK_TYPE_FACT_SYNC = "FACT_SYNC"
VALIDATION_CHECK_TYPE_PIPELINE = "PIPELINE"
ALLOWED_VALIDATION_CHECK_TYPES = (
    VALIDATION_CHECK_TYPE_FILE_RULE,
    VALIDATION_CHECK_TYPE_PDF_METADATA,
    VALIDATION_CHECK_TYPE_COMPANY_MATCH,
    VALIDATION_CHECK_TYPE_STRUCT_SCHEMA,
    VALIDATION_CHECK_TYPE_STRUCT_VALUE,
    VALIDATION_CHECK_TYPE_FACT_SYNC,
    VALIDATION_CHECK_TYPE_PIPELINE,
)

VALIDATION_LOG_STATUS_PROCESSING = "PROCESSING"
VALIDATION_LOG_STATUS_PASSED = "PASSED"
VALIDATION_LOG_STATUS_FAILED = "FAILED"
ALLOWED_VALIDATION_LOG_STATUSES = (
    VALIDATION_LOG_STATUS_PROCESSING,
    VALIDATION_LOG_STATUS_PASSED,
    VALIDATION_LOG_STATUS_FAILED,
)

VALIDATION_STAGES_SQL = ", ".join(f"'{item}'" for item in ALLOWED_VALIDATION_STAGES)
VALIDATION_CHECK_TYPES_SQL = ", ".join(
    f"'{item}'" for item in ALLOWED_VALIDATION_CHECK_TYPES
)
VALIDATION_LOG_STATUSES_SQL = ", ".join(
    f"'{item}'" for item in ALLOWED_VALIDATION_LOG_STATUSES
)
REPORT_PERIODS_SQL = ", ".join(f"'{item}'" for item in ALLOWED_REPORT_PERIODS)
REPORT_TYPES_SQL = ", ".join(f"'{item}'" for item in ALLOWED_REPORT_TYPES)


def normalize_validation_stage(value: str) -> str:
    normalized = str(value).strip().upper()
    if normalized not in ALLOWED_VALIDATION_STAGES:
        raise ValueError(
            f"stage 只允许 {', '.join(ALLOWED_VALIDATION_STAGES)}，当前值：{value}"
        )
    return normalized


def normalize_validation_check_type(value: str) -> str:
    normalized = str(value).strip().upper()
    if normalized not in ALLOWED_VALIDATION_CHECK_TYPES:
        raise ValueError(
            f"check_type 只允许 {', '.join(ALLOWED_VALIDATION_CHECK_TYPES)}，当前值：{value}"
        )
    return normalized


def normalize_validation_log_status(value: str) -> str:
    normalized = str(value).strip().upper()
    if normalized not in ALLOWED_VALIDATION_LOG_STATUSES:
        raise ValueError(
            f"status 只允许 {', '.join(ALLOWED_VALIDATION_LOG_STATUSES)}，当前值：{value}"
        )
    return normalized


class ValidationLog(Base):
    __tablename__ = "validation_log"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="校验日志ID")
    report_id = Column(
        Integer,
        ForeignKey("financial_report.id", ondelete="SET NULL"),
        nullable=True,
        comment="财报主表ID，文件建档前允许为空",
    )
    source_file_name = Column(String(255), comment="源文件名")
    stock_code = Column(CHAR(6), comment="股票代码快照")
    stock_abbr = Column(String(50), comment="股票简称快照")
    report_year = Column(Integer, comment="报告年份快照")
    report_period = Column(String(2), comment="报告期快照：Q1/HY/Q3/FY")
    report_type = Column(String(10), comment="报告类型快照：REPORT/SUMMARY")
    stage = Column(String(32), nullable=False, comment="主流程阶段")
    check_type = Column(String(32), nullable=False, comment="校验维度")
    status = Column(String(16), nullable=False, comment="阶段状态")
    is_blocking = Column(
        Boolean,
        nullable=False,
        default=False,
        server_default=text("0"),
        comment="是否阻断主流程",
    )
    error_code = Column(Integer, comment="错误码")
    message = Column(Text, nullable=False, comment="可读校验结果")
    details_json = Column(Text, comment="结构化细节JSON")
    created_at = Column(
        DateTime,
        server_default=func.now(),
        nullable=False,
        comment="创建时间",
    )
    updated_at = Column(
        DateTime,
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
        comment="更新时间",
    )

    @validates("stage")
    def validate_stage(self, _key: str, value: str) -> str:
        return normalize_validation_stage(value)

    @validates("check_type")
    def validate_check_type(self, _key: str, value: str) -> str:
        return normalize_validation_check_type(value)

    @validates("status")
    def validate_status(self, _key: str, value: str) -> str:
        return normalize_validation_log_status(value)

    @validates("stock_code")
    def validate_stock_code(self, _key: str, value: str | None) -> str | None:
        if value is None:
            return None
        return normalize_company_stock_code(value)

    @validates("stock_abbr", "source_file_name")
    def normalize_optional_short_text(
        self,
        _key: str,
        value: str | None,
    ) -> str | None:
        if value is None:
            return None
        normalized = str(value).strip()
        return normalized or None

    @validates("report_year")
    def validate_report_year(self, _key: str, value: int | str | None) -> int | None:
        if value is None:
            return None
        normalized = int(str(value).strip())
        if normalized < 2000 or normalized > 2100:
            raise ValueError(f"report_year 超出允许范围，当前值：{value}")
        return normalized

    @validates("report_period")
    def validate_report_period(self, _key: str, value: str | None) -> str | None:
        if value is None:
            return None
        return normalize_report_period(value)

    @validates("report_type")
    def validate_report_type(self, _key: str, value: str | None) -> str | None:
        if value is None:
            return None
        return normalize_report_type(value)

    @validates("message", "details_json")
    def normalize_text_fields(self, key: str, value: str | None) -> str | None:
        if value is None:
            if key == "message":
                raise ValueError("message 不能为空")
            return None

        normalized = str(value).strip()
        if key == "message" and not normalized:
            raise ValueError("message 不能为空")
        return normalized or None

    @validates("error_code")
    def validate_error_code(self, _key: str, value: int | str | None) -> int | None:
        if value is None:
            return None
        normalized = int(str(value).strip())
        if normalized < 0:
            raise ValueError("error_code 不能为负数")
        return normalized

    __table_args__ = (
        CheckConstraint(
            "stock_code IS NULL OR length(stock_code) = 6",
            name="ck_validation_log_stock_code_length",
        ),
        CheckConstraint(
            "report_year IS NULL OR (report_year >= 2000 AND report_year <= 2100)",
            name="ck_validation_log_report_year",
        ),
        CheckConstraint(
            f"report_period IS NULL OR report_period IN ({REPORT_PERIODS_SQL})",
            name="ck_validation_log_report_period",
        ),
        CheckConstraint(
            f"report_type IS NULL OR report_type IN ({REPORT_TYPES_SQL})",
            name="ck_validation_log_report_type",
        ),
        CheckConstraint(
            f"stage IN ({VALIDATION_STAGES_SQL})",
            name="ck_validation_log_stage",
        ),
        CheckConstraint(
            f"check_type IN ({VALIDATION_CHECK_TYPES_SQL})",
            name="ck_validation_log_check_type",
        ),
        CheckConstraint(
            f"status IN ({VALIDATION_LOG_STATUSES_SQL})",
            name="ck_validation_log_status",
        ),
        CheckConstraint(
            "error_code IS NULL OR error_code >= 0",
            name="ck_validation_log_error_code",
        ),
        Index(
            "idx_validation_log_report_stage_created",
            "report_id",
            "stage",
            "created_at",
        ),
        Index(
            "idx_validation_log_source_stage_created",
            "source_file_name",
            "stage",
            "created_at",
        ),
        Index(
            "idx_validation_log_report_identity",
            "stock_code",
            "report_year",
            "report_period",
            "report_type",
        ),
        Index(
            "idx_validation_log_status_stage",
            "status",
            "stage",
            "check_type",
        ),
    )
