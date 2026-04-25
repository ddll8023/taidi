"""事实表数据入库服务：构建持久化数据包并同步写入数据库"""
from typing import Any, NamedTuple

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.schemas import financial_report as schemas_financial_report
from app.services import financial_report as services_financial_report
from app.constants.analysis_data import FACT_MODEL_MAP, FACT_IDENTITY_FIELDS
from app.schemas.common import ErrorCode
from app.utils.exception import ServiceException
from app.utils.logger_config import setup_logger

logger = setup_logger(__name__)


class _PreparedFactRecord(NamedTuple):
    table_name: str
    payload: dict[str, Any]


class _ReportPersistenceBundle(NamedTuple):
    financial_report: Any
    fact_records: tuple[_PreparedFactRecord, ...]


def _build_report_persistence_bundle(
    financial_report,
    normalized_records: dict[str, dict[str, Any]],
):
    """基于已通过校验的结构化结果，准备事实表快照入库数据。"""
    report_identity = services_financial_report.build_report_fact_identity_payload(
        financial_report
    )
    prepared_fact_records = tuple(
        _PreparedFactRecord(
            table_name=table_name,
            payload=_build_fact_record_payload(
                FACT_MODEL_MAP[table_name],
                record,
                report_identity,
            ),
        )
        for table_name, record in normalized_records.items()
    )
    logger.info(
        "结构化结果已完成规范化校验: report_id=%s fact_tables=%s",
        financial_report.id,
        ",".join(record.table_name for record in prepared_fact_records),
    )
    return _ReportPersistenceBundle(
        financial_report=financial_report,
        fact_records=prepared_fact_records,
    )


def _persist_report_bundle(db: Session, bundle: _ReportPersistenceBundle):
    """同步主表与四张事实表，空表结果会清理旧残留行。"""
    prepared_map = {record.table_name: record for record in bundle.fact_records}
    report_id = bundle.financial_report.id

    for table_name, model_class in FACT_MODEL_MAP.items():
        stmt = select(model_class).where(model_class.report_id == report_id)
        existing = db.execute(stmt).scalar_one_or_none()
        prepared = prepared_map.get(table_name)

        if prepared is None:
            if existing is not None:
                db.delete(existing)
            continue

        persistence_payload = dict(prepared.payload)
        persistence_payload["report_id"] = report_id

        if existing is None:
            db.add(model_class(**persistence_payload))
            continue

        for key, value in persistence_payload.items():
            setattr(existing, key, value)

    bundle.financial_report.import_status = (
        schemas_financial_report.ImportStatus.SUCCESS
    )
    logger.info("事实表入库数据已同步: report_id=%s", report_id)


def _build_fact_record_payload(
    model_class,
    record: dict[str, Any],
    report_identity: dict[str, Any],
):
    """只接收模型指标字段，并由主表统一回填事实身份字段。"""
    if not isinstance(record, dict):
        raise ServiceException(
            ErrorCode.AI_SERVICE_ERROR, "结构化结果中的记录必须是对象"
        )

    payload = {
        column.name: record.get(column.name)
        for column in model_class.__table__.columns
        if column.name not in FACT_IDENTITY_FIELDS
    }
    payload.update(report_identity)
    return payload
