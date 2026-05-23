"""公司基本信息 Excel 导入服务"""

import os
import re
import tempfile

import pandas as pd
from fastapi import UploadFile
from sqlalchemy.orm import Session

from app.constants import (
    financial_report_base_info as constants_financial_report_base_info,
)
from app.db.database import commit_or_rollback
from app.models import company_basic_info as models_companysic_info
from app.schemas import import_company_base_info as schemas_import_company_base_info
from app.schemas.common import ErrorCode
from app.utils.exception import ServiceException
from app.utils.logger_config import setup_logger

logger = setup_logger(__name__)


def import_company_base_info(db: Session, file: UploadFile):
    """导入公司基本信息"""
    logger.info(f"开始导入公司基本信息: file={file.filename}")

    if not file.filename.endswith((".xlsx", ".xls")):
        logger.error(f"文件格式错误，仅支持xlsx和xls格式，实际文件名：{file.filename}")
        raise ServiceException(
            ErrorCode.PARAM_ERROR, "文件格式错误，仅支持xlsx和xls格式"
        )
    with tempfile.NamedTemporaryFile(
        delete=False, suffix=os.path.splitext(file.filename)[1]
    ) as f:
        f.write(file.file.read())
        temp_file_path = f.name

    try:
        df = pd.read_excel(temp_file_path, sheet_name=0)
        data = df.to_dict(orient="records")
    except Exception as exc:
        logger.error(
            f"Excel文件解析失败: file={file.filename} error={exc}", exc_info=True
        )
        raise ServiceException(
            ErrorCode.PARAM_ERROR, "Excel文件解析失败，请检查文件内容"
        ) from exc
    finally:
        try:
            logger.info(f"删除临时文件: file={temp_file_path}")
            os.unlink(temp_file_path)
        except Exception as exc:
            logger.error(
                f"删除临时文件失败: file={temp_file_path} error={exc}", exc_info=True
            )

    logger.info(f"Excel解析完成: file={file.filename} total_records={len(data)}")

    # 保存文件到数据库
    inserted_count = 0
    updated_count = 0

    for item in data:
        stock_code = str(
            item.get(
                constants_financial_report_base_info.FILE_COLUMN_MAPPING_DICT.get(
                    "stock_code"
                )
            )
        ).zfill(6)
        registered_capital_raw = item.get(
            constants_financial_report_base_info.FILE_COLUMN_MAPPING_DICT.get(
                "registered_capital_raw"
            )
        )
        registered_capital_yuan = _normalize_registered_capital(registered_capital_raw)

        # 根据股票代码查询是否已存在
        company_base_info_entity = db.get(
            models_companysic_info.CompanyBasicInfo, stock_code
        )

        # 如果存在，更新记录,否则插入新记录
        logger.debug(
            f"处理记录: stock_code={stock_code} 操作={'更新' if company_base_info_entity else '新增'}"
        )

        if company_base_info_entity:
            # 更新现有记录
            company_base_info_entity.stock_abbr = item.get(
                constants_financial_report_base_info.FILE_COLUMN_MAPPING_DICT.get(
                    "stock_abbr"
                )
            )
            company_base_info_entity.company_name = item.get(
                constants_financial_report_base_info.FILE_COLUMN_MAPPING_DICT.get(
                    "company_name"
                )
            )
            company_base_info_entity.english_name = item.get(
                constants_financial_report_base_info.FILE_COLUMN_MAPPING_DICT.get(
                    "english_name"
                )
            )
            company_base_info_entity.csrc_industry = item.get(
                constants_financial_report_base_info.FILE_COLUMN_MAPPING_DICT.get(
                    "csrc_industry"
                )
            )
            company_base_info_entity.listed_exchange = item.get(
                constants_financial_report_base_info.FILE_COLUMN_MAPPING_DICT.get(
                    "listed_exchange"
                )
            )
            company_base_info_entity.exchange = constants_financial_report_base_info.EXCHANGE_ALIAS_MAP.get(
                item.get(
                    constants_financial_report_base_info.FILE_COLUMN_MAPPING_DICT.get(
                        "listed_exchange"
                    )
                )
            )
            company_base_info_entity.security_category = item.get(
                constants_financial_report_base_info.FILE_COLUMN_MAPPING_DICT.get(
                    "security_category"
                )
            )
            company_base_info_entity.registered_region = item.get(
                constants_financial_report_base_info.FILE_COLUMN_MAPPING_DICT.get(
                    "registered_region"
                )
            )
            company_base_info_entity.registered_capital_raw = registered_capital_raw
            company_base_info_entity.registered_capital_yuan = registered_capital_yuan
            company_base_info_entity.employee_count = item.get(
                constants_financial_report_base_info.FILE_COLUMN_MAPPING_DICT.get(
                    "employee_count"
                )
            )
            company_base_info_entity.management_count = item.get(
                constants_financial_report_base_info.FILE_COLUMN_MAPPING_DICT.get(
                    "management_count"
                )
            )
            company_base_info_entity.source_row_no = item.get(
                constants_financial_report_base_info.FILE_COLUMN_MAPPING_DICT.get(
                    "source_row_no"
                )
            )
            company_base_info_entity.source_file_name = file.filename
            updated_count += 1
        else:
            # 插入新记录
            db_company_base_info = models_companysic_info.CompanyBasicInfo(
                stock_code=stock_code,
                stock_abbr=item.get(
                    constants_financial_report_base_info.FILE_COLUMN_MAPPING_DICT.get(
                        "stock_abbr"
                    )
                ),
                company_name=item.get(
                    constants_financial_report_base_info.FILE_COLUMN_MAPPING_DICT.get(
                        "company_name"
                    )
                ),
                english_name=item.get(
                    constants_financial_report_base_info.FILE_COLUMN_MAPPING_DICT.get(
                        "english_name"
                    )
                ),
                csrc_industry=item.get(
                    constants_financial_report_base_info.FILE_COLUMN_MAPPING_DICT.get(
                        "csrc_industry"
                    )
                ),
                listed_exchange=item.get(
                    constants_financial_report_base_info.FILE_COLUMN_MAPPING_DICT.get(
                        "listed_exchange"
                    )
                ),
                exchange=constants_financial_report_base_info.EXCHANGE_ALIAS_MAP.get(
                    item.get(
                        constants_financial_report_base_info.FILE_COLUMN_MAPPING_DICT.get(
                            "listed_exchange"
                        )
                    )
                ),
                security_category=item.get(
                    constants_financial_report_base_info.FILE_COLUMN_MAPPING_DICT.get(
                        "security_category"
                    )
                ),
                registered_region=item.get(
                    constants_financial_report_base_info.FILE_COLUMN_MAPPING_DICT.get(
                        "registered_region"
                    )
                ),
                registered_capital_raw=registered_capital_raw,
                registered_capital_yuan=registered_capital_yuan,
                employee_count=item.get(
                    constants_financial_report_base_info.FILE_COLUMN_MAPPING_DICT.get(
                        "employee_count"
                    )
                ),
                management_count=item.get(
                    constants_financial_report_base_info.FILE_COLUMN_MAPPING_DICT.get(
                        "management_count"
                    )
                ),
                source_row_no=item.get(
                    constants_financial_report_base_info.FILE_COLUMN_MAPPING_DICT.get(
                        "source_row_no"
                    )
                ),
                source_file_name=file.filename,
            )
            db.add(db_company_base_info)
            inserted_count += 1

    commit_or_rollback(db)

    logger.info(
        f"公司基本信息导入完成: file={file.filename} total={len(data)} inserted={inserted_count} updated={updated_count}"
    )

    # 返回结果
    return schemas_import_company_base_info.ImportCompanyBaseInfoResponse(
        total=len(data),
        inserted=inserted_count,
        updated=updated_count,
    )


"""辅助函数"""


def _normalize_registered_capital(capital_str: str | None = None):
    """将注册资本字符串标准化为以元为单位的数值"""
    if not capital_str:
        return None

    # 去除空格
    capital_str = str(capital_str).strip()

    # 匹配 "X亿元" 或 "X亿"
    billion_match = re.match(r"^([\d.]+)\s*亿\s*元?$", capital_str)
    if billion_match:
        return round(float(billion_match.group(1)) * 100000000, 2)

    # 匹配 "X万元" 或 "X万"
    million_match = re.match(r"^([\d.]+)\s*万\s*元?$", capital_str)
    if million_match:
        return round(float(million_match.group(1)) * 10000, 2)
