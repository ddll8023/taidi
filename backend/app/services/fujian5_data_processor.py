"""福建5表Excel解析服务"""
from __future__ import annotations

import io
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

from app.core.config import settings
from app.schemas.common import ErrorCode
from app.utils.exception import ServiceException
from app.utils.logger_config import setup_logger

logger = setup_logger(__name__)


# ========== 公共入口函数 ==========


def parse_stock_research_excel(file_path: str):
    """解析个股研报Excel文件"""
    if not os.path.exists(file_path):
        raise ServiceException(ErrorCode.DATA_NOT_FOUND, f"个股研报文件不存在: {file_path}")

    logger.info(f"开始解析个股研报 Excel: {file_path}")

    try:
        df = pd.read_excel(file_path)
    except Exception as e:
        logger.error(f"读取个股研报 Excel 失败: {file_path}, 错误: {e}")
        raise ServiceException(ErrorCode.PARAM_ERROR, f"读取 Excel 文件失败: {e}") from e

    return _parse_stock_research_df(df, "个股研报")


def parse_industry_research_excel(file_path: str):
    """解析行业研报Excel文件"""
    if not os.path.exists(file_path):
        raise ServiceException(ErrorCode.DATA_NOT_FOUND, f"行业研报文件不存在: {file_path}")

    logger.info(f"开始解析行业研报 Excel: {file_path}")

    try:
        df = pd.read_excel(file_path)
    except Exception as e:
        logger.error(f"读取行业研报 Excel 失败: {file_path}, 错误: {e}")
        raise ServiceException(ErrorCode.PARAM_ERROR, f"读取 Excel 文件失败: {e}") from e

    return _parse_industry_research_df(df, "行业研报")


def parse_stock_research_excel_from_upload(file_content: bytes):
    """从上传内容解析个股研报Excel"""
    logger.info("开始从上传文件解析个股研报 Excel")

    try:
        df = pd.read_excel(io.BytesIO(file_content))
    except Exception as e:
        logger.error(f"读取上传的个股研报 Excel 失败: {e}")
        raise ServiceException(ErrorCode.PARAM_ERROR, f"读取 Excel 文件失败: {e}") from e

    fields = list(df.columns)
    records = _parse_stock_research_df(df, "个股研报(上传)")
    return records, fields


def parse_industry_research_excel_from_upload(file_content: bytes):
    """从上传内容解析行业研报Excel"""
    logger.info("开始从上传文件解析行业研报 Excel")

    try:
        df = pd.read_excel(io.BytesIO(file_content))
    except Exception as e:
        logger.error(f"读取上传的行业研报 Excel 失败: {e}")
        raise ServiceException(ErrorCode.PARAM_ERROR, f"读取 Excel 文件失败: {e}") from e

    fields = list(df.columns)
    records = _parse_industry_research_df(df, "行业研报(上传)")
    return records, fields


def parse_fujian5_excel_data(
    fujian5_dir: str | None = None,
):
    """解析附件5研报数据的入口函数"""
    if fujian5_dir is None:
        fujian5_dir = settings.fujian5_UPLOAD_DIR

    logger.info(f"开始解析附件5研报数据,目录: {fujian5_dir}")

    stock_research_path = os.path.join(fujian5_dir, "个股_研报信息.xlsx")
    industry_research_path = os.path.join(fujian5_dir, "行业_研报信息.xlsx")

    stock_research_data = []
    industry_research_data = []

    if os.path.exists(stock_research_path):
        try:
            stock_research_data = parse_stock_research_excel(stock_research_path)
            logger.info(f"成功解析个股研报: {len(stock_research_data)} 条")
        except Exception as e:
            logger.error(f"解析个股研报失败: {e}")
            raise
    else:
        logger.warning(f"个股研报文件不存在: {stock_research_path}")

    if os.path.exists(industry_research_path):
        try:
            industry_research_data = parse_industry_research_excel(
                industry_research_path
            )
            logger.info(f"成功解析行业研报: {len(industry_research_data)} 条")
        except Exception as e:
            logger.error(f"解析行业研报失败: {e}")
            raise
    else:
        logger.warning(f"行业研报文件不存在: {industry_research_path}")

    logger.info(
        f"附件5研报数据解析完成: 个股 {len(stock_research_data)} 条, "
        f"行业 {len(industry_research_data)} 条"
    )

    return stock_research_data, industry_research_data


"""辅助函数"""


def _parse_stock_research_df(df: pd.DataFrame, source_label: str = "个股研报"):
    """解析个股研报DataFrame并提取有效记录"""
    required_columns = ["title", "stockCode", "stockName", "orgName", "publishDate"]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ServiceException(ErrorCode.PARAM_ERROR, f"{source_label}缺少必需字段: {missing_columns}")

    records = []
    for idx, row in df.iterrows():
        try:
            title = row["title"]
            if pd.isna(title) or not str(title).strip():
                continue
            title = str(title).strip()

            stock_code = row["stockCode"]
            if pd.isna(stock_code):
                continue
            try:
                stock_code = str(int(float(stock_code))).zfill(6)
            except (ValueError, TypeError):
                stock_code = str(stock_code).strip().zfill(6)

            stock_name = row["stockName"]
            if pd.isna(stock_name) or not str(stock_name).strip():
                continue
            stock_name = str(stock_name).strip()

            org_name = row["orgName"]
            if pd.isna(org_name) or not str(org_name).strip():
                continue
            org_name = str(org_name).strip()

            publish_date = row["publishDate"]
            if pd.isna(publish_date):
                continue

            try:
                publish_date_str = _normalize_publish_date(publish_date)
            except Exception as e:
                logger.warning(f"第 {idx + 2} 行日期格式错误: {publish_date}, 错误: {e}")
                continue

            record = {
                "title": title,
                "stockCode": stock_code,
                "stockName": stock_name,
                "orgName": org_name,
                "publishDate": publish_date_str,
            }
            records.append(record)

        except Exception as e:
            logger.error(f"解析第 {idx + 2} 行数据时出错: {e}")
            continue

    logger.info(f"成功解析{source_label} {len(records)} 条")
    return records


def _parse_industry_research_df(df: pd.DataFrame, source_label: str = "行业研报"):
    """解析行业研报DataFrame并提取有效记录"""
    required_columns = ["title", "industryName", "orgName", "publishDate"]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ServiceException(ErrorCode.PARAM_ERROR, f"{source_label}缺少必需字段: {missing_columns}")

    records = []
    for idx, row in df.iterrows():
        try:
            title = row["title"]
            if pd.isna(title) or not str(title).strip():
                continue
            title = str(title).strip()

            industry_name = row["industryName"]
            if pd.isna(industry_name) or not str(industry_name).strip():
                continue
            industry_name = str(industry_name).strip()

            org_name = row["orgName"]
            if pd.isna(org_name) or not str(org_name).strip():
                continue
            org_name = str(org_name).strip()

            publish_date = row["publishDate"]
            if pd.isna(publish_date):
                continue

            try:
                publish_date_str = _normalize_publish_date(publish_date)
            except Exception as e:
                logger.warning(f"第 {idx + 2} 行日期格式错误: {publish_date}, 错误: {e}")
                continue

            record = {
                "title": title,
                "industryName": industry_name,
                "orgName": org_name,
                "publishDate": publish_date_str,
            }
            records.append(record)

        except Exception as e:
            logger.error(f"解析第 {idx + 2} 行数据时出错: {e}")
            continue

    logger.info(f"成功解析{source_label} {len(records)} 条")
    return records


def _normalize_publish_date(publish_date):
    """将各种格式的发布日期统一转为YYYY-MM-DD字符串"""
    if isinstance(publish_date, (int, float)):
        publish_date = datetime(1899, 12, 30) + pd.Timedelta(days=int(publish_date))
        return publish_date.strftime("%Y-%m-%d")
    elif isinstance(publish_date, datetime):
        return publish_date.strftime("%Y-%m-%d")
    elif isinstance(publish_date, str):
        return pd.to_datetime(publish_date).strftime("%Y-%m-%d")
    else:
        return pd.to_datetime(publish_date).strftime("%Y-%m-%d")
