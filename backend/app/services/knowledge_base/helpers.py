"""知识库服务跨文件共享辅助函数"""
import re

from app.utils.logger_config import setup_logger

logger = setup_logger(__name__)

_metadata_cache: dict | None = None
_metadata_cache_loaded: bool = False


def clean_pdf_text(raw: str):
    """清洗 PDF 提取文本"""
    text = raw.strip()
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def get_metadata_map():
    """获取元数据映射缓存"""
    global _metadata_cache, _metadata_cache_loaded

    if _metadata_cache_loaded and _metadata_cache is not None:
        return _metadata_cache

    try:
        from app.services.fujian5_data_processor import parse_fujian5_excel_data

        stock_research_data, industry_research_data = parse_fujian5_excel_data()

        metadata_map: dict = {}

        for record in stock_research_data:
            title = record.get("title", "").strip()
            if title:
                metadata_map[title] = {
                    "stock_code": record.get("stockCode"),
                    "stock_abbr": record.get("stockName"),
                    "org_name": record.get("orgName"),
                    "publish_date": record.get("publishDate"),
                    "industry_name": None,
                    "doc_type": "RESEARCH_REPORT",
                }

        for record in industry_research_data:
            title = record.get("title", "").strip()
            if title:
                metadata_map[title] = {
                    "stock_code": None,
                    "stock_abbr": None,
                    "org_name": record.get("orgName"),
                    "publish_date": record.get("publishDate"),
                    "industry_name": record.get("industryName"),
                    "doc_type": "INDUSTRY_REPORT",
                }

        _metadata_cache = metadata_map
        _metadata_cache_loaded = True
        logger.info(f"元数据缓存加载完成: {len(metadata_map)} 条")
        return metadata_map

    except Exception as e:
        logger.error(f"加载元数据缓存失败: {e}")
        _metadata_cache = {}
        _metadata_cache_loaded = True
        return {}


def reload_metadata_cache():
    """重新加载元数据缓存"""
    global _metadata_cache_loaded
    _metadata_cache_loaded = False
    return get_metadata_map()
