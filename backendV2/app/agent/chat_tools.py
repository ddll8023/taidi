"""聊天 Agent 工具函数集"""

import json
import logging
from decimal import Decimal

from langchain_core.tools import tool
from langchain_core.messages import SystemMessage, HumanMessage
from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from sqlalchemy import select, text

from app.core.config import settings
from app.db.database import get_background_db_session
from app.models import company_basic_info as models_company_basic_info
from app.schemas import knowledge_base as schemas_knowledge_base
from app.services import knowledge_base as services_knowledge_base
from app.utils.model_factory import get_model

logger = logging.getLogger(__name__)


def make_chat_tools() -> list:
    """创建聊天工具集（每个工具使用独立数据库会话）"""

    # ──── 工具1: 查询结构化财务数据 ────

    @tool
    def query_financial_data_tool(intent_json: str) -> str:
        """查询结构化财务数据（利润表、资产负债表、现金流量表、核心指标表）。
        传入 JSON 格式的查询意图描述，工具内部自动生成并执行 SQL 查询。

        intent_json 参数格式示例：
        {
            "description": "查询金花股份2025年第三季度净利润",
            "tables": ["income_sheet"],
            "metrics": [{"field": "net_profit", "display_name": "净利润", "table": "income_sheet"}],
            "filters": {"stock_code": "600080", "report_year": 2025, "report_period": "Q3"}
        }
        """
        tool_db = get_background_db_session()
        try:
            intent = json.loads(intent_json) if isinstance(intent_json, str) else intent_json

            # 获取行业列表（用于 SQL 生成 prompt）
            industry_values = tool_db.scalars(
                select(models_company_basic_info.CompanyBasicInfo.csrc_industry).distinct()
            ).all()

            # 调用 LLM 生成 SQL
            system_prompt = PromptTemplate.from_template(
                settings.PROMPT_CONFIG.get_chat_config["sql_generate"]["system_prompt"]
            ).format(industry_values=industry_values)

            user_prompt = PromptTemplate.from_template(
                settings.PROMPT_CONFIG.get_chat_config["sql_generate"]["user_prompt_template"]
            ).format(intent_json=json.dumps(intent, ensure_ascii=False))

            prompt = [
                SystemMessage(content=system_prompt),
                HumanMessage(content=user_prompt),
            ]
            model = get_model.chat_model
            chain = model | JsonOutputParser()
            result: dict = chain.invoke(prompt)
            sql = result.get("sql", "")

            if not sql:
                return "SQL 生成失败，请检查查询参数"

            # 执行 SQL
            result_rows = tool_db.execute(text(sql)).all()
            rows = [dict(row._mapping) for row in result_rows]

            # 统一 Decimal → float
            for row in rows:
                for k, v in row.items():
                    if isinstance(v, Decimal):
                        row[k] = float(v)

            return json.dumps(
                {"sql": sql, "data": rows, "count": len(rows)},
                ensure_ascii=False,
            )

        except Exception as e:
            logger.error(f"财务数据查询工具异常: {e}", exc_info=True)
            return f"查询失败: {e}"

        finally:
            tool_db.close()

    # ──── 工具2: 搜索知识库 ────

    @tool
    def search_knowledge_base_tool(
        query: str,
        stock_codes: list[str] | None = None,
        industry_names: list[str] | None = None,
        top_k: int = 5,
    ) -> str:
        """搜索研报和财报原文知识库，获取非结构化的分析观点、行业趋势、公司评价等证据。

        Args:
            query: 搜索关键词
            stock_codes: 股票代码列表，按个股过滤（可选）
            industry_names: 行业名称列表，按行业过滤（可选）
            top_k: 返回结果数量
        """
        tool_db = get_background_db_session()
        try:
            search_request = schemas_knowledge_base.SearchKnowledgeRequest(
                query=query,
                stock_codes=stock_codes or None,
                industry_names=industry_names or None,
                top_k=top_k,
            )
            response = services_knowledge_base.search_knowledge(tool_db, search_request)

            if not response.results:
                return "未找到相关内容"

            parts = []
            for item in response.results:
                parts.append(
                    f"[相关度:{item.score:.2f}] {item.chunk_text[:500]}"
                )
            return "\n\n".join(parts)

        except Exception as e:
            logger.error(f"知识库检索工具异常: {e}", exc_info=True)
            return f"检索失败: {e}"

        finally:
            tool_db.close()

    # ──── 工具3: 查询公司信息 ────

    @tool
    def resolve_company_tool(company: str) -> str:
        """查询上市公司基本信息（股票代码、简称、所属行业等）。
        传入公司名称、简称或股票代码，返回匹配的公司信息列表。
        """
        tool_db = get_background_db_session()
        try:
            stmt = select(models_company_basic_info.CompanyBasicInfo).where(
                models_company_basic_info.CompanyBasicInfo.company_name.like(
                    f"%{company}%"
                )
                | models_company_basic_info.CompanyBasicInfo.stock_abbr.like(
                    f"%{company}%"
                )
                | models_company_basic_info.CompanyBasicInfo.stock_code.like(
                    f"%{company}%"
                )
            )
            results = tool_db.execute(stmt).scalars().all()

            if not results:
                return f"未找到匹配的公司: {company}"

            lines = []
            for r in results:
                lines.append(
                    f"{r.stock_code} {r.stock_abbr} {r.company_name}（{r.csrc_industry or '未分类'}）"
                )
            return "\n".join(lines)

        except Exception as e:
            logger.error(f"公司查询工具异常: {e}", exc_info=True)
            return f"查询失败: {e}"

        finally:
            tool_db.close()

    return [query_financial_data_tool, search_knowledge_base_tool, resolve_company_tool]
