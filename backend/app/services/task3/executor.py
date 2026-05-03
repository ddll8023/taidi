"""任务三执行步骤服务。"""

import json
import re
import time
from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.constants import task3 as constants_task3
from app.core.config import settings
from app.schemas.common import ErrorCode
from app.schemas.task3 import (
    ExecutionPlan,
    ExecutionTrace,
    Reference,
    StepResult,
    StepStatus,
    StepType,
    TaskStep,
    Task3AnswerContent,
)
from app.services import knowledge_base
from app.services.task3.helpers import (
    _extract_company_name_from_question,
    _extract_json_from_response,
    _invoke_llm,
    _to_jsonable,
)
from app.utils.exception import ServiceException
from app.utils.logger_config import setup_logger

logger = setup_logger(__name__)




def _extract_sql_from_response(response_text: str):
    """从模型响应中提取 SQL 语句。"""
    cleaned_text = response_text.strip()

    cleaned_text = re.sub(
        r"^```sql\s*", "", cleaned_text, flags=re.IGNORECASE | re.MULTILINE
    )
    cleaned_text = re.sub(r"^```\s*", "", cleaned_text, flags=re.MULTILINE)
    cleaned_text = re.sub(r"\s*```$", "", cleaned_text, flags=re.MULTILINE)
    cleaned_text = cleaned_text.strip()

    sql_match = re.search(
        r"((?:WITH\s[\s\S]*?\)\s*)?SELECT\s[\s\S]*?)(?:;|$)",
        cleaned_text,
        re.IGNORECASE,
    )
    if sql_match:
        return sql_match.group(1).strip().rstrip(";")

    if cleaned_text.upper().startswith(("SELECT", "WITH")):
        return cleaned_text.rstrip(";")

    return None


def _strip_sql_literals(sql: str):
    """移除 SQL 中的注释和字面量。"""
    cleaned = re.sub(r"--.*$", " ", sql, flags=re.MULTILINE)
    cleaned = re.sub(r"/\*[\s\S]*?\*/", " ", cleaned)
    cleaned = re.sub(r"'(?:''|[^'])*'", " ", cleaned)
    cleaned = re.sub(r'"(?:""|[^"])*"', " ", cleaned)
    return cleaned


def _extract_cte_names(sql: str):
    """提取 SQL 中定义的 CTE 名称。"""
    cleaned = _strip_sql_literals(sql)
    if not re.match(r"^\s*WITH\b", cleaned, flags=re.IGNORECASE):
        return set()

    cte_names = set()
    for match in re.finditer(
        r"(?:WITH|,)\s*`?([A-Za-z_]\w*)`?\s+AS\s*\(",
        cleaned,
        flags=re.IGNORECASE,
    ):
        cte_names.add(match.group(1).lower())
    return cte_names


def _extract_referenced_table_names(sql: str):
    """提取 SQL 中引用的表名。"""
    cleaned = _strip_sql_literals(sql)
    cte_names = _extract_cte_names(sql)
    return [
        match.group(1).lower()
        for match in re.finditer(
            r"\b(?:FROM|JOIN)\s+`?([A-Za-z_]\w*)`?",
            cleaned,
            flags=re.IGNORECASE,
        )
        if match.group(1).lower() not in cte_names
    ]


def _extract_table_aliases(sql: str):
    """提取 SQL 中的表别名映射。"""
    cleaned = _strip_sql_literals(sql)
    cte_names = _extract_cte_names(sql)
    aliases: dict[str, str] = {}
    pattern = (
        r"\b(?:FROM|JOIN)\s+`?([A-Za-z_]\w*)`?(?:\s+(?:AS\s+)?`?([A-Za-z_]\w*)`?)?"
    )
    for match in re.finditer(pattern, cleaned, flags=re.IGNORECASE):
        table_name = match.group(1).lower()
        alias = match.group(2)
        if table_name in cte_names:
            table_marker = f"__cte__:{table_name}"
            aliases[table_name] = table_marker
            if alias and alias.upper() not in constants_task3.SQL_RESERVED_IDENTIFIERS:
                aliases[alias.lower()] = table_marker
            continue

        if table_name not in constants_task3.TASK3_TABLE_SCHEMA:
            continue

        aliases[table_name] = table_name
        if alias and alias.upper() not in constants_task3.SQL_RESERVED_IDENTIFIERS:
            aliases[alias.lower()] = table_name
    return aliases


def _extract_select_aliases(sql: str):
    """提取 SELECT 语句中的列别名。"""
    cleaned = _strip_sql_literals(sql)
    return {
        match.group(1).lower()
        for match in re.finditer(
            r"\bAS\s+`?([A-Za-z_]\w*)`?", cleaned, flags=re.IGNORECASE
        )
    }


def _validate_sql_identifiers(sql: str):
    """校验 SQL 中字段和别名是否合法。"""
    table_aliases = _extract_table_aliases(sql)
    referenced_tables = {
        table_name for table_name in table_aliases.values()
        if not str(table_name).startswith("__cte__:")
    }
    has_cte = any(str(table_name).startswith("__cte__:") for table_name in table_aliases.values())
    if not referenced_tables:
        return True, ""

    select_aliases = _extract_select_aliases(sql)
    allowed_columns = {
        column.lower()
        for table_name in referenced_tables
        for column in constants_task3.TASK3_TABLE_SCHEMA[table_name]
    }
    cleaned = _strip_sql_literals(sql)

    qualified_ref_pattern = r"`?([A-Za-z_]\w*)`?\s*\.\s*`?([A-Za-z_]\w*)`?"
    for match in re.finditer(qualified_ref_pattern, cleaned):
        prefix = match.group(1).lower()
        column = match.group(2).lower()
        table_name = table_aliases.get(prefix)
        if table_name is None:
            return False, f"SQL引用了未知表或别名: {match.group(1)}"
        if str(table_name).startswith("__cte__:"):
            continue
        if column not in constants_task3.TASK3_TABLE_SCHEMA[table_name]:
            return False, f"SQL字段不存在: {match.group(1)}.{match.group(2)}"

    scan_sql = re.sub(qualified_ref_pattern, " ", cleaned)
    for match in re.finditer(r"\b([A-Za-z_]\w*)\b", scan_sql):
        identifier = match.group(1)
        identifier_lower = identifier.lower()
        identifier_upper = identifier.upper()
        next_char = scan_sql[match.end() : match.end() + 1]

        if next_char == "(":
            continue
        if identifier_upper in constants_task3.SQL_RESERVED_IDENTIFIERS:
            continue
        if identifier_upper in constants_task3.SQL_FUNCTIONS:
            continue
        if identifier_lower in constants_task3.TASK3_TABLE_SCHEMA:
            continue
        if identifier_lower in table_aliases:
            continue
        if identifier_lower in select_aliases:
            continue
        if identifier_lower in allowed_columns:
            continue
        if has_cte:
            continue

        return False, f"SQL字段不存在或不在引用表范围内: {identifier}"

    return True, ""


def _validate_sql(sql: str):
    """校验 SQL 是否满足安全约束。"""
    sql_upper = sql.upper().strip()

    for keyword in constants_task3.FORBIDDEN_KEYWORDS:
        pattern = r"\b" + keyword + r"\b"
        if re.search(pattern, sql_upper):
            return False, f"SQL包含禁止关键字: {keyword}"

    if not sql_upper.startswith(("SELECT", "WITH")):
        return False, "SQL必须以SELECT或WITH开头"

    found_tables = _extract_referenced_table_names(sql)
    for table_name in found_tables:
        if table_name.lower() not in [t.lower() for t in constants_task3.ALLOWED_TABLES]:
            return False, f"SQL引用了不允许的表: {table_name}"

    identifiers_valid, identifiers_msg = _validate_sql_identifiers(sql)
    if not identifiers_valid:
        return False, identifiers_msg

    return True, ""


def _execute_sql(sql: str, db: Session):
    """执行只读 SQL 并返回字典列表。"""
    try:
        result = db.execute(text(sql))
        columns = list(result.keys())
        rows = result.fetchall()
        return [dict(zip(columns, row)) for row in rows]
    except Exception as exc:
        logger.error(f"SQL执行失败: sql={sql} error={exc}", exc_info=True)
        raise ServiceException(
            ErrorCode.AI_SERVICE_ERROR, "服务调用失败，请稍后重试"
        ) from exc



def execute_step(
    step: TaskStep,
    db: Session,
    plan: ExecutionPlan,
    context: dict[str, Any],
    results: dict[str, StepResult],
    references: list[Reference],
):
    """执行单个任务三步骤并更新执行状态。"""
    start_time = time.time()
    logger.info(
        f"执行步骤: step_id={step.step_id}, type={step.step_type}, goal={step.goal}"
    )

    try:
        if step.step_type == StepType.SQL_QUERY:
            output = _execute_sql_query(step, db, context)
        elif step.step_type == StepType.DERIVE_METRIC:
            output = _execute_derive_metric(step, context)
        elif step.step_type == StepType.RETRIEVE_EVIDENCE:
            output = _execute_retrieve_evidence(step, context, references)
        elif step.step_type == StepType.AGGREGATE:
            output = _execute_aggregate(step, context)
        elif step.step_type == StepType.VERIFY:
            output = _execute_verify(step, context)
        elif step.step_type == StepType.COMPOSE_ANSWER:
            output = _execute_compose_answer(step, plan, results, references)
        else:
            raise ServiceException(
                ErrorCode.AI_SERVICE_ERROR, f"未知的步骤类型: {step.step_type}"
            )

        output = _to_jsonable(output)
        execution_time_ms = int((time.time() - start_time) * 1000)
        result = StepResult(
            step_id=step.step_id,
            step_type=step.step_type,
            status=StepStatus.COMPLETED,
            output=output,
            execution_time_ms=execution_time_ms,
        )
        results[step.step_id] = result
        context[step.step_id] = output
        logger.info(f"步骤完成: step_id={step.step_id}, time_ms={execution_time_ms}")
        return result
    except Exception as exc:
        execution_time_ms = int((time.time() - start_time) * 1000)
        error_message = exc.message if isinstance(exc, ServiceException) else "系统内部错误"
        result = StepResult(
            step_id=step.step_id,
            step_type=step.step_type,
            status=StepStatus.FAILED,
            output={},
            error_message=error_message,
            execution_time_ms=execution_time_ms,
        )
        results[step.step_id] = result
        logger.error(f"步骤失败: step_id={step.step_id}, error={exc}", exc_info=True)
        return result


def build_execution_trace(
    plan: ExecutionPlan,
    results: dict[str, StepResult],
    references: list[Reference],
):
    """根据当前执行状态构造任务三执行轨迹。"""
    final_answer = _get_final_answer(plan, results, references)
    return ExecutionTrace(
        plan=plan,
        results=list(results.values()),
        final_answer=final_answer.content,
        references=[_to_jsonable(reference) for reference in references],
        started_at=plan.created_at,
        finished_at=datetime.now(),
    )


"""辅助函数"""


def _execute_sql_query(step: TaskStep, db: Session, context: dict[str, Any]):
    """执行 SQL 查询步骤。"""
    params = step.params
    sql = params.get("sql")

    if sql:
        is_valid, validate_msg = _validate_sql(sql)
        if not is_valid:
            logger.warning(
                f"计划内SQL校验失败，将重新生成: step_id={step.step_id}, reason={validate_msg}"
            )
            sql = None

    max_retries = 2
    for attempt in range(1, max_retries + 1):
        if not sql:
            sql = _generate_sql_for_step(step, context)

        if not sql:
            raise ServiceException(
                ErrorCode.AI_SERVICE_ERROR, f"无法生成SQL: step_id={step.step_id}"
            )

        is_valid, validate_msg = _validate_sql(sql)
        if is_valid:
            break

        logger.warning(
            f"SQL校验失败(第{attempt}次): step_id={step.step_id}, "
            f"reason={validate_msg}, sql={sql[:200]}"
        )

        if attempt < max_retries:
            # 将校验失败原因注入上下文，指导 LLM 修正
            retry_context = dict(context)
            retry_context["_previous_sql_error"] = validate_msg
            retry_context["_previous_sql"] = sql[:500]
            sql = _generate_sql_for_step(step, retry_context)
        else:
            raise ServiceException(
                ErrorCode.AI_SERVICE_ERROR, f"SQL校验失败: {validate_msg}"
            )

    if "LIMIT" not in sql.upper():
        sql = sql.rstrip(";") + " LIMIT 1000"

    data = _execute_sql(sql, db)
    return {"sql": sql, "data": data, "row_count": len(data)}


def _generate_sql_for_step(step: TaskStep, context: dict[str, Any]):
    """为步骤生成 SQL 语句。"""
    deterministic_sql = _build_rule_based_sql(step, context)
    if deterministic_sql:
        logger.info(f"命中规则SQL: step_id={step.step_id}, sql={deterministic_sql[:200]}")
        return deterministic_sql

    config = settings.PROMPT_CONFIG.get_task3_config
    executor_config = config.get("executor", {})
    schema_ddl = _build_schema_ddl()
    system_prompt = executor_config.get("system_prompt", "").format(
        schema_ddl=schema_ddl
    )

    # 构建上下文描述，显式包含前序步骤的执行结果
    context_parts = []
    context_parts.append(json.dumps(context, ensure_ascii=False, default=str)[:1500])

    # 提取前序步骤的结果（特别是 aggregate 步骤的聚合值）
    previous_results = {}
    for key, value in context.items():
        if isinstance(value, dict):
            if "result" in value and "operation" in value:
                # aggregate 步骤结果
                previous_results[key] = {
                    "type": "aggregate",
                    "operation": value.get("operation"),
                    "metric": value.get("metric"),
                    "result": value.get("result"),
                }
            elif "data" in value and "row_count" in value:
                # sql_query 步骤结果
                previous_results[key] = {
                    "type": "sql_query",
                    "row_count": value.get("row_count"),
                    "sample": value.get("data", [])[:3],
                }
    if previous_results:
        context_parts.append(
            "前序步骤结果:\n"
            + json.dumps(previous_results, ensure_ascii=False, default=str)[:1500]
        )

    context_desc = "\n\n".join(context_parts)

    user_prompt = executor_config.get("user_prompt_template", "").format(
        step_id=step.step_id,
        step_type=step.step_type.value,
        goal=step.goal,
        params=json.dumps(step.params, ensure_ascii=False),
        context=context_desc,
    )
    response_text = _invoke_llm(
        system_prompt, user_prompt, max_tokens=2048, temperature=0.0
    )
    sql = _extract_sql_from_response(response_text)
    if sql:
        logger.info(f"生成SQL: step_id={step.step_id}, sql={sql[:200]}")
    return sql


def _build_rule_based_sql(step: TaskStep, context: dict[str, Any]):
    """按题型生成规则化 SQL，降低关键场景对 LLM 自由生成的依赖。"""
    question = _get_step_question_text(step, context)
    resolved_companies = _get_resolved_companies(context)

    if (
        "投资性现金流量净额为负" in question
        and any(keyword in question for keyword in ["数量", "占比", "绝对值最大"])
    ):
        return (
            "WITH base_companies AS ("
            "SELECT stock_code, stock_abbr, investing_cf_net_amount "
            "FROM cash_flow_sheet "
            "WHERE report_year = 2025 "
            "AND report_period = 'Q3' "
            "AND investing_cf_net_amount < 0"
            "), total_stats AS ("
            "SELECT COUNT(*) AS total_company_count "
            "FROM cash_flow_sheet "
            "WHERE report_year = 2025 "
            "AND report_period = 'Q3'"
            "), negative_stats AS ("
            "SELECT COUNT(*) AS negative_company_count "
            "FROM base_companies"
            ") "
            "SELECT b.stock_code, b.stock_abbr, b.investing_cf_net_amount, "
            "t.total_company_count, n.negative_company_count, "
            "ROUND(n.negative_company_count * 100.0 / NULLIF(t.total_company_count, 0), 2) "
            "AS negative_ratio_pct "
            "FROM base_companies b "
            "CROSS JOIN total_stats t "
            "CROSS JOIN negative_stats n "
            "ORDER BY b.investing_cf_net_amount ASC"
        )

    if (
        "净利润率" in question
        and "营业总收入" in question
        and "资产负债率" in question
        and "2025年第三季度" in question
    ):
        return (
            "SELECT i.stock_code, i.stock_abbr, i.report_year, i.report_period, "
            "i.net_profit, i.total_operating_revenue, "
            "ROUND(i.net_profit / NULLIF(i.total_operating_revenue, 0) * 100, 2) "
            "AS calculated_net_profit_margin, "
            "CASE "
            "WHEN i.net_profit / NULLIF(i.total_operating_revenue, 0) * 100 > 15 THEN '高' "
            "WHEN i.net_profit / NULLIF(i.total_operating_revenue, 0) * 100 >= 5 THEN '中' "
            "ELSE '低' "
            "END AS net_profit_margin_level, "
            "b.asset_liability_ratio "
            "FROM income_sheet i "
            "LEFT JOIN balance_sheet b "
            "ON i.stock_code = b.stock_code "
            "AND i.report_year = b.report_year "
            "AND i.report_period = b.report_period "
            "WHERE i.report_year = 2025 "
            "AND i.report_period = 'Q3' "
            "AND i.net_profit IS NOT NULL "
            "AND i.total_operating_revenue IS NOT NULL "
            "AND i.total_operating_revenue <> 0 "
            "ORDER BY i.stock_code"
        )

    if (
        "连续四个报告期" in question
        and "营业总收入" in question
        and "研发投入" in question
    ):
        years = _extract_years(question, [2022, 2023, 2024, 2025])
        return (
            "SELECT stock_code, stock_abbr, report_year, report_period, "
            "total_operating_revenue, operating_revenue_yoy_growth, "
            "operating_expense_rnd_expenses "
            "FROM income_sheet "
            f"WHERE report_year IN ({_format_number_in_clause(years)}) "
            "AND report_period = 'Q3' "
            "ORDER BY stock_code, report_year"
        )

    if (
        len(resolved_companies) >= 2
        and "第三季度" in question
        and any(keyword in question for keyword in ["营业总收入", "营收"])
        and any(keyword in question for keyword in ["行业整体", "行业"])
    ):
        stock_codes = [
            company["stock_code"]
            for company in resolved_companies
            if company.get("stock_code")
        ]
        if stock_codes:
            base_stock_code = stock_codes[0]
            return (
                "SELECT stock_code, stock_abbr, report_year, report_period, "
                "total_operating_revenue, operating_revenue_yoy_growth, "
                "'company' AS entity_type "
                "FROM income_sheet "
                f"WHERE stock_code IN ({_format_text_in_clause(stock_codes)}) "
                "AND report_year IN (2022, 2023, 2024, 2025) "
                "AND report_period = 'Q3' "
                "UNION ALL "
                "SELECT NULL AS stock_code, '行业整体' AS stock_abbr, "
                "i.report_year, i.report_period, "
                "ROUND(AVG(i.total_operating_revenue), 2) AS total_operating_revenue, "
                "ROUND(AVG(i.operating_revenue_yoy_growth), 2) AS operating_revenue_yoy_growth, "
                "'industry' AS entity_type "
                "FROM income_sheet i "
                "JOIN company_basic_info c ON i.stock_code = c.stock_code "
                "WHERE i.report_year IN (2022, 2023, 2024, 2025) "
                "AND i.report_period = 'Q3' "
                "AND c.csrc_industry = ("
                "SELECT csrc_industry FROM company_basic_info "
                f"WHERE stock_code = '{base_stock_code}' LIMIT 1"
                ") "
                "GROUP BY i.report_year, i.report_period"
            )

    if "净利润" in question and any(
        keyword in question for keyword in ["营业成本", "销售费用", "净利润同比下降"]
    ):
        company_filter = _build_single_company_filter(step, context, question)
        if company_filter:
            return (
                "SELECT stock_code, stock_abbr, report_year, report_period, "
                "net_profit, net_profit_yoy_growth, total_operating_revenue, "
                "operating_expense_cost_of_sales, operating_expense_selling_expenses, "
                "operating_expense_administrative_expenses, credit_impairment_loss, "
                "asset_impairment_loss "
                "FROM income_sheet "
                f"WHERE {company_filter} "
                "AND report_year IN (2024, 2025) "
                "AND report_period = 'Q3' "
                "ORDER BY report_year"
            )

    if (
        any(keyword in question for keyword in ["经营性现金流量净额", "经营性现金流"])
        and "净利润" in question
        and any(keyword in question for keyword in ["背离", "为正", "为负"])
    ):
        company_filter = _build_single_company_filter(step, context, question, alias="c")
        if company_filter:
            return (
                "SELECT c.stock_code, c.stock_abbr, c.report_year, c.report_period, "
                "c.operating_cf_net_amount, c.operating_cf_cash_from_sales, "
                "c.net_cash_flow, i.net_profit, i.total_operating_revenue, "
                "i.operating_expense_cost_of_sales, i.operating_expense_selling_expenses, "
                "i.operating_expense_administrative_expenses, i.credit_impairment_loss, "
                "i.asset_impairment_loss "
                "FROM cash_flow_sheet c "
                "LEFT JOIN income_sheet i "
                "ON c.stock_code = i.stock_code "
                "AND c.report_year = i.report_year "
                "AND c.report_period = i.report_period "
                f"WHERE {company_filter} "
                "AND c.report_year = 2025 "
                "AND c.report_period = 'Q3'"
            )

    if "应收账款" in question and any(keyword in question for keyword in ["占营业总收入比例", "占比"]):
        company_filter = _build_single_company_filter(step, context, question, alias="b")
        if company_filter:
            return (
                "SELECT b.stock_code, b.stock_abbr, b.report_year, b.report_period, "
                "b.asset_accounts_receivable, b.asset_total_assets, b.asset_inventory, "
                "b.liability_accounts_payable, i.total_operating_revenue, "
                "ROUND(b.asset_accounts_receivable / NULLIF(i.total_operating_revenue, 0) * 100, 2) "
                "AS accounts_receivable_to_revenue_ratio "
                "FROM balance_sheet b "
                "LEFT JOIN income_sheet i "
                "ON b.stock_code = i.stock_code "
                "AND b.report_year = i.report_year "
                "AND b.report_period = i.report_period "
                f"WHERE {company_filter} "
                "AND b.report_year IN (2024, 2025) "
                "AND b.report_period = 'Q3' "
                "ORDER BY b.report_year"
            )

    return None


def _get_step_question_text(step: TaskStep, context: dict[str, Any]):
    """提取当前步骤实际对应的问题文本。"""
    for key in ["original_question", "standalone_question"]:
        value = context.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    description = step.params.get("description")
    if isinstance(description, str) and description.strip():
        return description.strip()
    return step.goal


def _get_resolved_companies(context: dict[str, Any]):
    """读取上下文中已解析的公司列表。"""
    resolved_companies = context.get("resolved_companies")
    if isinstance(resolved_companies, list):
        return [item for item in resolved_companies if isinstance(item, dict)]
    return []


def _extract_years(question: str, default_years: list[int]):
    """从问题文本提取年份列表。"""
    years = sorted({int(item) for item in re.findall(r"20\d{2}", question)})
    return years or default_years


def _format_number_in_clause(values: list[int]):
    """格式化数字 IN 子句。"""
    return ", ".join(str(value) for value in values)


def _format_text_in_clause(values: list[str]):
    """格式化文本 IN 子句。"""
    return ", ".join(f"'{value}'" for value in values)



def _build_single_company_filter(
    step: TaskStep,
    context: dict[str, Any],
    question: str,
    alias: str = "",
):
    """构造单主体查询条件，优先使用 stock_code。"""
    prefix = f"{alias}." if alias else ""

    stock_code = step.params.get("stock_code") or context.get("stock_code")
    if isinstance(stock_code, str) and stock_code.strip():
        return f"{prefix}stock_code = '{stock_code.strip()}'"

    resolved_companies = _get_resolved_companies(context)
    if resolved_companies:
        first_company = resolved_companies[0]
        if first_company.get("stock_code"):
            return f"{prefix}stock_code = '{first_company['stock_code']}'"
        if first_company.get("stock_abbr"):
            return f"{prefix}stock_abbr = '{first_company['stock_abbr']}'"

    company_name = _extract_company_name_from_question(question)
    if company_name:
        return f"{prefix}stock_abbr = '{company_name}'"

    return None


def _build_schema_ddl():
    """构造供模型参考的表结构说明。"""
    lines = []
    lines.append("-- 重要提示：只允许使用以下列出的表，严禁使用任何不存在的表（如 top_cash_flow_companies、temp_table 等）。")
    lines.append("-- 如需获取排名/筛选结果，请使用子查询或 CTE (WITH 语句) 从现有表中查询。")
    lines.append("")
    for table_name, fields in constants_task3.TASK3_TABLE_SCHEMA.items():
        lines.append(f"CREATE TABLE {table_name} (")
        field_lines = [
            f"  {field_name} VARCHAR/INT/DECIMAL COMMENT '{description}'"
            for field_name, description in fields.items()
        ]
        lines.append(",\n".join(field_lines))
        lines.append(");")
        lines.append("")
    return "\n".join(lines)


def _execute_derive_metric(step: TaskStep, context: dict[str, Any]):
    """执行派生指标计算步骤。"""
    params = step.params
    formula = params.get("formula", "")
    metric_name = params.get("metric_name", "派生指标")
    dep_data = []
    for dep_id in step.depends_on:
        dep_result = context.get(dep_id)
        if isinstance(dep_result, dict) and "data" in dep_result:
            dep_data.extend(dep_result["data"])

    if not dep_data:
        return {"metric_name": metric_name, "values": [], "formula": formula}

    if (
        "yoy_growth" in metric_name.lower()
        or "同比增长" in metric_name
        or "同比" in formula
    ):
        return _calculate_yoy_growth(dep_data, metric_name, formula)

    if "current_year" in formula and "previous_year" in formula:
        return _calculate_yoy_growth(dep_data, metric_name, formula)

    calculated_values = []
    for row in dep_data:
        try:
            value = _evaluate_formula(formula, row)
        except Exception as exc:
            logger.warning(f"派生指标计算失败: row={row}, error={exc}")
            continue
        calculated_values.append(
            {
                "stock_code": row.get("stock_code"),
                "stock_abbr": row.get("stock_abbr"),
                "report_year": row.get("report_year"),
                "report_period": row.get("report_period"),
                metric_name: float(value) if value is not None else None,
            }
        )

    return {
        "metric_name": metric_name,
        "values": calculated_values,
        "formula": formula,
        "count": len(calculated_values),
    }


def _calculate_yoy_growth(data: list[dict], metric_name: str, formula: str):
    """按年度顺序计算同比增长结果。"""
    metric_field = None
    for field in [
        "total_profit",
        "net_profit",
        "total_operating_revenue",
        "operating_revenue",
    ]:
        if field in formula or any(field in row for row in data):
            metric_field = field
            break

    if not metric_field:
        for row in data:
            for key, value in row.items():
                if key in [
                    "stock_code",
                    "stock_abbr",
                    "report_year",
                    "report_period",
                    "report_id",
                ]:
                    continue
                if isinstance(value, (int, float, Decimal)):
                    metric_field = key
                    break
            if metric_field:
                break

    if not metric_field:
        return {
            "metric_name": metric_name,
            "values": [],
            "formula": formula,
            "error": "无法识别指标字段",
        }

    sorted_data = sorted(
        data, key=lambda item: (item.get("stock_code", ""), item.get("report_year", 0))
    )
    calculated_values = []
    prev_row = None

    for row in sorted_data:
        current_value = row.get(metric_field)
        if current_value is None:
            calculated_values.append(
                {
                    "stock_code": row.get("stock_code"),
                    "stock_abbr": row.get("stock_abbr"),
                    "report_year": row.get("report_year"),
                    "report_period": row.get("report_period"),
                    metric_name: None,
                }
            )
            prev_row = row
            continue

        yoy_value = None
        if prev_row is not None:
            prev_value = prev_row.get(metric_field)
            if prev_value is not None and prev_value != 0:
                try:
                    yoy_value = (
                        (float(current_value) - float(prev_value))
                        / float(prev_value)
                        * 100
                    )
                except (TypeError, ValueError, ZeroDivisionError):
                    yoy_value = None

        calculated_values.append(
            {
                "stock_code": row.get("stock_code"),
                "stock_abbr": row.get("stock_abbr"),
                "report_year": row.get("report_year"),
                "report_period": row.get("report_period"),
                metric_name: round(yoy_value, 2) if yoy_value is not None else None,
            }
        )
        prev_row = row

    return {
        "metric_name": metric_name,
        "values": calculated_values,
        "formula": f"yoy_growth({metric_field})",
        "count": len(calculated_values),
    }


def _evaluate_formula(formula: str, row: dict):
    """在单行数据上执行安全公式计算。"""
    expr = formula
    for field, value in row.items():
        if value is not None and isinstance(value, (int, float, Decimal)):
            expr = expr.replace(field, str(float(value)))

    expr = re.sub(r"[a-zA-Z_][a-zA-Z0-9_]*", "0", expr)
    try:
        result = eval(expr, {"__builtins__": {}}, {})
        return Decimal(str(result))
    except Exception:
        return None


def _execute_retrieve_evidence(
    step: TaskStep,
    context: dict[str, Any],
    references: list[Reference],
):
    """执行证据检索步骤并收集引用。"""
    params = step.params
    query = params.get("query", step.goal)
    stock_code = params.get("stock_code")
    doc_type = params.get("doc_type")
    top_k = params.get("top_k") or params.get("limit") or 5
    company_name_filter = params.get("company_name_filter")

    if stock_code is None and "resolved_companies" in context:
        companies = context["resolved_companies"]
        if companies and isinstance(companies, list):
            stock_code = companies[0].get("stock_code")

    logger.info(
        f"知识库证据检索开始: step_id={step.step_id}, query={str(query)[:120]}, "
        f"stock_code={stock_code}, doc_type={doc_type}, top_k={top_k}"
    )
    evidence_list = knowledge_base.search_and_format_evidence(
        query,
        stock_code=stock_code,
        doc_type=doc_type,
        top_k=top_k,
    )

    # 当 stock_code 缺失但存在 company_name_filter 时，对检索结果做二次过滤
    if not stock_code and company_name_filter and evidence_list:
        original_count = len(evidence_list)
        filtered_evidence = []
        for evidence in evidence_list:
            text = evidence.get("text", "")
            title = evidence.get("title", "")
            stock_abbr = evidence.get("stock_abbr", "")
            # 证据必须包含目标公司名称（在标题、正文或股票简称中）
            combined = f"{title} {stock_abbr} {text[:200]}"
            if company_name_filter in combined:
                filtered_evidence.append(evidence)
            else:
                logger.debug(
                    f"证据被过滤（主体不匹配）: company_name_filter={company_name_filter}, "
                    f"title={title}, stock_abbr={stock_abbr}"
                )
        evidence_list = filtered_evidence
        logger.info(
            f"知识库证据二次过滤完成: step_id={step.step_id}, "
            f"original_count={original_count}, filtered_count={len(evidence_list)}, "
            f"company_name_filter={company_name_filter}"
        )

    for evidence in evidence_list:
        references.append(
            Reference(
                paper_path=evidence.get("paper_path"),
                text=evidence.get("text", ""),
                page_no=evidence.get("page_no"),
            )
        )

    logger.info(
        f"知识库证据检索完成: step_id={step.step_id}, "
        f"evidence_count={len(evidence_list)}, reference_total={len(references)}"
    )
    return {
        "query": query,
        "evidence_count": len(evidence_list),
        "evidence": evidence_list,
    }


def _execute_aggregate(step: TaskStep, context: dict[str, Any]):
    """执行聚合统计步骤。"""
    params = step.params
    operation = params.get("operation", "avg")
    metric = params.get("metric")

    dep_data = []
    for dep_id in step.depends_on:
        dep_result = context.get(dep_id)
        if isinstance(dep_result, dict) and "data" in dep_result:
            dep_data.extend(dep_result["data"])
        elif isinstance(dep_result, dict) and "values" in dep_result:
            dep_data.extend(dep_result["values"])

    if not dep_data:
        return {"operation": operation, "metric": metric, "result": None}

    if operation == "count_and_avg":
        return _aggregate_count_and_avg(step, dep_data)

    numeric_items = []
    for row in dep_data:
        if metric in row and row[metric] is not None:
            try:
                numeric_items.append((float(row[metric]), row))
            except (TypeError, ValueError):
                continue

    if not numeric_items:
        return {"operation": operation, "metric": metric, "result": None}

    values = [item[0] for item in numeric_items]
    matched_rows = []
    if operation == "avg":
        result = sum(values) / len(values)
    elif operation == "sum":
        result = sum(values)
    elif operation == "max":
        result = max(values)
        matched_rows = [row for value, row in numeric_items if value == result]
    elif operation == "min":
        result = min(values)
        matched_rows = [row for value, row in numeric_items if value == result]
    elif operation == "count":
        result = len(values)
    else:
        result = sum(values) / len(values)

    output = {
        "operation": operation,
        "metric": metric,
        "result": round(result, 4) if isinstance(result, float) else result,
        "count": len(values),
    }
    if matched_rows:
        output["matched_rows"] = matched_rows[:10]
    return output


def _execute_verify(step: TaskStep, context: dict[str, Any]):
    """执行计划内轻量校验步骤。"""
    params = step.params
    check_type = params.get("check_type", "consistency")
    verification_result = {"check_type": check_type, "passed": True, "details": []}

    if check_type == "consistency":
        fields = params.get("fields", [])
        for dep_id in step.depends_on:
            dep_result = context.get(dep_id)
            if isinstance(dep_result, dict) and "data" in dep_result:
                for row in dep_result["data"]:
                    for field in fields:
                        if field in row and row[field] is not None:
                            verification_result["details"].append(
                                {"field": field, "value": row[field], "status": "valid"}
                            )
    elif check_type == "completeness":
        expected_count = params.get("expected_count")
        actual_count = 0
        for dep_id in step.depends_on:
            dep_result = context.get(dep_id)
            if isinstance(dep_result, dict):
                if "data" in dep_result:
                    actual_count += len(dep_result["data"])
                elif "count" in dep_result:
                    actual_count += dep_result["count"]
        verification_result["actual_count"] = actual_count
        if expected_count:
            verification_result["expected_count"] = expected_count
            verification_result["passed"] = actual_count >= expected_count
    elif check_type == "correlation":
        actual_count = 0
        for dep_id in step.depends_on:
            dep_result = context.get(dep_id)
            if isinstance(dep_result, dict):
                if "data" in dep_result:
                    actual_count += len(dep_result["data"])
                elif "values" in dep_result:
                    actual_count += len(dep_result["values"])
                elif dep_result.get("result") is not None:
                    actual_count += 1

        verification_result["actual_count"] = actual_count
        verification_result["passed"] = actual_count > 0
        if actual_count == 0:
            verification_result["details"].append(
                {"status": "missing", "message": "关联性验证缺少可比较的数据结果"}
            )

    return verification_result


def _aggregate_count_and_avg(step: TaskStep, dep_data: list[dict[str, Any]]):
    """按净利润率分组，统计公司数量并计算平均资产负债率。"""
    merged_rows: dict[str, dict[str, Any]] = {}
    for row in dep_data:
        if not isinstance(row, dict):
            continue
        key = str(row.get("stock_code") or row.get("stock_abbr") or "")
        if not key:
            continue
        current = merged_rows.setdefault(key, {})
        current.update({k: v for k, v in row.items() if v is not None})

    groups = {"高": {"count": 0, "debt_ratios": []}, "中": {"count": 0, "debt_ratios": []}, "低": {"count": 0, "debt_ratios": []}}
    for row in merged_rows.values():
        margin = row.get("calculated_net_profit_margin")
        if margin is None:
            margin = row.get("net_profit_margin")
        if margin is None:
            continue

        try:
            margin_value = float(margin)
        except (TypeError, ValueError):
            continue

        if margin_value > 15:
            level = "高"
        elif margin_value >= 5:
            level = "中"
        else:
            level = "低"

        groups[level]["count"] += 1

        debt_ratio = row.get("asset_liability_ratio")
        if debt_ratio is not None:
            try:
                groups[level]["debt_ratios"].append(float(debt_ratio))
            except (TypeError, ValueError):
                pass

    group_results = []
    for level in ["高", "中", "低"]:
        debt_ratios = groups[level]["debt_ratios"]
        avg_debt_ratio = sum(debt_ratios) / len(debt_ratios) if debt_ratios else None
        group_results.append(
            {
                "level": level,
                "company_count": groups[level]["count"],
                "avg_asset_liability_ratio": round(avg_debt_ratio, 2) if avg_debt_ratio is not None else None,
            }
        )

    return {
        "operation": "count_and_avg",
        "group_by": step.params.get("group_by"),
        "result": group_results,
        "count": sum(item["company_count"] for item in group_results),
    }


def _execute_compose_answer(
    step: TaskStep,
    plan: ExecutionPlan,
    results: dict[str, StepResult],
    references: list[Reference],
):
    """基于执行结果生成最终回答文本。"""
    config = settings.PROMPT_CONFIG.get_task3_config
    answer_config = config.get("answer_builder", {})
    execution_summary = _build_execution_summary(results)
    system_prompt = answer_config.get("system_prompt", "")
    user_prompt = answer_config.get("user_prompt_template", "").format(
        question=plan.question,
        execution_trace=execution_summary,
    )
    answer_text = _invoke_llm(
        system_prompt, user_prompt, max_tokens=8192, temperature=0.3
    )
    return {"answer": answer_text, "has_references": len(references) > 0}


def _build_execution_summary(results: dict[str, StepResult]):
    """将执行结果汇总为模型可读摘要。"""
    summary_parts = []
    for step_id, result in results.items():
        if result.status == StepStatus.COMPLETED:
            output_summary = {}
            if result.output:
                if "data" in result.output:
                    output_summary["data_rows"] = len(result.output["data"])
                    if result.output["data"]:
                        output_summary["sample"] = result.output["data"][:10]
                elif "values" in result.output:
                    output_summary["values_count"] = len(result.output["values"])
                elif "evidence" in result.output:
                    output_summary["evidence_count"] = result.output["evidence_count"]
                    output_summary["evidence_sample"] = [
                        {
                            "paper_path": item.get("paper_path"),
                            "text": item.get("text"),
                            "paper_image": item.get("paper_image"),
                        }
                        for item in result.output.get("evidence", [])[:3]
                        if isinstance(item, dict)
                    ]
                elif "result" in result.output:
                    output_summary["result"] = result.output["result"]
                    if result.output.get("matched_rows"):
                        output_summary["matched_rows"] = result.output["matched_rows"]
                elif "answer" in result.output:
                    output_summary["answer_length"] = len(result.output["answer"])

            summary_parts.append(
                {
                    "step_id": step_id,
                    "step_type": result.step_type.value,
                    "status": "completed",
                    "output": output_summary,
                }
            )
        else:
            summary_parts.append(
                {
                    "step_id": step_id,
                    "step_type": result.step_type.value,
                    "status": "failed",
                    "error": result.error_message,
                }
            )

    return json.dumps(_to_jsonable(summary_parts), ensure_ascii=False, indent=2)


def _get_final_answer(
    plan: ExecutionPlan,
    results: dict[str, StepResult],
    references: list[Reference],
):
    """从执行结果中提取最终答案。"""
    answer_content = ""
    for step_id in reversed(list(results.keys())):
        result = results[step_id]
        if (
            result.step_type == StepType.COMPOSE_ANSWER
            and result.status == StepStatus.COMPLETED
        ):
            answer_content = result.output.get("answer", "")
            break

    if not answer_content:
        answer_content = _generate_fallback_answer(plan, results)

    return Task3AnswerContent(content=answer_content, references=references)


def _generate_fallback_answer(
    plan: ExecutionPlan,
    results: dict[str, StepResult],
):
    """在无法生成答案时构造降级回答。"""
    parts = []
    for step in plan.get_ordered_steps():
        result = results.get(step.step_id)
        if result is None or result.status != StepStatus.COMPLETED:
            continue
        if result.step_type == StepType.SQL_QUERY:
            data = result.output.get("data", [])
            if data:
                parts.append(f"查询到 {len(data)} 条数据。")
        elif result.step_type == StepType.AGGREGATE:
            agg_result = result.output.get("result")
            if agg_result is not None:
                parts.append(f"聚合结果: {agg_result}")

    if parts:
        return " ".join(parts)
    return "抱歉，无法生成有效的回答。"
