import json
import re
import time
from datetime import datetime
from decimal import Decimal
from typing import Any

from langchain_core.messages import HumanMessage, SystemMessage
from sqlalchemy import text
from sqlalchemy.orm import Session

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
from app.services import visualization as services_visualization
from app.utils.exception import ServiceException
from app.utils.logger_config import setup_logger
from app.utils.model_factory import get_model

logger = setup_logger(__name__)

TASK3_TABLE_SCHEMA: dict[str, dict[str, str]] = {
    "income_sheet": {
        "report_id": "财报主表ID",
        "stock_code": "股票代码",
        "stock_abbr": "股票简称",
        "report_year": "报告年份",
        "report_period": "报告期：Q1/HY/Q3/FY",
        "report_type": "报告类型：REPORT/SUMMARY",
        "net_profit": "净利润(万元)",
        "net_profit_yoy_growth": "净利润同比(%)",
        "other_income": "其他收益(万元)",
        "total_operating_revenue": "营业总收入(万元)",
        "operating_revenue_yoy_growth": "营业总收入同比(%)",
        "operating_expense_cost_of_sales": "营业总支出-营业支出(万元)",
        "operating_expense_selling_expenses": "营业总支出-销售费用(万元)",
        "operating_expense_administrative_expenses": "营业总支出-管理费用(万元)",
        "operating_expense_financial_expenses": "营业总支出-财务费用(万元)",
        "operating_expense_rnd_expenses": "营业总支出-研发费用(万元)",
        "operating_expense_taxes_and_surcharges": "营业总支出-税金及附加(万元)",
        "total_operating_expenses": "营业总支出(万元)",
        "operating_profit": "营业利润(万元)",
        "total_profit": "利润总额(万元)",
        "asset_impairment_loss": "资产减值损失(万元)",
        "credit_impairment_loss": "信用减值损失(万元)",
    },
    "balance_sheet": {
        "report_id": "财报主表ID",
        "stock_code": "股票代码",
        "stock_abbr": "股票简称",
        "report_year": "报告年份",
        "report_period": "报告期：Q1/HY/Q3/FY",
        "report_type": "报告类型：REPORT/SUMMARY",
        "asset_cash_and_cash_equivalents": "资产-货币资金(万元)",
        "asset_accounts_receivable": "资产-应收账款(万元)",
        "asset_inventory": "资产-存货(万元)",
        "asset_trading_financial_assets": "资产-交易性金融资产(万元)",
        "asset_construction_in_progress": "资产-在建工程(万元)",
        "asset_total_assets": "资产-总资产(万元)",
        "asset_total_assets_yoy_growth": "资产-总资产同比(%)",
        "liability_accounts_payable": "负债-应付账款(万元)",
        "liability_advance_from_customers": "负债-预收账款(万元)",
        "liability_total_liabilities": "负债-总负债(万元)",
        "liability_total_liabilities_yoy_growth": "负债-总负债同比(%)",
        "liability_contract_liabilities": "负债-合同负债(万元)",
        "liability_short_term_loans": "负债-短期借款(万元)",
        "asset_liability_ratio": "资产负债率(%)",
        "equity_unappropriated_profit": "股东权益-未分配利润(万元)",
        "equity_total_equity": "股东权益合计(万元)",
    },
    "cash_flow_sheet": {
        "report_id": "财报主表ID",
        "stock_code": "股票代码",
        "stock_abbr": "股票简称",
        "report_year": "报告年份",
        "report_period": "报告期：Q1/HY/Q3/FY",
        "report_type": "报告类型：REPORT/SUMMARY",
        "net_cash_flow": "净现金流(元)",
        "net_cash_flow_yoy_growth": "净现金流-同比增长(%)",
        "operating_cf_net_amount": "经营性现金流-现金流量净额(万元)",
        "operating_cf_ratio_of_net_cf": "经营性现金流-净现金流占比(%)",
        "operating_cf_cash_from_sales": "经营性现金流-销售商品收到的现金(万元)",
        "investing_cf_net_amount": "投资性现金流-现金流量净额(万元)",
        "investing_cf_ratio_of_net_cf": "投资性现金流-净现金流占比(%)",
        "investing_cf_cash_for_investments": "投资性现金流-投资支付的现金(万元)",
        "investing_cf_cash_from_investment_recovery": "投资性现金流-收回投资收到的现金(万元)",
        "financing_cf_cash_from_borrowing": "融资性现金流-取得借款收到的现金(万元)",
        "financing_cf_cash_for_debt_repayment": "融资性现金流-偿还债务支付的现金(万元)",
        "financing_cf_net_amount": "融资性现金流-现金流量净额(万元)",
        "financing_cf_ratio_of_net_cf": "融资性现金流-净现金流占比(%)",
    },
    "core_performance_indicators_sheet": {
        "report_id": "财报主表ID",
        "stock_code": "股票代码",
        "stock_abbr": "股票简称",
        "report_year": "报告年份",
        "report_period": "报告期：Q1/HY/Q3/FY",
        "report_type": "报告类型：REPORT/SUMMARY",
        "eps": "每股收益(元)",
        "total_operating_revenue": "营业总收入(万元)",
        "operating_revenue_yoy_growth": "营业总收入-同比增长(%)",
        "operating_revenue_qoq_growth": "营业总收入-季度环比增长(%)",
        "net_profit_10k_yuan": "净利润(万元)",
        "net_profit_yoy_growth": "净利润-同比增长(%)",
        "net_profit_qoq_growth": "净利润-季度环比增长(%)",
        "net_asset_per_share": "每股净资产(元)",
        "roe": "净资产收益率(%)",
        "operating_cf_per_share": "每股经营现金流量(元)",
        "net_profit_excl_non_recurring": "扣非净利润(万元)",
        "net_profit_excl_non_recurring_yoy": "扣非净利润同比增长(%)",
        "gross_profit_margin": "销售毛利率(%)",
        "net_profit_margin": "销售净利率(%)",
        "roe_weighted_excl_non_recurring": "加权平均净资产收益率(扣非)(%)",
    },
    "company_basic_info": {
        "stock_code": "股票代码",
        "stock_abbr": "股票简称",
        "company_name": "公司名称",
        "english_name": "英文名称",
        "csrc_industry": "所属证监会行业",
        "listed_exchange": "上市交易所原始文本",
        "exchange": "标准化交易所代码：SH/SZ/BJ",
        "security_category": "证券类别",
        "registered_region": "注册区域",
        "registered_capital_raw": "注册资本原始文本",
        "registered_capital_yuan": "注册资本标准化数值(元)",
        "employee_count": "雇员人数",
        "management_count": "管理人员人数",
        "source_row_no": "附件1原始序号",
        "source_file_name": "附件1源文件名",
        "created_at": "创建时间",
        "updated_at": "更新时间",
    },
}

ALLOWED_TABLES = list(TASK3_TABLE_SCHEMA.keys())

FORBIDDEN_KEYWORDS = [
    "DROP",
    "DELETE",
    "UPDATE",
    "INSERT",
    "ALTER",
    "CREATE",
    "TRUNCATE",
    "GRANT",
    "REVOKE",
]
SQL_RESERVED_IDENTIFIERS = {
    "ALL",
    "AND",
    "AS",
    "ASC",
    "BETWEEN",
    "BY",
    "CASE",
    "CROSS",
    "DESC",
    "DISTINCT",
    "ELSE",
    "END",
    "EXISTS",
    "FALSE",
    "FROM",
    "FULL",
    "GROUP",
    "HAVING",
    "IN",
    "INNER",
    "IS",
    "JOIN",
    "LEFT",
    "LIKE",
    "LIMIT",
    "NOT",
    "NULL",
    "OFFSET",
    "ON",
    "OR",
    "ORDER",
    "OUTER",
    "RIGHT",
    "ROWS",
    "SELECT",
    "THEN",
    "TRUE",
    "WHEN",
    "WHERE",
    "WITH",
    "UNION",
}
SQL_FUNCTIONS = {
    "ABS",
    "AVG",
    "CAST",
    "COALESCE",
    "CONCAT",
    "COUNT",
    "IFNULL",
    "MAX",
    "MIN",
    "NULLIF",
    "ROUND",
    "SUM",
    "CURRENT_DATE",
    "CURRENT_TIME",
    "CURRENT_TIMESTAMP",
    "NOW",
    "CURDATE",
    "CURTIME",
    "DATE",
    "YEAR",
    "MONTH",
    "DAY",
    "QUARTER",
    "WEEK",
    "STR_TO_DATE",
    "DATE_FORMAT",
}
TASK3_QUESTION_CODE_PATTERN = re.compile(r"^B2\d{3}$")


def _get_task3_config() -> dict:
    return settings.PROMPT_CONFIG.get_task3_config


def _get_required_task3_question_id(context: dict[str, Any]) -> str:
    question_id = str(context.get("question_id") or "").strip()
    if not TASK3_QUESTION_CODE_PATTERN.fullmatch(question_id):
        logger.error("任务三图表题号非法: question_id=%s", question_id or "<empty>")
        raise ServiceException(ErrorCode.PARAM_ERROR, "任务三图表生成缺少合法题目编号")
    return question_id


def _invoke_llm(
    system_prompt: str,
    user_prompt: str,
    max_tokens: int = 8192,
    temperature: float = 0.1,
) -> str:
    try:
        model = get_model.build_chat_model(
            max_tokens=max_tokens, temperature=temperature
        )
        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_prompt),
        ]
        response = model.invoke(messages)
    except Exception as exc:
        logger.error("LLM调用失败: error=%s", str(exc))
        raise ServiceException(ErrorCode.AI_SERVICE_ERROR, "LLM调用失败") from exc

    content = getattr(response, "content", None)
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        text_parts = []
        for block in content:
            if isinstance(block, dict) and block.get("type") == "text":
                text_parts.append(str(block.get("text", "")))
        return "".join(text_parts).strip()
    return ""


def _extract_json_from_response(response_text: str) -> dict | None:
    json_match = re.search(r"\{[\s\S]*\}", response_text)
    if json_match:
        try:
            return json.loads(json_match.group())
        except json.JSONDecodeError:
            pass
    try:
        return json.loads(response_text)
    except json.JSONDecodeError:
        return None


def _extract_sql_from_response(response_text: str) -> str | None:
    cleaned_text = response_text.strip()

    cleaned_text = re.sub(
        r"^```sql\s*", "", cleaned_text, flags=re.IGNORECASE | re.MULTILINE
    )
    cleaned_text = re.sub(r"^```\s*", "", cleaned_text, flags=re.MULTILINE)
    cleaned_text = re.sub(r"\s*```$", "", cleaned_text, flags=re.MULTILINE)
    cleaned_text = cleaned_text.strip()

    sql_match = re.search(r"(SELECT\s[\s\S]*?)(?:;|$)", cleaned_text, re.IGNORECASE)
    if sql_match:
        return sql_match.group(1).strip().rstrip(";")

    if cleaned_text.upper().startswith("SELECT"):
        return cleaned_text.rstrip(";")

    return None


def _strip_sql_literals(sql: str) -> str:
    cleaned = re.sub(r"--.*$", " ", sql, flags=re.MULTILINE)
    cleaned = re.sub(r"/\*[\s\S]*?\*/", " ", cleaned)
    cleaned = re.sub(r"'(?:''|[^'])*'", " ", cleaned)
    cleaned = re.sub(r'"(?:""|[^"])*"', " ", cleaned)
    return cleaned


def _extract_referenced_table_names(sql: str) -> list[str]:
    cleaned = _strip_sql_literals(sql)
    return [
        match.group(1).lower()
        for match in re.finditer(
            r"\b(?:FROM|JOIN)\s+`?([A-Za-z_]\w*)`?",
            cleaned,
            flags=re.IGNORECASE,
        )
    ]


def _extract_table_aliases(sql: str) -> dict[str, str]:
    cleaned = _strip_sql_literals(sql)
    aliases: dict[str, str] = {}
    pattern = r"\b(?:FROM|JOIN)\s+`?([A-Za-z_]\w*)`?(?:\s+(?:AS\s+)?`?([A-Za-z_]\w*)`?)?"
    for match in re.finditer(pattern, cleaned, flags=re.IGNORECASE):
        table_name = match.group(1).lower()
        alias = match.group(2)
        if table_name not in TASK3_TABLE_SCHEMA:
            continue

        aliases[table_name] = table_name
        if alias and alias.upper() not in SQL_RESERVED_IDENTIFIERS:
            aliases[alias.lower()] = table_name
    return aliases


def _extract_select_aliases(sql: str) -> set[str]:
    cleaned = _strip_sql_literals(sql)
    return {
        match.group(1).lower()
        for match in re.finditer(
            r"\bAS\s+`?([A-Za-z_]\w*)`?", cleaned, flags=re.IGNORECASE
        )
    }


def _validate_sql_identifiers(sql: str) -> tuple[bool, str]:
    table_aliases = _extract_table_aliases(sql)
    referenced_tables = set(table_aliases.values())
    if not referenced_tables:
        return True, ""

    select_aliases = _extract_select_aliases(sql)
    allowed_columns = {
        column.lower()
        for table_name in referenced_tables
        for column in TASK3_TABLE_SCHEMA[table_name]
    }
    cleaned = _strip_sql_literals(sql)

    qualified_ref_pattern = r"`?([A-Za-z_]\w*)`?\s*\.\s*`?([A-Za-z_]\w*)`?"
    for match in re.finditer(qualified_ref_pattern, cleaned):
        prefix = match.group(1).lower()
        column = match.group(2).lower()
        table_name = table_aliases.get(prefix)
        if table_name is None:
            return False, f"SQL引用了未知表或别名: {match.group(1)}"
        if column not in TASK3_TABLE_SCHEMA[table_name]:
            return False, f"SQL字段不存在: {match.group(1)}.{match.group(2)}"

    scan_sql = re.sub(qualified_ref_pattern, " ", cleaned)
    for match in re.finditer(r"\b([A-Za-z_]\w*)\b", scan_sql):
        identifier = match.group(1)
        identifier_lower = identifier.lower()
        identifier_upper = identifier.upper()
        next_char = scan_sql[match.end() : match.end() + 1]

        if next_char == "(":
            continue
        if identifier_upper in SQL_RESERVED_IDENTIFIERS:
            continue
        if identifier_upper in SQL_FUNCTIONS:
            continue
        if identifier_lower in TASK3_TABLE_SCHEMA:
            continue
        if identifier_lower in table_aliases:
            continue
        if identifier_lower in select_aliases:
            continue
        if identifier_lower in allowed_columns:
            continue

        return False, f"SQL字段不存在或不在引用表范围内: {identifier}"

    return True, ""


def _validate_sql(sql: str) -> tuple[bool, str]:
    sql_upper = sql.upper().strip()

    for keyword in FORBIDDEN_KEYWORDS:
        pattern = r"\b" + keyword + r"\b"
        if re.search(pattern, sql_upper):
            return False, f"SQL包含禁止关键字: {keyword}"

    if not sql_upper.startswith("SELECT"):
        return False, "SQL必须以SELECT开头"

    found_tables = _extract_referenced_table_names(sql)
    for table_name in found_tables:
        if table_name.lower() not in [t.lower() for t in ALLOWED_TABLES]:
            return False, f"SQL引用了不允许的表: {table_name}"

    identifiers_valid, identifiers_msg = _validate_sql_identifiers(sql)
    if not identifiers_valid:
        return False, identifiers_msg

    return True, ""


def _execute_sql(sql: str, db: Session) -> list[dict]:
    try:
        result = db.execute(text(sql))
        columns = list(result.keys())
        rows = result.fetchall()
        return [dict(zip(columns, row)) for row in rows]
    except Exception as exc:
        logger.error("SQL执行失败: sql=%s error=%s", sql, str(exc), exc_info=True)
        raise ServiceException(
            ErrorCode.AI_SERVICE_ERROR, "服务调用失败，请稍后重试"
        ) from exc


def _to_jsonable(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    if isinstance(value, dict):
        return {key: _to_jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_to_jsonable(item) for item in value]
    return value


class Task3Executor:
    def __init__(self, db: Session, plan: ExecutionPlan):
        self.db = db
        self.plan = plan
        self.context: dict[str, Any] = dict(plan.context)
        self.results: dict[str, StepResult] = {}
        self.references: list[Reference] = []
        self.chart_paths: list[str] = []

    def execute_step(self, step: TaskStep) -> StepResult:
        start_time = time.time()
        logger.info("执行步骤: step_id=%s, type=%s, goal=%s", step.step_id, step.step_type, step.goal)

        try:
            if step.step_type == StepType.SQL_QUERY:
                output = self._execute_sql_query(step)
            elif step.step_type == StepType.DERIVE_METRIC:
                output = self._execute_derive_metric(step)
            elif step.step_type == StepType.RETRIEVE_EVIDENCE:
                output = self._execute_retrieve_evidence(step)
            elif step.step_type == StepType.AGGREGATE:
                output = self._execute_aggregate(step)
            elif step.step_type == StepType.VERIFY:
                output = self._execute_verify(step)
            elif step.step_type == StepType.COMPOSE_ANSWER:
                output = self._execute_compose_answer(step)
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
            self.results[step.step_id] = result
            self.context[step.step_id] = output

            logger.info("步骤完成: step_id=%s, time_ms=%d", step.step_id, execution_time_ms)
            return result

        except Exception as exc:
            execution_time_ms = int((time.time() - start_time) * 1000)
            result = StepResult(
                step_id=step.step_id,
                step_type=step.step_type,
                status=StepStatus.FAILED,
                output={},
                error_message=str(exc)[:2000],
                execution_time_ms=execution_time_ms,
            )
            self.results[step.step_id] = result
            logger.error("步骤失败: step_id=%s, error=%s", step.step_id, str(exc))
            return result

    def _execute_sql_query(self, step: TaskStep) -> dict:
        params = step.params
        sql = params.get("sql")

        if sql:
            is_valid, validate_msg = _validate_sql(sql)
            if not is_valid:
                logger.warning(
                    "计划内SQL校验失败，将重新生成: step_id=%s, reason=%s",
                    step.step_id,
                    validate_msg,
                )
                sql = None

        if not sql:
            sql = self._generate_sql_for_step(step)

        if not sql:
            raise ServiceException(
                ErrorCode.AI_SERVICE_ERROR, f"无法生成SQL: step_id={step.step_id}"
            )

        is_valid, validate_msg = _validate_sql(sql)
        if not is_valid:
            raise ServiceException(
                ErrorCode.AI_SERVICE_ERROR, f"SQL校验失败: {validate_msg}"
            )

        if "LIMIT" not in sql.upper():
            sql = sql.rstrip(";") + " LIMIT 1000"

        data = _execute_sql(sql, self.db)

        return {
            "sql": sql,
            "data": data,
            "row_count": len(data),
        }

    def _generate_sql_for_step(self, step: TaskStep) -> str | None:
        config = _get_task3_config()
        executor_config = config.get("executor", {})

        schema_ddl = self._build_schema_ddl()

        system_prompt = executor_config.get("system_prompt", "").format(schema_ddl=schema_ddl)
        user_prompt = executor_config.get("user_prompt_template", "").format(
            step_id=step.step_id,
            step_type=step.step_type.value,
            goal=step.goal,
            params=json.dumps(step.params, ensure_ascii=False),
            context=json.dumps(self.context, ensure_ascii=False, default=str)[:2000],
        )

        response_text = _invoke_llm(system_prompt, user_prompt, max_tokens=2048, temperature=0.0)
        sql = _extract_sql_from_response(response_text)

        if sql:
            logger.info("生成SQL: step_id=%s, sql=%s", step.step_id, sql[:200])

        return sql

    def _build_schema_ddl(self) -> str:
        lines = []

        for table_name, fields in TASK3_TABLE_SCHEMA.items():
            lines.append(f"CREATE TABLE {table_name} (")
            field_lines = [
                f"  {field_name} VARCHAR/INT/DECIMAL COMMENT '{description}'"
                for field_name, description in fields.items()
            ]
            lines.append(",\n".join(field_lines))
            lines.append(");")
            lines.append("")

        return "\n".join(lines)

    def _execute_derive_metric(self, step: TaskStep) -> dict:
        params = step.params
        formula = params.get("formula", "")
        metric_name = params.get("metric_name", "派生指标")

        dep_data = []
        for dep_id in step.depends_on:
            if dep_id in self.context:
                dep_result = self.context[dep_id]
                if isinstance(dep_result, dict) and "data" in dep_result:
                    dep_data.extend(dep_result["data"])

        if not dep_data:
            return {
                "metric_name": metric_name,
                "values": [],
                "formula": formula,
            }

        if "yoy_growth" in metric_name.lower() or "同比增长" in metric_name or "同比" in formula:
            return self._calculate_yoy_growth(dep_data, metric_name, formula)

        if "current_year" in formula and "previous_year" in formula:
            return self._calculate_yoy_growth(dep_data, metric_name, formula)

        calculated_values = []
        for row in dep_data:
            try:
                value = self._evaluate_formula(formula, row)
                calculated_values.append({
                    "stock_code": row.get("stock_code"),
                    "stock_abbr": row.get("stock_abbr"),
                    "report_year": row.get("report_year"),
                    "report_period": row.get("report_period"),
                    metric_name: float(value) if value is not None else None,
                })
            except Exception as e:
                logger.warning("派生指标计算失败: row=%s, error=%s", row, str(e))

        return {
            "metric_name": metric_name,
            "values": calculated_values,
            "formula": formula,
            "count": len(calculated_values),
        }

    def _calculate_yoy_growth(self, data: list[dict], metric_name: str, formula: str) -> dict:
        metric_field = None
        for field in ["total_profit", "net_profit", "total_operating_revenue", "operating_revenue"]:
            if field in formula or any(field in row for row in data):
                metric_field = field
                break

        if not metric_field:
            for row in data:
                for key in row:
                    if key not in ["stock_code", "stock_abbr", "report_year", "report_period", "report_id"]:
                        if isinstance(row[key], (int, float, Decimal)):
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

        sorted_data = sorted(data, key=lambda x: (x.get("stock_code", ""), x.get("report_year", 0)))

        calculated_values = []
        prev_row = None

        for row in sorted_data:
            current_value = row.get(metric_field)
            if current_value is None:
                calculated_values.append({
                    "stock_code": row.get("stock_code"),
                    "stock_abbr": row.get("stock_abbr"),
                    "report_year": row.get("report_year"),
                    "report_period": row.get("report_period"),
                    metric_name: None,
                })
                prev_row = row
                continue

            yoy_value = None
            if prev_row is not None:
                prev_value = prev_row.get(metric_field)
                if prev_value is not None and prev_value != 0:
                    try:
                        yoy_value = (float(current_value) - float(prev_value)) / float(prev_value) * 100
                    except (TypeError, ValueError, ZeroDivisionError):
                        pass

            calculated_values.append({
                "stock_code": row.get("stock_code"),
                "stock_abbr": row.get("stock_abbr"),
                "report_year": row.get("report_year"),
                "report_period": row.get("report_period"),
                metric_name: round(yoy_value, 2) if yoy_value is not None else None,
            })
            prev_row = row

        return {
            "metric_name": metric_name,
            "values": calculated_values,
            "formula": f"yoy_growth({metric_field})",
            "count": len(calculated_values),
        }

    def _evaluate_formula(self, formula: str, row: dict) -> Decimal | None:
        import re
        from decimal import Decimal, InvalidOperation

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

    def _execute_retrieve_evidence(self, step: TaskStep) -> dict:
        params = step.params
        query = params.get("query", step.goal)
        stock_code = params.get("stock_code")
        doc_type = params.get("doc_type")
        top_k = params.get("top_k") or params.get("limit") or 5

        if stock_code is None and "resolved_companies" in self.context:
            companies = self.context["resolved_companies"]
            if companies and isinstance(companies, list) and len(companies) > 0:
                stock_code = companies[0].get("stock_code")

        logger.info(
            "知识库证据检索开始: step_id=%s, query=%s, stock_code=%s, doc_type=%s, top_k=%s",
            step.step_id,
            str(query)[:120],
            stock_code,
            doc_type,
            top_k,
        )

        evidence_list = knowledge_base.search_and_format_evidence(
            query,
            stock_code=stock_code,
            doc_type=doc_type,
            top_k=top_k,
        )

        for evidence in evidence_list:
            ref = Reference(
                paper_path=evidence.get("paper_path"),
                text=evidence.get("text", ""),
                page_no=evidence.get("page_no"),
            )
            self.references.append(ref)

        logger.info(
            "知识库证据检索完成: step_id=%s, evidence_count=%d, reference_total=%d",
            step.step_id,
            len(evidence_list),
            len(self.references),
        )

        return {
            "query": query,
            "evidence_count": len(evidence_list),
            "evidence": evidence_list,
        }

    def _execute_aggregate(self, step: TaskStep) -> dict:
        params = step.params
        operation = params.get("operation", "avg")
        metric = params.get("metric")
        group_by = params.get("group_by")

        dep_data = []
        for dep_id in step.depends_on:
            if dep_id in self.context:
                dep_result = self.context[dep_id]
                if isinstance(dep_result, dict) and "data" in dep_result:
                    dep_data.extend(dep_result["data"])
                elif isinstance(dep_result, dict) and "values" in dep_result:
                    dep_data.extend(dep_result["values"])

        if not dep_data:
            return {
                "operation": operation,
                "metric": metric,
                "result": None,
            }

        numeric_items = []
        for row in dep_data:
            if metric in row and row[metric] is not None:
                try:
                    numeric_items.append((float(row[metric]), row))
                except (TypeError, ValueError):
                    pass

        if not numeric_items:
            return {
                "operation": operation,
                "metric": metric,
                "result": None,
            }

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

    def _execute_verify(self, step: TaskStep) -> dict:
        params = step.params
        check_type = params.get("check_type", "consistency")

        verification_result = {
            "check_type": check_type,
            "passed": True,
            "details": [],
        }

        if check_type == "consistency":
            fields = params.get("fields", [])
            for dep_id in step.depends_on:
                if dep_id in self.context:
                    dep_result = self.context[dep_id]
                    if isinstance(dep_result, dict) and "data" in dep_result:
                        for row in dep_result["data"]:
                            for field in fields:
                                if field in row and row[field] is not None:
                                    verification_result["details"].append({
                                        "field": field,
                                        "value": row[field],
                                        "status": "valid",
                                    })

        elif check_type == "completeness":
            expected_count = params.get("expected_count")
            actual_count = 0
            for dep_id in step.depends_on:
                if dep_id in self.context:
                    dep_result = self.context[dep_id]
                    if isinstance(dep_result, dict):
                        if "data" in dep_result:
                            actual_count += len(dep_result["data"])
                        elif "count" in dep_result:
                            actual_count += dep_result["count"]

            verification_result["actual_count"] = actual_count
            if expected_count:
                verification_result["expected_count"] = expected_count
                verification_result["passed"] = actual_count >= expected_count

        return verification_result

    def _execute_compose_answer(self, step: TaskStep) -> dict:
        config = _get_task3_config()
        answer_config = config.get("answer_builder", {})

        execution_summary = self._build_execution_summary()

        system_prompt = answer_config.get("system_prompt", "")
        user_prompt = answer_config.get("user_prompt_template", "").format(
            question=self.plan.question,
            execution_trace=execution_summary,
        )

        answer_text = _invoke_llm(
            system_prompt, user_prompt, max_tokens=8192, temperature=0.3
        )

        chart_path, chart_type = self._generate_chart_for_answer(step)

        return {
            "answer": answer_text,
            "chart_path": chart_path,
            "chart_type": chart_type,
            "has_references": len(self.references) > 0,
        }

    def _build_execution_summary(self) -> str:
        summary_parts = []

        for step_id, result in self.results.items():
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

                summary_parts.append({
                    "step_id": step_id,
                    "step_type": result.step_type.value,
                    "status": "completed",
                    "output": output_summary,
                })
            else:
                summary_parts.append({
                    "step_id": step_id,
                    "step_type": result.step_type.value,
                    "status": "failed",
                    "error": result.error_message,
                })

        return json.dumps(_to_jsonable(summary_parts), ensure_ascii=False, indent=2)

    def _generate_chart_for_answer(self, step: TaskStep) -> tuple[str | None, str | None]:
        data_for_chart = None

        for step_id in reversed(list(self.results.keys())):
            result = self.results[step_id]
            if result.step_type == StepType.SQL_QUERY and result.status == StepStatus.COMPLETED:
                if result.output.get("data"):
                    data_for_chart = result.output["data"]
                    break

        if not data_for_chart or len(data_for_chart) < 2:
            return None, None

        from app.schemas.chat import IntentResult, QueryType

        intent = IntentResult(
            query_type=QueryType.TREND if len(data_for_chart) >= 3 else QueryType.SINGLE_VALUE,
        )

        question_id = _get_required_task3_question_id(self.context)
        try:
            chart_sequence = int(self.context.get("chart_sequence", 1))
        except (TypeError, ValueError) as exc:
            raise ServiceException(ErrorCode.PARAM_ERROR, "任务三图表顺序编号非法") from exc
        if chart_sequence < 1:
            raise ServiceException(ErrorCode.PARAM_ERROR, "任务三图表顺序编号非法")

        chart_path, chart_type = services_visualization.generate_chart(
            data=data_for_chart,
            intent=intent,
            question_id=question_id,
            sequence=chart_sequence,
        )

        if chart_path:
            self.chart_paths.append(chart_path)

        return chart_path, chart_type

    def get_final_answer(self) -> Task3AnswerContent:
        answer_content = ""
        for step_id in reversed(list(self.results.keys())):
            result = self.results[step_id]
            if result.step_type == StepType.COMPOSE_ANSWER and result.status == StepStatus.COMPLETED:
                answer_content = result.output.get("answer", "")
                break

        if not answer_content:
            answer_content = self._generate_fallback_answer()

        return Task3AnswerContent(
            content=answer_content,
            image=self.chart_paths,
            references=self.references,
        )

    def _generate_fallback_answer(self) -> str:
        parts = []

        for step in self.plan.get_ordered_steps():
            if step.step_id in self.results:
                result = self.results[step.step_id]
                if result.status == StepStatus.COMPLETED:
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

    def get_execution_trace(self) -> ExecutionTrace:
        return ExecutionTrace(
            plan=self.plan,
            results=list(self.results.values()),
            final_answer=self.get_final_answer().content,
            references=[_to_jsonable(r) for r in self.references],
            started_at=self.plan.created_at,
            finished_at=datetime.now(),
        )
