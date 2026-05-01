"""Chat 服务跨文件共享的通用辅助函数"""

import os
import re

import yaml

from langchain_core.messages import HumanMessage, SystemMessage

from app.constants import chat as constants_chat
from app.schemas import chat as schemas_chat
from app.schemas.common import ErrorCode
from app.utils.exception import ServiceException
from app.utils.logger_config import setup_logger
from app.utils.model_factory import get_model

logger = setup_logger(__name__)


"""辅助函数"""


def extract_topn_limit(question: str):
    """从问题中提取 TopN 的数值"""
    if not question:
        return None

    digit_match = re.search(r"前\s*(\d+)", question)
    if digit_match:
        return int(digit_match.group(1))

    chinese_match = re.search(r"前\s*([一二两三四五六七八九十]+)", question)
    if not chinese_match:
        return None

    numeral = chinese_match.group(1)
    if numeral == "十":
        return 10
    if "十" in numeral:
        parts = numeral.split("十")
        tens = constants_chat.CHINESE_NUMERAL_MAP.get(parts[0], 1) if parts[0] else 1
        ones = (
            constants_chat.CHINESE_NUMERAL_MAP.get(parts[1], 0)
            if len(parts) > 1 and parts[1]
            else 0
        )
        return tens * 10 + ones

    return constants_chat.CHINESE_NUMERAL_MAP.get(numeral)


def build_schema_ddl_text():
    """构建表结构 DDL 文本"""
    lines = []
    for table_name, table_info in constants_chat.SCHEMA_INFO.items():
        lines.append(f"CREATE TABLE {table_name} (")
        all_fields = {}
        if "identity_fields" in table_info:
            for f in table_info["identity_fields"]:
                all_fields[f] = "VARCHAR/INT"
        if "metric_fields" in table_info:
            for f, desc in table_info["metric_fields"].items():
                all_fields[f] = "DECIMAL"
        if "fields" in table_info:
            for f, desc in table_info["fields"].items():
                all_fields[f] = "VARCHAR"
        field_lines = [f"  {f} {t} -- {desc}" for f, t in all_fields.items()]
        lines.append(",\n".join(field_lines))
        lines.append(");")
        lines.append("")
    return "\n".join(lines)


def invoke_llm(
    system_prompt: str,
    user_prompt: str,
    max_tokens: int = 32768,
    temperature: float = 0.1,
):
    """调用 LLM 并返回文本响应"""
    logger.info(f"调用LLM: prompt_chars={len(system_prompt) + len(user_prompt)}")
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
        logger.error(f"LLM调用失败: error={exc}", exc_info=True)
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


def build_allowed_column_names():
    """从 SCHEMA_INFO 构建所有允许的列名集合"""
    column_names: set[str] = set()
    for table_name, table_info in constants_chat.SCHEMA_INFO.items():
        if "identity_fields" in table_info:
            column_names.update(table_info["identity_fields"])
        if "metric_fields" in table_info:
            column_names.update(table_info["metric_fields"].keys())
        if "fields" in table_info:
            column_names.update(table_info["fields"].keys())
    return column_names


def extract_select_columns(sql: str):
    """从 SELECT 子句中提取所有列名（不含表别名前缀）"""
    sql_stripped = re.sub(r"--.*$", "", sql.strip(), flags=re.MULTILINE)

    def _find_top_level_keyword(statement: str, keyword: str, start: int = 0):
        upper_statement = statement.upper()
        keyword_upper = keyword.upper()
        keyword_len = len(keyword_upper)
        depth = 0
        in_single_quote = False
        in_double_quote = False
        i = start

        while i < len(statement):
            char = statement[i]

            if in_single_quote:
                if char == "'" and statement[i + 1 : i + 2] == "'":
                    i += 2
                    continue
                if char == "'":
                    in_single_quote = False
            elif in_double_quote:
                if char == '"' and statement[i + 1 : i + 2] == '"':
                    i += 2
                    continue
                if char == '"':
                    in_double_quote = False
            else:
                if char == "'":
                    in_single_quote = True
                elif char == '"':
                    in_double_quote = True
                elif char == "(":
                    depth += 1
                elif char == ")" and depth > 0:
                    depth -= 1
                elif depth == 0 and upper_statement.startswith(keyword_upper, i):
                    prev_char = statement[i - 1] if i > 0 else " "
                    next_index = i + keyword_len
                    next_char = (
                        statement[next_index] if next_index < len(statement) else " "
                    )
                    if not (prev_char.isalnum() or prev_char == "_") and not (
                        next_char.isalnum() or next_char == "_"
                    ):
                        return i

            i += 1

        return -1

    select_index = _find_top_level_keyword(sql_stripped, "SELECT")
    if select_index < 0:
        return []

    from_index = _find_top_level_keyword(sql_stripped, "FROM", start=select_index + 6)
    if from_index < 0:
        select_part = sql_stripped[select_index + 6 :]
    else:
        select_part = sql_stripped[select_index + 6 : from_index]

    columns: list[str] = []
    depth = 0
    in_single_quote = False
    in_double_quote = False
    token_start = 0
    i = 0

    while i < len(select_part):
        c = select_part[i]

        if in_single_quote:
            if c == "'":
                in_single_quote = False
        elif in_double_quote:
            if c == '"':
                in_double_quote = False
        else:
            if c == "'":
                in_single_quote = True
            elif c == '"':
                in_double_quote = True
            elif c == "(":
                depth += 1
            elif c == ")":
                depth -= 1
            elif depth == 0 and c == ",":
                token = select_part[token_start:i].strip()
                if token:
                    columns.append(token)
                token_start = i + 1
        i += 1

    tail = select_part[token_start:].strip()
    if tail:
        columns.append(tail)

    return columns


def _extract_column_refs_from_expression(expression: str):
    """从单个SELECT表达式中提取真实字段名，忽略函数名、关键字和别名。"""
    expression_without_alias = re.sub(
        r"\s+AS\s+(?:`[^`]+`|\"[^\"]+\"|'[^']+'|[^\s,]+)\s*$",
        "",
        expression.strip(),
        flags=re.IGNORECASE,
    )
    expression_clean = re.sub(r"'(?:''|[^'])*'", " ", expression_without_alias)
    expression_clean = re.sub(r'"(?:\"\"|[^\"])*"', " ", expression_clean)

    refs: list[str] = []
    seen_refs: set[str] = set()
    for match in re.finditer(
        r"(?:\b([A-Za-z_]\w*)\s*\.\s*)?\b([A-Za-z_]\w*)\b",
        expression_clean,
    ):
        candidate = match.group(2)
        next_char = expression_clean[match.end() : match.end() + 1]
        if next_char == "(":
            continue

        if candidate.upper() in constants_chat.SQL_RESERVED_IDENTIFIERS:
            continue

        if candidate not in seen_refs:
            refs.append(candidate)
            seen_refs.add(candidate)

    return refs


def extract_column_refs_from_select(columns: list[str]):
    """从SELECT列表中提取字段引用，用于白名单校验。"""
    bare_refs: list[str] = []
    seen_refs: set[str] = set()
    for col in columns:
        for ref in _extract_column_refs_from_expression(col):
            if ref not in seen_refs:
                bare_refs.append(ref)
                seen_refs.add(ref)

    return bare_refs


def extract_declared_column_aliases(sql: str):
    """提取 SQL 中通过 AS 声明的列别名，允许后续 SELECT 引用这些派生列。"""
    aliases: set[str] = set()
    for match in re.finditer(
        r"""(?ix)
        \bAS\s+
        (?:
            `([^`]+)` |
            "([^"]+)" |
            '([^']+)' |
            ([A-Za-z_]\w*)
        )
        """,
        sql,
    ):
        alias = next((group for group in match.groups() if group), "")
        if alias and alias.upper() not in constants_chat.SQL_RESERVED_IDENTIFIERS:
            aliases.add(alias.lower())

    return aliases


def normalize_result_column_name(column):
    """将列名标准化为纯小写无符号形式"""
    return re.sub(r"[\W_]+", "", str(column)).lower()


def build_ten_thousand_unit_column_names():
    """构建所有标注为万元的结果列候选名，用于结果归一化。"""
    names: set[str] = set()

    for table_info in constants_chat.SCHEMA_INFO.values():
        for mapping_key in ("metric_fields", "fields"):
            field_mapping = table_info.get(mapping_key, {})
            if not isinstance(field_mapping, dict):
                continue

            for field_name, display_name in field_mapping.items():
                display_text = str(display_name)
                if "万元" not in display_text:
                    continue

                names.add(normalize_result_column_name(field_name))
                names.add(normalize_result_column_name(display_text))

                display_text_without_unit = re.sub(
                    r"[（(]?\s*万元\s*[）)]?",
                    "",
                    display_text,
                )
                names.add(normalize_result_column_name(display_text_without_unit))

    return {name for name in names if name}


def extract_sql_from_response(response_text: str):
    """从 LLM 响应中提取 SQL 语句"""
    code_block_match = re.search(
        r"```(?:sql)?\s*([\s\S]*?)```", response_text, re.IGNORECASE
    )
    cleaned_text = (
        code_block_match.group(1).strip() if code_block_match else response_text.strip()
    )

    sql_match = re.search(
        r"((?:WITH|SELECT)\b[\s\S]*?)(?:;|$)", cleaned_text, re.IGNORECASE
    )
    if sql_match:
        return sql_match.group(1).strip().rstrip(";")

    if re.match(r"(?i)^(WITH|SELECT)\b", cleaned_text):
        return cleaned_text.rstrip(";")

    return None


def extract_declared_cte_names(sql: str):
    """提取 SQL 中 WITH 子句声明的 CTE 名称"""
    stripped_sql = sql.strip()
    with_match = re.match(r"(?is)^WITH(?:\s+RECURSIVE)?\s+", stripped_sql)
    if not with_match:
        return set()

    cte_names: set[str] = set()
    cursor = with_match.end()
    sql_length = len(stripped_sql)

    while cursor < sql_length:
        while cursor < sql_length and stripped_sql[cursor].isspace():
            cursor += 1

        cte_match = re.match(
            r"(?is)(\w+)\s*(?:\([^)]*\)\s*)?AS\s*\(",
            stripped_sql[cursor:],
        )
        if not cte_match:
            break

        cte_names.add(cte_match.group(1).lower())
        cursor += cte_match.end()

        depth = 1
        in_single_quote = False
        in_double_quote = False
        while cursor < sql_length and depth > 0:
            current_char = stripped_sql[cursor]
            next_char = stripped_sql[cursor + 1] if cursor + 1 < sql_length else ""

            if in_single_quote:
                if current_char == "'" and next_char == "'":
                    cursor += 2
                    continue
                if current_char == "'":
                    in_single_quote = False
            elif in_double_quote:
                if current_char == '"' and next_char == '"':
                    cursor += 2
                    continue
                if current_char == '"':
                    in_double_quote = False
            else:
                if current_char == "'":
                    in_single_quote = True
                elif current_char == '"':
                    in_double_quote = True
                elif current_char == "(":
                    depth += 1
                elif current_char == ")":
                    depth -= 1

            cursor += 1

        while cursor < sql_length and stripped_sql[cursor].isspace():
            cursor += 1

        if cursor < sql_length and stripped_sql[cursor] == ",":
            cursor += 1
            continue

        break

    return cte_names


def load_derived_metrics_config():
    """加载派生指标配置文件"""
    app_dir = os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )
    config_path = os.path.join(
        app_dir,
        "config",
        "prompts",
        "derived_metrics.yaml",
    )
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.warning(f"加载派生指标配置失败: {e}")
        return {}


def generate_derived_metric_sql(
    intent: schemas_chat.IntentResult,
    derived_type: schemas_chat.DerivedMetricType,
):
    """根据派生指标类型生成SQL模板"""
    from app.services.chat.sql_builder import (
        _build_company_filter,
        _extract_comparison_time_points,
        _generate_qoq_comparison_sql,
        _resolve_prestored_derived_metric_source,
        resolve_current_report_period,
    )

    config = load_derived_metrics_config()
    if not config:
        return None

    first_metric = intent.get_first_metric()
    metric = first_metric or {}
    metric_field = metric.get("field", "")
    table_name = metric.get("table", "")
    metric_field, table_name = _resolve_prestored_derived_metric_source(
        metric_field,
        table_name,
        derived_type,
    )
    time_range = intent.time_range or {}
    report_year = time_range.get("report_year", 2025)
    report_period = resolve_current_report_period(time_range.get("report_period", "FY"))
    if report_period not in constants_chat.VALID_REPORT_PERIODS:
        logger.warning(f"非法report_period: {report_period}")
        return None
    comparison_points = _extract_comparison_time_points(time_range)

    if isinstance(report_year, list) and not (
        intent.query_type == schemas_chat.QueryType.COMPARISON
        and len(comparison_points) == 2
    ):
        logger.warning(
            f"report_year为数组格式 {report_year}，派生指标模板不支持多年查询，返回None让LLM生成SQL"
        )
        return None

    JOIN_DERIVED_TYPES = {
        schemas_chat.DerivedMetricType.YOY_GROWTH,
        schemas_chat.DerivedMetricType.QOQ_GROWTH,
        schemas_chat.DerivedMetricType.DIFFERENCE,
    }
    table_alias = "t1" if derived_type in JOIN_DERIVED_TYPES else ""
    company_filter = _build_company_filter(intent, table_alias=table_alias)

    type_key = derived_type.value
    template_config = config.get(type_key)
    if not template_config:
        return None

    sql_template = template_config.get("sql_template", "")

    try:
        if derived_type == schemas_chat.DerivedMetricType.YOY_GROWTH:
            return sql_template.format(
                metric_field=metric_field,
                table_name=table_name,
                company_filter=company_filter,
                report_year=report_year,
                report_period=report_period,
            )

        elif derived_type == schemas_chat.DerivedMetricType.QOQ_GROWTH:
            period_sequence = template_config.get("period_sequence", {})
            if (
                intent.query_type == schemas_chat.QueryType.COMPARISON
                and len(comparison_points) == 2
            ):
                comparison_sql = _generate_qoq_comparison_sql(
                    intent,
                    metric_field,
                    table_name,
                    comparison_points,
                    period_sequence,
                )
                if comparison_sql:
                    return comparison_sql

            period_info = period_sequence.get(
                report_period, {"prev_year_offset": 0, "prev_period": "Q1"}
            )
            return sql_template.format(
                metric_field=metric_field,
                table_name=table_name,
                company_filter=company_filter,
                report_year=report_year,
                report_period=report_period,
                prev_year_offset=period_info.get("prev_year_offset", 0),
                prev_period=period_info.get("prev_period", "Q1"),
            )

        elif derived_type == schemas_chat.DerivedMetricType.CAGR:
            start_year = report_year - 3 if isinstance(report_year, int) else 2022
            return sql_template.format(
                metric_field=metric_field,
                table_name=table_name,
                company_filter=company_filter,
                start_year=start_year,
                end_year=report_year,
                report_period=report_period,
            )

        elif derived_type == schemas_chat.DerivedMetricType.RATIO:
            common_ratios = template_config.get("common_ratios", {})
            ratio_config = None
            for ratio_key, ratio_info in common_ratios.items():
                if ratio_info.get("numerator") == metric_field:
                    ratio_config = ratio_info
                    break

            if ratio_config:
                return sql_template.format(
                    numerator_field=ratio_config.get("numerator", metric_field),
                    denominator_field=ratio_config.get(
                        "denominator", "total_operating_income"
                    ),
                    table_name=table_name,
                    company_filter=company_filter,
                    report_year=report_year,
                    report_period=report_period,
                )
            return None

        elif derived_type == schemas_chat.DerivedMetricType.INDUSTRY_AVG:
            metric_name = metric.get("display_name", metric_field)
            return sql_template.format(
                metric_field=metric_field,
                metric_name=metric_name,
                table_name=table_name,
                report_year=report_year,
                report_period=report_period,
            )

        elif derived_type == schemas_chat.DerivedMetricType.MEDIAN:
            return sql_template.format(
                metric_field=metric_field,
                table_name=table_name,
                report_year=report_year,
                report_period=report_period,
            )

        elif derived_type == schemas_chat.DerivedMetricType.DIFFERENCE:
            if "," in metric_field or "," in table_name:
                return None
            return sql_template.format(
                metric_field=metric_field,
                table_name=table_name,
                year_1=report_year,
                year_2=report_year - 1 if isinstance(report_year, int) else 2024,
                report_period=report_period,
            )

        elif derived_type == schemas_chat.DerivedMetricType.CORRELATION:
            numeric_cols = [metric_field]
            if len(numeric_cols) < 2:
                return None
            start_year = report_year - 3 if isinstance(report_year, int) else 2022
            return sql_template.format(
                metric_field_1=numeric_cols[0],
                metric_field_2=(
                    numeric_cols[1] if len(numeric_cols) > 1 else numeric_cols[0]
                ),
                table_name=table_name,
                company_filter=company_filter,
                start_year=start_year,
                end_year=report_year,
            )

        return None
    except Exception as e:
        logger.warning(f"派生指标SQL模板生成失败: {e}")
        return None


def is_cross_table_topn_ratio_question(question: str):
    """判断问题是否为跨表 TopN 占比查询"""
    if not question:
        return False
    return (
        "排名前" in question
        and extract_topn_limit(question) is not None
        and all(
            keyword in question
            for keyword in constants_chat.CROSS_TABLE_TOPN_RATIO_KEYWORDS
        )
        and any(
            keyword in question
            for keyword in ["占未分配利润", "占未分配利润的比例", "比例"]
        )
    )

