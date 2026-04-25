"""SQL 生成与解析服务"""
import json
import re

from sqlalchemy.orm import Session

from app.constants import chat as constants_chat
from app.schemas import chat as schemas_chat
from app.schemas.common import ErrorCode
from app.utils.exception import ServiceException
from app.utils.logger_config import setup_logger
from app.services.chat.context import (
    _build_schema_ddl_text,
    _generate_continuity_sql,
    _get_chat_config,
    _invoke_llm,
)
from app.services.chat.metric import _generate_derived_metric_sql
from app.services.chat.query_handler import (
    _extract_topn_limit,
    _generate_cross_table_topn_ratio_sql,
    _generate_multi_metric_topn_intersection_sql,
)

logger = setup_logger(__name__)


"""辅助函数"""


def _build_allowed_column_names():
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


def _extract_select_columns(sql: str):
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


def _extract_column_refs_from_select(columns: list[str]):
    """从SELECT列表中提取字段引用，用于白名单校验。"""
    bare_refs: list[str] = []
    seen_refs: set[str] = set()
    for col in columns:
        for ref in _extract_column_refs_from_expression(col):
            if ref not in seen_refs:
                bare_refs.append(ref)
                seen_refs.add(ref)

    return bare_refs


def _extract_declared_column_aliases(sql: str):
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


def _normalize_result_column_name(column):
    return re.sub(r"[\W_]+", "", str(column)).lower()


def _build_ten_thousand_unit_column_names():
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

                names.add(_normalize_result_column_name(field_name))
                names.add(_normalize_result_column_name(display_text))

                display_text_without_unit = re.sub(
                    r"[（(]?\s*万元\s*[）)]?",
                    "",
                    display_text,
                )
                names.add(_normalize_result_column_name(display_text_without_unit))

    return {name for name in names if name}


def _extract_sql_from_response(response_text: str) -> str | None:
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


def _is_tcm_contest_universe_question(question: str) -> bool:
    return any(
        keyword in question for keyword in constants_chat.TCM_CONTEST_UNIVERSE_KEYWORDS
    )


def _extract_declared_cte_names(sql: str) -> set[str]:
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


def _normalize_sql_for_mysql_compatibility(sql: str) -> str:
    normalized_sql = sql.strip()

    distinct_topn_pattern = re.compile(
        r"""
        SELECT\s+DISTINCT\s+(?P<selected_col>\w+)\s+
        FROM\s+(?P<table_name>\w+)\s*
        (?P<where_clause>[\s\S]*?)
        ORDER\s+BY\s+(?P<order_expr>[\w\.]+)\s+(?P<direction>ASC|DESC)\s+
        LIMIT\s+(?P<limit>\d+)
        """,
        re.IGNORECASE | re.VERBOSE,
    )

    def _rewrite_distinct_topn(match: re.Match[str]) -> str:
        selected_col = match.group("selected_col")
        table_name = match.group("table_name")
        where_clause = match.group("where_clause").rstrip()
        order_expr = match.group("order_expr")
        direction = match.group("direction")
        limit = match.group("limit")

        if re.search(r"\bGROUP\s+BY\b", where_clause, re.IGNORECASE):
            return match.group(0)

        if order_expr.split(".")[-1].lower() == selected_col.lower():
            return match.group(0)

        rewritten_where_clause = f"{where_clause}\n        GROUP BY {selected_col}"
        return (
            f"SELECT {selected_col}\n"
            f"            FROM {table_name}{rewritten_where_clause}\n"
            f"            ORDER BY MAX({order_expr}) {direction}\n"
            f"            LIMIT {limit}"
        )

    rewritten_sql = distinct_topn_pattern.sub(_rewrite_distinct_topn, normalized_sql)
    if rewritten_sql != normalized_sql:
        logger.info("已重写 DISTINCT + ORDER BY TopN 子查询，兼容 MySQL 执行规则")

    return rewritten_sql


def _normalize_sql_for_question(sql: str, intent: schemas_chat.IntentResult) -> str:
    question = intent.question or ""
    normalized_sql = _normalize_sql_for_mysql_compatibility(sql.strip())

    if not question or not _is_tcm_contest_universe_question(question):
        return normalized_sql

    original_sql = normalized_sql
    for pattern in constants_chat.TCM_CONTEST_INDUSTRY_SQL_PATTERNS:
        normalized_sql = re.sub(
            rf"(?i)\b(WHERE|AND)\s+{pattern}",
            lambda match: f"{match.group(1)} 1=1",
            normalized_sql,
        )

    if normalized_sql != original_sql:
        logger.info("已中和中药样本问题中的行业过滤条件")

    return normalized_sql


def _generate_sql(intent: schemas_chat.IntentResult, db: Session) -> str:
    first_metric = intent.get_first_metric()
    metric_field = first_metric.get("field", "") if first_metric else ""
    is_prestored_derived = metric_field in constants_chat.PRESTORED_DERIVED_FIELDS

    multi_metric_topn_sql = _generate_multi_metric_topn_intersection_sql(intent)
    if multi_metric_topn_sql:
        logger.info("使用多指标TopN交集模板生成SQL")
        return multi_metric_topn_sql

    cross_table_topn_ratio_sql = _generate_cross_table_topn_ratio_sql(intent)
    if cross_table_topn_ratio_sql:
        logger.info("使用跨表TopN占比模板生成SQL")
        return cross_table_topn_ratio_sql

    if intent.query_type == schemas_chat.QueryType.CONTINUITY:
        continuity_sql = _generate_continuity_sql(intent)
        if continuity_sql:
            logger.info("使用连续性查询模板生成SQL")
            return continuity_sql

    if (
        intent.derived_metric_type
        and intent.is_derived_query()
        and not is_prestored_derived
        and intent.capability != schemas_chat.QueryCapability.AGGREGATION
    ):
        template_sql = _generate_derived_metric_sql(intent, intent.derived_metric_type)
        if template_sql:
            logger.info("使用派生指标模板生成SQL: %s", intent.derived_metric_type.value)
            return template_sql

    config = _get_chat_config()
    sql_config = config.get("sql_generate", {})

    schema_ddl = _build_schema_ddl_text()
    intent_json = json.dumps(intent.model_dump(), ensure_ascii=False)
    if is_prestored_derived:
        derived_metric_type_str = "无（预存字段，先查表）"
    else:
        derived_metric_type_str = (
            intent.derived_metric_type.value if intent.derived_metric_type else "无"
        )

    system_prompt = sql_config.get("system_prompt", "").replace(
        "{schema_ddl}", schema_ddl
    )
    user_prompt = (
        sql_config.get("user_prompt_template", "")
        .replace("{intent_json}", intent_json)
        .replace("{derived_metric_type}", derived_metric_type_str)
    )

    response_text = _invoke_llm(
        system_prompt, user_prompt, max_tokens=2048, temperature=0.0
    )
    logger.info("SQL生成结果: %s", response_text[:500])

    sql = _extract_sql_from_response(response_text)
    if not sql:
        raise ServiceException(ErrorCode.AI_SERVICE_ERROR, "LLM未能生成有效的SQL语句")
    return sql
