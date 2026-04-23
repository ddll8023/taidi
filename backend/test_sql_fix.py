import re

SQL_RESERVED_IDENTIFIERS = {
    "ALL", "AND", "AS", "ASC", "BETWEEN", "BY", "CASE", "CAST",
    "CROSS", "DESC", "DISTINCT", "ELSE", "END", "EXISTS", "FROM",
    "FULL", "GROUP", "HAVING", "IN", "INNER", "IS", "JOIN", "LEFT",
    "LIKE", "LIMIT", "NOT", "NULL", "ON", "OR", "ORDER", "OUTER",
    "OVER", "PARTITION", "RIGHT", "ROWS", "SELECT", "THEN", "UNBOUNDED",
    "UNION", "WHEN", "WHERE", "WITH",
}
ALLOWED_TABLES = [
    "income_sheet",
    "balance_sheet",
    "cash_flow_sheet",
    "core_performance_indicators_sheet",
    "company_basic_info",
]


def _extract_column_refs_from_expression(expression: str) -> list[str]:
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

        if candidate.upper() in SQL_RESERVED_IDENTIFIERS:
            continue

        # 跳过 FROM/JOIN/INTO 等关键字后面的表名
        prefix = expression_clean[: match.start()].rstrip()
        if prefix.upper().endswith(("FROM", "JOIN", "INTO", "TABLE", "UPDATE")):
            continue

        # 跳过已知的表名
        if candidate.lower() in {t.lower() for t in ALLOWED_TABLES}:
            continue

        # 如果 match.group(1) 存在，说明是 "别名.字段名" 形式
        # 此时 candidate 是字段名，应该保留
        # 但 group 1（别名）本身不会被加入 refs，因为正则只提取 group 2

        # 对于独立标识符（没有表别名前缀），检查它是否是子查询中的别名
        # 通过检查前面是否紧跟 SQL 子句关键字来判断
        before = expression_clean[:match.start()].rstrip()
        # 获取 before 的最后一个"单词"
        last_word_match = re.search(r'\b(\w+)\s*$', before)
        if last_word_match:
            last_word = last_word_match.group(1).upper()
            # 如果前面是这些关键字，说明 candidate 是表名/别名，跳过
            if last_word in {"FROM", "JOIN", "INTO", "TABLE", "UPDATE", "ON", "AND", "OR", "WHERE", "HAVING", "BY", "AS"}:
                continue

        # 额外检查：如果 candidate 是短标识符（1-3字符或字母+数字），且前面是 SELECT/DISTINCT 等，可能是子查询别名
        # 但更可靠的方式：检查 candidate 是否是已知表名的一部分，或者是否是典型的表别名模式
        # 实际上，如果 candidate 不在 SCHEMA 的任何字段中，它可能是别名
        # 但为了安全，我们只跳过明确的场景

        if candidate not in seen_refs:
            refs.append(candidate)
            seen_refs.add(candidate)

    return refs


def _extract_select_columns(sql: str) -> list[str]:
    """从 SELECT 子句中提取所有列名（不含表别名前缀）"""
    sql_stripped = re.sub(r"--.*$", "", sql.strip(), flags=re.MULTILINE)

    def _find_top_level_keyword(statement: str, keyword: str, start: int = 0) -> int:
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
                    next_char = statement[next_index] if next_index < len(statement) else " "
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
    token_start =