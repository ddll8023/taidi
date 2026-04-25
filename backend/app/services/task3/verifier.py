"""任务三结果校验服务。"""

import re
from decimal import Decimal

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models.balance_sheet import BalanceSheet
from app.models.cash_flow_sheet import CashFlowSheet
from app.models.company_basic_info import CompanyBasicInfo
from app.models.income_sheet import IncomeSheet
from app.schemas.task3 import (
    ExecutionTrace,
    Reference,
    StepResult,
    StepStatus,
    StepType,
    Task3AnswerQualityResponse,
    Task3VerificationResultResponse,
)
from app.utils.logger_config import setup_logger

logger = setup_logger(__name__)

ATTRIBUTION_KEYWORDS = [
    "原因",
    "为什么",
    "为何",
    "导致",
    "解释",
    "归因",
    "驱动因素",
    "影响因素",
    "背离",
    "差异原因",
    "变动原因",
    "分析原因",
]

FINANCIAL_DATA_KEYWORDS = [
    "资产负债表",
    "现金流",
    "现金流量",
    "利润表",
    "营业收入",
    "营业总收入",
    "应收账款",
    "占比",
    "比例",
    "财务",
    "总资产",
    "总负债",
    "净利润",
    "营收",
]

CAUSE_KEYWORDS = [
    "原因",
    "是因为",
    "由于",
    "主要因为",
    "影响因素",
    "导致",
    "造成",
    "为何",
    "为什么",
]

MISSING_FINANCIAL_DATA_PATTERNS = [
    r"无财务数据",
    r"缺少[^。；\n]{0,30}财务数据",
    r"没有[^。；\n]{0,30}财务数据",
    r"无[^。；\n]{0,30}资产负债表",
    r"缺少[^。；\n]{0,30}资产负债表",
    r"无[^。；\n]{0,30}应收账款[^。；\n]{0,20}收入数据",
    r"缺少[^。；\n]{0,30}应收账款[^。；\n]{0,20}收入数据",
]

INCOMPLETE_ANSWER_PATTERNS = [
    r"无法计算",
    r"无法完成",
    r"无法验证",
    r"无法执行",
    r"无法直接回答",
    r"暂时无法",
    r"未检索到",
    r"未获取到",
    r"无数据",
    r"不完整",
    r"部分失败",
    r"数据仅覆盖",
    r"缺少20\d{2}",
    r"建议执行补充查询",
    r"请确认是否已完成",
]

REFERENCE_REQUIRED_KEYWORDS = [
    "研报",
    "研究报告",
    "券商报告",
    "结合研报",
    "风险",
    "风险预警",
    "核心竞争力",
    "评价",
    "规划",
    "项目说明",
    "可靠性",
]


def verify_execution_trace(
    db: Session, trace: ExecutionTrace
):
    """校验任务三执行轨迹并返回结构化结果。"""
    result = _create_verification_result()
    _verify_completeness(trace, result)
    _verify_route_requirements(trace, result)
    _verify_consistency(trace, result)
    _verify_reasonableness(trace, result)
    _verify_subject_consistency(db, trace, result)
    _verify_false_missing_claim(db, trace, result)
    _verify_multi_intent_coverage(trace, result)
    _verify_metric_requirement_coverage(trace, result)
    _verify_attribution_evidence(trace, result)
    _verify_references(trace, result)
    logger.info(
        "校验完成: passed=%s, errors=%d, warnings=%d",
        result.passed,
        len(result.errors),
        len(result.warnings),
    )
    return result


def verify_answer_quality(
    answer: str, references: list[Reference], question: str
):
    """校验答案文本与引用是否满足基本质量要求。"""
    result = Task3AnswerQualityResponse(
        has_answer=bool(answer and answer.strip()),
        answer_length=len(answer) if answer else 0,
        has_references=len(references) > 0,
        reference_count=len(references),
    )

    if not result.has_answer:
        result.warnings.append("答案为空")

    if len(answer) < 20:
        result.warnings.append("答案过短，可能不完整")

    has_cause_question = any(keyword in question for keyword in CAUSE_KEYWORDS)
    has_cause_answer = any(keyword in answer for keyword in CAUSE_KEYWORDS)

    if has_cause_question and not has_cause_answer:
        result.warnings.append("问题询问原因，但答案未解释原因")

    if has_cause_question and not references:
        result.warnings.append("原因类问题应提供引用来源")

    return result


"""辅助函数"""


def _create_verification_result():
    """创建空的校验结果结构。"""
    return Task3VerificationResultResponse()


def _add_error(result: Task3VerificationResultResponse, message: str):
    """向校验结果追加错误。"""
    result.errors.append(message)
    result.passed = False


def _add_warning(result: Task3VerificationResultResponse, message: str):
    """向校验结果追加警告。"""
    result.warnings.append(message)


def _is_attribution_with_financial_data(question: str):
    return any(keyword in question for keyword in ATTRIBUTION_KEYWORDS) and any(
        keyword in question for keyword in FINANCIAL_DATA_KEYWORDS
    )


def _get_answer_result(trace: ExecutionTrace):
    return next(
        (
            item
            for item in trace.results
            if item.step_type == StepType.COMPOSE_ANSWER and item.status == StepStatus.COMPLETED
        ),
        None,
    )


def _get_answer_text(trace: ExecutionTrace):
    answer_result = _get_answer_result(trace)
    if answer_result is not None:
        return str(answer_result.output.get("answer", ""))
    return str(trace.final_answer or "")


def _resolve_target_companies(db: Session, trace: ExecutionTrace):
    context = trace.plan.context or {}
    resolved_companies = context.get("resolved_companies")
    if isinstance(resolved_companies, list) and resolved_companies:
        normalized = []
        seen = set()
        for item in resolved_companies:
            if not isinstance(item, dict):
                continue
            stock_code = str(item.get("stock_code") or "").strip()
            stock_abbr = str(item.get("stock_abbr") or "").strip()
            company_name = str(item.get("company_name") or "").strip()
            key = stock_code or stock_abbr or company_name
            if not key or key in seen:
                continue
            normalized.append(
                {
                    "stock_code": stock_code,
                    "stock_abbr": stock_abbr,
                    "company_name": company_name,
                }
            )
            seen.add(key)
        if normalized:
            return normalized

    stock_code = context.get("stock_code")
    if stock_code:
        row = db.execute(
            select(
                CompanyBasicInfo.stock_code,
                CompanyBasicInfo.stock_abbr,
                CompanyBasicInfo.company_name,
            ).where(CompanyBasicInfo.stock_code == str(stock_code))
        ).first()
        if row:
            return [
                {
                    "stock_code": str(row.stock_code),
                    "stock_abbr": str(row.stock_abbr or ""),
                    "company_name": str(row.company_name or ""),
                }
            ]

    question = trace.plan.question or ""
    rows = db.execute(
        select(
            CompanyBasicInfo.stock_code,
            CompanyBasicInfo.stock_abbr,
            CompanyBasicInfo.company_name,
        )
    ).all()

    matched = []
    seen = set()
    for row in rows:
        stock_code = str(row.stock_code)
        stock_abbr = str(row.stock_abbr or "")
        company_name = str(row.company_name or "")
        if (stock_abbr and stock_abbr in question) or (company_name and company_name in question):
            if stock_code in seen:
                continue
            matched.append(
                {
                    "stock_code": stock_code,
                    "stock_abbr": stock_abbr,
                    "company_name": company_name,
                }
            )
            seen.add(stock_code)
    return matched


def _verify_completeness(
    trace: ExecutionTrace, result: Task3VerificationResultResponse
):
    """校验执行轨迹的完整性。"""
    completed_steps = [item for item in trace.results if item.status == StepStatus.COMPLETED]
    failed_steps = [item for item in trace.results if item.status == StepStatus.FAILED]
    skipped_steps = [item for item in trace.results if item.status == StepStatus.SKIPPED]

    result.details["completeness"] = {
        "total_steps": len(trace.plan.steps),
        "completed_steps": len(completed_steps),
        "failed_steps": len(failed_steps),
        "skipped_steps": len(skipped_steps),
    }

    for step_result in failed_steps:
        _add_warning(result, f"步骤 {step_result.step_id} 执行失败: {step_result.error_message}")

    for step_result in skipped_steps:
        _add_warning(result, f"步骤 {step_result.step_id} 被跳过: {step_result.error_message}")

    has_compose_answer = any(
        item.step_type == StepType.COMPOSE_ANSWER and item.status == StepStatus.COMPLETED
        for item in trace.results
    )
    if not has_compose_answer:
        _add_error(result, "缺少最终答案组装步骤")


def _verify_route_requirements(
    trace: ExecutionTrace, result: Task3VerificationResultResponse
):
    """校验题型与执行路由是否匹配。"""
    question = trace.plan.question or ""
    plan_step_types = [step.step_type for step in trace.plan.steps]
    has_sql_step = StepType.SQL_QUERY in plan_step_types
    has_retrieve_step = StepType.RETRIEVE_EVIDENCE in plan_step_types

    route_type = "unknown"
    if plan_step_types == [StepType.RETRIEVE_EVIDENCE, StepType.COMPOSE_ANSWER]:
        route_type = "knowledge_only"
    elif plan_step_types == [
        StepType.SQL_QUERY,
        StepType.RETRIEVE_EVIDENCE,
        StepType.VERIFY,
        StepType.COMPOSE_ANSWER,
    ]:
        route_type = "hybrid"
    elif plan_step_types == [StepType.SQL_QUERY, StepType.COMPOSE_ANSWER]:
        route_type = "sql_only"
    elif plan_step_types:
        route_type = "dynamic"

    result.details["route_check"] = {
        "question": question,
        "route_type": route_type,
        "step_types": [step_type.value for step_type in plan_step_types],
    }

    if _is_attribution_with_financial_data(question):
        if not has_sql_step:
            _add_error(result, "归因分析类财务问题缺少 SQL 查询步骤")
        if not has_retrieve_step:
            _add_warning(result, "归因分析类财务问题缺少知识库检索步骤")


def _verify_consistency(
    trace: ExecutionTrace, result: Task3VerificationResultResponse
):
    """校验多个 SQL 结果之间的一致性。"""
    sql_results = [
        item
        for item in trace.results
        if item.step_type == StepType.SQL_QUERY and item.status == StepStatus.COMPLETED
    ]
    if len(sql_results) < 2:
        return

    for index, first_result in enumerate(sql_results):
        for second_result in sql_results[index + 1:]:
            _check_cross_result_consistency(first_result, second_result, result)


def _check_cross_result_consistency(
    result1: StepResult,
    result2: StepResult,
    verification_result: Task3VerificationResultResponse,
):
    """对比两个 SQL 结果在关键字段上的差异。"""
    data1 = result1.output.get("data", [])
    data2 = result2.output.get("data", [])
    if not data1 or not data2:
        return

    common_fields = set()
    if isinstance(data1[0], dict):
        common_fields.update(data1[0].keys())
    if isinstance(data2[0], dict):
        common_fields.intersection_update(data2[0].keys())

    key_fields = {"stock_code", "stock_abbr", "report_year", "report_period"}
    common_key_fields = common_fields & key_fields
    if not common_key_fields:
        return

    for field in common_key_fields:
        values1 = {
            str(row[field])
            for row in data1
            if isinstance(row, dict) and field in row and row[field] is not None
        }
        values2 = {
            str(row[field])
            for row in data2
            if isinstance(row, dict) and field in row and row[field] is not None
        }
        if values1 and values2 and values1 != values2:
            _add_warning(
                verification_result,
                f"步骤 {result1.step_id} 和 {result2.step_id} 在字段 {field} 上存在不一致",
            )


def _verify_reasonableness(
    trace: ExecutionTrace, result: Task3VerificationResultResponse
):
    """校验结果数值的合理性。"""
    for step_result in trace.results:
        if step_result.step_type == StepType.SQL_QUERY and step_result.status == StepStatus.COMPLETED:
            _check_data_reasonableness(step_result, result)


def _check_data_reasonableness(
    step_result: StepResult,
    verification_result: Task3VerificationResultResponse,
):
    """检查同比和比率字段是否存在异常值。"""
    data = step_result.output.get("data", [])
    if not data:
        return

    yoy_fields = [
        "net_profit_yoy_growth",
        "operating_revenue_yoy_growth",
        "asset_total_assets_yoy_growth",
        "liability_total_liabilities_yoy_growth",
        "net_cash_flow_yoy_growth",
    ]
    ratio_fields = [
        "asset_liability_ratio",
        "gross_profit_margin",
        "net_profit_margin",
        "roe",
    ]

    for row in data:
        for field in yoy_fields:
            if field in row and row[field] is not None:
                try:
                    value = float(row[field])
                except (TypeError, ValueError):
                    continue
                if abs(value) > 1000:
                    _add_warning(
                        verification_result,
                        f"步骤 {step_result.step_id} 中字段 {field} 的值 {value}% 异常（超过1000%）",
                    )

        for field in ratio_fields:
            if field in row and row[field] is not None:
                try:
                    value = float(row[field])
                except (TypeError, ValueError):
                    continue
                if value < -100 or value > 200:
                    _add_warning(
                        verification_result,
                        f"步骤 {step_result.step_id} 中字段 {field} 的值 {value}% 可能异常",
                    )


def _verify_subject_consistency(
    db: Session,
    trace: ExecutionTrace,
    result: Task3VerificationResultResponse,
):
    """校验检索证据主体是否与题目主体一致。"""
    target_companies = _resolve_target_companies(db, trace)
    if not target_companies:
        return

    target_codes = {
        item["stock_code"]
        for item in target_companies
        if item.get("stock_code")
    }
    target_names = {
        name
        for item in target_companies
        for name in (item.get("stock_abbr"), item.get("company_name"))
        if name
    }

    evidence_list = []
    for step_result in trace.results:
        if step_result.step_type != StepType.RETRIEVE_EVIDENCE:
            continue
        if step_result.status != StepStatus.COMPLETED:
            continue
        evidence = step_result.output.get("evidence", [])
        if isinstance(evidence, list):
            evidence_list.extend(item for item in evidence if isinstance(item, dict))

    if not evidence_list:
        return

    matched_count = 0
    mismatched_subjects = set()
    for evidence in evidence_list:
        evidence_code = str(evidence.get("stock_code") or "").strip()
        title = str(evidence.get("title") or "")
        stock_abbr = str(evidence.get("stock_abbr") or "")
        text = str(evidence.get("text") or "")
        combined = f"{title} {stock_abbr} {text[:200]}"
        is_match = False
        if evidence_code and evidence_code in target_codes:
            is_match = True
        elif any(name in combined for name in target_names):
            is_match = True

        if is_match:
            matched_count += 1
            continue

        if evidence_code:
            mismatched_subjects.add(evidence_code)
        if stock_abbr:
            mismatched_subjects.add(stock_abbr)

    result.details["subject_consistency"] = {
        "targets": target_companies,
        "evidence_count": len(evidence_list),
        "matched_count": matched_count,
        "mismatched_subjects": sorted(mismatched_subjects),
    }

    if evidence_list and matched_count == 0:
        target_desc = "/".join(sorted(target_names | target_codes))
        _add_error(result, f"检索证据与题目主体不一致，目标主体为 {target_desc}")


def _answer_claims_missing_financial_data(answer_text: str):
    if not answer_text:
        return False
    return any(re.search(pattern, answer_text) for pattern in MISSING_FINANCIAL_DATA_PATTERNS)


def _answer_indicates_incomplete(answer_text: str):
    if not answer_text:
        return False
    return any(re.search(pattern, answer_text) for pattern in INCOMPLETE_ANSWER_PATTERNS)


def _has_explicit_multi_steps(question: str):
    step_markers = re.findall(r"[①②③④⑤⑥⑦⑧⑨⑩]|\d+\.", question or "")
    clause_count = len(
        [item for item in re.split(r"[；;。]\s*|\n+", question or "") if item.strip()]
    )
    return len(step_markers) >= 3 or clause_count >= 3


def _question_requires_references(question: str):
    """判断题目是否明确要求证据或引用支撑。"""
    return any(keyword in (question or "") for keyword in REFERENCE_REQUIRED_KEYWORDS)


def _has_sql_data(trace: ExecutionTrace):
    for step_result in trace.results:
        if step_result.step_type != StepType.SQL_QUERY:
            continue
        if step_result.status != StepStatus.COMPLETED:
            continue
        data = step_result.output.get("data", [])
        if isinstance(data, list) and data:
            return True
    return False


def _has_structured_financial_data(db: Session, stock_code: str):
    count_queries = [
        select(func.count()).select_from(IncomeSheet).where(IncomeSheet.stock_code == stock_code),
        select(func.count()).select_from(CashFlowSheet).where(CashFlowSheet.stock_code == stock_code),
        select(func.count()).select_from(BalanceSheet).where(BalanceSheet.stock_code == stock_code),
    ]
    for stmt in count_queries:
        count = db.execute(stmt).scalar() or 0
        if count > 0:
            return True
    return False


def _verify_false_missing_claim(
    db: Session,
    trace: ExecutionTrace,
    result: Task3VerificationResultResponse,
):
    """校验答案是否错误声称结构化财务数据缺失。"""
    answer_text = _get_answer_text(trace)
    if not _answer_claims_missing_financial_data(answer_text):
        return

    target_companies = _resolve_target_companies(db, trace)
    if not target_companies:
        return

    target_stock_code = str(target_companies[0].get("stock_code") or "")
    sql_has_data = _has_sql_data(trace)
    db_has_data = bool(target_stock_code) and _has_structured_financial_data(db, target_stock_code)

    result.details["data_availability_check"] = {
        "target_stock_code": target_stock_code,
        "sql_has_data": sql_has_data,
        "db_has_data": db_has_data,
    }

    if sql_has_data or db_has_data:
        _add_error(result, f"答案错误声称目标公司缺少财务数据，但结构化数据已存在: {target_stock_code}")


def _verify_multi_intent_coverage(
    trace: ExecutionTrace,
    result: Task3VerificationResultResponse,
):
    """校验显式多步骤问题是否只给出了部分结论。"""
    question = trace.plan.question or ""
    if not _has_explicit_multi_steps(question):
        return

    answer_text = _get_answer_text(trace)
    if _answer_indicates_incomplete(answer_text):
        _add_error(result, "多步骤问题存在未完成子任务，当前答案仅部分覆盖题目要求")

    completed_sql_steps = [
        item for item in trace.results
        if item.step_type == StepType.SQL_QUERY and item.status == StepStatus.COMPLETED
    ]
    if len(completed_sql_steps) == 1 and _question_requires_references(question):
        retrieve_steps = [
            item for item in trace.results
            if item.step_type == StepType.RETRIEVE_EVIDENCE and item.status == StepStatus.COMPLETED
        ]
        if not retrieve_steps:
            _add_error(result, "多步骤题缺少证据检索步骤，执行链未完整覆盖题目要求")


def _verify_metric_requirement_coverage(
    trace: ExecutionTrace,
    result: Task3VerificationResultResponse,
):
    """校验关键指标是否被正确回答，避免偷换题目指标。"""
    question = trace.plan.question or ""
    answer_text = _get_answer_text(trace)

    if "流动比率" in question:
        support_keywords = ["流动资产", "流动负债", "字段缺失", "字段不足", "底库不支持", "无法计算流动比率"]
        if not any(keyword in answer_text for keyword in support_keywords):
            _add_error(result, "题目要求流动比率，但回答未明确说明缺少流动资产/流动负债字段，存在指标偷换风险")


def _verify_attribution_evidence(
    trace: ExecutionTrace,
    result: Task3VerificationResultResponse,
):
    """校验归因类财务问题是否具备最基本的财务证据与引用支撑。"""
    question = trace.plan.question or ""
    if not _is_attribution_with_financial_data(question):
        return

    answer_text = _get_answer_text(trace)
    if not any(keyword in answer_text for keyword in CAUSE_KEYWORDS):
        return

    sql_row_count = 0
    for step_result in trace.results:
        if step_result.step_type != StepType.SQL_QUERY:
            continue
        if step_result.status != StepStatus.COMPLETED:
            continue
        data = step_result.output.get("data", [])
        if isinstance(data, list):
            sql_row_count += len(data)

    if sql_row_count == 0:
        _add_error(result, "归因分析类财务问题缺少有效结构化财务数据，却输出了原因结论")

    if not trace.references:
        _add_error(result, "归因分析类问题缺少引用证据，不应直接输出原因结论")


def _verify_references(
    trace: ExecutionTrace, result: Task3VerificationResultResponse
):
    """校验原因类答案是否附带引用。"""
    answer_result = _get_answer_result(trace)
    if answer_result is None:
        return

    answer_text = answer_result.output.get("answer", "")
    references = trace.references
    question = trace.plan.question or ""

    if any(keyword in answer_text for keyword in CAUSE_KEYWORDS) and not references:
        _add_error(result, "回答包含原因类结论，但缺少引用来源")

    if _question_requires_references(question) and not references:
        if answer_text and not _answer_indicates_incomplete(answer_text):
            _add_error(result, "题目要求研报/证据支撑，但回答未提供引用来源")

    for reference in references:
        if not reference.get("text"):
            _add_warning(result, "存在空引用证据")


def _verify_calculation(formula: str, expected_value: float, actual_data: dict):
    """校验公式计算值是否接近期望值。"""
    expr = formula
    for field, value in actual_data.items():
        if value is not None and isinstance(value, (int, float, Decimal)):
            expr = expr.replace(field, str(float(value)))
    try:
        sanitized_expr = re.sub(r"[a-zA-Z_][a-zA-Z0-9_]*", "0", expr)
        result = eval(sanitized_expr, {"__builtins__": {}}, {})
    except Exception as exc:
        logger.warning("计算验证失败: formula=%s, error=%s", formula, str(exc))
        return False

    tolerance = abs(expected_value) * 0.01 + 0.01
    return abs(result - expected_value) <= tolerance


def _verify_sql_result_count(
    step_result: StepResult,
    verification_result: Task3VerificationResultResponse,
    expected_min: int | None = None,
    expected_max: int | None = None,
):
    """校验 SQL 结果条数是否在预期范围内。"""
    data = step_result.output.get("data", [])
    count = len(data)

    if expected_min is not None and count < expected_min:
        _add_warning(
            verification_result,
            f"步骤 {step_result.step_id} 返回数据行数 {count} 少于预期最小值 {expected_min}",
        )
        return False

    if expected_max is not None and count > expected_max:
        _add_warning(
            verification_result,
            f"步骤 {step_result.step_id} 返回数据行数 {count} 超过预期最大值 {expected_max}",
        )
        return False

    return True
