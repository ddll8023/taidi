"""任务三结果校验服务。"""

import re
from decimal import Decimal

from sqlalchemy.orm import Session

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


def verify_execution_trace(
    db: Session, trace: ExecutionTrace
) -> Task3VerificationResultResponse:
    """校验任务三执行轨迹并返回结构化结果。"""
    result = _create_verification_result()
    _verify_completeness(trace, result)
    _verify_consistency(trace, result)
    _verify_reasonableness(trace, result)
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
) -> Task3AnswerQualityResponse:
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

    cause_keywords = ["原因", "是因为", "由于", "为何", "为什么"]
    has_cause_question = any(keyword in question for keyword in cause_keywords)
    has_cause_answer = any(keyword in answer for keyword in cause_keywords)

    if has_cause_question and not has_cause_answer:
        result.warnings.append("问题询问原因，但答案未解释原因")

    if has_cause_question and not references:
        result.warnings.append("原因类问题应提供引用来源")

    return result


"""辅助函数"""


def _create_verification_result() -> Task3VerificationResultResponse:
    """创建空的校验结果结构。"""
    return Task3VerificationResultResponse()


def _add_error(result: Task3VerificationResultResponse, message: str) -> None:
    """向校验结果追加错误。"""
    result.errors.append(message)
    result.passed = False


def _add_warning(result: Task3VerificationResultResponse, message: str) -> None:
    """向校验结果追加警告。"""
    result.warnings.append(message)


def _verify_completeness(
    trace: ExecutionTrace, result: Task3VerificationResultResponse
) -> None:
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


def _verify_consistency(
    trace: ExecutionTrace, result: Task3VerificationResultResponse
) -> None:
    """校验多个 SQL 结果之间的一致性。"""
    sql_results = [
        item
        for item in trace.results
        if item.step_type == StepType.SQL_QUERY and item.status == StepStatus.COMPLETED
    ]
    if len(sql_results) < 2:
        return

    for index, first_result in enumerate(sql_results):
        for second_result in sql_results[index + 1 :]:
            _check_cross_result_consistency(first_result, second_result, result)


def _check_cross_result_consistency(
    result1: StepResult,
    result2: StepResult,
    verification_result: Task3VerificationResultResponse,
) -> None:
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
            str(row[field]) for row in data1 if isinstance(row, dict) and field in row and row[field] is not None
        }
        values2 = {
            str(row[field]) for row in data2 if isinstance(row, dict) and field in row and row[field] is not None
        }
        if values1 and values2 and values1 != values2:
            _add_warning(
                verification_result,
                f"步骤 {result1.step_id} 和 {result2.step_id} 在字段 {field} 上存在不一致",
            )


def _verify_reasonableness(
    trace: ExecutionTrace, result: Task3VerificationResultResponse
) -> None:
    """校验结果数值的合理性。"""
    for step_result in trace.results:
        if step_result.step_type == StepType.SQL_QUERY and step_result.status == StepStatus.COMPLETED:
            _check_data_reasonableness(step_result, result)


def _check_data_reasonableness(
    step_result: StepResult,
    verification_result: Task3VerificationResultResponse,
) -> None:
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


def _verify_references(
    trace: ExecutionTrace, result: Task3VerificationResultResponse
) -> None:
    """校验原因类答案是否附带引用。"""
    answer_result = next(
        (
            item
            for item in trace.results
            if item.step_type == StepType.COMPOSE_ANSWER and item.status == StepStatus.COMPLETED
        ),
        None,
    )
    if answer_result is None:
        return

    answer_text = answer_result.output.get("answer", "")
    references = trace.references
    cause_keywords = [
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

    if any(keyword in answer_text for keyword in cause_keywords) and not references:
        _add_warning(result, "回答包含原因类结论，但缺少引用来源")

    for reference in references:
        if not reference.get("text"):
            _add_warning(result, "存在空引用证据")


def _verify_calculation(formula: str, expected_value: float, actual_data: dict) -> bool:
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
) -> bool:
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
