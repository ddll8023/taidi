import re
from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.schemas.task3 import (
    ExecutionTrace,
    Reference,
    StepResult,
    StepStatus,
    StepType,
)
from app.utils.logger_config import setup_logger

logger = setup_logger(__name__)


class VerificationResult:
    def __init__(self):
        self.passed: bool = True
        self.errors: list[str] = []
        self.warnings: list[str] = []
        self.details: dict[str, Any] = {}

    def add_error(self, message: str):
        self.errors.append(message)
        self.passed = False

    def add_warning(self, message: str):
        self.warnings.append(message)

    def to_dict(self) -> dict:
        return {
            "passed": self.passed,
            "errors": self.errors,
            "warnings": self.warnings,
            "details": self.details,
        }


class Task3Verifier:
    def __init__(self, db: Session, trace: ExecutionTrace):
        self.db = db
        self.trace = trace
        self.result = VerificationResult()

    def verify_all(self) -> VerificationResult:
        self.verify_completeness()
        self.verify_consistency()
        self.verify_reasonableness()
        self.verify_references()

        logger.info(
            "校验完成: passed=%s, errors=%d, warnings=%d",
            self.result.passed,
            len(self.result.errors),
            len(self.result.warnings),
        )
        return self.result

    def verify_completeness(self):
        plan = self.trace.plan
        completed_steps = [
            r for r in self.trace.results if r.status == StepStatus.COMPLETED
        ]
        failed_steps = [
            r for r in self.trace.results if r.status == StepStatus.FAILED
        ]
        skipped_steps = [
            r for r in self.trace.results if r.status == StepStatus.SKIPPED
        ]

        total_steps = len(plan.steps)
        completed_count = len(completed_steps)

        self.result.details["completeness"] = {
            "total_steps": total_steps,
            "completed_steps": completed_count,
            "failed_steps": len(failed_steps),
            "skipped_steps": len(skipped_steps),
        }

        if failed_steps:
            for step_result in failed_steps:
                self.result.add_warning(
                    f"步骤 {step_result.step_id} 执行失败: {step_result.error_message}"
                )

        if skipped_steps:
            for step_result in skipped_steps:
                self.result.add_warning(
                    f"步骤 {step_result.step_id} 被跳过: {step_result.error_message}"
                )

        has_compose_answer = any(
            r.step_type == StepType.COMPOSE_ANSWER and r.status == StepStatus.COMPLETED
            for r in self.trace.results
        )
        if not has_compose_answer:
            self.result.add_error("缺少最终答案组装步骤")

    def verify_consistency(self):
        sql_results = []
        for result in self.trace.results:
            if result.step_type == StepType.SQL_QUERY and result.status == StepStatus.COMPLETED:
                sql_results.append(result)

        if len(sql_results) < 2:
            return

        for i, result1 in enumerate(sql_results):
            for result2 in sql_results[i + 1:]:
                self._check_cross_result_consistency(result1, result2)

    def _check_cross_result_consistency(
        self, result1: StepResult, result2: StepResult
    ):
        data1 = result1.output.get("data", [])
        data2 = result2.output.get("data", [])

        if not data1 or not data2:
            return

        common_fields = set()
        if data1 and isinstance(data1[0], dict):
            common_fields.update(data1[0].keys())
        if data2 and isinstance(data2[0], dict):
            common_fields.intersection_update(data2[0].keys())

        key_fields = {"stock_code", "stock_abbr", "report_year", "report_period"}
        common_key_fields = common_fields & key_fields

        if not common_key_fields:
            return

        for field in common_key_fields:
            values1 = set()
            values2 = set()

            for row in data1:
                if field in row and row[field] is not None:
                    values1.add(str(row[field]))

            for row in data2:
                if field in row and row[field] is not None:
                    values2.add(str(row[field]))

            if values1 and values2 and values1 != values2:
                self.result.add_warning(
                    f"步骤 {result1.step_id} 和 {result2.step_id} 在字段 {field} 上存在不一致"
                )

    def verify_reasonableness(self):
        for result in self.trace.results:
            if result.step_type == StepType.SQL_QUERY and result.status == StepStatus.COMPLETED:
                self._check_data_reasonableness(result)

    def _check_data_reasonableness(self, result: StepResult):
        data = result.output.get("data", [])
        if not data:
            return

        yoy_fields = [
            "net_profit_yoy_growth",
            "operating_revenue_yoy_growth",
            "asset_total_assets_yoy_growth",
            "liability_total_liabilities_yoy_growth",
            "net_cash_flow_yoy_growth",
        ]

        for row in data:
            for field in yoy_fields:
                if field in row and row[field] is not None:
                    try:
                        value = float(row[field])
                        if abs(value) > 1000:
                            self.result.add_warning(
                                f"步骤 {result.step_id} 中字段 {field} 的值 {value}% 异常（超过1000%）"
                            )
                    except (TypeError, ValueError):
                        pass

        ratio_fields = [
            "asset_liability_ratio",
            "gross_profit_margin",
            "net_profit_margin",
            "roe",
        ]

        for row in data:
            for field in ratio_fields:
                if field in row and row[field] is not None:
                    try:
                        value = float(row[field])
                        if value < -100 or value > 200:
                            self.result.add_warning(
                                f"步骤 {result.step_id} 中字段 {field} 的值 {value}% 可能异常"
                            )
                    except (TypeError, ValueError):
                        pass

    def verify_references(self):
        answer_result = None
        for result in self.trace.results:
            if result.step_type == StepType.COMPOSE_ANSWER and result.status == StepStatus.COMPLETED:
                answer_result = result
                break

        if not answer_result:
            return

        answer_text = answer_result.output.get("answer", "")
        references = self.trace.references

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

        has_cause_statement = any(kw in answer_text for kw in cause_keywords)

        if has_cause_statement and not references:
            self.result.add_warning("回答包含原因类结论，但缺少引用来源")

        if references:
            for ref in references:
                if not ref.get("text"):
                    self.result.add_warning("存在空引用证据")

    def verify_calculation(self, formula: str, expected_value: float, actual_data: dict) -> bool:
        try:
            expr = formula
            for field, value in actual_data.items():
                if value is not None and isinstance(value, (int, float, Decimal)):
                    expr = expr.replace(field, str(float(value)))

            expr = re.sub(r"[a-zA-Z_][a-zA-Z0-9_]*", "0", expr)

            result = eval(expr, {"__builtins__": {}}, {})

            tolerance = abs(expected_value) * 0.01 + 0.01
            return abs(result - expected_value) <= tolerance

        except Exception as e:
            logger.warning("计算验证失败: formula=%s, error=%s", formula, str(e))
            return False

    def verify_sql_result_count(
        self, result: StepResult, expected_min: int | None = None, expected_max: int | None = None
    ) -> bool:
        data = result.output.get("data", [])
        count = len(data)

        if expected_min is not None and count < expected_min:
            self.result.add_warning(
                f"步骤 {result.step_id} 返回数据行数 {count} 少于预期最小值 {expected_min}"
            )
            return False

        if expected_max is not None and count > expected_max:
            self.result.add_warning(
                f"步骤 {result.step_id} 返回数据行数 {count} 超过预期最大值 {expected_max}"
            )
            return False

        return True


def verify_execution_trace(db: Session, trace: ExecutionTrace) -> dict:
    verifier = Task3Verifier(db, trace)
    result = verifier.verify_all()
    return result.to_dict()


def verify_answer_quality(
    answer: str, references: list[Reference], question: str
) -> dict:
    result = {
        "has_answer": bool(answer and answer.strip()),
        "answer_length": len(answer) if answer else 0,
        "has_references": len(references) > 0,
        "reference_count": len(references),
        "warnings": [],
    }

    if not result["has_answer"]:
        result["warnings"].append("答案为空")

    if len(answer) < 20:
        result["warnings"].append("答案过短，可能不完整")

    cause_keywords = ["原因", "是因为", "由于", "为何", "为什么"]
    has_cause_question = any(kw in question for kw in cause_keywords)
    has_cause_answer = any(kw in answer for kw in cause_keywords)

    if has_cause_question and not has_cause_answer:
        result["warnings"].append("问题询问原因，但答案未解释原因")

    if has_cause_question and not references:
        result["warnings"].append("原因类问题应提供引用来源")

    return result
