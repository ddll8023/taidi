import json
import re
from datetime import datetime
from typing import Any

from langchain_core.messages import HumanMessage, SystemMessage
from sqlalchemy.orm import Session

from app.core.config import settings
from app.schemas.common import ErrorCode
from app.schemas.task3 import (
    ExecutionPlan,
    Reference,
    StepResult,
    StepStatus,
    StepType,
    TaskStep,
)
from app.utils.exception import ServiceException
from app.utils.logger_config import setup_logger
from app.utils.model_factory import get_model

logger = setup_logger(__name__)


def _get_task3_config() -> dict:
    return settings.PROMPT_CONFIG.get_task3_config


def _invoke_llm(
    system_prompt: str,
    user_prompt: str,
    max_tokens: int = 8192,
    temperature: float = 0.1,
) -> str:
    logger.info("调用LLM: prompt_chars=%d", len(system_prompt) + len(user_prompt))
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


def analyze_question(question: str, context: dict | None = None) -> dict:
    config = _get_task3_config()
    planner_config = config.get("planner", {})

    system_prompt = planner_config.get("system_prompt", "")

    context_str = ""
    if context:
        context_str = json.dumps(context, ensure_ascii=False, indent=2)
    else:
        context_str = "无额外上下文"

    user_prompt = planner_config.get("user_prompt_template", "").format(
        question=question,
        context=context_str,
    )

    response_text = _invoke_llm(
        system_prompt, user_prompt, max_tokens=4096, temperature=0.0
    )
    logger.info("规划分析结果: %s", response_text[:500])

    parsed = _extract_json_from_response(response_text)
    if parsed is None:
        logger.warning("规划分析返回非JSON，使用默认计划")
        return _create_default_plan(question)

    return parsed


def _create_default_plan(question: str) -> dict:
    return {
        "steps": [
            {
                "step_id": "s1",
                "step_type": "sql_query",
                "goal": f"查询与问题相关的数据: {question[:100]}",
                "depends_on": [],
                "params": {},
                "priority": 0,
            },
            {
                "step_id": "s2",
                "step_type": "compose_answer",
                "goal": "生成最终答案",
                "depends_on": ["s1"],
                "params": {"include_references": False},
                "priority": 100,
            },
        ],
        "context": {},
        "reasoning": "默认简单计划：单步查询后直接回答",
    }


def create_execution_plan(
    question: str,
    context: dict | None = None,
) -> ExecutionPlan:
    plan_dict = analyze_question(question, context)

    steps = []
    for step_data in plan_dict.get("steps", []):
        try:
            step_type_str = step_data.get("step_type", "sql_query")
            step_type = StepType(step_type_str)
        except ValueError:
            step_type = StepType.SQL_QUERY

        raw_params = step_data.get("params", {})
        params = raw_params if isinstance(raw_params, dict) else {}
        if step_type == StepType.SQL_QUERY and "sql" in params:
            params = {key: value for key, value in params.items() if key != "sql"}
            params.setdefault("description", step_data.get("goal", ""))

        step = TaskStep(
            step_id=step_data.get("step_id", f"s{len(steps) + 1}"),
            step_type=step_type,
            goal=step_data.get("goal", ""),
            depends_on=step_data.get("depends_on", []),
            params=params,
            priority=step_data.get("priority", len(steps) * 10),
        )
        steps.append(step)

    if not steps:
        steps = [
            TaskStep(
                step_id="s1",
                step_type=StepType.SQL_QUERY,
                goal=f"查询与问题相关的数据",
                depends_on=[],
                params={},
                priority=0,
            ),
            TaskStep(
                step_id="s2",
                step_type=StepType.COMPOSE_ANSWER,
                goal="生成最终答案",
                depends_on=["s1"],
                params={"include_references": False},
                priority=100,
            ),
        ]

    plan = ExecutionPlan(
        question=question,
        steps=steps,
        context=plan_dict.get("context", context or {}),
        created_at=datetime.now(),
    )

    logger.info(
        "执行计划创建完成: question=%s, steps=%d",
        question[:50],
        len(steps),
    )
    return plan


def detect_multi_intent(question: str) -> bool:
    multi_intent_keywords = [
        "并", "同时", "分别", "各自", "以及",
        "排名", "前几", "Top", "最高", "最低",
        "对比", "比较", "差异", "区别",
        "原因", "为什么", "为何", "如何",
        "趋势", "变化", "增长", "下降",
        "行业", "平均", "均值",
        "是否一致", "核实", "校验", "重新计算",
    ]

    question_lower = question.lower()
    for keyword in multi_intent_keywords:
        if keyword.lower() in question_lower:
            return True

    return False


def estimate_complexity(question: str) -> str:
    score = 0

    if detect_multi_intent(question):
        score += 2

    if any(kw in question for kw in ["排名", "前", "Top", "最高", "最低"]):
        score += 1
    if any(kw in question for kw in ["原因", "为什么", "为何"]):
        score += 1
    if any(kw in question for kw in ["对比", "比较", "差异"]):
        score += 1
    if any(kw in question for kw in ["趋势", "变化", "近"]):
        score += 1
    if any(kw in question for kw in ["行业", "平均", "均值"]):
        score += 1
    if any(kw in question for kw in ["是否一致", "核实", "校验", "重新计算"]):
        score += 2

    if score >= 4:
        return "high"
    elif score >= 2:
        return "medium"
    else:
        return "low"


def plan_task3_question(
    question: str,
    context: dict | None = None,
    db: Session | None = None,
) -> ExecutionPlan:
    complexity = estimate_complexity(question)
    logger.info("问题复杂度评估: question=%s, complexity=%s", question[:50], complexity)

    if complexity == "low":
        plan = _create_simple_plan(question, context)
    else:
        plan = create_execution_plan(question, context)

    if db and complexity in ["medium", "high"]:
        enriched_context = _enrich_context_from_db(plan.context, db)
        plan.context = enriched_context

    return plan


def _create_simple_plan(question: str, context: dict | None = None) -> ExecutionPlan:
    steps = [
        TaskStep(
            step_id="s1",
            step_type=StepType.SQL_QUERY,
            goal="查询相关数据",
            depends_on=[],
            params={},
            priority=0,
        ),
        TaskStep(
            step_id="s2",
            step_type=StepType.COMPOSE_ANSWER,
            goal="生成最终答案",
            depends_on=["s1"],
            params={"include_references": False},
            priority=100,
        ),
    ]

    return ExecutionPlan(
        question=question,
        steps=steps,
        context=context or {},
        created_at=datetime.now(),
    )


def _enrich_context_from_db(context: dict, db: Session) -> dict:
    from app.models.company_basic_info import CompanyBasicInfo
    from sqlalchemy import select

    enriched = dict(context)

    if "companies" in enriched:
        company_names = enriched["companies"]
        if isinstance(company_names, list) and company_names:
            stmt = select(
                CompanyBasicInfo.stock_code,
                CompanyBasicInfo.stock_abbr,
                CompanyBasicInfo.company_name,
            )
            results = db.execute(stmt).all()

            resolved_companies = []
            for name in company_names:
                for row in results:
                    if name in (row.stock_code, row.stock_abbr, row.company_name):
                        resolved_companies.append({
                            "stock_code": row.stock_code,
                            "stock_abbr": row.stock_abbr,
                            "company_name": row.company_name,
                        })
                        break

            if resolved_companies:
                enriched["resolved_companies"] = resolved_companies

    return enriched


def validate_plan(plan: ExecutionPlan) -> tuple[bool, list[str]]:
    errors = []

    if not plan.steps:
        errors.append("执行计划不能为空")
        return False, errors

    step_ids = {s.step_id for s in plan.steps}

    for step in plan.steps:
        for dep_id in step.depends_on:
            if dep_id not in step_ids:
                errors.append(f"步骤 {step.step_id} 依赖不存在的步骤 {dep_id}")

    if not any(s.step_type == StepType.COMPOSE_ANSWER for s in plan.steps):
        errors.append("执行计划必须包含 compose_answer 步骤")

    visited = set()
    for step in plan.get_ordered_steps():
        for dep_id in step.depends_on:
            if dep_id not in visited:
                errors.append(f"步骤 {step.step_id} 的依赖 {dep_id} 尚未执行")
        visited.add(step.step_id)

    return len(errors) == 0, errors


def get_next_executable_steps(
    plan: ExecutionPlan,
    completed_step_ids: set[str],
    failed_step_ids: set[str] | None = None,
) -> list[TaskStep]:
    if failed_step_ids is None:
        failed_step_ids = set()

    executable = []
    for step in plan.get_ordered_steps():
        if step.step_id in completed_step_ids:
            continue
        if step.step_id in failed_step_ids:
            continue

        all_deps_met = True
        for dep_id in step.depends_on:
            if dep_id not in completed_step_ids:
                all_deps_met = False
                break
            if dep_id in failed_step_ids:
                all_deps_met = False
                break

        if all_deps_met:
            executable.append(step)

    return executable


def execute_plan(
    plan: ExecutionPlan,
    db: Session,
    stop_on_failure: bool = False,
) -> "ExecutionTrace":
    from app.services.task3_executor import Task3Executor

    is_valid, errors = validate_plan(plan)
    if not is_valid:
        raise ServiceException(
            ErrorCode.AI_SERVICE_ERROR,
            f"执行计划无效: {'; '.join(errors)}",
        )

    executor = Task3Executor(db, plan)

    completed_step_ids: set[str] = set()
    failed_step_ids: set[str] = set()

    max_iterations = len(plan.steps) * 2
    iteration = 0

    while len(completed_step_ids) + len(failed_step_ids) < len(plan.steps):
        iteration += 1
        if iteration > max_iterations:
            logger.warning("执行调度超过最大迭代次数，终止执行")
            break

        executable_steps = get_next_executable_steps(
            plan, completed_step_ids, failed_step_ids
        )

        if not executable_steps:
            remaining = set(s.step_id for s in plan.steps) - completed_step_ids - failed_step_ids
            if remaining:
                logger.warning("存在无法执行的步骤: %s", remaining)
                for step_id in remaining:
                    step = plan.get_step(step_id)
                    if step:
                        result = StepResult(
                            step_id=step_id,
                            step_type=step.step_type,
                            status=StepStatus.SKIPPED,
                            output={},
                            error_message="依赖步骤失败，跳过执行",
                        )
                        executor.results[step_id] = result
                        failed_step_ids.add(step_id)
            break

        for step in executable_steps:
            result = executor.execute_step(step)

            if result.status == StepStatus.COMPLETED:
                completed_step_ids.add(step.step_id)
            else:
                failed_step_ids.add(step.step_id)
                if stop_on_failure:
                    logger.warning("步骤执行失败，停止执行: step_id=%s", step.step_id)
                    break

        if stop_on_failure and failed_step_ids:
            break

    trace = executor.get_execution_trace()
    logger.info(
        "执行计划完成: steps=%d, completed=%d, failed=%d",
        len(plan.steps),
        len(completed_step_ids),
        len(failed_step_ids),
    )

    return trace


def process_task3_question(
    question: str,
    db: Session,
    context: dict | None = None,
) -> "Task3Response":
    plan = plan_task3_question(question, context, db)

    trace = execute_plan(plan, db)

    answer = trace.plan.question
    for result in trace.results:
        if result.step_type == StepType.COMPOSE_ANSWER and result.status == StepStatus.COMPLETED:
            if result.output.get("answer"):
                answer = result.output["answer"]
            break

    from app.schemas.task3 import Task3AnswerContent

    answer_content = Task3AnswerContent(
        content=answer,
        image=[],
        references=[],
    )

    for result in trace.results:
        if result.step_type == StepType.COMPOSE_ANSWER and result.status == StepStatus.COMPLETED:
            if result.output.get("chart_path"):
                answer_content.image.append(result.output["chart_path"])
            break

    if trace.references:
        answer_content.references = [Reference(**r) for r in trace.references]

    from app.schemas.task3 import Task3Response

    sql = None
    for result in trace.results:
        if result.step_type == StepType.SQL_QUERY and result.status == StepStatus.COMPLETED:
            sql = result.output.get("sql")
            break

    chart_type = None
    for result in trace.results:
        if result.step_type == StepType.COMPOSE_ANSWER and result.status == StepStatus.COMPLETED:
            chart_type = result.output.get("chart_type")
            break

    return Task3Response(
        answer=answer_content,
        sql=sql,
        chart_type=chart_type,
        execution_trace=trace,
    )
