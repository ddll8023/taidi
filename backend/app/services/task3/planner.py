"""任务三问题规划与执行调度服务。"""

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

COMPANY_ALIAS_MAP = {
    "华润三九": ["三九", "999"],
}

KNOWLEDGE_ONLY_KEYWORDS = [
    "医保",
    "医保目录",
    "国家医保",
    "商保",
    "集采",
    "谈判",
    "政策",
    "目录",
    "新增",
    "产品有哪些",
    "名单",
    "北向资金",
    "外资",
    "撤退",
    "市场观点",
    "行业风向",
    "行业事件",
    "原因",
    "为什么",
    "为何",
    "导致",
    "因素",
    "影响",
    "驱动",
    "分析原因",
    "涨跌原因",
    "变化原因",
    "增长原因",
    "下降原因",
    "研报",
    "研究报告",
    "券商报告",
    "结合研报",
    "分析",
    "风险",
    "优势",
    "质量",
    "评估",
    "判断",
]

INDUSTRY_KNOWLEDGE_KEYWORDS = [
    "医保",
    "商保",
    "集采",
    "谈判",
    "政策",
    "目录",
    "新增",
    "北向资金",
    "外资",
    "行业",
]

FINANCIAL_DATA_KEYWORDS = [
    "货币资金",
    "总资产",
    "资产负债表",
    "现金流量",
    "现金流",
    "利润表",
    "营业收入",
    "净利润",
    "负债",
    "比率",
    "毛利率",
    "净利率",
    "ROE",
    "EPS",
    "占比",
    "比例",
    "同比增长",
    "环比",
    "增速",
    "金额",
    "万元",
    "亿元",
    "应收账款",
    "存货",
    "在建工程",
    "短期借款",
    "总负债",
    "经营性现金流",
    "投资性现金流",
    "融资性现金流",
    "每股收益",
    "净资产收益率",
    "销售毛利率",
    "销售净利率",
    "扣非净利润",
    "资产负债率",
    "总资产同比",
    "净现金流",
    "财务",
    "主营业务收入",
    "营收",
    "盈利",
    "亏损",
    "利润总额",
    "经营性现金流量净额",
    "资产减值",
    "信用减值",
    "折旧",
    "摊销",
    "营业总收入",
    "增长率",
    "费用率",
]

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
    "背离现象",
    "差异原因",
    "变动原因",
    "下降原因",
    "增长原因",
    "变化原因",
    "分析原因",
    "涨跌原因",
    "分析",
    "风险",
    "优势",
    "质量",
    "评估",
    "判断",
]

MULTI_INTENT_ACTION_KEYWORDS = [
    "查询",
    "统计",
    "计算",
    "找出",
    "列出",
    "筛选",
    "提取",
    "对比",
    "比较",
    "分析",
    "验证",
    "说明",
    "评价",
    "评估",
    "给出",
]


def _is_attribution_with_financial_data(question: str) -> bool:
    has_attribution = any(kw in question for kw in ATTRIBUTION_KEYWORDS)
    has_financial = any(kw in question for kw in FINANCIAL_DATA_KEYWORDS)
    return has_attribution and has_financial


def _get_task3_config() -> dict:
    """获取任务三提示词配置。"""
    return settings.PROMPT_CONFIG.get_task3_config


def _invoke_llm(
    system_prompt: str,
    user_prompt: str,
    max_tokens: int = 8192,
    temperature: float = 0.1,
) -> str:
    """调用大模型并返回文本响应。"""
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
    """从模型响应中提取 JSON 结构。"""
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
    """分析问题意图并返回结构化结果。"""
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
    """为未识别场景构造默认执行计划。"""
    if _is_hybrid_question(question):
        return {
            "steps": [
                {
                    "step_id": "s1",
                    "step_type": "sql_query",
                    "goal": f"查询与问题相关的结构化财务数据: {question[:100]}",
                    "depends_on": [],
                    "params": {"description": question},
                    "priority": 0,
                },
                {
                    "step_id": "s2",
                    "step_type": "retrieve_evidence",
                    "goal": f"检索与问题相关的研报证据: {question[:100]}",
                    "depends_on": [],
                    "params": {
                        "query": question,
                        "doc_type": _infer_doc_types_for_question(question),
                        "top_k": 8,
                    },
                    "priority": 10,
                },
                {
                    "step_id": "s3",
                    "step_type": "verify",
                    "goal": "校验关键结果完整性",
                    "depends_on": ["s1"],
                    "params": {"check_type": "completeness"},
                    "priority": 50,
                },
                {
                    "step_id": "s4",
                    "step_type": "compose_answer",
                    "goal": "生成最终答案",
                    "depends_on": ["s1", "s2", "s3"],
                    "params": {"include_references": True, "format": "evidence_based"},
                    "priority": 100,
                },
            ],
            "context": {},
            "reasoning": "默认混合计划：结构化查询 + 研报检索 + 校验 + 回答",
        }

    if detect_multi_intent(question):
        return {
            "steps": [
                {
                    "step_id": "s1",
                    "step_type": "sql_query",
                    "goal": f"查询与问题相关的数据: {question[:100]}",
                    "depends_on": [],
                    "params": {"description": question},
                    "priority": 0,
                },
                {
                    "step_id": "s2",
                    "step_type": "aggregate",
                    "goal": "对查询结果进行统计或分组汇总",
                    "depends_on": ["s1"],
                    "params": {"operation": "count"},
                    "priority": 20,
                },
                {
                    "step_id": "s3",
                    "step_type": "verify",
                    "goal": "校验关键结果完整性",
                    "depends_on": ["s1", "s2"],
                    "params": {"check_type": "completeness"},
                    "priority": 50,
                },
                {
                    "step_id": "s4",
                    "step_type": "compose_answer",
                    "goal": "生成最终答案",
                    "depends_on": ["s1", "s2", "s3"],
                    "params": {"include_references": False},
                    "priority": 100,
                },
            ],
            "context": {},
            "reasoning": "默认多意图计划：查询 + 聚合 + 校验 + 回答",
        }

    if _is_knowledge_only_question(question):
        return _create_knowledge_plan_dict(question)

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


def _is_knowledge_only_question(question: str) -> bool:
    """判断问题是否属于知识库检索优先场景。"""
    return any(keyword.lower() in question.lower() for keyword in KNOWLEDGE_ONLY_KEYWORDS)


def _is_hybrid_question(question: str) -> bool:
    """判断问题是否属于混合型（需要SQL查询+知识库检索）。"""
    has_financial = any(kw in question for kw in FINANCIAL_DATA_KEYWORDS)
    has_knowledge = any(kw in question for kw in KNOWLEDGE_ONLY_KEYWORDS)

    # 明确包含"研报"、"研究报告"、"结合研报"等字样，直接判定为混合
    has_explicit_research = any(
        kw in question for kw in ["研报", "研究报告", "券商报告", "结合研报"]
    )

    return (has_financial and has_knowledge) or has_explicit_research


def _infer_doc_types_for_question(question: str) -> list[str]:
    """根据问题内容推断文档类型过滤条件。"""
    if any(keyword.lower() in question.lower() for keyword in INDUSTRY_KNOWLEDGE_KEYWORDS):
        return ["INDUSTRY_REPORT"]
    return ["RESEARCH_REPORT", "INDUSTRY_REPORT"]


def _extract_stock_code_from_context(context: dict | None) -> str | None:
    """从上下文中提取股票代码。"""
    if not context:
        return None

    resolved_companies = context.get("resolved_companies")
    if isinstance(resolved_companies, list) and resolved_companies:
        first_company = resolved_companies[0]
        if isinstance(first_company, dict) and first_company.get("stock_code"):
            return str(first_company["stock_code"])

    stock_code = context.get("stock_code")
    if stock_code:
        return str(stock_code)
    return None


def _resolve_stock_code_from_question(question: str, db: Session | None = None) -> str | None:
    """从问题文本中提取公司名称并解析股票代码。"""
    if not db:
        return None
    from app.models.company_basic_info import CompanyBasicInfo
    from sqlalchemy import select

    stmt = select(
        CompanyBasicInfo.stock_code,
        CompanyBasicInfo.stock_abbr,
        CompanyBasicInfo.company_name,
    )
    results = db.execute(stmt).all()

    for row in results:
        if _question_mentions_company(
            question,
            str(row.stock_abbr or ""),
            str(row.company_name or ""),
        ):
            return str(row.stock_code)
    return None


def _extract_company_name_from_question(question: str) -> str | None:
    """从问题文本中直接提取公司名称（不依赖数据库）。

    当公司不在 company_basic_info 表中时，作为检索过滤的兜底手段。
    """
    import re

    generic_company_keywords = [
        "所有公司",
        "全部公司",
        "哪些公司",
        "公司中",
        "各公司",
        "多家公司",
        "行业内公司",
        "中药公司",
        "上市公司",
    ]
    if any(keyword in question for keyword in generic_company_keywords):
        return None

    # 只匹配常见的公司名称后缀（2字及以上），避免单字后缀误匹配
    company_suffixes = (
        "药业|集团|科技|股份|生物|医疗|医药|健康|制药|银行|保险|证券|基金"
        "|地产|航空|电力|能源|化工|机械|电子|通信|传媒|食品|饮料|零售|物流"
        "|汽车|钢铁|煤炭|石油|纺织|建筑|建材|家居|家电|计算机|软件|互联网"
        "|新能源|新材料|环保|教育|旅游|酒店|农业|畜牧|渔业|矿业|冶金|水泥"
        "|玻璃|造纸|印刷|包装|家具|服装|化妆品|珠宝|钟表|眼镜|乐器|体育"
        "|文化|艺术|出版|广播|电视|电影|演艺|会展|咨询|法律|会计|审计|税务"
        "|评估|检测|认证|担保|典当|拍卖|租赁|物业|园林|园艺|花卉|苗木|养殖"
        "|种植|加工|制造|生产|贸易|商业|服务|餐饮|娱乐|休闲|保健|养老|护理"
        "|康复|医美|口腔|眼科|骨科|肿瘤|心血管|内分泌|呼吸|消化|神经|精神"
        "|皮肤|妇科|儿科|产科|体检|疫苗|血液|影像|检验|病理|药剂|中医|中药"
        "|西药|原料药|制剂|器械|耗材|诊断|基因|细胞|组织|器官|移植|免疫"
        "|血清|血浆|蛋白|抗体|激素|维生素|氨基酸|抗生素|化学药|生物药"
        "|现代中药|经典名方|配方颗粒|饮片|中成药|保健品|保健食品|功能性食品"
        "|特医食品|婴幼儿配方|乳制品|调味品|添加剂|饲料|肥料|农药|种子|种苗"
        "|水产|海洋|林业|草原|湿地|土壤|水利|气象|地震|环境|生态|资源|矿产"
        "|土地|岛屿|海岸|港口|航道|机场|铁路|公路|桥梁|隧道|地铁|轻轨|公交"
        "|出租|客运|货运|仓储|配送|快递|邮政|电信|移动|联通|广电|网络|数据"
        "|人工智能|区块链|物联网|大数据|云计算|边缘计算|量子|芯片|半导体"
        "|集成电路|显示|面板|光伏|风电|核电|水电|火电|气电|热电|余热|储能"
        "|氢能|生物质|污泥|污水|废气|固废|危废|医废|噪声|振动|辐射|放射性"
        "|电磁|重金属|有机物|无机物|酸碱|盐类|油脂|涂料|染料|颜料|油墨"
        "|胶粘剂|密封材料|绝缘材料|磁性材料|光学材料|纳米材料|超导材料"
        "|复合材料|功能材料|智能材料|绿色材料|生物材料|医用材料|诊断试剂"
        "|基因检测|测序|合成|编辑|克隆|干细胞|免疫细胞|细胞治疗|基因治疗"
        "|抗体药物|重组蛋白|血液制品|诊断|器械|设备|仪器|仪表|工具|模具"
        "|夹具|量具|刀具|磨具|砂轮|轴承|齿轮|链条|皮带|弹簧|紧固件|密封件"
        "|液压件|气动件|电器|电机|变压器|开关|电缆|电线|光纤|光缆|天线|雷达"
        "|导航|制导|遥控|遥测|遥感|传感|监测|监控|报警|消防|安防|安检|防伪"
        "|溯源|追溯|追踪|定位|地图|地理|测绘|勘察|设计|施工|监理|运维|运营"
        "|管理|平台|系统|软件|硬件|固件|中间件|数据库|操作系统|办公软件"
        "|工业软件|嵌入式"
    )

    patterns = [
        rf'([\u4e00-\u9fa5]{{2,6}}(?:{company_suffixes}))',
    ]

    for pattern in patterns:
        match = re.search(pattern, question)
        if match:
            candidate = match.group(1)
            # 过滤明显不是公司名的误匹配（如包含"年"、"季度"、"所有"等）
            noise_words = ["年", "季度", "所有", "第", "同比", "环比", "增长", "下降", "变化"]
            if any(noise in candidate for noise in noise_words):
                continue
            return candidate

    # 兜底：提取问题开头连续的中文词汇（通常是主语/公司名称）
    match = re.match(r'([\u4e00-\u9fa5]{2,8})', question)
    if match:
        candidate = match.group(1)
        noise_words = ["年", "季度", "所有", "第", "同比", "环比", "增长", "下降", "变化"]
        if not any(noise in candidate for noise in noise_words):
            return candidate

    return None


def _resolve_companies_from_question(question: str, db: Session | None = None) -> list[dict]:
    """从问题文本中解析所有提及的公司信息。"""
    if not db:
        return []
    from app.models.company_basic_info import CompanyBasicInfo
    from sqlalchemy import select

    stmt = select(
        CompanyBasicInfo.stock_code,
        CompanyBasicInfo.stock_abbr,
        CompanyBasicInfo.company_name,
    )
    results = db.execute(stmt).all()

    resolved = []
    for row in results:
        if _question_mentions_company(
            question,
            str(row.stock_abbr or ""),
            str(row.company_name or ""),
        ):
            resolved.append({
                "stock_code": row.stock_code,
                "stock_abbr": row.stock_abbr,
                "company_name": row.company_name,
            })
    return resolved


def _question_mentions_company(
    question: str,
    stock_abbr: str,
    company_name: str,
) -> bool:
    """判断问题是否命中公司标准名或别名。"""
    candidates = {stock_abbr, company_name}
    if stock_abbr in COMPANY_ALIAS_MAP:
        candidates.update(COMPANY_ALIAS_MAP[stock_abbr])

    normalized_question = question.strip()
    for candidate in candidates:
        if candidate and candidate in normalized_question:
            return True
    return False


def _create_knowledge_plan_dict(question: str, context: dict | None = None, db: Session | None = None) -> dict:
    """为知识库型问题生成计划字典。"""
    params: dict[str, Any] = {
        "query": question,
        "doc_type": _infer_doc_types_for_question(question),
        "top_k": 8,
    }
    stock_code = _extract_stock_code_from_context(context)
    if not stock_code:
        stock_code = _resolve_stock_code_from_question(question, db)
    if stock_code:
        params["stock_code"] = stock_code
    else:
        # 兜底：当公司不在数据库中时，提取公司名称用于检索结果二次过滤
        company_name = _extract_company_name_from_question(question)
        if company_name:
            params["company_name_filter"] = company_name

    return {
        "steps": [
            {
                "step_id": "s1",
                "step_type": "retrieve_evidence",
                "goal": f"从知识库检索可支撑回答的证据: {question[:120]}",
                "depends_on": [],
                "params": params,
                "priority": 0,
            },
            {
                "step_id": "s2",
                "step_type": "compose_answer",
                "goal": "基于检索证据生成最终答案",
                "depends_on": ["s1"],
                "params": {"include_references": True, "format": "evidence_based"},
                "priority": 100,
            },
        ],
        "context": context or {},
        "reasoning": "知识库优先问题：通过研报/行业报告证据回答",
    }


def _create_knowledge_plan(question: str, context: dict | None = None, db: Session | None = None) -> ExecutionPlan:
    """创建知识库检索优先的执行计划。"""
    plan_dict = _create_knowledge_plan_dict(question, context, db)
    merged_context = dict(context) if context else {}
    if plan_dict.get("context"):
        merged_context.update(plan_dict["context"])
    return ExecutionPlan(
        question=question,
        steps=[
            TaskStep(
                step_id=step["step_id"],
                step_type=StepType(step["step_type"]),
                goal=step["goal"],
                depends_on=step["depends_on"],
                params=step["params"],
                priority=step["priority"],
            )
            for step in plan_dict["steps"]
        ],
        context=merged_context,
        created_at=datetime.now(),
    )


def _create_hybrid_plan(
    question: str,
    context: dict | None = None,
    db: Session | None = None,
) -> ExecutionPlan:
    """创建混合型执行计划（SQL查询+知识库检索+答案组装）。"""
    stock_code = _extract_stock_code_from_context(context)
    if not stock_code:
        stock_code = _resolve_stock_code_from_question(question, db)

    resolved_companies = []
    if context and context.get("resolved_companies"):
        resolved_companies = context["resolved_companies"]
    elif db:
        resolved_companies = _resolve_companies_from_question(question, db)

    merged_context = dict(context) if context else {}
    if stock_code:
        merged_context["stock_code"] = stock_code
    if resolved_companies:
        merged_context["resolved_companies"] = resolved_companies

    sql_params: dict[str, Any] = {
        "description": question,
    }
    if stock_code:
        sql_params["stock_code"] = stock_code

    evidence_params: dict[str, Any] = {
        "query": question,
        "doc_type": _infer_doc_types_for_question(question),
        "top_k": 8,
    }
    if stock_code:
        evidence_params["stock_code"] = stock_code
    else:
        # 兜底：当公司不在数据库中时，提取公司名称用于检索结果二次过滤
        company_name = _extract_company_name_from_question(question)
        if company_name:
            evidence_params["company_name_filter"] = company_name

    steps = [
        TaskStep(
            step_id="s1",
            step_type=StepType.SQL_QUERY,
            goal=f"查询结构化财务数据: {question[:120]}",
            depends_on=[],
            params=sql_params,
            priority=0,
        ),
        TaskStep(
            step_id="s2",
            step_type=StepType.RETRIEVE_EVIDENCE,
            goal=f"从知识库检索研报证据: {question[:120]}",
            depends_on=[],
            params=evidence_params,
            priority=0,
        ),
        TaskStep(
            step_id="s3",
            step_type=StepType.VERIFY,
            goal="校验SQL查询结果与题目假设的一致性",
            depends_on=["s1"],
            params={"check_type": "consistency"},
            priority=50,
        ),
        TaskStep(
            step_id="s4",
            step_type=StepType.COMPOSE_ANSWER,
            goal="综合结构化数据与研报证据生成归因分析答案",
            depends_on=["s1", "s2", "s3"],
            params={"include_references": True, "format": "evidence_based"},
            priority=100,
        ),
    ]

    return ExecutionPlan(
        question=question,
        steps=steps,
        context=merged_context,
        created_at=datetime.now(),
    )


def create_execution_plan(
    question: str,
    context: dict | None = None,
) -> ExecutionPlan:
    """根据问题分析结果创建执行计划。"""
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

    merged_context = dict(context) if context else {}
    if plan_dict.get("context"):
        merged_context.update(plan_dict["context"])

    plan = ExecutionPlan(
        question=question,
        steps=steps,
        context=merged_context,
        created_at=datetime.now(),
    )

    logger.info(
        "执行计划创建完成: question=%s, steps=%d",
        question[:50],
        len(steps),
    )
    return plan


def detect_multi_intent(question: str) -> bool:
    """检测问题是否包含多个子意图。"""
    multi_intent_keywords = [
        "并", "同时", "分别", "各自", "以及",
        "排名", "前几", "Top", "最高", "最低",
        "对比", "比较", "差异", "区别",
        "原因", "为什么", "为何", "如何", "导致", "因素",
        "趋势", "变化", "增长", "下降",
        "行业", "平均", "均值", "共同点", "共同因素",
        "是否一致", "核实", "校验", "重新计算",
    ]

    question_lower = question.lower()
    for keyword in multi_intent_keywords:
        if keyword.lower() in question_lower:
            return True

    if _count_multi_intent_actions(question) >= 3:
        return True

    if _count_question_clauses(question) >= 3:
        return True

    return False


def estimate_complexity(question: str) -> str:
    """估算问题复杂度等级。"""
    score = 0
    action_count = _count_multi_intent_actions(question)
    clause_count = _count_question_clauses(question)

    if detect_multi_intent(question):
        score += 2

    # 检测多步骤序列标记（如 ①②③④ 或 1. 2. 3. 4.），这类问题至少 medium
    step_markers = re.findall(r"[①②③④⑤⑥⑦⑧⑨⑩]|\d+\.", question)
    if len(step_markers) >= 3:
        score += 2

    if clause_count >= 3:
        score += 2
    elif clause_count == 2:
        score += 1

    if action_count >= 4:
        score += 2
    elif action_count >= 2:
        score += 1

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
    """规划任务三问题并返回执行计划。"""
    if db:
        context = _ensure_context_resolved(question, context, db)

    if _is_attribution_with_financial_data(question):
        plan = _create_hybrid_plan(question, context, db)
        logger.info("归因+财务混合型规划: question=%s, steps=%d", question[:50], len(plan.steps))
        return plan

    if _is_hybrid_question(question):
        plan = _create_hybrid_plan(question, context, db)
        logger.info("混合型规划: question=%s, steps=%d", question[:50], len(plan.steps))
        return plan

    if _is_knowledge_only_question(question):
        plan = _create_knowledge_plan(question, context, db)
        logger.info("知识库优先规划: question=%s, steps=%d", question[:50], len(plan.steps))
        return plan

    complexity = estimate_complexity(question)
    logger.info("问题复杂度评估: question=%s, complexity=%s", question[:50], complexity)

    if complexity == "low":
        plan = _create_simple_plan(question, context)
    else:
        plan = create_execution_plan(question, context)

    if db and complexity in ["medium", "high"]:
        enriched_context = _enrich_context_from_db(plan.context, db)
        plan.context = enriched_context

    # 兜底：如果计划步骤数过少（<=2）但问题明显是多步骤/多意图，尝试用混合计划兜底
    if len(plan.steps) <= 2 and _has_multiple_explicit_steps(question):
        logger.warning(
            "计划步骤过少但问题含多步骤标记，使用混合计划兜底: question=%s",
            question[:50],
        )
        plan = _create_hybrid_plan(question, context, db)

    return plan


def _has_multiple_explicit_steps(question: str) -> bool:
    """检测问题是否包含显式的多步骤标记（如 ①②③④ 或 1. 2. 3. 4.）。"""
    step_markers = re.findall(r"[①②③④⑤⑥⑦⑧⑨⑩]|\d+\.", question)
    return len(step_markers) >= 3 or _count_question_clauses(question) >= 3


def _ensure_context_resolved(
    question: str,
    context: dict | None,
    db: Session,
) -> dict:
    """确保上下文中包含从问题文本解析的公司信息。"""
    merged = dict(context) if context else {}
    resolution_question = question
    original_question = merged.get("original_question")
    if isinstance(original_question, str) and original_question.strip():
        resolution_question = original_question.strip()

    if not merged.get("resolved_companies"):
        resolved = _resolve_companies_from_question(resolution_question, db)
        if resolved:
            merged["resolved_companies"] = resolved

    if not merged.get("stock_code"):
        stock_code = _resolve_stock_code_from_question(resolution_question, db)
        if stock_code:
            merged["stock_code"] = stock_code

    return merged


def _create_simple_plan(question: str, context: dict | None = None) -> ExecutionPlan:
    """构造简单问题的降级计划。"""
    steps = [
        TaskStep(
            step_id="s1",
            step_type=StepType.SQL_QUERY,
            goal=f"根据问题查询结构化财务数据: {question[:120]}",
            depends_on=[],
            params={"description": question},
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
        context=dict(context) if context else {},
        created_at=datetime.now(),
    )


def _count_question_clauses(question: str) -> int:
    """按分号、句号等分隔估算题目中的独立子句数量。"""
    clauses = [
        item.strip()
        for item in re.split(r"[；;。]\s*|\n+", question)
        if item.strip()
    ]
    return len(clauses)


def _count_multi_intent_actions(question: str) -> int:
    """统计题目中出现的核心动作词数量。"""
    return sum(1 for keyword in MULTI_INTENT_ACTION_KEYWORDS if keyword in question)


def _enrich_context_from_db(context: dict, db: Session) -> dict:
    """补充上下文中的公司解析信息。"""
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
    """校验执行计划的依赖关系与关键步骤。"""
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
    """获取当前可以执行的步骤列表。"""
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
    """执行任务三计划并返回执行轨迹。"""
    from app.services.task3 import executor as services_task3_executor

    is_valid, errors = validate_plan(plan)
    if not is_valid:
        raise ServiceException(
            ErrorCode.AI_SERVICE_ERROR,
            f"执行计划无效: {'; '.join(errors)}",
        )

    context: dict[str, Any] = dict(plan.context)
    results: dict[str, StepResult] = {}
    references: list[Reference] = []

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
                        results[step_id] = result
                        failed_step_ids.add(step_id)
            break

        for step in executable_steps:
            result = services_task3_executor.execute_step(
                step=step,
                db=db,
                plan=plan,
                context=context,
                results=results,
                references=references,
            )

            if result.status == StepStatus.COMPLETED:
                completed_step_ids.add(step.step_id)
            else:
                failed_step_ids.add(step.step_id)
                if stop_on_failure:
                    logger.warning("步骤执行失败，停止执行: step_id=%s", step.step_id)
                    break

        if stop_on_failure and failed_step_ids:
            break

    trace = services_task3_executor.build_execution_trace(
        plan=plan,
        results=results,
        references=references,
    )
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
    """处理任务三问题并组装最终回答。"""
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
        references=[],
    )

    if trace.references:
        answer_content.references = [Reference(**r) for r in trace.references]

    from app.schemas.task3 import Task3Response

    sql = None
    for result in trace.results:
        if result.step_type == StepType.SQL_QUERY and result.status == StepStatus.COMPLETED:
            sql = result.output.get("sql")
            break

    return Task3Response(
        answer=answer_content,
        sql=sql,
        execution_trace=trace,
    )
