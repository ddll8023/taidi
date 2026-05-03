"""任务三跨文件共享辅助函数"""

import json
import re
from datetime import datetime
from decimal import Decimal
from typing import Any

from langchain_core.messages import HumanMessage, SystemMessage

from app.constants import task3 as constants_task3
from app.core.config import settings
from app.schemas.common import ErrorCode
from app.utils.exception import ServiceException
from app.utils.logger_config import setup_logger
from app.utils.model_factory import get_model

logger = setup_logger(__name__)


def _to_jsonable(value: Any):
    """将复杂对象递归转换为 JSON 可序列化结构。"""
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


def _parse_question_rounds(question_value):
    """解析题目文本为统一的轮次结构。"""
    if isinstance(question_value, list):
        parsed = question_value
    else:
        try:
            parsed = json.loads(question_value)
        except (json.JSONDecodeError, TypeError):
            parsed = [{"Q": str(question_value or "")}]

    if not isinstance(parsed, list):
        parsed = [{"Q": str(parsed)}]

    rounds = []
    for item in parsed:
        if isinstance(item, dict):
            q_text = str(item.get("Q", "")).strip()
        else:
            q_text = str(item).strip()
        if q_text:
            rounds.append({"Q": q_text})

    if not rounds:
        rounds.append({"Q": str(question_value or "").strip()})
    return rounds


def _extract_json_from_response(response_text: str):
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


def _get_task3_config():
    """获取任务三提示词配置。"""
    return settings.PROMPT_CONFIG.get_task3_config


def _invoke_llm(
    system_prompt: str,
    user_prompt: str,
    max_tokens: int = 8192,
    temperature: float = 0.1,
):
    """调用大模型并返回文本响应。"""
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
        logger.error(f"LLM调用失败: error={str(exc)}")
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


def _is_attribution_with_financial_data(question: str):
    """判断题目是否同时涉及归因分析和财务数据。"""
    has_attribution = any(kw in question for kw in constants_task3.ATTRIBUTION_KEYWORDS)
    has_financial = any(kw in question for kw in constants_task3.FINANCIAL_DATA_KEYWORDS)
    return has_attribution and has_financial


def _extract_company_name_from_question(question: str):
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
        rf'([一-龥]{{2,6}}(?:{company_suffixes}))',
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
    match = re.match(r'([一-龥]{2,8})', question)
    if match:
        candidate = match.group(1)
        noise_words = ["年", "季度", "所有", "第", "同比", "环比", "增长", "下降", "变化"]
        if not any(noise in candidate for noise in noise_words):
            return candidate

    return None
