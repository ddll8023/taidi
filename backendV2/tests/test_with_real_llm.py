"""
集成测试：使用项目中的真实 get_model 调用 LLM

用法：
  uv run --directory backendV2 python tests/test_with_real_llm.py

注意：这会实际调用 LLM API，消耗 token 和费用。
"""

import json
import os
import sys
import yaml
from langchain_openai import ChatOpenAI

# 路径
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, PROJECT_ROOT)

# 加载配置
config_path = os.path.join(PROJECT_ROOT, "app/prompts/struct.yaml")
with open(config_path, "r", encoding="utf-8") as f:
    struct_config = yaml.safe_load(f)

# 模拟的PDF上下文（资产负债表页段）
FAKE_CONTEXT = """合并资产负债表
编制单位：贵州茅台酒股份有限公司
单位：元

项目                期末余额        期初余额
流动资产：             
 eps            6,000,000,000  5,000,000,000
 应收账款              100,000,000     80,000,000
 存货                4,000,000,000  3,500,000,000
 流动资产合计       10,100,000,000  8,580,000,000

非流动资产：
 固定资产            2,000,000,000  1,800,000,000
 在建工程              500,000,000    400,000,000
 无形资产              300,000,000    280,000,000

资产总计             12,900,000,000 11,060,000,000

流动负债：
 短期借款              200,000,000    150,000,000
 应付账款              800,000,000    700,000,000
 合同负债            1,500,000,000  1,200,000,000

负债合计              2,500,000,000  2,050,000,000

股东权益：
 股本                  100,000,000    100,000,000
 未分配利润          8,000,000,000  6,500,000,000
 股东权益合计       10,400,000,000  9,010,000,000

负债和股东权益总计   12,900,000,000 11,060,000,000"""


def test_build_prompt(table_name: str = "balance_sheet") -> str:
    """构造发送给 LLM 的完整 Prompt"""
    prompt_template = (
        struct_config["shared_rules"]
        + "\n"
        + struct_config["table_prompts"][table_name]
    )
    return prompt_template


# def test_with_mock_chain():
#     """测试1：用 LangChain 的 PromptTemplate + JsonOutputParser 走一遍"""
#     print("=" * 60)
#     print("测试1：LangChain PromptTemplate → JsonOutputParser 链路")
#     print("=" * 60)

#     from langchain_core.output_parsers import JsonOutputParser
#     from langchain_core.prompts import PromptTemplate

#     table_name = "cash_flow_sheet"
#     template = (
#         struct_config["shared_rules"]
#         + "\n"
#         + struct_config["table_prompts"][table_name]
#     )
#     prompt_template = PromptTemplate.from_template(template)

#     # 只测试 Prompt 构建，不调 LLM
#     prompt_text = prompt_template.format(context_text=FAKE_CONTEXT)
#     print(f"\n▶ 表: {table_name}")
#     print(f"▶ 完整 Prompt 长度: {len(prompt_text)} 字符")
#     print(f"▶ Prompt 预览（前500字符）:\n{prompt_text[:500]}\n...")
#     print(f"▶ Prompt 尾部（后200字符）:\n...{prompt_text[-200:]}\n")

#     # 检查 {{ }} 转义是否正确（format 后应变成单括号）
#     assert "{{" not in prompt_text, "YAML 转义后的 {{ 不应出现在最终 prompt 中"
#     print("✅ {{ → { 转义正确")

#     return prompt_text


def test_with_real_llm():
    """测试2：真实调用 get_model.chat_model 调 LLM"""
    print("=" * 60)
    print("测试2：真实 LLM 调用（消耗 token）")
    print("=" * 60)

    from langchain_core.output_parsers import JsonOutputParser
    from langchain_core.prompts import PromptTemplate

    table_name = "core_performance_indicators_sheet"
    prompt_text = test_build_prompt(table_name)

    llm = ChatOpenAI(
        model="deepseek/deepseek-v4-flash(free)",
        api_key="sk-f6Zdlhho0YY2gvXsoDTPpHt76wAFlMgDFDppT8qhVJrm2NYh",
        base_url="https://open.cherryin.cc/v1",
    )
    print(f"\n▶ 模型: deepseek/deepseek-v4-flash(free)")
    print(f"▶ 表: {table_name}")
    print(f"▶ Prompt 长度: {len(prompt_text)} tokens ≈ {len(prompt_text) // 4}")
    print(f"\n▶ 正在调用 LLM（等待响应...）")

    try:
        prompt_template = PromptTemplate.from_template(prompt_text)
        chain = prompt_template | llm | JsonOutputParser()

        parsed = chain.invoke({"context_text": FAKE_CONTEXT})
        print(f"✅ JsonOutputParser 解析结果类型: {type(parsed).__name__}")
        print(f"解析结果:\n{json.dumps(parsed, ensure_ascii=False, indent=2)}\n")

    except Exception as e:
        print(f"❌ LLM 调用失败: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    import sys
    from app.core.config import settings

    test_with_real_llm()
