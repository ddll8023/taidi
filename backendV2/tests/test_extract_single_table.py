"""
测试 _extract_single_table 的 Prompt 构建和 LLM 调用流程

测试思路：
1. 加载 struct.yaml → 验证配置能正确读取
2. 模拟填充所有占位符 → 验证最终 Prompt 字符串格式正确
3. 用 Mock LLM 模拟返回 JSON → 验证 JsonOutputParser 解析正常
"""
import json
import os
import sys
import yaml
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

# ── 路径设置 ──
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.insert(0, PROJECT_ROOT)


# ── 测试用 Fake LLM 响应 ──
FAKE_LLM_RESPONSE = json.dumps([
    {"eps": 1.23, "total_operating_revenue": 100000.00, "net_profit_10k_yuan": 20000.00}
])


# ── Fixtures ──

@pytest.fixture
def struct_config():
    """加载 struct.yaml 配置"""
    config_path = os.path.join(PROJECT_ROOT, "backendV2/app/prompts/struct.yaml")
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


@pytest.fixture
def report_identity():
    """模拟的财报身份信息"""
    return {
        "stock_code": "600519",
        "stock_abbr": "贵州茅台",
        "report_year": 2023,
        "report_period": "FY",
        "report_period_cn": "年度",
        "report_type": "REPORT",
        "report_label": "年度报告",
        "report_title": "2023年年度报告",
        "page_numbers": "35,36,37,38,39",
    }


@pytest.fixture
def fake_context_text():
    """模拟的PDF上下文文本"""
    return """合并资产负债表
编制单位：贵州茅台酒股份有限公司
单位：元

项目                期末余额        期初余额
流动资产：             
 货币资金            6,000,000,000  5,000,000,000
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


# ── 测试用例 ──

class TestStructConfig:
    """测试1：验证 struct.yaml 配置能正确加载"""

    def test_config_loads(self, struct_config):
        """1.1 YAML 能正常读取，不报错"""
        assert struct_config is not None
        assert "shared_rules" in struct_config
        assert "table_prompts" in struct_config

    def test_shared_rules_is_string(self, struct_config):
        """1.2 shared_rules 是纯字符串"""
        assert isinstance(struct_config["shared_rules"], str)
        assert len(struct_config["shared_rules"]) > 100

    def test_all_four_tables_present(self, struct_config):
        """1.3 四张表都存在"""
        tables = struct_config["table_prompts"]
        expected = [
            "core_performance_indicators_sheet",
            "balance_sheet",
            "cash_flow_sheet",
            "income_sheet",
        ]
        for t in expected:
            assert t in tables, f"缺少表: {t}"

    def test_table_prompt_is_string(self, struct_config):
        """1.4 每张表的 prompt 是纯字符串"""
        for table_name, prompt in struct_config["table_prompts"].items():
            assert isinstance(prompt, str), f"{table_name} 不是字符串"
            assert len(prompt) > 200, f"{table_name} 内容太短"
            assert "{context_text}" in prompt, f"{table_name} 缺少 {{context_text}} 占位符"
            assert "{{" in prompt, f"{table_name} 的 JSON 示例缺少 {{ 转义"
            assert "}}" in prompt, f"{table_name} 的 JSON 示例缺少 }} 转义"


class TestPromptFilling:
    """测试2：模拟填充占位符后的 Prompt 格式"""

    def test_fill_placeholders(self, struct_config, report_identity, fake_context_text):
        """2.1 所有占位符都能正常替换"""
        table_name = "balance_sheet"
        prompt_template = struct_config["table_prompts"][table_name]

        fill_vars = {
            **report_identity,
            "shared_rules": struct_config["shared_rules"],
            "context_text": fake_context_text,
        }

        prompt = prompt_template.format(**fill_vars)

        # 验证替换后不再有未闭合的占位符
        assert "{stock_code}" not in prompt
        assert "{shared_rules}" not in prompt
        assert "{context_text}" not in prompt

    def test_prompt_structure(self, struct_config, report_identity, fake_context_text):
        """2.2 最终 Prompt 结构完整（含字段清单、JSON示例、上下文）"""
        table_name = "balance_sheet"
        prompt = struct_config["table_prompts"]["balance_sheet"].format(
            context_text=fake_context_text,
        )

        # 包含关键段落
        assert "asset_cash_and_cash_equivalents" in prompt
        assert "资产-货币资金" in prompt
        assert "JSON 数组" in prompt
        assert "财报上下文" in prompt or "当前可用上下文" in prompt
        assert "合并资产负债表" in prompt or fake_context_text[:50] in prompt

    def test_json_example_format(self, struct_config, report_identity, fake_context_text):
        """2.3 JSON 示例中所有字段都是英文名且值为 null"""
        table_name = "balance_sheet"
        prompt = struct_config["table_prompts"][table_name].format(
            **report_identity,
            shared_rules=struct_config["shared_rules"],
            context_text=fake_context_text,
        )

        # 找到 JSON 示例部分
        json_start = prompt.find('[\n')
        json_end = prompt.find('\n]')
        json_str = prompt[json_start:json_end + 2]

        example_data = json.loads(json_str)
        assert isinstance(example_data, list)
        assert len(example_data) == 1

        record = example_data[0]
        # 检查几个关键字段
        assert "asset_cash_and_cash_equivalents" in record
        assert "asset_total_assets" in record
        assert "equity_total_equity" in record
        # 所有值应为 null
        for key, value in record.items():
            assert value is None, f"{key} 的值不是 null，而是 {value}"

    def test_prompt_has_no_unfilled_braces(self, struct_config, report_identity, fake_context_text):
        """2.4 填充后不应残留 {xxx} 样式的未替换占位符"""
        prompt = struct_config["table_prompts"]["balance_sheet"].format(
            **report_identity,
            shared_rules=struct_config["shared_rules"],
            context_text=fake_context_text,
        )

        # 允许 {{ 和 }}（YAML 转义的字面花括号）
        # 但不允许 {xxx} 或 {xxx:yyy} 形式（Python format 占位符）
        import re
        unfilled = re.findall(r'(?<!\{)\{[^{}:\s]+\}(?!\})', prompt)
        assert len(unfilled) == 0, f"发现未替换的占位符: {unfilled}"


class TestMockLLMFlow:
    """测试3：模拟 LLM 调用的完整流程"""

    def test_mock_llm_returns_json(self, struct_config, fake_context_text):
        """3.1 Mock LLM 返回正确 JSON，JsonOutputParser 能解析"""
        from unittest.mock import MagicMock
        from langchain_core.output_parsers import JsonOutputParser
        from langchain_core.prompts import PromptTemplate

        # 模拟 LLM 响应：返回一个带 content 属性的假对象
        class FakeLLMResponse:
            content = FAKE_LLM_RESPONSE

        mock_llm = MagicMock()
        mock_llm.invoke.return_value = FakeLLMResponse()

        # 构建 prompt
        table_name = "core_performance_indicators_sheet"
        # 当前 _extract_single_table 的拼接方式：shared_rules + "\n" + 表 prompt
        template = (
            struct_config["shared_rules"]
            + "\n"
            + struct_config["table_prompts"][table_name]
        )

        prompt_template = PromptTemplate.from_template(template)
        chain = prompt_template | mock_llm | JsonOutputParser()

        result = chain.invoke({
            "context_text": fake_context_text,
        })

        # JsonOutputParser 解析结果
        print(f"\nMock LLM 解析结果类型: {type(result).__name__}")
        print(f"解析结果: {json.dumps(result, ensure_ascii=False, indent=2)}")

        # 验证解析成功：JSON 数组 → list，JSON 对象 → dict
        assert isinstance(result, (list, dict)), f"期望 list 或 dict，得到 {type(result)}"
        if isinstance(result, list):
            assert len(result) > 0
            assert isinstance(result[0], dict)
            assert result[0].get("eps") == 1.23
        else:
            assert result.get("eps") == 1.23

    def test_extract_single_table_needs_all_vars(self, struct_config):
        """3.2 验证当前 _extract_single_table 缺少占位符的问题"""
        # 当前代码这样拼接：
        template = (
            struct_config["shared_rules"]
            + "\n"
            + struct_config["table_prompts"]["balance_sheet"]
        )

        # 查看模板中有哪些占位符
        import re
        placeholders = set(re.findall(r'\{(\w+)\}', template))
        print(f"\n模板中需要的占位符: {placeholders}")

        # 当前代码只传了 {"context_text": ...}，少了这些：
        required = {"stock_code", "stock_abbr", "report_year", "report_period",
                    "report_period_cn", "report_type", "report_label", "report_title",
                    "page_numbers", "shared_rules", "context_text"}
        missing = required - placeholders
        extra = placeholders - required

        # shared_rules 在配置中直接拼接进去了，不需要作为占位符
        # 但模板中还有 {shared_rules} 占位符！
        if "shared_rules" in placeholders:
            print("⚠️  模板中包含 {shared_rules} 占位符，但当前代码已经拼接了 shared_rules 文本")
            print("   需要修改代码：要么去掉拼接，用占位符替换；要么去掉模板中的 {shared_rules}")

        print(f"\n所有占位符: {placeholders}")
        print(f"当前代码只传了: context_text")
        print(f"缺少的占位符: {placeholders - {'shared_rules'} - {'context_text'}}")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
