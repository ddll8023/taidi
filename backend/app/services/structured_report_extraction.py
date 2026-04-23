from __future__ import annotations

import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any

from pypdf import PdfReader

from app.core.config import settings
from app.models import financial_report as models_financial_report
from app.schemas.common import ErrorCode
from app.utils.exception import ServiceException
from app.utils.file import save_json
from app.utils.logger_config import setup_logger
from app.utils.model_factory import get_model


logger = setup_logger(__name__)

CORE_TABLE_NAME = "core_performance_indicators_sheet"
TABLE_ORDER = (
    CORE_TABLE_NAME,
    "balance_sheet",
    "cash_flow_sheet",
    "income_sheet",
)
MAX_CONCURRENT_TABLES = 4
REPORT_PERIOD_DISPLAY = {
    "Q1": "一季度",
    "HY": "半年度",
    "Q3": "三季度",
    "FY": "年度",
}
SECTION_SPECS = {
    CORE_TABLE_NAME: {
        "strong_keywords": (
            "主要会计数据和财务指标",
            "主要财务数据",
            "主要财务指标",
            "会计数据和财务指标",
            "主要会计数据",
        ),
        "weak_keywords": (
            "财务指标",
            "会计数据",
            "每股收益",
            "净资产收益率",
        ),
        "markers": (
            "单位：元",
            "币种：人民币",
            "营业收入",
            "净利润",
            "每股净资产",
            "经营现金流量",
        ),
        "window_config_key": "core_window_pages",
        "default_window_pages": 5,
        "primary_context_label": "主要财务数据页段",
    },
    "balance_sheet": {
        "strong_keywords": ("合并资产负债表", "1、合并资产负债表"),
        "weak_keywords": ("资产负债表",),
        "markers": ("编制单位", "单位：元", "项目", "期末余额", "流动资产"),
        "window_config_key": "statement_window_pages",
        "default_window_pages": 6,
        "primary_context_label": "合并资产负债表页段",
    },
    "cash_flow_sheet": {
        "strong_keywords": ("合并现金流量表", "5、合并现金流量表"),
        "weak_keywords": ("现金流量表",),
        "markers": (
            "编制单位",
            "单位：元",
            "项目",
            "经营活动产生的现金流量净额",
            "现金及现金等价物净增加额",
        ),
        "window_config_key": "statement_window_pages",
        "default_window_pages": 6,
        "primary_context_label": "合并现金流量表页段",
    },
    "income_sheet": {
        "strong_keywords": ("合并利润表", "3、合并利润表"),
        "weak_keywords": ("利润表",),
        "markers": ("单位：元", "项目", "营业总收入", "营业利润", "利润总额"),
        "window_config_key": "statement_window_pages",
        "default_window_pages": 6,
        "primary_context_label": "合并利润表页段",
    },
}


@dataclass(frozen=True)
class StructuredExtractionArtifact:
    payload: dict[str, list[dict[str, Any]]]
    structured_json_path: str
    trace: dict[str, Any]
    use_full_pdf: bool = False


@dataclass(frozen=True)
class PdfPageText:
    page_number: int
    text: str


@dataclass(frozen=True)
class TableExtractionContext:
    table_name: str
    page_numbers: tuple[int, ...]
    context_text: str
    source_mode: str
    anchor_page: int | None
    used_core_supplement: bool


@dataclass(frozen=True)
class TableExtractionResult:
    table_name: str
    records: list[dict[str, Any]]
    page_numbers: tuple[int, ...]
    source_mode: str
    stop_reason: str | None
    skipped: bool
    used_core_supplement: bool


def extract_structured_report(
    file_path: str,
    financial_report: models_financial_report.FinancialReport,
) -> StructuredExtractionArtifact:
    report_id = getattr(financial_report, "id", None)
    logger.info("开始结构化抽取流程: report_id=%s file_path=%s", report_id, file_path)
    total_start_time = time.time()

    config = settings.PROMPT_CONFIG.get_struct_config
    page_texts = read_pdf_pages(file_path)
    
    is_summary = is_summary_report(page_texts)
    if is_summary:
        logger.info("检测到摘要版报告，使用全PDF模式: report_id=%s", report_id)
    
    table_contexts = build_table_contexts(page_texts, config, force_full_pdf=is_summary)

    use_full_pdf = any(
        context.source_mode == "full_pdf" for context in table_contexts.values()
    ) or is_summary

    extraction_start_time = time.time()
    table_results = extract_tables_parallel(
        financial_report=financial_report,
        table_contexts=table_contexts,
        config=config,
    )
    extraction_elapsed = time.time() - extraction_start_time

    empty_tables = [
        result.table_name
        for result in table_results
        if not result.records and not result.skipped
    ]

    if empty_tables and not use_full_pdf:
        logger.info(
            "检测到空表，尝试扩大搜索范围: report_id=%s empty_tables=%s",
            getattr(financial_report, "id", None),
            ",".join(empty_tables),
        )
        fallback_contexts = build_fallback_contexts(page_texts, empty_tables, config)
        for table_name in empty_tables:
            if table_name in fallback_contexts:
                fallback_result = extract_single_table(
                    financial_report=financial_report,
                    table_name=table_name,
                    context=fallback_contexts[table_name],
                    config=config,
                )
                if fallback_result.records:
                    table_results = [
                        fallback_result if result.table_name == table_name else result
                        for result in table_results
                    ]
                    logger.info(
                        "扩大搜索范围后成功抽取: report_id=%s table=%s records=%s",
                        getattr(financial_report, "id", None),
                        table_name,
                        len(fallback_result.records),
                    )

    payload = {
        table_name: next(
            result.records
            for result in table_results
            if result.table_name == table_name
        )
        for table_name in TABLE_ORDER
    }
    structured_json_path = save_structured_payload(file_path, payload)
    trace = build_extraction_trace(table_contexts, table_results)

    total_elapsed = time.time() - total_start_time
    logger.info(
        "分表结构化抽取完成: report_id=%s tables=%s extraction_elapsed=%.2fs total_elapsed=%.2fs use_full_pdf=%s",
        report_id,
        ",".join(TABLE_ORDER),
        extraction_elapsed,
        total_elapsed,
        use_full_pdf,
    )
    return StructuredExtractionArtifact(
        payload=payload,
        structured_json_path=structured_json_path,
        trace=trace,
        use_full_pdf=use_full_pdf,
    )


def read_pdf_pages(file_path: str) -> tuple[PdfPageText, ...]:
    logger.info("开始读取 PDF 文件: file_path=%s", file_path)
    try:
        reader = PdfReader(file_path)
    except Exception as exc:
        raise ServiceException(
            ErrorCode.AI_SERVICE_ERROR,
            "无法读取财报 PDF 文本，结构化抽取中断",
        ) from exc

    pages: list[PdfPageText] = []
    for index, page in enumerate(reader.pages, start=1):
        raw_text = page.extract_text() or ""
        text = normalize_pdf_page_text(raw_text)
        pages.append(PdfPageText(page_number=index, text=text))
    logger.info(
        "PDF 文件读取完成: total_pages=%d total_chars=%d",
        len(pages),
        sum(len(p.text) for p in pages),
    )
    return tuple(pages)


def normalize_pdf_page_text(text: str) -> str:
    normalized = text.replace("\r", "\n")
    normalized = re.sub(r"[\x00-\x08\x0b-\x0c\x0e-\x1f]", " ", normalized)
    normalized = re.sub(r"[ \t]+", " ", normalized)
    lines = [line.strip() for line in normalized.split("\n")]
    lines = [line for line in lines if line]
    return "\n".join(lines).strip()


SHORT_PDF_THRESHOLD = 10
MISSING_ANCHOR_THRESHOLD = 2
SUMMARY_KEYWORDS = ("摘要", "年报摘要", "年度报告摘要")


def is_summary_report(page_texts: tuple[PdfPageText, ...]) -> bool:
    """检测是否为摘要版报告"""
    if not page_texts:
        return False
    
    for page in page_texts[:3]:
        text = page.text
        if any(keyword in text for keyword in SUMMARY_KEYWORDS):
            logger.info("检测到摘要版报告: keyword found in page %d", page.page_number)
            return True
    
    return False


def build_table_contexts(
    page_texts: tuple[PdfPageText, ...],
    config: dict[str, Any],
    force_full_pdf: bool = False,
) -> dict[str, TableExtractionContext]:
    page_map = {page.page_number: page for page in page_texts}
    total_pages = len(page_texts)
    logger.info(
        "开始构建表格上下文: total_pages=%d force_full_pdf=%s",
        total_pages,
        force_full_pdf,
    )
    anchor_pages = {
        table_name: find_best_anchor_page(page_texts, SECTION_SPECS[table_name])
        for table_name in TABLE_ORDER
    }
    logger.info(
        "锚点页面定位完成: %s",
        ", ".join(f"{name}={page}" for name, page in anchor_pages.items()),
    )

    missing_anchor_count = sum(1 for anchor in anchor_pages.values() if anchor is None)
    use_full_pdf = force_full_pdf or total_pages <= SHORT_PDF_THRESHOLD or missing_anchor_count >= MISSING_ANCHOR_THRESHOLD

    if use_full_pdf:
        logger.info(
            "检测到短PDF或多个锚点缺失，使用全部PDF内容: total_pages=%d missing_anchors=%d",
            total_pages,
            missing_anchor_count,
        )
        return build_full_pdf_contexts(page_texts, page_map, anchor_pages, config)

    core_context_pages = build_context_window_pages(
        page_map=page_map,
        anchor_page=anchor_pages[CORE_TABLE_NAME],
        next_anchor_page=min(
            (
                anchor
                for table_name, anchor in anchor_pages.items()
                if table_name != CORE_TABLE_NAME and anchor is not None
            ),
            default=None,
        ),
        max_pages=int(
            config.get("extraction", {}).get(
                SECTION_SPECS[CORE_TABLE_NAME]["window_config_key"],
                SECTION_SPECS[CORE_TABLE_NAME]["default_window_pages"],
            )
        ),
    )

    table_contexts: dict[str, TableExtractionContext] = {}
    for table_name in TABLE_ORDER:
        if table_name == CORE_TABLE_NAME:
            context_pages = core_context_pages
            logger.info(
                "构建核心指标表上下文: table=%s pages=%s text_length=%d",
                table_name,
                [p.page_number for p in context_pages],
                sum(len(p.text) for p in context_pages),
            )
            table_contexts[table_name] = TableExtractionContext(
                table_name=table_name,
                page_numbers=tuple(page.page_number for page in context_pages),
                context_text=render_table_context(
                    table_name=table_name,
                    table_pages=context_pages,
                    core_pages=tuple(),
                ),
                source_mode="core_only" if context_pages else "unavailable",
                anchor_page=anchor_pages[table_name],
                used_core_supplement=False,
            )
            continue

        statement_anchor = anchor_pages[table_name]
        next_anchor = min(
            (
                anchor
                for other_name, anchor in anchor_pages.items()
                if (
                    other_name != CORE_TABLE_NAME
                    and other_name != table_name
                    and anchor is not None
                    and statement_anchor is not None
                    and anchor > statement_anchor
                )
            ),
            default=None,
        )
        statement_pages = build_context_window_pages(
            page_map=page_map,
            anchor_page=statement_anchor,
            next_anchor_page=next_anchor,
            max_pages=int(
                config.get("extraction", {}).get(
                    SECTION_SPECS[table_name]["window_config_key"],
                    SECTION_SPECS[table_name]["default_window_pages"],
                )
            ),
        )

        deduplicated_page_numbers = dedupe_page_numbers(
            tuple(page.page_number for page in statement_pages),
            tuple(page.page_number for page in core_context_pages),
        )
        used_core_supplement = bool(core_context_pages)
        if statement_pages and core_context_pages:
            source_mode = "statement_plus_core"
        elif statement_pages:
            source_mode = "statement_only"
        elif core_context_pages:
            source_mode = "core_only"
        else:
            source_mode = "unavailable"

        table_contexts[table_name] = TableExtractionContext(
            table_name=table_name,
            page_numbers=deduplicated_page_numbers,
            context_text=render_table_context(
                table_name=table_name,
                table_pages=statement_pages,
                core_pages=core_context_pages,
            ),
            source_mode=source_mode,
            anchor_page=statement_anchor,
            used_core_supplement=used_core_supplement and bool(core_context_pages),
        )
        logger.info(
            "构建报表上下文: table=%s anchor_page=%s statement_pages=%s core_supplement=%s total_pages=%d text_length=%d source_mode=%s",
            table_name,
            statement_anchor,
            [p.page_number for p in statement_pages],
            [p.page_number for p in core_context_pages] if core_context_pages else [],
            len(deduplicated_page_numbers),
            len(table_contexts[table_name].context_text),
            source_mode,
        )

    return table_contexts


def build_full_pdf_contexts(
    page_texts: tuple[PdfPageText, ...],
    page_map: dict[int, PdfPageText],
    anchor_pages: dict[str, int | None],
    config: dict[str, Any],
) -> dict[str, TableExtractionContext]:
    all_pages = tuple(page_texts)
    all_page_numbers = tuple(page.page_number for page in all_pages)
    full_context_text = render_full_pdf_context(all_pages)

    table_contexts: dict[str, TableExtractionContext] = {}
    for table_name in TABLE_ORDER:
        table_contexts[table_name] = TableExtractionContext(
            table_name=table_name,
            page_numbers=all_page_numbers,
            context_text=full_context_text,
            source_mode="full_pdf",
            anchor_page=anchor_pages[table_name],
            used_core_supplement=False,
        )
        logger.info(
            "构建全PDF上下文: table=%s total_pages=%d text_length=%d",
            table_name,
            len(all_page_numbers),
            len(full_context_text),
        )

    return table_contexts


def render_full_pdf_context(pages: tuple[PdfPageText, ...]) -> str:
    rendered_pages = [f"[第{page.page_number}页]\n{page.text}" for page in pages]
    return "【完整PDF内容】\n" + "\n\n".join(rendered_pages)


def find_best_anchor_page(
    page_texts: tuple[PdfPageText, ...],
    section_spec: dict[str, Any],
) -> int | None:
    best_page_number: int | None = None
    best_score = 0

    for page in page_texts:
        score = score_anchor_page(page.text, section_spec)
        if score > best_score:
            best_score = score
            best_page_number = page.page_number

    return best_page_number


def score_anchor_page(page_text: str, section_spec: dict[str, Any]) -> int:
    text = page_text.strip()
    if not text:
        return 0

    score = 0
    strong_keywords = tuple(section_spec.get("strong_keywords", ()))
    weak_keywords = tuple(section_spec.get("weak_keywords", ()))
    markers = tuple(section_spec.get("markers", ()))

    if any(keyword in text for keyword in strong_keywords):
        score += 100
    elif weak_keywords and any(keyword in text for keyword in weak_keywords):
        score += 45
    else:
        marker_count = sum(1 for marker in markers if marker in text)
        if marker_count >= 2:
            score += 30
        else:
            return 0

    if "目录" in text[:200]:
        score -= 120

    marker_score = sum(5 for marker in markers if marker in text)
    score += min(marker_score, 25)

    digit_count = sum(1 for char in text if char.isdigit())
    if digit_count >= 30:
        score += min(digit_count // 30, 4) * 5

    return score


def build_context_window_pages(
    page_map: dict[int, PdfPageText],
    anchor_page: int | None,
    next_anchor_page: int | None,
    max_pages: int,
) -> tuple[PdfPageText, ...]:
    if anchor_page is None:
        return tuple()

    end_page = anchor_page + max_pages - 1
    if next_anchor_page is not None:
        end_page = min(end_page, next_anchor_page - 1)

    pages: list[PdfPageText] = []
    for page_number in range(anchor_page, end_page + 1):
        page = page_map.get(page_number)
        if page is None or not page.text:
            continue
        pages.append(page)
    return tuple(pages)


def dedupe_page_numbers(*groups: tuple[int, ...]) -> tuple[int, ...]:
    ordered: list[int] = []
    seen: set[int] = set()
    for group in groups:
        for page_number in group:
            if page_number in seen:
                continue
            seen.add(page_number)
            ordered.append(page_number)
    return tuple(ordered)


def render_table_context(
    table_name: str,
    table_pages: tuple[PdfPageText, ...],
    core_pages: tuple[PdfPageText, ...],
) -> str:
    sections: list[str] = []
    if table_pages:
        sections.append(
            render_context_section(
                SECTION_SPECS[table_name]["primary_context_label"],
                table_pages,
            )
        )
    if core_pages and table_name != CORE_TABLE_NAME:
        sections.append(render_context_section("主要财务数据补充页段", core_pages))
    return "\n\n".join(section for section in sections if section).strip()


def render_context_section(label: str, pages: tuple[PdfPageText, ...]) -> str:
    rendered_pages = [f"[第{page.page_number}页]\n{page.text}" for page in pages]
    return f"【{label}】\n" + "\n\n".join(rendered_pages)


def extract_tables_parallel(
    financial_report: models_financial_report.FinancialReport,
    table_contexts: dict[str, TableExtractionContext],
    config: dict[str, Any],
) -> list[TableExtractionResult]:
    report_id = getattr(financial_report, "id", None)
    logger.info(
        "开始并行抽取: report_id=%s tables=%s max_workers=%d",
        report_id,
        ",".join(TABLE_ORDER),
        MAX_CONCURRENT_TABLES,
    )
    start_time = time.time()

    results_map: dict[str, TableExtractionResult] = {}

    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_TABLES) as executor:
        future_to_table = {
            executor.submit(
                extract_single_table,
                financial_report,
                table_name,
                table_contexts[table_name],
                config,
            ): table_name
            for table_name in TABLE_ORDER
        }

        for future in as_completed(future_to_table):
            table_name = future_to_table[future]
            try:
                result = future.result()
                results_map[table_name] = result
                logger.info(
                    "并行抽取完成: report_id=%s table=%s records=%d",
                    report_id,
                    table_name,
                    len(result.records),
                )
            except Exception as exc:
                logger.error(
                    "并行抽取失败: report_id=%s table=%s error=%s",
                    report_id,
                    table_name,
                    str(exc),
                )
                results_map[table_name] = TableExtractionResult(
                    table_name=table_name,
                    records=[],
                    page_numbers=table_contexts[table_name].page_numbers,
                    source_mode=table_contexts[table_name].source_mode,
                    stop_reason=None,
                    skipped=False,
                    used_core_supplement=table_contexts[table_name].used_core_supplement,
                )

    ordered_results = [results_map[table_name] for table_name in TABLE_ORDER]

    success_count = sum(1 for r in ordered_results if r.records or r.skipped)
    failed_count = len(TABLE_ORDER) - success_count
    total_elapsed = time.time() - start_time

    logger.info(
        "并行抽取全部完成: report_id=%s total_elapsed=%.2fs success=%d failed=%d",
        report_id,
        total_elapsed,
        success_count,
        failed_count,
    )

    return ordered_results


def extract_single_table(
    financial_report: models_financial_report.FinancialReport,
    table_name: str,
    context: TableExtractionContext,
    config: dict[str, Any],
) -> TableExtractionResult:
    report_id = getattr(financial_report, "id", None)
    if not context.context_text:
        logger.info(
            "分表结构化抽取跳过: report_id=%s table=%s reason=no_context",
            report_id,
            table_name,
        )
        return TableExtractionResult(
            table_name=table_name,
            records=[],
            page_numbers=context.page_numbers,
            source_mode=context.source_mode,
            stop_reason=None,
            skipped=True,
            used_core_supplement=context.used_core_supplement,
        )

    prompt = build_table_prompt(
        table_name=table_name,
        financial_report=financial_report,
        context=context,
        config=config,
    )
    max_tokens = int(
        config.get("table_prompts", {}).get(table_name, {}).get("max_tokens")
        or config.get("extraction", {}).get("default_max_tokens", 2048)
    )
    logger.info(
        "开始调用 AI 模型: report_id=%s table=%s pages=%s prompt_length=%d max_tokens=%d",
        report_id,
        table_name,
        list(context.page_numbers),
        len(prompt),
        max_tokens,
    )
    logger.debug(
        "AI 请求 Prompt 内容: report_id=%s table=%s\n%s",
        report_id,
        table_name,
        prompt[:2000] + "..." if len(prompt) > 2000 else prompt,
    )

    start_time = time.time()
    response, stop_reason = invoke_structured_model(
        prompt=prompt,
        max_tokens=max_tokens,
    )
    elapsed_time = time.time() - start_time

    response_text = extract_text_from_response(response)
    logger.info(
        "AI 模型响应完成: report_id=%s table=%s response_length=%d elapsed=%.2fs stop_reason=%s",
        report_id,
        table_name,
        len(response_text),
        elapsed_time,
        stop_reason,
    )
    logger.debug(
        "AI 响应内容: report_id=%s table=%s\n%s",
        report_id,
        table_name,
        response_text[:1000] + "..." if len(response_text) > 1000 else response_text,
    )

    records = parse_table_records(
        table_name=table_name,
        response_text=response_text,
        field_map=config["table_prompts"][table_name]["fields"],
    )

    logger.info(
        "分表结构化抽取完成: report_id=%s table=%s pages=%s records=%s",
        getattr(financial_report, "id", None),
        table_name,
        ",".join(str(page_number) for page_number in context.page_numbers) or "-",
        len(records),
    )
    return TableExtractionResult(
        table_name=table_name,
        records=records,
        page_numbers=context.page_numbers,
        source_mode=context.source_mode,
        stop_reason=stop_reason,
        skipped=False,
        used_core_supplement=context.used_core_supplement,
    )


def build_table_prompt(
    table_name: str,
    financial_report: FinancialReport,
    context: TableExtractionContext,
    config: dict[str, Any],
) -> str:
    table_config = config["table_prompts"][table_name]
    shared_rules = config.get("shared_rules", [])
    field_map = table_config["fields"]
    page_numbers = (
        "、".join(str(page_number) for page_number in context.page_numbers) or "无"
    )

    lines = [
        f"你现在只负责抽取 {table_name}（{table_config['table_name_cn']}）。",
        "",
        "当前报告身份：",
        f"- 股票代码：{financial_report.stock_code}",
        f"- 股票简称：{financial_report.stock_abbr}",
        f"- 报告年份：{financial_report.report_year}",
        f"- 报告期间代码：{financial_report.report_period}",
        f"- 报告期间中文：{REPORT_PERIOD_DISPLAY.get(financial_report.report_period, financial_report.report_label)}",
        f"- 报告类型：{financial_report.report_type}",
        f"- 报告标签：{financial_report.report_label}",
        f"- 报告标题：{financial_report.report_title}",
        f"- 已选上下文页码：{page_numbers}",
        "",
        f"本表上下文提示：{table_config['context_hint']}",
        "",
        "通用规则：",
    ]
    lines.extend(f"{index}. {rule}" for index, rule in enumerate(shared_rules, start=1))
    lines.extend(
        [
            "",
            "当前表字段清单（字段名 -> 中文口径）：",
        ]
    )
    lines.extend(
        f"- {field_name}: {field_label}"
        for field_name, field_label in field_map.items()
    )
    lines.extend(
        [
            "",
            "当前可用上下文：",
            context.context_text,
            "",
            "你必须只返回 JSON 数组，格式示例如下：",
            build_json_array_example(field_map),
        ]
    )
    return "\n".join(lines).strip()


def build_json_array_example(field_map: dict[str, str]) -> str:
    example_record = {field_name: None for field_name in field_map}
    return json.dumps([example_record], ensure_ascii=False, indent=2)


def invoke_structured_model(prompt: str, max_tokens: int) -> tuple[Any, str | None]:
    logger.info(
        "调用 AI 模型: model=%s max_tokens=%d prompt_chars=%d",
        settings.CHAT_MODEL,
        max_tokens,
        len(prompt),
    )
    try:
        model = get_model.build_chat_model(max_tokens=max_tokens, temperature=0.0)
        response = model.invoke(prompt)
    except Exception as exc:
        logger.error("AI 模型调用失败: error=%s", str(exc))
        raise ServiceException(
            ErrorCode.AI_SERVICE_ERROR, "结构化抽取模型调用失败"
        ) from exc

    response_metadata = getattr(response, "response_metadata", {}) or {}
    stop_reason = response_metadata.get("stop_reason")
    usage = response_metadata.get("usage", {})
    logger.info(
        "AI 模型返回: stop_reason=%s input_tokens=%s output_tokens=%s",
        stop_reason,
        usage.get("input_tokens", "N/A"),
        usage.get("output_tokens", "N/A"),
    )
    if stop_reason == "max_tokens":
        raise ServiceException(
            ErrorCode.AI_SERVICE_ERROR,
            "结构化抽取模型输出被截断，请缩短上下文页段或提高输出上限",
        )
    return response, stop_reason


def extract_text_from_response(response: Any) -> str:
    content = getattr(response, "content", None)
    if isinstance(content, str):
        return content.strip()

    if isinstance(content, list):
        text_parts: list[str] = []
        for block in content:
            if not isinstance(block, dict):
                continue
            if block.get("type") != "text":
                continue
            text = str(block.get("text", "")).strip()
            if text:
                text_parts.append(text)
        return "".join(text_parts).strip()

    text = getattr(response, "text", "")
    if isinstance(text, str):
        return text.strip()

    return ""


def parse_table_records(
    table_name: str,
    response_text: str,
    field_map: dict[str, str],
) -> list[dict[str, Any]]:
    if not response_text:
        raise ServiceException(
            ErrorCode.AI_SERVICE_ERROR,
            f"{table_name} 未返回可解析文本",
        )

    normalized_text = strip_code_fence(response_text)
    try:
        parsed = json.loads(normalized_text)
    except json.JSONDecodeError as exc:
        raise ServiceException(
            ErrorCode.AI_SERVICE_ERROR,
            f"{table_name} 输出不是合法 JSON",
        ) from exc

    if isinstance(parsed, dict):
        parsed = parsed.get(table_name)
    if not isinstance(parsed, list):
        raise ServiceException(
            ErrorCode.AI_SERVICE_ERROR,
            f"{table_name} 必须返回 JSON 数组",
        )
    if len(parsed) > 1:
        raise ServiceException(
            ErrorCode.AI_SERVICE_ERROR,
            f"{table_name} 只允许返回一条记录",
        )
    if not parsed:
        return []

    record = parsed[0]
    if not isinstance(record, dict):
        raise ServiceException(
            ErrorCode.AI_SERVICE_ERROR,
            f"{table_name} 的记录必须是对象",
        )

    unexpected_fields = sorted(set(record) - set(field_map))
    if unexpected_fields:
        raise ServiceException(
            ErrorCode.AI_SERVICE_ERROR,
            f"{table_name} 输出了未定义字段：{', '.join(unexpected_fields)}",
        )

    normalized_record = {field_name: record.get(field_name) for field_name in field_map}
    if all(value is None for value in normalized_record.values()):
        return []
    return [normalized_record]


def strip_code_fence(text: str) -> str:
    normalized = text.strip()
    if normalized.startswith("```json"):
        normalized = normalized.removeprefix("```json").strip()
    elif normalized.startswith("```"):
        normalized = normalized.removeprefix("```").strip()
    if normalized.endswith("```"):
        normalized = normalized[:-3].strip()
    return normalized


def save_structured_payload(
    file_path: str,
    payload: dict[str, list[dict[str, Any]]],
) -> str:
    pdf_name = os.path.splitext(os.path.basename(file_path))[0]
    json_file_name = f"{pdf_name}_structured_{int(time.time())}.json"
    saved_path = save_json(settings.json_UPLOAD_DIR, json_file_name, payload)
    logger.info("结构化结果 JSON 已保存: %s", saved_path)
    return saved_path


def build_extraction_trace(
    table_contexts: dict[str, TableExtractionContext],
    table_results: list[TableExtractionResult],
) -> dict[str, Any]:
    result_map = {result.table_name: result for result in table_results}
    return {
        "table_contexts": {
            table_name: {
                "page_numbers": list(context.page_numbers),
                "source_mode": context.source_mode,
                "anchor_page": context.anchor_page,
                "used_core_supplement": context.used_core_supplement,
                "context_length": len(context.context_text),
            }
            for table_name, context in table_contexts.items()
        },
        "table_results": {
            table_name: {
                "record_count": len(result_map[table_name].records),
                "stop_reason": result_map[table_name].stop_reason,
                "skipped": result_map[table_name].skipped,
                "source_mode": result_map[table_name].source_mode,
                "used_core_supplement": result_map[table_name].used_core_supplement,
            }
            for table_name in TABLE_ORDER
        },
    }


def build_fallback_contexts(
    page_texts: tuple[PdfPageText, ...],
    empty_tables: list[str],
    config: dict[str, Any],
) -> dict[str, TableExtractionContext]:
    """为抽取失败的表构建扩大搜索范围的上下文"""
    page_map = {page.page_number: page for page in page_texts}
    total_pages = len(page_texts)

    fallback_contexts: dict[str, TableExtractionContext] = {}

    for table_name in empty_tables:
        spec = SECTION_SPECS[table_name]
        max_pages = min(
            int(
                config.get("extraction", {}).get(
                    spec["window_config_key"],
                    spec["default_window_pages"],
                )
            )
            * 3,
            total_pages,
        )

        anchor_page = find_best_anchor_page(page_texts, spec)
        if anchor_page is None:
            scored_pages = []
            for page in page_texts:
                marker_count = sum(
                    1 for marker in spec["markers"] if marker in page.text
                )
                digit_count = sum(1 for char in page.text if char.isdigit())
                score = marker_count * 10 + min(digit_count // 50, 5)
                if score > 0:
                    scored_pages.append((page.page_number, score))

            if scored_pages:
                scored_pages.sort(key=lambda x: x[1], reverse=True)
                anchor_page = scored_pages[0][0]
            else:
                anchor_page = 1

        context_pages = build_context_window_pages(
            page_map=page_map,
            anchor_page=anchor_page,
            next_anchor_page=None,
            max_pages=max_pages,
        )

        if not context_pages:
            context_pages = page_texts[:max_pages]

        context_text = render_table_context(
            table_name=table_name,
            table_pages=context_pages,
            core_pages=tuple(),
        )

        fallback_contexts[table_name] = TableExtractionContext(
            table_name=table_name,
            page_numbers=tuple(page.page_number for page in context_pages),
            context_text=context_text,
            source_mode="fallback_expanded",
            anchor_page=anchor_page,
            used_core_supplement=False,
        )

        logger.info(
            "构建回退上下文: table=%s anchor_page=%s pages=%s",
            table_name,
            anchor_page,
            len(context_pages),
        )

    return fallback_contexts
