"""智能问数图表生成服务"""
from decimal import Decimal
import os
import re
from typing import Any

import numpy as np

from app.constants import visualization as constants_visualization
from app.schemas.chat import IntentResult, QueryType
from app.schemas.common import ErrorCode
from app.utils.exception import ServiceException
from app.utils.logger_config import setup_logger

logger = setup_logger(__name__)

CHART_DIR = os.path.join(os.getcwd(), "result")


# ========== 公共入口函数 ==========


def get_chart_image_path(filename: str):
    """获取图表图片路径，不存在则抛出异常"""
    file_path = os.path.join(CHART_DIR, filename)
    if not os.path.exists(file_path):
        raise ServiceException(ErrorCode.DATA_NOT_FOUND, "图片不存在")
    return file_path


def generate_chart(
    data: list[dict],
    intent: IntentResult,
    question_id: str,
    sequence: int = 1,
    requested_chart_type: str | None = None,
):
    """根据查询结果和意图生成图表文件，并返回文件路径与图表类型"""
    chart_type = _select_chart_type(data, intent, requested_chart_type)
    if chart_type is None:
        logger.info(f"无需生成图表: question_id={question_id}")
        return None, None

    logger.info(f"生成图表: question_id={question_id} chart_type={chart_type}")

    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        plt.rcParams["font.sans-serif"] = ["SimHei", "Microsoft YaHei", "Arial"]
        plt.rcParams["axes.unicode_minus"] = False
    except ImportError:
        raise ServiceException(ErrorCode.AI_SERVICE_ERROR, "图表渲染环境未就绪")

    os.makedirs(CHART_DIR, exist_ok=True)
    file_name = f"{question_id}_{sequence}.jpg"
    file_path = os.path.join(CHART_DIR, file_name)

    try:
        if chart_type == "line":
            _render_line_chart(data, intent, plt)
        elif chart_type == "bar":
            _render_bar_chart(data, intent, plt)
        elif chart_type == "pie":
            _render_pie_chart(data, intent, plt)
        elif chart_type == "horizontal_bar":
            _render_horizontal_bar_chart(data, intent, plt)
        elif chart_type == "grouped_bar":
            _render_grouped_bar_chart(data, intent, plt)
        elif chart_type == "radar":
            _render_radar_chart(data, intent, plt)
        elif chart_type == "histogram":
            _render_histogram_chart(data, intent, plt)
        elif chart_type == "scatter":
            _render_scatter_chart(data, intent, plt)
        elif chart_type == "box":
            _render_box_chart(data, intent, plt)
        else:
            raise ServiceException(ErrorCode.INTERNAL_ERROR, "不支持的图表类型")

        plt.savefig(file_path, dpi=150, bbox_inches="tight")
        plt.close()
        logger.info(f"图表已保存: {file_path}")
        return file_path, chart_type
    except ServiceException:
        plt.close("all")
        raise
    except Exception as exc:
        logger.error(f"图表生成失败: {exc}")
        plt.close("all")
        raise ServiceException(ErrorCode.AI_SERVICE_ERROR, "图表生成失败")


"""辅助函数"""


def _detect_requested_chart_type(question: str):
    """从问题中检测用户请求的图表类型"""
    for chart_type, keywords in constants_visualization.CHART_TYPE_KEYWORDS.items():
        for keyword in keywords:
            if keyword in question:
                return chart_type
    return None


def _select_chart_type(
    data: list[dict],
    intent: IntentResult,
    requested_chart_type: str | None = None,
):
    """根据数据和意图选择最合适的图表类型"""
    if not data:
        return None

    if len(data) == 1:
        return None

    if requested_chart_type:
        if _validate_chart_type(requested_chart_type, data):
            return requested_chart_type

    question = intent.question or ""
    detected_type = _detect_requested_chart_type(question)
    if detected_type and _validate_chart_type(detected_type, data):
        return detected_type

    if intent.query_type == QueryType.TREND:
        if len(data) >= 3:
            return "line"

    if intent.query_type == QueryType.RANKING:
        if 3 <= len(data) <= 10:
            return "bar"
        if len(data) > 10:
            return "horizontal_bar"

    if intent.query_type == QueryType.COMPARISON:
        if intent.is_multi_company():
            return "grouped_bar"
        if 3 <= len(data) <= 10:
            return "bar"

    numeric_cols = _find_numeric_columns(data)
    if len(numeric_cols) == 0:
        return None

    if len(data) >= 3 and _has_time_dimension(data):
        return "line"

    if 3 <= len(data) <= 10:
        return "bar"

    if len(data) > 10:
        return "horizontal_bar"

    return None


def _validate_chart_type(chart_type: str, data: list[dict]):
    """验证图表类型是否适用于当前数据"""
    if chart_type not in constants_visualization.CHART_TYPES:
        return False

    numeric_cols = _find_numeric_columns(data)
    if len(numeric_cols) == 0:
        return False

    if chart_type == "radar":
        return len(data) >= 3 and len(numeric_cols) >= 3

    if chart_type == "scatter":
        return len(numeric_cols) >= 2

    if chart_type == "box":
        return len(data) >= 5

    if chart_type == "histogram":
        return len(data) >= 10

    return True


def _find_numeric_columns(data: list[dict]):
    """查找数据中所有数值列"""
    if not data:
        return []

    numeric_cols = []
    for key in _collect_columns(data):
        if _is_dimension_column(key):
            continue

        values = [
            row.get(key)
            for row in data
            if key in row and row.get(key) not in (None, "")
        ]
        if not values:
            continue

        if all(_is_numeric_value(value) for value in values):
            numeric_cols.append(key)
    return numeric_cols


def _has_time_dimension(data: list[dict]):
    """判断数据是否包含时间维度列"""
    if not data:
        return False
    return any(_is_time_column(key) for key in _collect_columns(data))


def _collect_columns(data: list[dict]):
    """收集数据中所有列名，保持首次出现顺序"""
    columns: list[str] = []
    seen: set[str] = set()
    for row in data:
        for key in row.keys():
            if key not in seen:
                seen.add(key)
                columns.append(key)
    return columns


def _normalize_column_name(column: Any):
    """标准化列名：去除非字母数字字符并转小写"""
    return re.sub(r"[\W_]+", "", str(column)).lower()


def _is_time_column(column: str):
    """判断列名是否为时间维度"""
    key_normalized = _normalize_column_name(column)
    return any(token in key_normalized for token in constants_visualization.TIME_COLUMN_TOKENS)


def _is_dimension_column(column: str):
    """判断列名是否为维度列"""
    key_normalized = _normalize_column_name(column)
    return any(token in key_normalized for token in constants_visualization.DIMENSION_COLUMN_TOKENS)


def _is_numeric_value(value: Any):
    """判断值是否为数值类型或可转为数值的字符串"""
    if value is None or isinstance(value, bool):
        return False

    if isinstance(value, (int, float, Decimal)):
        return True

    if isinstance(value, str):
        cleaned = value.strip().replace(",", "")
        if cleaned.endswith("%"):
            cleaned = cleaned[:-1]
        if not cleaned:
            return False
        try:
            float(cleaned)
            return True
        except ValueError:
            return False

    return False


def _to_float(value: Any):
    """将值转为浮点数，无法转换时返回0.0"""
    if value is None:
        return 0.0
    if isinstance(value, bool):
        return float(int(value))
    if isinstance(value, (int, float, Decimal)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.strip().replace(",", "")
        if cleaned.endswith("%"):
            cleaned = cleaned[:-1]
        if cleaned:
            try:
                return float(cleaned)
            except ValueError:
                return 0.0
    return 0.0


def _pick_metric_column(numeric_cols: list[str], intent: IntentResult):
    """从数值列中匹配意图指定的指标列"""
    if not numeric_cols:
        return ""

    metric_field = ""
    metric_display_name = ""
    first_metric = intent.get_first_metric()
    if first_metric:
        metric_field = str(first_metric.get("field") or "")
        metric_display_name = str(first_metric.get("display_name") or "")

    normalized_field = _normalize_column_name(metric_field) if metric_field else ""
    normalized_display_name = (
        _normalize_column_name(metric_display_name) if metric_display_name else ""
    )

    best_col = numeric_cols[0]
    best_score = -1
    for index, column in enumerate(numeric_cols):
        normalized_column = _normalize_column_name(column)
        score = 0

        if metric_field and column == metric_field:
            score = 100
        elif normalized_field and normalized_column == normalized_field:
            score = 95
        elif normalized_display_name and normalized_column == normalized_display_name:
            score = 90
        elif normalized_display_name and normalized_display_name in normalized_column:
            score = 80
        elif normalized_field and normalized_field in normalized_column:
            score = 70

        score -= index
        if score > best_score:
            best_score = score
            best_col = column

    return best_col


def _pick_category_column(data: list[dict], numeric_cols: list[str]):
    """从数据中选取最佳分类列"""
    candidate_cols = [
        column
        for column in _collect_columns(data)
        if column not in numeric_cols and not _is_time_column(column)
    ]
    if not candidate_cols:
        return None

    best_col = candidate_cols[0]
    best_score = -1
    for index, column in enumerate(candidate_cols):
        normalized_column = _normalize_column_name(column)
        score = 0
        if any(token in normalized_column for token in constants_visualization.CATEGORY_COLUMN_TOKENS):
            score = 100
        score -= index
        if score > best_score:
            best_score = score
            best_col = column

    return best_col


def _extract_report_year(row: dict):
    """从行数据中提取报告年份"""
    for column in row.keys():
        normalized_column = _normalize_column_name(column)
        if normalized_column == "reportyear" or normalized_column == "year":
            value = row.get(column)
            if value in (None, ""):
                return None
            try:
                return int(str(value).strip())
            except ValueError:
                return None
    return None


def _extract_report_period(row: dict):
    """从行数据中提取报告期（Q1/HY/Q3/FY）"""
    for column in row.keys():
        normalized_column = _normalize_column_name(column)
        if normalized_column == "reportperiod" or normalized_column == "period":
            value = row.get(column)
            if value in (None, ""):
                return None
            return str(value).strip().upper()
    return None


def _build_time_label(row: dict):
    """构建时间轴标签（如 2024FY）"""
    report_year = _extract_report_year(row)
    report_period = _extract_report_period(row)

    if report_year is not None and report_period:
        return f"{report_year}{report_period}"
    if report_year is not None:
        return str(report_year)
    if report_period:
        return report_period

    for column in _collect_columns([row]):
        if _is_time_column(column):
            value = row.get(column)
            if value not in (None, ""):
                return str(value)
    return None


def _sort_trend_rows(data: list[dict]):
    """按年份、报告期对趋势数据排序"""
    if not data:
        return []

    if not any(_extract_report_year(row) is not None for row in data):
        return data

    return sorted(
        data,
        key=lambda row: (
            (
                _extract_report_year(row)
                if _extract_report_year(row) is not None
                else 9999
            ),
            constants_visualization.PERIOD_SORT_MAP.get(_extract_report_period(row) or "", 99),
            _build_time_label(row) or "",
        ),
    )


def _build_x_labels(
    data: list[dict],
    intent: IntentResult,
    numeric_cols: list[str],
):
    """根据数据构建X轴标签列表"""
    if not data:
        return []

    if intent.query_type == QueryType.TREND:
        time_labels = [_build_time_label(row) for row in data]
        if all(label is not None for label in time_labels):
            return [str(label) for label in time_labels]

    if intent.query_type in (QueryType.RANKING, QueryType.COMPARISON):
        category_col = _pick_category_column(data, numeric_cols)
        if category_col:
            return [str(row.get(category_col, "")) for row in data]

    if _has_time_dimension(data):
        time_labels = [_build_time_label(row) for row in data]
        if all(label is not None for label in time_labels):
            return [str(label) for label in time_labels]

    category_col = _pick_category_column(data, numeric_cols)
    if category_col:
        return [str(row.get(category_col, "")) for row in data]

    non_numeric_cols = [
        column for column in _collect_columns(data) if column not in numeric_cols
    ]
    if non_numeric_cols:
        x_col = non_numeric_cols[0]
        return [str(row.get(x_col, "")) for row in data]

    return [str(index + 1) for index, _row in enumerate(data)]


def _extract_chart_data(
    data: list[dict], intent: IntentResult
):
    """提取图表所需的标签、数值、标题和Y轴标签"""
    x_labels = []
    y_values = []
    title = "数据图表"
    y_label = ""

    if not data:
        return x_labels, y_values, title, y_label

    rows = (
        _sort_trend_rows(data) if intent.query_type == QueryType.TREND else list(data)
    )
    numeric_cols = _find_numeric_columns(data)
    if not numeric_cols:
        return x_labels, y_values, title, y_label

    y_col = _pick_metric_column(numeric_cols, intent)

    first_metric = intent.get_first_metric()
    if first_metric and first_metric.get("display_name"):
        y_label = first_metric["display_name"]
        title = f"{y_label}分析"
    else:
        y_label = y_col
        title = f"{y_col}分析"

    if intent.company is not None:
        if isinstance(intent.company, list):
            values = [
                c.get("value", "")
                for c in intent.company
                if isinstance(c, dict) and c.get("value")
            ]
            if values:
                if len(values) <= 3:
                    title = f"{'、'.join(values)} {title}"
                else:
                    title = f"{values[0]}等{len(values)}家公司 {title}"
        elif isinstance(intent.company, dict) and intent.company.get("value"):
            title = f"{intent.company['value']} {title}"

    x_labels = _build_x_labels(rows, intent, numeric_cols)

    for row in rows:
        val = row.get(y_col, 0)
        y_values.append(_to_float(val))

    return x_labels, y_values, title, y_label


def _render_line_chart(data: list[dict], intent: IntentResult, plt):
    """渲染折线图"""
    x_labels, y_values, title, y_label = _extract_chart_data(data, intent)
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(x_labels, y_values, marker="o", linewidth=2, markersize=6)
    ax.set_title(title, fontsize=14)
    ax.set_xlabel("时间", fontsize=12)
    ax.set_ylabel(y_label, fontsize=12)
    ax.grid(True, alpha=0.3)
    for i, val in enumerate(y_values):
        ax.annotate(
            f"{val:,.2f}",
            (x_labels[i], val),
            textcoords="offset points",
            xytext=(0, 10),
            ha="center",
            fontsize=9,
        )
    fig.tight_layout()


def _render_bar_chart(data: list[dict], intent: IntentResult, plt):
    """渲染柱状图"""
    x_labels, y_values, title, y_label = _extract_chart_data(data, intent)
    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.bar(x_labels, y_values, color="steelblue", alpha=0.8)
    ax.set_title(title, fontsize=14)
    ax.set_ylabel(y_label, fontsize=12)
    ax.grid(True, alpha=0.3, axis="y")
    for bar, val in zip(bars, y_values):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height(),
            f"{val:,.2f}",
            ha="center",
            va="bottom",
            fontsize=9,
        )
    plt.xticks(rotation=45, ha="right")
    fig.tight_layout()


def _render_pie_chart(data: list[dict], intent: IntentResult, plt):
    """渲染饼图"""
    x_labels, y_values, title, y_label = _extract_chart_data(data, intent)
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.pie(y_values, labels=x_labels, autopct="%1.1f%%", startangle=90)
    ax.set_title(title, fontsize=14)
    fig.tight_layout()


def _render_horizontal_bar_chart(data: list[dict], intent: IntentResult, plt):
    """渲染水平条形图"""
    x_labels, y_values, title, y_label = _extract_chart_data(data, intent)
    fig, ax = plt.subplots(figsize=(10, max(6, len(x_labels) * 0.4)))
    y_pos = range(len(x_labels))
    bars = ax.barh(y_pos, y_values, color="steelblue", alpha=0.8)
    ax.set_yticks(y_pos)
    ax.set_yticklabels(x_labels)
    ax.set_title(title, fontsize=14)
    ax.set_xlabel(y_label, fontsize=12)
    ax.grid(True, alpha=0.3, axis="x")
    for bar, val in zip(bars, y_values):
        ax.text(
            bar.get_width(),
            bar.get_y() + bar.get_height() / 2,
            f"{val:,.2f}",
            ha="left",
            va="center",
            fontsize=9,
        )
    fig.tight_layout()


def _render_grouped_bar_chart(data: list[dict], intent: IntentResult, plt):
    """渲染分组柱状图"""
    if not data:
        return

    numeric_cols = _find_numeric_columns(data)
    if not numeric_cols:
        return

    category_col = _pick_category_column(data, numeric_cols)
    if not category_col:
        return

    categories = list(set(str(row.get(category_col, "")) for row in data))
    categories.sort()

    if len(numeric_cols) < 2:
        x_labels, y_values, title, y_label = _extract_chart_data(data, intent)
        fig, ax = plt.subplots(figsize=(10, 6))
        bars = ax.bar(x_labels, y_values, color="steelblue", alpha=0.8)
        ax.set_title(title, fontsize=14)
        ax.set_ylabel(y_label, fontsize=12)
        ax.grid(True, alpha=0.3, axis="y")
        for bar, val in zip(bars, y_values):
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height(),
                f"{val:,.2f}",
                ha="center",
                va="bottom",
                fontsize=9,
            )
        plt.xticks(rotation=45, ha="right")
        fig.tight_layout()
        return

    y_col = _pick_metric_column(numeric_cols, intent)
    company_values = {}
    for row in data:
        cat = str(row.get(category_col, ""))
        val = _to_float(row.get(y_col, 0))
        if cat not in company_values:
            company_values[cat] = val

    x = np.arange(len(categories))
    width = 0.35

    fig, ax = plt.subplots(figsize=(12, 6))
    values = [company_values.get(cat, 0) for cat in categories]
    bars = ax.bar(x, values, width, color="steelblue", alpha=0.8)

    ax.set_title(f"{y_col}对比分析", fontsize=14)
    ax.set_xticks(x)
    ax.set_xticklabels(categories, rotation=45, ha="right")
    ax.set_ylabel(y_col, fontsize=12)
    ax.grid(True, alpha=0.3, axis="y")

    for bar, val in zip(bars, values):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height(),
            f"{val:,.2f}",
            ha="center",
            va="bottom",
            fontsize=9,
        )
    fig.tight_layout()


def _render_radar_chart(data: list[dict], intent: IntentResult, plt):
    """渲染雷达图"""
    if not data or len(data) < 3:
        return

    numeric_cols = _find_numeric_columns(data)
    if len(numeric_cols) < 3:
        return

    category_col = _pick_category_column(data, numeric_cols)
    if not category_col:
        return

    categories = [str(row.get(category_col, "")) for row in data[:6]]
    values_by_category = {}
    for col in numeric_cols[:6]:
        values_by_category[col] = []
        for row in data[:6]:
            values_by_category[col].append(_to_float(row.get(col, 0)))

    angles = np.linspace(0, 2 * np.pi, len(categories), endpoint=False).tolist()
    angles += angles[:1]

    fig, ax = plt.subplots(figsize=(10, 10), subplot_kw=dict(polar=True))

    colors = ["steelblue", "coral", "green", "purple", "orange", "red"]
    for idx, (col, values) in enumerate(list(values_by_category.items())[:3]):
        values_normalized = values / max(values) if max(values) > 0 else values
        values_closed = values_normalized + values_normalized[:1]
        ax.plot(
            angles,
            values_closed,
            "o-",
            linewidth=2,
            label=col,
            color=colors[idx % len(colors)],
        )
        ax.fill(angles, values_closed, alpha=0.25, color=colors[idx % len(colors)])

    ax.set_xticks(angles[:-1])
    ax.set_xticklabels(categories)
    ax.set_title("多维度雷达图对比", fontsize=14)
    ax.legend(loc="upper right", bbox_to_anchor=(1.3, 1.0))
    fig.tight_layout()


def _render_histogram_chart(data: list[dict], intent: IntentResult, plt):
    """渲染直方图"""
    x_labels, y_values, title, y_label = _extract_chart_data(data, intent)

    fig, ax = plt.subplots(figsize=(10, 6))
    n, bins, patches = ax.hist(
        y_values,
        bins=min(20, len(y_values) // 2),
        color="steelblue",
        alpha=0.8,
        edgecolor="white",
    )
    ax.set_title(f"{title} - 分布直方图", fontsize=14)
    ax.set_xlabel(y_label, fontsize=12)
    ax.set_ylabel("频次", fontsize=12)
    ax.grid(True, alpha=0.3, axis="y")

    for i, patch in enumerate(patches):
        height = patch.get_height()
        if height > 0:
            ax.text(
                patch.get_x() + patch.get_width() / 2,
                height,
                f"{int(height)}",
                ha="center",
                va="bottom",
                fontsize=9,
            )
    fig.tight_layout()


def _render_scatter_chart(data: list[dict], intent: IntentResult, plt):
    """渲染散点图"""
    if not data:
        return

    numeric_cols = _find_numeric_columns(data)
    if len(numeric_cols) < 2:
        x_labels, y_values, title, y_label = _extract_chart_data(data, intent)
        x_values = list(range(len(y_values)))
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.scatter(x_values, y_values, c="steelblue", alpha=0.6, s=50)
        ax.set_title(f"{title} - 散点图", fontsize=14)
        ax.set_xlabel("序号", fontsize=12)
        ax.set_ylabel(y_label, fontsize=12)
        ax.grid(True, alpha=0.3)
        fig.tight_layout()
        return

    x_col = numeric_cols[0]
    y_col = numeric_cols[1] if len(numeric_cols) > 1 else numeric_cols[0]

    x_values = [_to_float(row.get(x_col, 0)) for row in data]
    y_values = [_to_float(row.get(y_col, 0)) for row in data]

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.scatter(x_values, y_values, c="steelblue", alpha=0.6, s=50)
    ax.set_title(f"{x_col} vs {y_col} 散点图", fontsize=14)
    ax.set_xlabel(x_col, fontsize=12)
    ax.set_ylabel(y_col, fontsize=12)
    ax.grid(True, alpha=0.3)

    if len(x_values) > 2:
        try:
            z = np.polyfit(x_values, y_values, 1)
            p = np.poly1d(z)
            x_line = np.linspace(min(x_values), max(x_values), 100)
            ax.plot(x_line, p(x_line), "r--", alpha=0.8, label="趋势线")
            ax.legend()
        except Exception:
            pass

    fig.tight_layout()


def _render_box_chart(data: list[dict], intent: IntentResult, plt):
    """渲染箱线图"""
    if not data:
        return

    numeric_cols = _find_numeric_columns(data)
    if not numeric_cols:
        return

    category_col = _pick_category_column(data, numeric_cols)

    fig, ax = plt.subplots(figsize=(10, 6))

    if category_col:
        categories = list(set(str(row.get(category_col, "")) for row in data))
        categories.sort()
        box_data = []
        for cat in categories:
            cat_values = [
                _to_float(row.get(numeric_cols[0], 0))
                for row in data
                if str(row.get(category_col, "")) == cat
            ]
            if cat_values:
                box_data.append(cat_values)

        if box_data:
            bp = ax.boxplot(
                box_data, labels=categories[: len(box_data)], patch_artist=True
            )
            for patch in bp["boxes"]:
                patch.set_facecolor("steelblue")
                patch.set_alpha(0.7)
            ax.set_title(f"{numeric_cols[0]}箱线图分析", fontsize=14)
            ax.set_xlabel(category_col, fontsize=12)
            ax.set_ylabel(numeric_cols[0], fontsize=12)
    else:
        y_values = [_to_float(row.get(numeric_cols[0], 0)) for row in data]
        bp = ax.boxplot([y_values], labels=[numeric_cols[0]], patch_artist=True)
        for patch in bp["boxes"]:
            patch.set_facecolor("steelblue")
            patch.set_alpha(0.7)
        ax.set_title(f"{numeric_cols[0]}箱线图分析", fontsize=14)
        ax.set_ylabel(numeric_cols[0], fontsize=12)

    ax.grid(True, alpha=0.3, axis="y")
    plt.xticks(rotation=45, ha="right")
    fig.tight_layout()
