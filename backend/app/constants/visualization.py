"""智能问数图表生成服务常量定义"""

CHART_TYPES = [
    "line",
    "bar",
    "pie",
    "horizontal_bar",
    "grouped_bar",
    "radar",
    "histogram",
    "scatter",
    "box",
]

PERIOD_SORT_MAP = {
    "Q1": 1,
    "HY": 2,
    "Q3": 3,
    "FY": 4,
}

TIME_COLUMN_TOKENS = ("year", "period", "date", "time", "年", "期", "日期", "时间")

CATEGORY_COLUMN_TOKENS = (
    "stock_abbr",
    "company",
    "company_name",
    "name",
    "abbr",
    "简称",
    "名称",
    "企业",
    "公司",
)

DIMENSION_COLUMN_TOKENS = TIME_COLUMN_TOKENS + (
    "id",
    "code",
    "type",
    "rank",
    "编号",
    "代码",
    "类型",
    "排名",
)

CHART_TYPE_KEYWORDS = {
    "horizontal_bar": ["水平柱状图", "横向柱状图", "水平条形图", "条形图"],
    "grouped_bar": ["分组柱状图", "双条形图", "并列柱状图", "对比柱状图"],
    "radar": ["雷达图", "蜘蛛图", "雷达"],
    "histogram": ["直方图", "分布图", "频率分布", "历史分布", "直方图展示"],
    "scatter": ["散点图", "散点", "相关性图"],
    "box": ["箱线图", "箱型图", "盒须图"],
}
