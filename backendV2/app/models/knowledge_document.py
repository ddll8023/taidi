from datetime import datetime, date

from sqlalchemy import (
    CHAR,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    SmallInteger,
    String,
    Text,
    func,
    text,
)

from app.db.database import Base


class KnowledgeDocument(Base):
    __tablename__ = "knowledge_document"

    id = Column[int](Integer, primary_key=True, autoincrement=True, comment="文档ID")
    doc_type = Column[str](
        String(32),
        nullable=False,
        comment="文档类型：RESEARCH_REPORT个股研报/FINANCIAL_REPORT财报原文",
    )
    doc_hash = Column[str](String(64), comment="文档哈希值(SHA256哈希)")
    source_path = Column[str](String(500), comment="PDF源文件路径")

    # 公共字段
    title = Column[str](String(500), nullable=False, comment="文档标题")
    org_code = Column[str](String(8), comment="发布报告的券商编码")
    org_name = Column[str](String(255), comment="发布报告的券商全称")
    publish_date = Column[date](Date, comment="报告的发布日期")
    researcher = Column[str](String(200), comment="报告的研究员姓名")
    industry_name = Column[str](String(255), comment="所属的行业名称")
    em_rating_name = Column[str](String(100), comment="当前报告的评级名称")
    last_em_rating_name = Column[str](String(100), comment="上一次报告的评级名称")
    s_rating_name = Column[str](String(100), comment="国际通用的评级名称")
    s_rating_code = Column[str](String(100), comment="国际通用的评级编码")

    # 个股研报字段
    stock_abbr = Column[str](String(50), comment="股票简称(个股研报)")
    stock_code = Column[str](
        CHAR(6),
        nullable=True,
        comment="股票代码(个股研报)",
    )
    predict_next_two_year_eps = Column[str](
        String(50), comment="预测未来两年的每股收益（EPS）(个股研报)"
    )
    predict_next_two_year_pe = Column[str](
        String(50), comment="预测未来两年的市盈率（PE）(个股研报)"
    )
    predict_next_year_eps = Column[str](
        String(50), comment="预测下一年的每股收益（EPS）(个股研报)"
    )
    predict_next_year_pe = Column[str](
        String(50), comment="预测下一年的市盈率（PE）(个股研报)"
    )
    predict_this_year_eps = Column[str](
        String(50), comment="预测本年度的每股收益（EPS）(个股研报)"
    )
    predict_this_year_pe = Column[str](
        String(50), comment="预测本年度的市盈率（PE）(个股研报)"
    )
    predict_last_year_eps = Column[str](
        String(50), comment="预测上一年的每股收益（EPS）(个股研报)"
    )
    predict_last_year_pe = Column[str](
        String(50), comment="预测上一年的市盈率（PE）(个股研报)"
    )
    indv_is_new = Column[str](
        String(3),
        comment="是否为新标的标识（“001”代表非新标的；缺失值为新标的）(个股研报)",
    )
    new_listing_date = Column[date](Date, comment="标的股票的上市日期(个股研报)")
    new_purchase_date = Column[date](Date, comment="标的股票的申购日期(个股研报)")
    new_issue_price = Column[str](String(50), comment="标的股票的发行价格(个股研报)")
    new_pe_issue_a = Column[str](String(50), comment="标的股票发行时的市盈率(个股研报)")
    indv_aim_price_t = Column[str](
        String(50), comment="目标价上限（部分数据为空）(个股研报)"
    )
    indv_aim_price_l = Column[str](
        String(50), comment="目标价下限（部分数据为空）(个股研报)"
    )
    market = Column[str](String(100), comment="标的股票所属的交易所(个股研报)")

    # 行业研报字段
    org_S_Name = Column[str](String(255), comment="发布报告的券商简称(行业研报)")

    financial_report_id = Column[int](
        Integer,
        ForeignKey("financial_report.id", ondelete="SET NULL"),
        nullable=True,
        comment="关联财报主表ID（仅FINANCIAL_REPORT类型）",
    )
    chunk_count = Column[int](
        Integer,
        nullable=False,
        default=0,
        server_default=text("0"),
        comment="切块数量",
    )
    chunk_status = Column[int](
        SmallInteger,
        nullable=False,
        default=0,
        comment="切块状态：0待切块，1切块中，2切块完成，3切块失败",
    )
    chunk_error_message = Column[str](Text, comment="切块失败原因")
    metadata_status = Column[int](
        SmallInteger,
        nullable=False,
        default=0,
        comment="元数据状态：0未加载，1已加载（待上传PDF），2PDF已上传",
    )
    vector_status = Column[int](
        SmallInteger,
        nullable=False,
        default=0,
        comment="向量状态：0未向量化，1向量化中，2已向量化，3向量化失败",
    )
    vector_error_message = Column[str](Text, comment="向量化失败原因")

    parse_status = Column[int](
        SmallInteger,
        nullable=False,
        default=0,
        server_default=text("0"),
        comment="解析状态：0未解析，1解析中，2解析完成，3解析失败",
    )
    parse_error_message = Column[str](Text, comment="解析失败原因")

    clean_status = Column[int](
        SmallInteger,
        nullable=False,
        default=0,
        server_default=text("0"),
        comment="清洗状态：0未清洗，1已清洗",
    )

    created_at = Column[datetime](
        DateTime,
        server_default=func.now(),
        nullable=False,
        comment="创建时间",
    )
    updated_at = Column[datetime](
        DateTime,
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
        comment="更新时间",
    )

    __table_args__ = (
        Index(
            "idx_knowledge_document_lookup",
            "doc_type",
            "stock_code",
            "publish_date",
        ),
        Index(
            "idx_knowledge_document_stock_abbr",
            "stock_abbr",
        ),
        Index(
            "idx_knowledge_document_doc_hash",
            "doc_hash",
            unique=True,
        ),
        Index(
            "idx_knowledge_document_status",
            "metadata_status",
            "chunk_status",
            "vector_status",
        ),
        Index(
            "idx_knowledge_document_parse_status",
            "parse_status",
        ),
    )
