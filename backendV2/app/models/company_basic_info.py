from datetime import datetime

from sqlalchemy import (
    CHAR,
    DECIMAL,
    Column,
    DateTime,
    Index,
    Integer,
    String,
    func,
)
from app.db.database import Base


class CompanyBasicInfo(Base):
    __tablename__ = "company_basic_info"

    stock_code = Column[str](
        CHAR(6), primary_key=True, comment="股票代码，统一补零为6位"
    )
    stock_abbr = Column[str](String(50), nullable=False, comment="A股简称")
    company_name = Column[str](String(255), nullable=False, comment="公司名称")
    english_name = Column[str](String(255), comment="英文名称")
    csrc_industry = Column[str](String(255), comment="所属证监会行业")
    listed_exchange = Column[str](String(50), nullable=False, comment="上市交易所原始文本")
    exchange = Column[str](String(2), nullable=False, comment="标准化交易所代码：SH/SZ/BJ")
    security_category = Column[str](String(100), comment="证券类别")
    registered_region = Column[str](String(100), comment="注册区域")
    registered_capital_raw = Column[str](String(50), comment="注册资本原始文本")
    registered_capital_yuan = Column[float](
        DECIMAL(20, 2),
        comment="注册资本标准化数值，单位：元",
    )
    employee_count = Column[int](Integer, comment="雇员人数")
    management_count = Column[int](Integer, comment="管理人员人数")
    source_row_no = Column[int](Integer, nullable=False, comment="原始序号")
    source_file_name = Column[str](String(512), nullable=False, comment="源文件名")
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
        Index("idx_company_basic_info_stock_abbr", "stock_abbr"),
        Index("idx_company_basic_info_company_name", "company_name"),
        Index(
            "idx_company_basic_info_lookup",
            "exchange",
            "csrc_industry",
            "security_category",
        ),
    )
