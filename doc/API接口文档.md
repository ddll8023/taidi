# API 接口文档

## 概述

- **基础 URL**：`http://localhost:8000`
- **统一响应格式**：所有接口返回 `ApiResponse` 结构体：

```json
{
  "code": 0,
  "message": "success",
  "data": { ... }
}
```

- **错误响应**：

```json
{
  "code": 1001,
  "message": "错误描述",
  "data": null
}
```

- **错误码表**：

| 错误码 | 说明 |
|--------|------|
| 0 | 成功 |
| 1001 | 参数错误 |
| 1002 | 数据未找到 |
| 2001 | 未登录 |
| 2002 | Token 已过期 |
| 2003 | 权限不足 |
| 3001 | 不支持的文件格式 |
| 3002 | 文件过大 |
| 4001 | AI 服务错误 |
| 5001 | 内部错误 |
| 6001 | 密码错误 |

- **Swagger 文档**：`http://localhost:8000/docs`

---

## 一、数据上传处理（/api/v1/data）

负责财报 PDF 上传、解析、查询、删除及公司信息导入。

### 1.1 上传单个 PDF

- **POST** `/api/v1/data/upload`
- **描述**：上传单个 PDF 文件，仅执行建档入库（阶段一）
- **Content-Type**：`multipart/form-data`

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| file | File | body | 是 | PDF 文件 |

**响应格式**：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "report_id": 1,
    "stock_code": "600519",
    "stock_abbr": "贵州茅台",
    "report_title": "贵州茅台2023年年度报告",
    "parse_status": 0
  }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| report_id | int | 财报记录 ID |
| stock_code | string | 股票代码 |
| stock_abbr | string | 股票简称 |
| report_title | string | 报告标题 |
| parse_status | int | 解析状态：0 待处理 |

---

### 1.2 批量上传 PDF

- **POST** `/api/v1/data/upload/batch`
- **描述**：批量上传 PDF 文件，仅执行建档入库（阶段一）
- **Content-Type**：`multipart/form-data`

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| files | File[] | body | 是 | PDF 文件列表 |

**响应格式**：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "total": 5,
    "success_count": 4,
    "failed_count": 1,
    "success_reports": [
      { "report_id": 1, "stock_code": "600519", "stock_abbr": "贵州茅台", "report_title": "...", "file_name": "贵州茅台2023年年报.pdf" }
    ],
    "failed_files": [
      { "file_name": "bad.pdf", "error": "文件名无法识别" }
    ]
  }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| total | int | 上传文件总数 |
| success_count | int | 成功建档数量 |
| failed_count | int | 失败数量 |
| success_reports | list[dict] | 成功建档的财报记录列表 |
| failed_files | list[dict] | 失败文件列表（含 file_name、error） |

---

### 1.3 提交单个财报解析任务

- **POST** `/api/v1/data/parse/{report_id}`
- **描述**：提交单个财报解析任务（异步后台执行）

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| report_id | int | path | 是 | 财报 ID |
| force | bool | query | 否 | 强制重新解析（包括已解析成功的），默认 `false` |

**响应格式**：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "report_id": 1,
    "status": "submitted"
  }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| report_id | int | 财报 ID |
| status | string | 提交状态：`submitted` |

---

### 1.4 提交批量解析任务

- **POST** `/api/v1/data/parse/batch`
- **描述**：提交批量解析任务（异步后台执行）
- **Content-Type**：`application/json`

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| report_ids | int[] | body | 是 | 财报 ID 列表 |

**请求体示例**：

```json
{
  "report_ids": [1, 2, 3]
}
```

**响应格式**：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "submitted_count": 2,
    "skipped_count": 1,
    "submitted_report_ids": [1, 2],
    "skipped_report_ids": [3]
  }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| submitted_count | int | 已提交数量 |
| skipped_count | int | 跳过数量（已解析成功且未强制重解析） |
| submitted_report_ids | list[int] | 已提交的财报 ID 列表 |
| skipped_report_ids | list[int] | 跳过的财报 ID 列表 |

---

### 1.5 提交所有待处理解析任务

- **POST** `/api/v1/data/parse/all`
- **描述**：提交所有待处理财报的解析任务（异步后台执行）

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| limit | int | query | 否 | 最大处理数量，默认 `100` |

**响应格式**：同 [1.4 批量解析](#14-提交批量解析任务)

---

### 1.6 批量查询解析状态

- **POST** `/api/v1/data/parse/status/batch`
- **描述**：根据财报 ID 列表批量查询解析状态
- **Content-Type**：`application/json`

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| report_ids | int[] | body | 是 | 财报 ID 列表 |

**请求体示例**：

```json
{
  "report_ids": [1, 2, 3]
}
```

**响应格式**：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "results": [
      { "report_id": 1, "parse_status": 1, "parse_status_text": "成功", "validate_message": null },
      { "report_id": 2, "parse_status": 2, "parse_status_text": "失败", "validate_message": "结构化抽取失败" }
    ],
    "processing_count": 1,
    "completed_count": 2,
    "total_count": 3
  }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| results | list | 各报告解析状态列表 |
| results[].report_id | int | 报告 ID |
| results[].parse_status | int | 解析状态：0 待处理 / 1 成功 / 2 失败 |
| results[].parse_status_text | string | 解析状态文本 |
| results[].validate_message | string\|null | 校验结果说明 |
| processing_count | int | 处理中数量 |
| completed_count | int | 已完成数量 |
| total_count | int | 总数 |

---

### 1.7 获取财报列表

- **GET** `/api/v1/data`
- **描述**：分页查询财报数据列表

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| page | int | query | 否 | 页码，从 1 开始，默认 `1` |
| page_size | int | query | 否 | 每页数量，最小 10，默认 `10` |
| keyword | string | query | 否 | 文件名关键词搜索 |
| parse_status | int | query | 否 | 解析状态筛选：0 待处理 / 1 成功 / 2 失败 |
| report_type | string | query | 否 | 报告类型筛选 |
| stock_code | string | query | 否 | 股票代码筛选，最长 6 字符 |
| stock_abbr | string | query | 否 | 股票简称筛选 |
| report_year | int | query | 否 | 报告年份筛选，范围 2000~2100 |
| report_period | string | query | 否 | 报告期筛选 |
| import_status | int | query | 否 | 入库状态筛选：0 待入库 / 1 成功 / 2 失败 |
| vector_status | int | query | 否 | 向量化状态筛选：0 待向量化 / 1 向量化中 / 2 成功 / 3 失败 / 4 跳过 |
| sort_by | string | query | 否 | 排序字段：`created_at` / `updated_at` |
| sort_order | string | query | 否 | 排序方式：`desc` / `asc` |

**响应格式**：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "lists": [
      {
        "id": 1,
        "file_name": "贵州茅台2023年年报.pdf",
        "report_title": "贵州茅台2023年年度报告",
        "stock_code": "600519",
        "stock_abbr": "贵州茅台",
        "report_year": 2023,
        "report_period": "FY",
        "report_type": "REPORT",
        "parse_status": 1,
        "import_status": 1,
        "vector_status": 0,
        "created_at": "2025-01-01T00:00:00"
      }
    ],
    "pagination": {
      "page": 1,
      "page_size": 10,
      "total": 100,
      "total_pages": 10
    }
  }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| lists | list | 财报记录列表 |
| lists[].id | int | 财报 ID |
| lists[].file_name | string | 源文件名 |
| lists[].report_title | string | 报告标题 |
| lists[].stock_code | string | 股票代码 |
| lists[].stock_abbr | string | 股票简称 |
| lists[].report_year | int | 报告年份 |
| lists[].report_period | string | 报告期：Q1/HY/Q3/FY |
| lists[].report_type | string | 报告类型：REPORT/SUMMARY |
| lists[].parse_status | int | 解析状态：0 待处理 / 1 成功 / 2 失败 |
| lists[].import_status | int | 入库状态：0 待入库 / 1 成功 / 2 失败 |
| lists[].vector_status | int | 向量化状态：0 待向量化 / 1 向量化中 / 2 成功 / 3 失败 / 4 跳过 |
| lists[].created_at | datetime | 创建时间 |
| pagination | object | 分页信息 |
| pagination.page | int | 当前页码 |
| pagination.page_size | int | 每页数量 |
| pagination.total | int | 总记录数 |
| pagination.total_pages | int | 总页数 |

---

### 1.8 获取单个财报详情

- **GET** `/api/v1/data/{report_id}`
- **描述**：获取单个财报记录的详情

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| report_id | int | path | 是 | 财报记录 ID |

**响应格式**：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "id": 1,
    "file_name": "贵州茅台2023年年报.pdf",
    "report_title": "贵州茅台2023年年度报告",
    "stock_code": "600519",
    "stock_abbr": "贵州茅台",
    "report_year": 2023,
    "report_period": "FY",
    "report_type": "REPORT",
    "report_label": "2023年年度报告",
    "exchange": "SSE",
    "report_date": "2024-03-30T00:00:00",
    "parse_status": 1,
    "review_status": 1,
    "validate_status": 1,
    "validate_message": null,
    "import_status": 1,
    "vector_status": 0,
    "vector_model": null,
    "vector_dim": null,
    "vector_version": null,
    "vector_error_message": null,
    "vectorized_at": null,
    "created_at": "2025-01-01T00:00:00",
    "updated_at": "2025-01-02T00:00:00",
    "core_performance_indicators": {
      "eps": 59.49,
      "total_operating_revenue": 15056000.00,
      "operating_revenue_yoy_growth": 18.04,
      "net_profit_10k_yuan": 7473400.00,
      "net_profit_yoy_growth": 19.16,
      "roe": 34.19,
      "gross_profit_margin": 91.96,
      "net_profit_margin": 49.64
    },
    "balance_sheet": {
      "asset_total_assets": 272700000.00,
      "liability_total_liabilities": 49360000.00,
      "equity_total_equity": 223340000.00,
      "asset_liability_ratio": 18.10
    },
    "cash_flow_sheet": {
      "operating_cf_net_amount": 6659300.00,
      "investing_cf_net_amount": -3256700.00,
      "financing_cf_net_amount": -5742500.00,
      "net_cash_flow": -2340900.00
    },
    "income_sheet": {
      "net_profit": 7473400.00,
      "total_operating_revenue": 15056000.00,
      "operating_profit": 10371000.00,
      "total_profit": 10367000.00
    }
  }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| id | int | 财报 ID |
| file_name | string | 源文件名 |
| report_title | string | 报告标题 |
| stock_code | string | 股票代码 |
| stock_abbr | string | 股票简称 |
| report_year | int | 报告年份 |
| report_period | string | 报告期：Q1/HY/Q3/FY |
| report_type | string | 报告类型：REPORT/SUMMARY |
| report_label | string | 报告标签 |
| exchange | string | 交易所标识 |
| report_date | datetime\|null | 报告披露日期 |
| parse_status | int | 解析状态：0 待处理 / 1 成功 / 2 失败 |
| review_status | int | 审核状态：0 待审核 / 1 已通过 / 2 已驳回 |
| validate_status | int | 校验状态：0 待校验 / 1 已通过 / 2 已失败 |
| validate_message | string\|null | 校验结果说明 |
| import_status | int | 入库状态：0 待入库 / 1 成功 / 2 失败 |
| vector_status | int | 向量化状态：0 待向量化 / 1 向量化中 / 2 成功 / 3 失败 / 4 跳过 |
| vector_model | string\|null | 向量模型 |
| vector_dim | int\|null | 向量维度 |
| vector_version | string\|null | 向量版本 |
| vector_error_message | string\|null | 向量化失败原因 |
| vectorized_at | datetime\|null | 向量化完成时间 |
| created_at | datetime | 创建时间 |
| updated_at | datetime | 更新时间 |
| core_performance_indicators | object\|null | 核心业绩指标表（eps、roe、毛利率等） |
| balance_sheet | object\|null | 资产负债表（总资产、总负债、权益等） |
| cash_flow_sheet | object\|null | 现金流量表（经营/投资/融资现金流等） |
| income_sheet | object\|null | 利润表（净利润、营收、各项费用等） |

---

### 1.9 获取结构化 JSON 内容

- **GET** `/api/v1/data/{report_id}/json`
- **描述**：获取财报的结构化 JSON 文件内容

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| report_id | int | path | 是 | 财报记录 ID |

**响应格式**：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "file_name": "xxx_structured.json",
    "file_size": 12345,
    "content": { ... }
  }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| file_name | string | JSON 文件名 |
| file_size | int | 文件大小（字节） |
| content | any | JSON 解析后的结构化内容 |

---

### 1.10 删除财报记录

- **DELETE** `/api/v1/data/{report_id}`
- **描述**：删除财报记录及其关联数据（事实表数据、磁盘文件）

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| report_id | int | path | 是 | 财报记录 ID |

**响应格式**：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "id": 1
  }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| id | int | 已删除的财报 ID |

---

### 1.11 导入公司基本信息

- **POST** `/api/v1/data/import-companies`
- **描述**：上传附件1 Excel 文件，导入公司基础信息
- **Content-Type**：`multipart/form-data`

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| file | File | body | 是 | Excel 文件（.xlsx 或 .xls） |

**响应格式**：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "total": 5000,
    "inserted": 100,
    "updated": 4900
  }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| total | int | 处理的总记录数 |
| inserted | int | 新增记录数 |
| updated | int | 更新记录数 |

---

## 二、智能问数（/api/v1/chat）

负责对话消息处理、会话管理、导出及图表访问。

### 2.1 发送对话消息

- **POST** `/api/v1/chat`
- **描述**：发送对话消息，返回 AI 回答（含 SQL、图表等）
- **Content-Type**：`application/json`

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| session_id | string | body | 否 | 会话 ID（新会话不传） |
| question | string | body | 是 | 用户提问内容，长度 1~500 字符 |

**请求体示例**：

```json
{
  "session_id": "uuid-string",
  "question": "贵州茅台2023年净利润是多少？"
}
```

**响应格式**：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "session_id": "550e8400-e29b-41d4-a716-446655440000",
    "answer": {
      "content": "贵州茅台2023年净利润为**747.34亿元**。\n\n| 指标 | 数值 |\n|------|------|\n| 净利润 | 747.34亿元 |",
      "image": ["chart_20250101_120000_abc123.jpg"]
    },
    "need_clarification": false,
    "sql": "SELECT net_profit FROM income_sheet WHERE stock_code='600519' AND report_year=2023",
    "chart_type": "bar"
  }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| session_id | string | 会话 ID（UUID） |
| answer | object | 回答内容 |
| answer.content | string | 回答文本（Markdown 格式） |
| answer.image | list[string] | 图表图片文件名列表 |
| need_clarification | bool | 是否需要用户澄清 |
| sql | string\|null | 生成的 SQL 语句 |
| chart_type | string\|null | 图表类型：line/bar/pie/horizontal_bar/grouped_bar/radar/histogram/scatter/box |

---

### 2.2 获取会话列表

- **GET** `/api/v1/chat/sessions`
- **描述**：分页获取会话列表

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| page | int | query | 否 | 页码，从 1 开始，默认 `1` |
| page_size | int | query | 否 | 每页数量，最大 100，默认 `10` |

**响应格式**：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "lists": [
      {
        "id": "550e8400-e29b-41d4-a716-446655440000",
        "name": "贵州茅台财务分析",
        "status": 0,
        "created_at": "2025-01-01T10:00:00",
        "updated_at": "2025-01-01T10:05:00"
      }
    ],
    "pagination": {
      "page": 1,
      "page_size": 10,
      "total": 5,
      "total_pages": 1
    }
  }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| lists | list | 会话列表 |
| lists[].id | string | 会话 UUID |
| lists[].name | string\|null | 会话名称 |
| lists[].status | int | 状态：0 活跃 / 1 已关闭 |
| lists[].created_at | datetime | 创建时间 |
| lists[].updated_at | datetime | 更新时间 |
| pagination | object | 分页信息 |

---

### 2.3 获取会话历史

- **GET** `/api/v1/chat/history/{session_id}`
- **描述**：获取指定会话的消息历史记录

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| session_id | string | path | 是 | 会话 ID |

**响应格式**：

```json
{
  "code": 0,
  "message": "success",
  "data": [
    {
      "id": 1,
      "role": "user",
      "content": "贵州茅台2023年净利润是多少？",
      "sql": null,
      "image": [],
      "created_at": "2025-01-01T10:00:00"
    },
    {
      "id": 2,
      "role": "assistant",
      "content": "贵州茅台2023年净利润为**747.34亿元**。",
      "sql": "SELECT net_profit FROM income_sheet WHERE ...",
      "image": ["chart_xxx.jpg"],
      "created_at": "2025-01-01T10:00:05"
    }
  ]
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| [].id | int | 消息 ID |
| [].role | string | 角色：user / assistant |
| [].content | string | 消息内容 |
| [].sql | string\|null | 生成的 SQL |
| [].image | list[string] | 图表图片文件名列表 |
| [].created_at | datetime | 创建时间 |

---

### 2.4 导出结果

- **POST** `/api/v1/chat/export`
- **描述**：导出对话结果为 Excel（result_2.xlsx）
- **Content-Type**：`application/json`

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| questions | list[dict] | body | 是 | 问题列表 |

**响应格式**：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "file_path": "backend/result/result_2.xlsx"
  }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| file_path | string | 导出文件路径 |

---

### 2.5 关闭会话

- **PUT** `/api/v1/chat/sessions/{session_id}/close`
- **描述**：关闭指定会话

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| session_id | string | path | 是 | 会话 ID |

**响应格式**：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "status": 1
  }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| id | string | 会话 ID |
| status | int | 状态：1 已关闭 |

---

### 2.6 删除会话

- **DELETE** `/api/v1/chat/sessions/{session_id}`
- **描述**：删除会话及其所有消息

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| session_id | string | path | 是 | 会话 ID |

**响应格式**：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "deleted": true
  }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| id | string | 已删除的会话 ID |
| deleted | bool | 是否删除成功 |

---

### 2.7 重命名会话

- **PUT** `/api/v1/chat/sessions/{session_id}/rename`
- **描述**：重命名指定会话
- **Content-Type**：`application/json`

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| session_id | string | path | 是 | 会话 ID |
| name | string | body | 是 | 新名称，长度 1~100 字符 |

**请求体示例**：

```json
{
  "name": "贵州茅台财务分析"
}
```

**响应格式**：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "name": "贵州茅台财务分析"
  }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| id | string | 会话 ID |
| name | string | 新名称 |

---

### 2.8 获取图表图片

- **GET** `/api/v1/chat/images/{filename}`
- **描述**：获取图表图片文件，返回图片二进制流（`image/jpeg`）

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| filename | string | path | 是 | 图片文件名 |

**响应格式**：图片二进制流（`Content-Type: image/jpeg`），失败时返回 JSON 错误响应。

---

## 三、知识库管理（/api/v1/knowledge-base）

负责文档注册、切块、向量化、检索及系统初始化。

### 3.1 系统初始化

- **POST** `/api/v1/knowledge-base/init`
- **描述**：加载 Excel 元数据到 knowledge_document 表（系统初始化）
- **Content-Type**：`multipart/form-data`

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| stock_excel | File | body | 是 | 个股研报 Excel 文件 |
| industry_excel | File | body | 是 | 行业研报 Excel 文件 |
| force_reload | bool | query | 否 | 是否强制重新加载，默认 `false` |

**响应格式**：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "success": true,
    "message": "系统初始化成功",
    "stock_metadata_count": 5000,
    "industry_metadata_count": 200,
    "total_count": 5200,
    "duplicates": 0,
    "errors": []
  }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| success | bool | 是否成功 |
| message | string | 结果消息 |
| stock_metadata_count | int | 个股研报元数据数量 |
| industry_metadata_count | int | 行业研报元数据数量 |
| total_count | int | 总数量 |
| duplicates | int | 重复数量 |
| errors | list[dict] | 错误列表 |

---

### 3.2 查询系统初始化状态

- **GET** `/api/v1/knowledge-base/init-status`
- **描述**：查询系统是否已完成初始化

**响应格式**：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "initialized": true,
    "stock_metadata_count": 5000,
    "industry_metadata_count": 200,
    "total_metadata_count": 5200
  }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| initialized | bool | 是否已初始化 |
| stock_metadata_count | int | 个股研报元数据数量 |
| industry_metadata_count | int | 行业研报元数据数量 |
| total_metadata_count | int | 总元数据数量 |

---

### 3.3 知识库统计

- **GET** `/api/v1/knowledge-base/stats`
- **描述**：获取知识库整体统计信息（文档数、切块数、向量化进度等）

**响应格式**：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "documents": {
      "total": 5200,
      "by_chunk_status": { "0": 100, "2": 5000, "3": 100 },
      "by_vector_status": { "0": 200, "2": 4800, "3": 200 },
      "by_doc_type": { "RESEARCH_REPORT": 5000, "INDUSTRY_REPORT": 200 }
    },
    "chunks": {
      "total": 52000,
      "by_vector_status": { "0": 2000, "2": 48000, "3": 2000 }
    }
  }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| documents | object | 文档统计 |
| documents.total | int | 文档总数 |
| documents.by_chunk_status | dict | 按切块状态分组统计 |
| documents.by_vector_status | dict | 按向量状态分组统计 |
| documents.by_doc_type | dict | 按文档类型分组统计 |
| chunks | object | 切块统计 |
| chunks.total | int | 切块总数 |
| chunks.by_vector_status | dict | 按向量状态分组统计 |

---

### 3.4 文档列表

- **GET** `/api/v1/knowledge-base/documents`
- **描述**：分页查询知识库文档列表

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| doc_type | string | query | 否 | 按文档类型筛选 |
| stock_code | string | query | 否 | 按股票代码筛选 |
| metadata_status | int | query | 否 | 按元数据状态筛选 |
| chunk_status | int | query | 否 | 按切块状态筛选 |
| vector_status | int | query | 否 | 按向量状态筛选 |
| page | int | query | 否 | 页码，从 1 开始，默认 `1` |
| page_size | int | query | 否 | 每页数量，最大 100，默认 `20` |

**响应格式**：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "lists": [
      {
        "id": 1,
        "doc_type": "RESEARCH_REPORT",
        "title": "贵州茅台2024年研报",
        "source_path": "uploads/xxx.pdf",
        "stock_code": "600519",
        "stock_abbr": "贵州茅台",
        "publish_date": "2024-06-01",
        "org_name": "中信证券",
        "industry_name": "白酒",
        "researcher": "张三",
        "em_rating_name": "买入",
        "predict_this_year_eps": "60.50",
        "predict_this_year_pe": "25.3",
        "financial_report_id": null,
        "page_count": 30,
        "chunk_count": 15,
        "chunk_status": 2,
        "metadata_status": 2,
        "error_message": null,
        "vector_status": 2,
        "vector_model": "text-embedding-3-small",
        "vector_dim": 1536,
        "vector_version": "v1",
        "created_at": "2025-01-01T00:00:00",
        "updated_at": "2025-01-02T00:00:00"
      }
    ],
    "pagination": {
      "page": 1,
      "page_size": 20,
      "total": 5200,
      "total_pages": 260
    }
  }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| lists | list | 文档列表 |
| lists[].id | int | 文档 ID |
| lists[].doc_type | string | 文档类型：RESEARCH_REPORT / FINANCIAL_REPORT / INDUSTRY_REPORT |
| lists[].title | string | 文档标题 |
| lists[].source_path | string | PDF 源文件路径 |
| lists[].stock_code | string\|null | 股票代码 |
| lists[].stock_abbr | string\|null | 股票简称 |
| lists[].publish_date | date\|null | 发布日期 |
| lists[].org_name | string\|null | 研究机构名称 |
| lists[].industry_name | string\|null | 行业名称 |
| lists[].researcher | string\|null | 研究员 |
| lists[].em_rating_name | string\|null | 评级 |
| lists[].predict_this_year_eps | string\|null | 预测当年 EPS |
| lists[].predict_this_year_pe | string\|null | 预测当年 PE |
| lists[].financial_report_id | int\|null | 关联的财报记录 ID |
| lists[].page_count | int\|null | PDF 总页数 |
| lists[].chunk_count | int | 切块数量 |
| lists[].chunk_status | int | 切块状态：0 待处理 / 1 处理中 / 2 已完成 / 3 失败 |
| lists[].metadata_status | int | 元数据状态：0 未加载 / 1 已加载 / 2 PDF 已上传 |
| lists[].error_message | string\|null | 错误信息 |
| lists[].vector_status | int | 向量化状态：0 待处理 / 1 处理中 / 2 成功 / 3 失败 / 4 跳过 |
| lists[].vector_model | string\|null | 向量模型 |
| lists[].vector_dim | int\|null | 向量维度 |
| lists[].vector_version | string\|null | 向量版本 |
| lists[].created_at | datetime\|null | 创建时间 |
| lists[].updated_at | datetime\|null | 更新时间 |
| pagination | object | 分页信息 |

---

### 3.5 批量查询文档状态

- **POST** `/api/v1/knowledge-base/documents/status/batch`
- **描述**：根据文档 ID 列表批量查询文档状态
- **Content-Type**：`application/json`

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| document_ids | int[] | body | 是 | 文档 ID 列表 |

**响应格式**：

```json
{
  "code": 0,
  "message": "success",
  "data": [
    { "id": 1, "chunk_status": 2, "vector_status": 2, "chunk_count": 15 },
    { "id": 2, "chunk_status": 0, "vector_status": 0, "chunk_count": 0 }
  ]
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| [].id | int | 文档 ID |
| [].chunk_status | int | 切块状态 |
| [].vector_status | int | 向量化状态 |
| [].chunk_count | int | 切块数量 |

---

### 3.6 切块列表

- **GET** `/api/v1/knowledge-base/chunks`
- **描述**：分页查询知识库切块列表

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| document_id | int | query | 否 | 按文档 ID 筛选 |
| vector_status | int | query | 否 | 按向量状态筛选 |
| page | int | query | 否 | 页码，从 1 开始，默认 `1` |
| page_size | int | query | 否 | 每页数量，最大 100，默认 `20` |

**响应格式**：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "lists": [
      {
        "id": 1,
        "document_id": 1,
        "page_no": 1,
        "chunk_index": 0,
        "chunk_text": "贵州茅台酒股份有限公司成立于...",
        "chunk_hash": "abc123def456",
        "char_count": 500,
        "vector_status": 2,
        "milvus_id": 12345,
        "created_at": "2025-01-01T00:00:00",
        "updated_at": "2025-01-01T00:00:00"
      }
    ],
    "pagination": {
      "page": 1,
      "page_size": 20,
      "total": 52000,
      "total_pages": 2600
    }
  }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| lists | list | 切块列表 |
| lists[].id | int | 切块 ID |
| lists[].document_id | int | 所属文档 ID |
| lists[].page_no | int\|null | 页码 |
| lists[].chunk_index | int | 切块序号 |
| lists[].chunk_text | string | 切块文本内容 |
| lists[].chunk_hash | string | 文本哈希 |
| lists[].char_count | int | 字符数 |
| lists[].vector_status | int | 向量化状态：0 待处理 / 1 处理中 / 2 已完成 / 3 失败 |
| lists[].milvus_id | int\|null | Milvus 中的 ID |
| lists[].created_at | datetime\|null | 创建时间 |
| lists[].updated_at | datetime\|null | 更新时间 |
| pagination | object | 分页信息 |

---

### 3.7 单文档切块

- **POST** `/api/v1/knowledge-base/chunk/{document_id}`
- **描述**：提交单个文档的切块任务（异步后台执行）

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| document_id | int | path | 是 | 文档 ID |
| force | bool | query | 否 | 强制重新切块，默认 `false` |

**响应格式**：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "document_id": 1,
    "status": "submitted",
    "message": "切块任务已提交"
  }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| document_id | int | 文档 ID |
| status | string | 提交状态 |
| message | string | 结果消息 |

---

### 3.8 批量切块

- **POST** `/api/v1/knowledge-base/chunk/batch`
- **描述**：提交批量切块任务（异步后台执行）
- **Content-Type**：`application/json`

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| document_ids | int[] | body | 是 | 文档 ID 列表 |

**响应格式**：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "submitted": 10,
    "skipped": 2,
    "submitted_ids": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    "message": "批量切块任务已提交"
  }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| submitted | int | 已提交数量 |
| skipped | int | 跳过数量 |
| submitted_ids | list[int] | 已提交的文档 ID 列表 |
| message | string | 结果消息 |

---

### 3.9 全部待处理切块

- **POST** `/api/v1/knowledge-base/chunk/all`
- **描述**：提交所有待处理文档的切块任务（异步后台执行）
- **Content-Type**：`application/json`

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| limit | int | body | 否 | 最大处理数量，默认 `100` |
| doc_type | string | body | 否 | 文档类型筛选 |

**响应格式**：同 [3.8 批量切块](#38-批量切块)

---

### 3.10 单文档向量化

- **POST** `/api/v1/knowledge-base/vectorize/{document_id}`
- **描述**：向量化单个文档的所有切块（异步后台执行）

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| document_id | int | path | 是 | 文档 ID |
| batch_size | int | query | 否 | 每批处理数量，默认 `20`，范围 1-200 |
| force | bool | query | 否 | 是否强制重试失败/已完成切块，默认 `false` |

**响应格式**：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "document_id": 1,
    "status": "submitted",
    "message": "向量化任务已提交",
    "total_chunks": 15
  }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| document_id | int | 文档 ID |
| status | string | 提交状态 |
| message | string | 结果消息 |
| total_chunks | int | 待向量化的切块总数 |

---

### 3.11 批量向量化

- **POST** `/api/v1/knowledge-base/vectorize`
- **描述**：批量向量化待处理的文档切块（异步后台执行）
- **Content-Type**：`application/json`

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| batch_size | int | body | 否 | 每批处理数量，默认 `20`，范围 1-200 |
| force | bool | body | 否 | 是否强制重试，默认 `false` |

**响应格式**：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "submitted": 50,
    "message": "批量向量化任务已提交"
  }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| submitted | int | 已提交的文档数量 |
| message | string | 结果消息 |

---

### 3.12 重置向量状态

- **POST** `/api/v1/knowledge-base/reset-vector-status/{document_id}`
- **描述**：重置文档的向量状态（用于取消处理中任务或重新向量化）

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| document_id | int | path | 是 | 文档 ID |
| target_status | int | query | 否 | 目标状态，默认 `0`（PENDING） |

**响应格式**：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "document_id": 1,
    "old_status": 1,
    "new_status": 0,
    "chunk_reset_count": 15,
    "message": "向量状态已重置"
  }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| document_id | int | 文档 ID |
| old_status | int | 原状态 |
| new_status | int | 新状态 |
| chunk_reset_count | int | 重置的切块数量 |
| message | string | 结果消息 |

---

### 3.13 知识库语义检索

- **POST** `/api/v1/knowledge-base/search`
- **描述**：知识库语义检索（调试用）
- **Content-Type**：`application/json`

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| query | string | body | 是 | 查询文本 |
| stock_code | string | body | 否 | 股票代码筛选 |
| doc_type | string | body | 否 | 文档类型筛选 |
| top_k | int | body | 否 | 返回结果数量，默认 `5`，范围 1-100 |

**响应格式**：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "results": [
      {
        "chunk_id": 1,
        "document_id": 1,
        "text": "贵州茅台2023年实现营业总收入1505.60亿元...",
        "score": 0.95,
        "page_no": 5,
        "doc_title": "贵州茅台2024年研报",
        "stock_code": "600519",
        "doc_type": "RESEARCH_REPORT"
      }
    ]
  }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| results | list | 检索结果列表 |
| results[].chunk_id | int | 切块 ID |
| results[].document_id | int | 文档 ID |
| results[].text | string | 匹配文本 |
| results[].score | float | 相似度分数 |
| results[].page_no | int\|null | 页码 |
| results[].doc_title | string | 文档标题 |
| results[].stock_code | string\|null | 股票代码 |
| results[].doc_type | string | 文档类型 |

---

### 3.14 增量上传 PDF

- **POST** `/api/v1/knowledge-base/upload-pdf`
- **描述**：增量上传 PDF 文件，立即处理（匹配元数据 + 切块）
- **Content-Type**：`multipart/form-data`

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| pdfs | File[] | body | 是 | PDF 文件列表 |
| doc_type | string | query | 否 | 文档类型：`RESEARCH_REPORT` 或 `INDUSTRY_REPORT`，默认 `RESEARCH_REPORT` |

**响应格式**：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "success": true,
    "message": "处理完成",
    "processed_count": 10,
    "failed_count": 2,
    "total_processed": 10,
    "total_pending": 100,
    "failed_documents": [
      { "pdf_name": "bad.pdf", "reason": "无法匹配元数据", "suggestion": "请检查文件名" }
    ],
    "errors": []
  }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| success | bool | 是否成功 |
| message | string | 结果消息 |
| processed_count | int | 本次处理成功数量 |
| failed_count | int | 本次处理失败数量 |
| total_processed | int | 累计已处理数量 |
| total_pending | int | 剩余待处理数量 |
| failed_documents | list | 失败文档列表（含 pdf_name、reason、suggestion） |
| errors | list[dict] | 错误列表 |

---

### 3.15 单文档上传 PDF

- **POST** `/api/v1/knowledge-base/upload-single-pdf`
- **描述**：上传单个文档的 PDF（用于文档列表中的"上传 PDF"按钮）
- **Content-Type**：`multipart/form-data`

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| document_id | int | query | 是 | 文档 ID |
| pdf_file | File | body | 是 | PDF 文件 |

**响应格式**：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "success": true,
    "message": "PDF上传成功",
    "document_id": 1,
    "chunk_count": 15
  }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| success | bool | 是否成功 |
| message | string | 结果消息 |
| document_id | int | 文档 ID |
| chunk_count | int | 生成的切块数量 |

---

### 3.16 重试失败文档

- **POST** `/api/v1/knowledge-base/retry-failed`
- **描述**：重新上传 PDF 并重试失败的文档处理
- **Content-Type**：`multipart/form-data`

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| document_id | int | query | 是 | 文档 ID |
| pdf_file | File | body | 是 | PDF 文件 |

**响应格式**：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "success": true,
    "message": "重试成功",
    "document_id": 1,
    "chunk_count": 15
  }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| success | bool | 是否成功 |
| message | string | 结果消息 |
| document_id | int | 文档 ID |
| chunk_count | int | 生成的切块数量 |

---

### 3.17 查询处理进度

- **GET** `/api/v1/knowledge-base/progress`
- **描述**：查询增量处理的当前进度

**响应格式**：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "total_documents": 5200,
    "metadata_loaded": 5200,
    "pdf_uploaded": 5000,
    "chunked": 4800,
    "vectorized": 4500,
    "pending_pdf_upload": 200,
    "pending_chunk": 200,
    "pending_vectorize": 300,
    "failed_chunk": 50,
    "failed_vectorize": 30,
    "progress_percentage": 86.5,
    "recent_processed": [
      { "id": 1, "title": "贵州茅台研报", "doc_type": "RESEARCH_REPORT", "status": "向量化完成", "updated_at": "2025-01-01T10:00:00" }
    ]
  }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| total_documents | int | 文档总数 |
| metadata_loaded | int | 元数据已加载数 |
| pdf_uploaded | int | PDF 已上传数 |
| chunked | int | 已切块数 |
| vectorized | int | 已向量化数 |
| pending_pdf_upload | int | 待上传 PDF 数 |
| pending_chunk | int | 待切块数 |
| pending_vectorize | int | 待向量化数 |
| failed_chunk | int | 切块失败数 |
| failed_vectorize | int | 向量化失败数 |
| progress_percentage | float | 整体进度百分比 |
| recent_processed | list | 最近处理的文档列表 |
| recent_processed[].id | int | 文档 ID |
| recent_processed[].title | string | 文档标题 |
| recent_processed[].doc_type | string | 文档类型 |
| recent_processed[].status | string | 处理状态描述 |
| recent_processed[].updated_at | datetime\|null | 更新时间 |

---

## 四、任务二工作台（/api/v1/task2）

基于智能问数能力，对附件4中的财务问题批量执行问答。

### 4.1 获取工作台概览

- **GET** `/api/v1/task2/workspace`
- **描述**：获取当前工作台概览信息（导入状态、题目统计等）

**响应格式**：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "id": 1,
    "source_file_name": "附件4.xlsx",
    "source_file_path": "uploads/附件4.xlsx",
    "import_status": 2,
    "total_questions": 50,
    "answered_count": 30,
    "failed_count": 5,
    "pending_count": 15,
    "last_export_path": "backend/result/result_2.xlsx",
    "last_exported_at": "2025-01-01T12:00:00",
    "created_at": "2025-01-01T10:00:00",
    "updated_at": "2025-01-01T12:00:00"
  }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| id | int | 工作台 ID |
| source_file_name | string\|null | 附件4 源文件名 |
| source_file_path | string\|null | 附件4 源文件路径 |
| import_status | int | 导入状态：0 未导入 / 1 导入中 / 2 已导入 / 3 导入失败 |
| total_questions | int | 题目总数 |
| answered_count | int | 已回答数量 |
| failed_count | int | 失败数量 |
| pending_count | int | 待处理数量 |
| last_export_path | string\|null | 最近导出文件路径 |
| last_exported_at | datetime\|null | 最近导出时间 |
| created_at | datetime | 创建时间 |
| updated_at | datetime | 更新时间 |

---

### 4.2 导入附件4

- **POST** `/api/v1/task2/workspace/import`
- **描述**：上传并解析附件4，初始化工作台
- **Content-Type**：`multipart/form-data`

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| file | File | body | 是 | 附件4 文件 |

**响应格式**：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "workspace_id": 1,
    "source_file_name": "附件4.xlsx",
    "total_questions": 50,
    "message": "导入成功"
  }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| workspace_id | int | 工作台 ID |
| source_file_name | string | 源文件名 |
| total_questions | int | 解析出的题目总数 |
| message | string | 导入结果消息 |

---

### 4.3 获取题目列表

- **GET** `/api/v1/task2/questions`
- **描述**：获取任务二题目列表

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| status | int | query | 否 | 状态筛选：0 待处理 / 1 回答中 / 2 已完成 / 3 失败 |

**响应格式**：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "items": [
      {
        "id": 1,
        "workspace_id": 1,
        "question_code": "Q001",
        "question_type": "single_value",
        "question_raw_json": "{\"rounds\": [...]}",
        "rounds_json": [{"round": 1, "question": "贵州茅台2023年净利润是多少？"}],
        "status": 2,
        "session_id": "uuid-xxx",
        "answer_json": [{"round": 1, "answer": "747.34亿元"}],
        "sql_text": "SELECT net_profit FROM income_sheet WHERE ...",
        "chart_type": "bar",
        "image_paths_json": ["chart_xxx.jpg"],
        "last_error": null,
        "answered_at": "2025-01-01T11:00:00",
        "created_at": "2025-01-01T10:00:00",
        "updated_at": "2025-01-01T11:00:00"
      }
    ],
    "total": 50,
    "pending_count": 15,
    "answered_count": 30,
    "failed_count": 5
  }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| items | list | 题目列表 |
| items[].id | int | 题目 ID |
| items[].workspace_id | int | 关联工作台 ID |
| items[].question_code | string | 题目编号 |
| items[].question_type | string\|null | 问题类型 |
| items[].question_raw_json | string\|null | 原始问题 JSON 字符串 |
| items[].rounds_json | list\|null | 解析后的多轮问题数组 |
| items[].status | int | 状态：0 待处理 / 1 回答中 / 2 已完成 / 3 失败 |
| items[].session_id | string\|null | 关联的会话 ID |
| items[].answer_json | list\|null | 回答 JSON 数组 |
| items[].sql_text | string\|null | 生成的 SQL 语句 |
| items[].chart_type | string\|null | 图表类型 |
| items[].image_paths_json | list\|null | 图表文件路径列表 |
| items[].last_error | string\|null | 最后一次错误信息 |
| items[].answered_at | datetime\|null | 回答完成时间 |
| items[].created_at | datetime | 创建时间 |
| items[].updated_at | datetime | 更新时间 |
| total | int | 总数 |
| pending_count | int | 待处理数量 |
| answered_count | int | 已回答数量 |
| failed_count | int | 失败数量 |

---

### 4.4 获取单题详情

- **GET** `/api/v1/task2/questions/{question_id}`
- **描述**：获取指定题目的详细信息

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| question_id | int | path | 是 | 题目 ID |

**响应格式**：同 [4.3 题目列表](#43-获取题目列表) 中的 `items[]` 单项结构。

---

### 4.5 回答单题

- **POST** `/api/v1/task2/questions/{question_id}/answer`
- **描述**：执行单题回答

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| question_id | int | path | 是 | 题目 ID |

**响应格式**：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "question_id": 1,
    "question_code": "Q001",
    "status": 2,
    "answer_json": [{"round": 1, "answer": "747.34亿元"}],
    "sql_text": "SELECT net_profit FROM income_sheet WHERE ...",
    "chart_type": "bar",
    "image_paths": ["chart_xxx.jpg"]
  }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| question_id | int | 题目 ID |
| question_code | string | 题目编号 |
| status | int | 状态：0 待处理 / 1 回答中 / 2 已完成 / 3 失败 |
| answer_json | list\|null | 回答 JSON 数组 |
| sql_text | string\|null | 生成的 SQL 语句 |
| chart_type | string\|null | 图表类型 |
| image_paths | list\|null | 图表文件路径列表 |

---

### 4.6 删除回答

- **DELETE** `/api/v1/task2/questions/{question_id}/answer`
- **描述**：删除指定题目的当前回答

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| question_id | int | path | 是 | 题目 ID |

**响应格式**：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "question_id": 1,
    "question_code": "Q001",
    "status": 0,
    "message": "回答已删除"
  }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| question_id | int | 题目 ID |
| question_code | string | 题目编号 |
| status | int | 删除后状态（重置为 0 待处理） |
| message | string | 结果消息 |

---

### 4.7 重新回答

- **POST** `/api/v1/task2/questions/{question_id}/rerun`
- **描述**：删除旧结果后重新执行回答

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| question_id | int | path | 是 | 题目 ID |

**响应格式**：同 [4.5 回答单题](#45-回答单题)

---

### 4.8 批量回答

- **POST** `/api/v1/task2/questions/batch-answer`
- **描述**：批量回答题目

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| scope | string | query | 否 | 处理范围：`all` 全部 / `unfinished` 未完成 / `failed` 失败，默认 `unfinished` |

**响应格式**：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "total": 20,
    "processed": 20,
    "success": 18,
    "failed": 2,
    "message": "批量回答完成",
    "results": [
      {
        "question_code": "Q001",
        "status": "success",
        "result": { "question_id": 1, "question_code": "Q001", "status": 2, ... },
        "error": null
      },
      {
        "question_code": "Q002",
        "status": "failed",
        "result": null,
        "error": "SQL执行失败"
      }
    ]
  }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| total | int | 待处理总数 |
| processed | int | 已处理数量 |
| success | int | 成功数量 |
| failed | int | 失败数量 |
| message | string\|null | 结果消息 |
| results | list\|null | 各题处理结果 |
| results[].question_code | string | 题目编号 |
| results[].status | string | 处理状态：success / failed |
| results[].result | object\|null | 成功时的回答结果 |
| results[].error | string\|null | 失败时的错误信息 |

---

### 4.9 导出结果

- **POST** `/api/v1/task2/export`
- **描述**：导出任务二结果为 Excel（result_2.xlsx）

**响应格式**：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "xlsx_path": "backend/result/result_2.xlsx",
    "json_path": "backend/result/result_2.json",
    "total_questions": 50,
    "answered_count": 45,
    "failed_count": 5,
    "exported_at": "2025-01-01T12:00:00"
  }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| xlsx_path | string | 导出 Excel 文件路径 |
| json_path | string | 导出 JSON 文件路径 |
| total_questions | int | 题目总数 |
| answered_count | int | 已回答数量 |
| failed_count | int | 失败数量 |
| exported_at | string | 导出时间 |

---

### 4.10 获取最近导出信息

- **GET** `/api/v1/task2/export/latest`
- **描述**：获取最近一次导出结果的信息

**响应格式**：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "xlsx_path": "backend/result/result_2.xlsx",
    "exported_at": "2025-01-01T12:00:00"
  }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| xlsx_path | string | 最近导出文件路径 |
| exported_at | string\|null | 最近导出时间 |

---

## 五、任务三-增强智能问数（/api/v1/task3）

支持独立模式（直接提问/规划/执行/校验）和工作台模式（导入附件6/批量处理/导出）。

### 5.1 独立模式

#### 5.1.1 处理问题

- **POST** `/api/v1/task3/question`
- **描述**：处理任务三问题（完整流程：规划 → 执行 → 生成答案）
- **Content-Type**：`application/json`

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| question | string | body | 是 | 问题内容，长度 1~2000 字符 |
| context | dict | body | 否 | 附加上下文 |

**响应格式**：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "question_id": "Q001",
    "answer": {
      "content": "根据财报数据和研报分析，贵州茅台2023年...",
      "references": [
        {
          "paper_path": "uploads/xxx.pdf",
          "text": "贵州茅台2023年实现营业总收入1505.60亿元...",
          "page_no": 5,
          "paper_image": "chart_xxx.jpg"
        }
      ]
    },
    "sql": "SELECT net_profit FROM income_sheet WHERE ...",
    "execution_trace": {
      "plan": {
        "question": "贵州茅台2023年净利润及增长原因？",
        "steps": [
          {
            "step_id": "s1",
            "step_type": "sql_query",
            "goal": "查询贵州茅台2023年净利润",
            "depends_on": [],
            "params": {},
            "priority": 0
          }
        ],
        "context": {},
        "created_at": "2025-01-01T10:00:00"
      },
      "results": [
        {
          "step_id": "s1",
          "step_type": "sql_query",
          "status": "completed",
          "output": { "net_profit": 7473400 },
          "error_message": null,
          "execution_time_ms": 150
        }
      ],
      "final_answer": "贵州茅台2023年净利润为747.34亿元...",
      "references": [],
      "started_at": "2025-01-01T10:00:00",
      "finished_at": "2025-01-01T10:00:05"
    }
  }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| question_id | string\|null | 问题编号 |
| answer | object | 回答内容 |
| answer.content | string | 回答文本 |
| answer.references | list | 引用来源列表 |
| answer.references[].paper_path | string\|null | 文档路径 |
| answer.references[].text | string | 支撑结论的摘要证据 |
| answer.references[].page_no | int\|null | 页码 |
| answer.references[].paper_image | string\|null | 图表或页图路径 |
| sql | string\|null | 生成的 SQL 语句 |
| execution_trace | object\|null | 执行轨迹 |
| execution_trace.plan | object | 执行计划（含 steps 步骤列表） |
| execution_trace.results | list | 各步骤执行结果 |
| execution_trace.final_answer | string\|null | 最终答案 |
| execution_trace.references | list | 引用来源列表 |
| execution_trace.started_at | datetime\|null | 开始时间 |
| execution_trace.finished_at | datetime\|null | 结束时间 |

---

#### 5.1.2 生成执行计划

- **POST** `/api/v1/task3/plan`
- **描述**：生成执行计划（不执行）
- **Content-Type**：`application/json`

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| question | string | body | 是 | 问题内容 |
| context | dict | body | 否 | 附加上下文信息 |

**请求体示例**：

```json
{
  "question": "贵州茅台2023年净利润及增长原因？",
  "context": {}
}
```

**响应格式**：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "plan": {
      "question": "贵州茅台2023年净利润及增长原因？",
      "steps": [
        {
          "step_id": "s1",
          "step_type": "sql_query",
          "goal": "查询贵州茅台2023年净利润",
          "depends_on": [],
          "params": {},
          "priority": 0
        },
        {
          "step_id": "s2",
          "step_type": "derive_metric",
          "goal": "计算净利润同比增长率",
          "depends_on": ["s1"],
          "params": {},
          "priority": 1
        }
      ],
      "context": {},
      "created_at": "2025-01-01T10:00:00"
    },
    "reasoning": "该问题需要先查询净利润数据，再计算同比增长率，因此拆分为两个步骤。"
  }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| plan | object | 执行计划 |
| plan.question | string | 原始用户问题 |
| plan.steps | list | 子任务步骤列表 |
| plan.steps[].step_id | string | 步骤唯一标识 |
| plan.steps[].step_type | string | 步骤类型：sql_query / derive_metric / retrieve_evidence / aggregate / verify / compose_answer |
| plan.steps[].goal | string | 步骤目标描述 |
| plan.steps[].depends_on | list[string] | 依赖的步骤 ID 列表 |
| plan.steps[].params | dict | 步骤参数 |
| plan.steps[].priority | int | 执行优先级 |
| plan.context | dict | 规划上下文 |
| plan.created_at | datetime\|null | 计划创建时间 |
| reasoning | string\|null | 规划推理过程 |

---

#### 5.1.3 生成计划并执行

- **POST** `/api/v1/task3/execute`
- **描述**：生成执行计划并执行（不生成最终答案）
- **Content-Type**：`application/json`

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| question | string | body | 是 | 问题内容，长度 1~2000 字符 |
| context | dict | body | 否 | 附加上下文信息 |

**请求体示例**：

```json
{
  "question": "贵州茅台2023年净利润及增长原因？",
  "context": {}
}
```

**响应格式**：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "plan": {
      "question": "贵州茅台2023年净利润及增长原因？",
      "steps": [
        {
          "step_id": "s1",
          "step_type": "sql_query",
          "goal": "查询贵州茅台2023年净利润",
          "depends_on": [],
          "params": {},
          "priority": 0
        }
      ],
      "context": {},
      "created_at": "2025-01-01T10:00:00"
    },
    "trace": {
      "plan": { ... },
      "results": [
        {
          "step_id": "s1",
          "step_type": "sql_query",
          "status": "completed",
          "output": { "net_profit": 7473400 },
          "error_message": null,
          "execution_time_ms": 150
        }
      ],
      "final_answer": null,
      "references": [],
      "started_at": "2025-01-01T10:00:00",
      "finished_at": "2025-01-01T10:00:05"
    }
  }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| plan | object | 执行计划（同 5.1.2） |
| trace | object | 执行轨迹 |
| trace.plan | object | 执行计划 |
| trace.results | list | 各步骤执行结果 |
| trace.results[].step_id | string | 步骤 ID |
| trace.results[].step_type | string | 步骤类型 |
| trace.results[].status | string | 执行状态：pending / running / completed / failed / skipped |
| trace.results[].output | dict | 执行输出结果 |
| trace.results[].error_message | string\|null | 错误信息 |
| trace.results[].execution_time_ms | int\|null | 执行耗时（毫秒） |
| trace.final_answer | string\|null | 最终答案 |
| trace.references | list | 引用来源列表 |
| trace.started_at | datetime\|null | 开始时间 |
| trace.finished_at | datetime\|null | 结束时间 |

---

#### 5.1.4 校验结果

- **POST** `/api/v1/task3/verify`
- **描述**：验证问题处理结果（规划 → 执行 → 校验）
- **Content-Type**：`application/json`

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| question | string | body | 是 | 问题内容，长度 1~2000 字符 |
| context | dict | body | 否 | 附加上下文信息 |

**请求体示例**：

```json
{
  "question": "贵州茅台2023年净利润及增长原因？",
  "context": {}
}
```

**响应格式**：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "answer": "贵州茅台2023年净利润为747.34亿元，同比增长19.16%。",
    "verification": {
      "passed": true,
      "errors": [],
      "warnings": ["净利润数据与年报披露一致，但增长率计算口径需确认"],
      "details": {
        "data_consistency": true,
        "calculation_accuracy": true,
        "source_reliability": true
      }
    },
    "references": [
      {
        "paper_path": "uploads/xxx.pdf",
        "text": "贵州茅台2023年实现净利润747.34亿元...",
        "page_no": 5
      }
    ]
  }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| answer | string\|null | 最终答案 |
| verification | object | 校验结果 |
| verification.passed | bool | 是否通过校验 |
| verification.errors | list[string] | 错误列表 |
| verification.warnings | list[string] | 警告列表 |
| verification.details | dict | 详细信息 |
| references | list[dict] | 引用列表 |

---

#### 5.1.5 导出单个问题

- **POST** `/api/v1/task3/export/single`
- **描述**：导出单个问题结果（独立模式，不依赖工作台）

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| question_id | string | query | 是 | 问题编号 |
| question | string | query | 是 | 问题内容 |

**响应格式**：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "id": "Q001",
    "question": "贵州茅台2023年净利润及增长原因？",
    "sql": "SELECT net_profit FROM income_sheet WHERE ...",
    "answer": {
      "content": "贵州茅台2023年净利润为747.34亿元...",
      "references": [
        { "paper_path": "uploads/xxx.pdf", "text": "...", "page_no": 5 }
      ]
    },
    "success": true,
    "error": null
  }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| id | string | 题目编号 |
| question | string | 问题内容 |
| sql | string\|null | 生成的 SQL |
| answer | object | 导出答案 |
| answer.content | string | 回答内容 |
| answer.references | list[dict] | 引用列表 |
| success | bool | 是否成功 |
| error | string\|null | 失败原因 |

---

### 5.2 工作台模式

#### 5.2.1 获取工作台概览

- **GET** `/api/v1/task3/workspace`
- **描述**：获取当前工作台概览信息（导入状态、题目统计等）

**响应格式**：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "id": 1,
    "source_file_name": "附件6.xlsx",
    "source_file_path": "uploads/附件6.xlsx",
    "import_status": 2,
    "total_questions": 30,
    "answered_count": 20,
    "failed_count": 3,
    "pending_count": 7,
    "last_export_path": "backend/result/result_3.xlsx",
    "last_exported_at": "2025-01-01T12:00:00",
    "created_at": "2025-01-01T10:00:00",
    "updated_at": "2025-01-01T12:00:00"
  }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| id | int | 工作台 ID |
| source_file_name | string\|null | 附件6 源文件名 |
| source_file_path | string\|null | 附件6 源文件路径 |
| import_status | int | 导入状态：0 未导入 / 1 导入中 / 2 已导入 / 3 导入失败 |
| total_questions | int | 题目总数 |
| answered_count | int | 已回答数量 |
| failed_count | int | 失败数量 |
| pending_count | int | 待处理数量 |
| last_export_path | string\|null | 最近导出文件路径 |
| last_exported_at | datetime\|null | 最近导出时间 |
| created_at | datetime | 创建时间 |
| updated_at | datetime | 更新时间 |

---

#### 5.2.2 导入附件6

- **POST** `/api/v1/task3/workspace/import`
- **描述**：上传并解析附件6，初始化工作台
- **Content-Type**：`multipart/form-data`

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| file | File | body | 是 | 附件6 文件（.xlsx） |

**响应格式**：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "workspace_id": 1,
    "source_file_name": "附件6.xlsx",
    "total_questions": 30,
    "message": "导入成功"
  }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| workspace_id | int | 工作台 ID |
| source_file_name | string | 源文件名 |
| total_questions | int | 解析出的题目总数 |
| message | string | 导入结果消息 |

---

#### 5.2.3 获取题目列表

- **GET** `/api/v1/task3/questions`
- **描述**：分页获取任务三题目列表

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| status | int | query | 否 | 状态筛选：0 待处理 / 1 回答中 / 2 已完成 / 3 失败 |
| page | int | query | 否 | 页码，从 1 开始，默认 `1` |
| page_size | int | query | 否 | 每页数量，默认 `10` |

**响应格式**：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "lists": [
      {
        "id": 1,
        "workspace_id": 1,
        "question_code": "Q001",
        "question_type": "complex",
        "question_raw_json": "{\"question\": \"...\"}",
        "status": 2,
        "answer_json": [{"round": 1, "answer": "..."}],
        "sql_text": "SELECT ...",
        "execution_plan": { "question": "...", "steps": [...] },
        "verification": { "passed": true, "errors": [], "warnings": [] },
        "retrieval_summary": { "total_chunks": 5, "top_score": 0.95 },
        "last_error": null,
        "answered_at": "2025-01-01T11:00:00",
        "created_at": "2025-01-01T10:00:00",
        "updated_at": "2025-01-01T11:00:00"
      }
    ],
    "pagination": {
      "page": 1,
      "page_size": 10,
      "total": 30,
      "total_pages": 3
    }
  }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| lists | list | 题目列表 |
| lists[].id | int | 题目 ID |
| lists[].workspace_id | int | 关联工作台 ID |
| lists[].question_code | string | 题目编号 |
| lists[].question_type | string\|null | 问题类型 |
| lists[].question_raw_json | string\|null | 原始问题 JSON 字符串 |
| lists[].status | int | 状态：0 待处理 / 1 回答中 / 2 已完成 / 3 失败 |
| lists[].answer_json | list\|null | 回答 JSON 数组 |
| lists[].sql_text | string\|null | 生成的 SQL 语句 |
| lists[].execution_plan | dict\|null | 执行计划对象 |
| lists[].verification | dict\|null | 校验结果对象 |
| lists[].retrieval_summary | dict\|null | 知识库检索摘要 |
| lists[].last_error | string\|null | 最后一次错误信息 |
| lists[].answered_at | datetime\|null | 回答完成时间 |
| lists[].created_at | datetime | 创建时间 |
| lists[].updated_at | datetime | 更新时间 |
| pagination | object | 分页信息 |

---

#### 5.2.4 获取单题详情

- **GET** `/api/v1/task3/questions/{question_id}`
- **描述**：获取指定题目的详细信息

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| question_id | int | path | 是 | 题目 ID |

**响应格式**：同 [5.2.3 题目列表](#523-获取题目列表) 中的 `lists[]` 单项结构。

---

#### 5.2.5 回答单题

- **POST** `/api/v1/task3/questions/{question_id}/answer`
- **描述**：执行单题回答

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| question_id | int | path | 是 | 题目 ID |

**响应格式**：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "id": 1,
    "status": 2,
    "answered_at": "2025-01-01T11:00:00"
  }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| id | int | 题目 ID |
| status | int | 题目状态：0 待处理 / 1 回答中 / 2 已完成 / 3 失败 |
| answered_at | datetime\|null | 回答完成时间 |

---

#### 5.2.6 删除回答

- **DELETE** `/api/v1/task3/questions/{question_id}/answer`
- **描述**：删除指定题目的当前回答

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| question_id | int | path | 是 | 题目 ID |

**响应格式**：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "id": 1,
    "status": 0,
    "answered_at": null
  }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| id | int | 题目 ID |
| status | int | 删除后状态（重置为 0 待处理） |
| answered_at | datetime\|null | 回答完成时间（清空为 null） |

---

#### 5.2.7 重新回答

- **POST** `/api/v1/task3/questions/{question_id}/rerun`
- **描述**：删除旧结果后重新执行回答

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| question_id | int | path | 是 | 题目 ID |

**响应格式**：同 [5.2.5 回答单题](#525-回答单题)

---

#### 5.2.8 批量回答

- **POST** `/api/v1/task3/questions/batch-answer`
- **描述**：批量回答题目

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| scope | string | query | 否 | 处理范围：`all` 全部 / `unfinished` 未完成 / `failed` 失败，默认 `unfinished` |

**响应格式**：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "success": 18,
    "failed": 2
  }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| success | int | 成功数量 |
| failed | int | 失败数量 |

---

#### 5.2.9 导出结果

- **POST** `/api/v1/task3/export`
- **描述**：导出任务三结果为 Excel（result_3.xlsx，工作台模式）

**响应格式**：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "xlsx_path": "backend/result/result_3.xlsx",
    "success_count": 25,
    "fail_count": 5,
    "total": 30
  }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| xlsx_path | string | 导出文件路径 |
| success_count | int | 成功数量 |
| fail_count | int | 失败数量 |
| total | int | 总题目数 |

---

#### 5.2.10 获取最近导出信息

- **GET** `/api/v1/task3/export/latest`
- **描述**：获取最近一次导出结果的信息

**响应格式**：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "xlsx_path": "backend/result/result_3.xlsx",
    "exported_at": "2025-01-01T12:00:00",
    "total_questions": 30,
    "answered_count": 25
  }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| xlsx_path | string | 导出文件路径 |
| exported_at | string\|null | 导出时间 |
| total_questions | int | 总题目数 |
| answered_count | int | 已回答数量 |