# BackendV2 API 接口文档

## 概述

- **基础 URL**：`http://localhost:7389`
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

| 错误码 | 说明             |
| ------ | ---------------- |
| 0      | 成功             |
| 1001   | 参数错误         |
| 1002   | 数据未找到       |
| 2001   | 未登录           |
| 2002   | Token 已过期     |
| 2003   | 权限不足         |
| 2004   | Token 无效       |
| 3001   | 不支持的文件格式 |
| 3002   | 文件过大         |
| 4001   | AI 服务错误      |
| 5001   | 内部错误         |
| 6001   | 密码错误         |

- **Swagger 文档**：`http://localhost:8000/docs`

---

## 一、公司基本信息导入（/api/v1/company_base_info）

上传 Excel 文件，导入上市公司基础信息（股票代码、公司名称、交易所、行业等），支持新增与按股票代码更新。

### 1.1 导入公司基本信息

- **POST** `/api/v1/company_base_info/upload`
- **描述**：上传 Excel 文件（附件1），导入公司基础信息。已存在的股票代码记录将被更新，不存在的新增。
- **Content-Type**：`multipart/form-data`

| 参数 | 类型 | 位置 | 必填 | 说明                                |
| ---- | ---- | ---- | ---- | ----------------------------------- |
| file | File | body | 是   | Excel 文件（`.xlsx` 或 `.xls`） |

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

| 字段     | 类型 | 说明           |
| -------- | ---- | -------------- |
| total    | int  | 处理的总记录数 |
| inserted | int  | 新增记录数     |
| updated  | int  | 更新记录数     |

---

## 二、结构化财报 PDF（/api/v1/analyze-data）✅ 已完成

上传财报 PDF 文件，自动提取元数据（股票代码、报告年份、报告类型等）并建档入库。

### 2.1 上传财报 PDF 文件 

- **POST** `/api/v1/analyze-data/upload`
- **描述**：上传一个或多个财报 PDF 文件，自动解析文件名和 PDF 内容提取元数据，写入 `financial_report` 表。支持批量上传，每个文件独立处理（一个失败不影响其他）。
- **Content-Type**：`multipart/form-data`

| 参数      | 类型   | 位置 | 必填 | 说明                           |
| --------- | ------ | ---- | ---- | ------------------------------ |
| file_list | File[] | body | 是   | PDF 文件列表，支持同时上传多个 |

**响应格式**：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "total": 3,
    "success_count": 2,
    "failed_count": 1,
    "success_reports": [
      {
        "report_id": 1,
        "stock_code": "603127",
        "stock_abbr": "",
        "report_title": "2023 年 年度报告",
        "file_name": "603127_20230331_5Z5L.pdf"
      }
    ],
    "failed_files": [
      {
        "file_name": "无效文件.txt",
        "error": "文件 xxx 不是PDF文件"
      }
    ]
  }
}
```

**响应字段说明**：

| 字段            | 类型                | 说明                   |
| --------------- | ------------------- | ---------------------- |
| total           | int                 | 上传文件总数           |
| success_count   | int                 | 成功建档数量           |
| failed_count    | int                 | 失败数量               |
| success_reports | SuccessReportItem[] | 成功建档的财报记录列表 |
| failed_files    | FailedFileItem[]    | 失败文件列表           |

**SuccessReportItem**：

| 字段         | 类型 | 说明            |
| ------------ | ---- | --------------- |
| report_id    | str  | 财报记录 ID     |
| stock_code   | str  | 股票代码（6位） |
| stock_abbr   | str  | 股票简称        |
| report_title | str  | 财报标题        |
| file_name    | str  | 原始文件名      |

**FailedFileItem**：

| 字段      | 类型 | 说明     |
| --------- | ---- | -------- |
| file_name | str  | 文件名   |
| error     | str  | 错误描述 |

### 2.3 提交财报解析任务

- **POST** `/api/v1/analyze-data/parse`
- **描述**：提交财报 PDF 解析任务（异步后台执行），支持单个或批量提交。后台解析流程：PDF 全文提取 → LLM 结构化抽取 → 规范化校验 → 写入四张事实表。
- **Content-Type**：`application/json`

**请求体（JSON）**：

| 参数 | 类型 | 必填 | 说明 |
| ---- | ---- | ---- | ---- |
| report_ids | int[] | 是 | 待解析的财报记录 ID 列表 |

**请求示例**：
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
    "total": 3,
    "start_parse_count": 2,
    "skip_report_ids": [
      {"report_id": 3, "reason": "未找到财报记录"}
    ]
  }
}
```

**响应字段说明**：

| 字段 | 类型 | 说明 |
| ---- | ---- | ---- |
| total | int | 请求解析的文件总数 |
| start_parse_count | int | 实际开始解析的文件数量 |
| skip_report_ids | object[] | 跳过的记录列表（含原因） |

**后台流程说明**：

```
提交解析请求
  │
  ├─ 校验 report_ids，跳过无效/无存储路径的记录
  ├─ 标记 parse_status = 3（解析中）
  ├─ 立即返回响应（异步）
  │
  └─ 后台线程池（最多5个并发）
       └─ 对每个 report_id 执行：
            ├─ 1. PDF 全文提取（PyPDFLoader）
            ├─ 2. LLM 结构化抽取（四张表并行）
            ├─ 3. 规范化校验 + 保存 JSON 留痕
            ├─ 4. 写入四张事实表
            └─ 5. parse_status → 1（成功）/ 2（失败）
```

### 2.3 获取财报详情

- **POST** `/api/v1/analyze-data/detail`
- **描述**：获取单条财报记录的详细信息，包含基础信息和四张结构化事实表数据（核心业绩指标、资产负债表、利润表、现金流量表）。已解析的记录会返回对应的结构化字段值，未解析或字段缺失则返回 `null`。
- **Content-Type**：`application/json`

**请求体（JSON）**：

| 参数 | 类型 | 必填 | 说明 |
| ---- | ---- | ---- | ---- |
| report_id | int | 是 | 财报记录 ID |

**请求示例**：
```json
{
  "report_id": 1
}
```

**响应格式**：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "id": 1,
    "file_name": "603127_20230331.pdf",
    "report_title": "2023 年 年度报告",
    "stock_code": "603127",
    "stock_abbr": "昭衍新药",
    "report_year": 2023,
    "report_period": "FY",
    "report_type": "REPORT",
    "report_label": "年度报告",
    "exchange": "SH",
    "report_date": "2024-04-20",
    "parse_status": 1,
    "import_status": 1,
    "created_at": "2025-01-01T12:00:00",
    "updated_at": "2025-01-01T12:00:00",
    "core_performance_indicators": {
      "eps": 1.23,
      "total_operating_revenue": 1000000.00,
      "roe": 15.5,
      "...": null
    },
    "balance_sheet": {
      "asset_cash_and_cash_equivalents": 6000000.00,
      "...": null
    },
    "income_sheet": {
      "net_profit": 500000.00,
      "...": null
    },
    "cash_flow_sheet": {
      "net_cash_flow": 100000.00,
      "...": null
    }
  }
}
```

**响应字段说明**：

| 字段 | 类型 | 说明 |
| ---- | ---- | ---- |
| id | int | 财报记录 ID |
| file_name | str | 文件名 |
| report_title | str | 财报标题 |
| stock_code | str | 股票代码 |
| stock_abbr | str | 股票简称 |
| report_year | int | 报告年份 |
| report_period | str | 报告期间（Q1/Q2/Q3/Q4/HY/FY） |
| report_type | str | 报告类型（REPORT/SUMMARY） |
| report_label | str | 报告中文标签 |
| exchange | str | 交易所（SH/SZ/BJ） |
| report_date | str | 报告披露日期 |
| parse_status | int | 解析状态：0 待处理 / 1 成功 / 2 失败 / 3 解析中 |
| import_status | int | 入库状态：0 待入库 / 1 成功 / 2 失败 |
| created_at | str | 创建时间 |
| updated_at | str | 更新时间 |
| core_performance_indicators | object | 核心业绩指标（解析成功时有值，否则 null） |
| balance_sheet | object | 资产负债表（解析成功时有值，否则 null） |
| income_sheet | object | 利润表（解析成功时有值，否则 null） |
| cash_flow_sheet | object | 现金流量表（解析成功时有值，否则 null） |

### 2.4 删除财报记录

- **POST** `/api/v1/analyze-data/delete`
- **描述**：删除单条财报记录及其关联的四张事实表数据（级联删除）。
- **Content-Type**：`application/json`

**请求体（JSON）**：

| 参数 | 类型 | 必填 | 说明 |
| ---- | ---- | ---- | ---- |
| report_id | int | 是 | 财报记录 ID |

**请求示例**：
```json
{
  "report_id": 1
}
```

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

**响应字段说明**：

| 字段 | 类型 | 说明 |
| ---- | ---- | ---- |
| id | int | 已删除的财报记录 ID |

### 2.2 获取财报记录列表

- **POST** `/api/v1/analyze-data/list`
- **描述**：分页查询已建档的财报记录列表，支持多条件筛选与排序。

**请求体（JSON）**：

| 参数 | 类型 | 必填 | 默认值 | 说明 |
| ---- | ---- | ---- | ------ | ---- |
| page | int | 否 | 1 | 页码（≥1） |
| page_size | int | 否 | 10 | 每页数量（≥10） |
| keyword | str | 否 | null | 报告标题关键词模糊搜索 |
| report_type | str | 否 | null | 报告类型筛选（`REPORT` / `SUMMARY`） |
| report_year | int | 否 | null | 报告年份筛选 |
| parse_status | int | 否 | null | 解析状态：0 待处理 / 1 成功 / 2 失败 / 3 解析中 |
| import_status | int | 否 | null | 入库状态：0 待入库 / 1 成功 / 2 失败 |
| sort_by | str | 否 | `updated_at` | 排序字段：`created_at` / `updated_at` |
| sort_order | str | 否 | `desc` | 排序方式：`desc` / `asc` |

**响应格式**：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "lists": [
      {
        "id": 1,
        "file_name": "603127_20230331.pdf",
        "report_title": "2023 年 年度报告",
        "stock_code": "603127",
        "stock_abbr": "昭衍新药",
        "report_year": 2023,
        "report_period": "FY",
        "report_type": "REPORT",
        "parse_status": 1,
        "import_status": 1,
        "created_at": "2025-01-01T12:00:00"
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

**列表项字段说明**：

| 字段 | 类型 | 说明 |
| ---- | ---- | ---- |
| id | int | 财报记录 ID |
| file_name | str | 文件名 |
| report_title | str | 财报标题 |
| stock_code | str | 股票代码 |
| stock_abbr | str | 股票简称 |
| report_year | int | 报告年份 |
| report_period | str | 报告期间（Q1/Q2/Q3/Q4/HY/FY） |
| report_type | str | 报告类型（REPORT/SUMMARY） |
| parse_status | int | 解析状态：0 待处理 / 1 成功 / 2 失败 / 3 解析中 |
| import_status | int | 入库状态：0 待入库 / 1 成功 / 2 失败 |
| created_at | str | 创建时间 |

---

## 三、智能问数（/api/v1/chat）✅ 已完成

自然语言对话式财务数据查询，支持多轮问答、SQL 生成、SSE 流式推送进度与回答。

### 3.1 发送对话消息（SSE 流式）

- **POST** `/api/v1/chat`
- **描述**：发送对话消息，返回 `text/event-stream` 流式响应。服务端按顺序推送事件：**步骤进度 → 回答 token → 最终结果**。
- **Content-Type**：`application/json`
- **响应类型**：`text/event-stream`

**请求体（JSON）：**

| 参数 | 类型 | 必填 | 说明 |
| ---- | ---- | ---- | ---- |
| session_id | str | 否 | 会话 ID（新对话不传，由服务端生成） |
| question | str | 是 | 用户问题，1~500 字符 |

**请求示例：**
```json
{
  "question": "贵州茅台2023年净利润是多少？"
}
```

### 3.2 SSE 事件流

服务端按顺序推送以下事件，客户端通过 `EventSource` 或 `fetch + ReadableStream` 读取：

#### event: step — 进度事件

```text
event: step
data: {"step": "intent", "message": "正在识别意图..."}

event: step
data: {"step": "intent_done", "message": "意图识别完成"}

event: step
data: {"step": "sql", "message": "正在生成查询语句..."}

event: step
data: {"step": "sql_done", "message": "查询语句生成完成"}

event: step
data: {"step": "query", "message": "正在查询数据..."}

event: step
data: {"step": "query_done", "message": "数据查询完成"}

event: step
data: {"step": "answer", "message": "正在综合分析生成回答..."}

event: step
data: {"step": "answer_done", "message": "回答生成完成"}
```

| 字段 | 类型 | 说明 |
| ---- | ---- | ---- |
| step | str | 步骤标识：intent / intent_done / sql / sql_done / query / query_done / answer / answer_done |
| message | str | 可读的进度描述 |

#### event: token — 回答 token 事件

```text
event: token
data: {"content": "贵州茅台2023年净利润为"}

event: token
data: {"content": "**747.34亿元**。"}
```

| 字段 | 类型 | 说明 |
| ---- | ---- | ---- |
| content | str | 回答文本片段（Markdown 格式），客户端逐段拼接 |

#### event: result — 最终结果事件

```text
event: result
data: {"session_id": "550e8400-e29b-41d4-a716-446655440000", "answer": {"content": "贵州茅台2023年净利润为**747.34亿元**。"}, "sql": "SELECT net_profit FROM income_sheet WHERE stock_code='600519' AND report_year=2023 AND report_period='FY'"}
```

| 字段 | 类型 | 说明 |
| ---- | ---- | ---- |
| session_id | str | 会话 ID（UUID），多轮对话后续请求需携带 |
| answer.content | str | 完整回答文本（Markdown 格式） |
| sql | str | 生成的 SQL 语句 |

#### event: error — 错误事件

```text
event: error
data: {"code": 4001, "message": "生成SQL语句失败"}
```

| 字段 | 类型 | 说明 |
| ---- | ---- | ---- |
| code | int | 错误码 |
| message | str | 错误描述 |

### 3.3 多轮对话说明

1. **首次请求**不传 `session_id`，服务端自动生成新的会话 ID，通过 `result` 事件返回。
2. **后续请求**携带返回的 `session_id`，服务端自动加载会话历史上下文。
3. 服务端内置滑动窗口机制：当消息数超过阈值时，自动将最早的两轮对话压缩为 LLM 摘要，保持上下文长度可控。

### 3.4 通信流程

```
客户端                         服务端
  │                              │
  ├─ POST /api/v1/chat ──────────┤
  │   {question: "..."}          │
  │                              ├─ 创建/加载会话
  │  ← event: step(intent) ─────┤
  │  ← event: step(intent_done) ─┤  意图识别（LLM）
  │  ← event: step(sql) ────────┤
  │  ← event: step(sql_done) ───┤  生成 SQL（LLM）
  │  ← event: step(query) ──────┤
  │  ← event: step(query_done) ──┤  执行查询
  │  ← event: step(answer) ─────┤
  │  ← event: token (逐段) ─────┤  流式生成回答（LLM stream）
  │  ← event: step(answer_done) ─┤
  │  ← event: result ───────────┤  返回最终结果
  │                              │
```

### 3.5 获取对话列表

- **POST** `/api/v1/chat/list`
- **描述**：分页获取历史对话会话列表。
- **Content-Type**：`application/json`

**请求体（JSON）：**

| 参数 | 类型 | 必填 | 默认值 | 说明 |
| ---- | ---- | ---- | ------ | ---- |
| page | int | 否 | 1 | 页码 |
| page_size | int | 否 | 10 | 每页数量 |

**请求示例：**
```json
{
  "page": 1,
  "page_size": 10
}
```

**响应格式：**

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "lists": [
      {
        "id": "550e8400-e29b-41d4-a716-446655440000",
        "session_name": "贵州茅台2023年净利润",
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

**响应字段说明：**

| 字段 | 类型 | 说明 |
| ---- | ---- | ---- |
| lists[].id | str | 会话 ID（UUID） |
| lists[].session_name | str | 会话名称 |
| lists[].status | int | 状态：0 活跃 / 1 已关闭 |
| lists[].created_at | str | 创建时间 |
| lists[].updated_at | str | 更新时间 |
| pagination | object | 分页信息 |

### 3.6 获取对话详情

- **POST** `/api/v1/chat/detail`
- **描述**：获取单个会话的详细信息，包含会话基础信息及全部对话消息（用户问题、AI 回答、生成 SQL、意图识别结果等）。
- **Content-Type**：`application/json`

**请求体（JSON）：**

| 参数 | 类型 | 必填 | 说明 |
| ---- | ---- | ---- | ---- |
| session_id | str | 是 | 会话 ID |

**请求示例：**
```json
{
  "session_id": "550e8400-e29b-41d4-a716-446655440000"
}
```

**响应格式：**

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "session_name": "贵州茅台2023年净利润",
    "status": 0,
    "messages": [
      {
        "id": 1,
        "query": "贵州茅台2023年净利润是多少？",
        "answer": "贵州茅台2023年净利润为**747.34亿元**。",
        "sql_query": "SELECT net_profit FROM income_sheet WHERE stock_code='600519' AND report_year=2023 AND report_period='FY'",
        "sql_result": [{"net_profit": 747.34}],
        "intent_result": {"companys": [{"stock_code": "600519", "company_name": "贵州茅台"}], "metrics": [{"name": "净利润"}], "time_range": {"year": 2023}, "query_type": "single", "confidence": 0.95},
        "created_at": "2025-01-01T10:00:00"
      }
    ],
    "created_at": "2025-01-01T10:00:00",
    "updated_at": "2025-01-01T10:05:00"
  }
}
```

**响应字段说明：**

| 字段 | 类型 | 说明 |
| ---- | ---- | ---- |
| id | str | 会话 ID（UUID） |
| session_name | str | 会话名称 |
| status | int | 状态：0 活跃 / 1 已关闭 |
| messages | object[] | 对话消息列表，按时间顺序排列 |
| messages[].id | int | 消息 ID |
| messages[].query | str | 用户问题 |
| messages[].answer | str | AI 回答（Markdown 格式） |
| messages[].sql_query | str | 生成的 SQL 查询语句 |
| messages[].sql_result | object[] | SQL 执行结果（截断至 100 行） |
| messages[].intent_result | object | 意图识别结果（公司、指标、时间等） |
| messages[].created_at | str | 消息创建时间 |
| created_at | str | 会话创建时间 |
| updated_at | str | 会话更新时间 |

---

## 四、后端处理流程说明

### 4.1 上传建档流程

```
客户端上传 PDF
  │
  ├─ 1. 校验文件类型（仅 .pdf）
  ├─ 2. 保存 PDF 到 uploads/financial_report/
  ├─ 3. 用 PyPDFLoader 读取首页文本
  ├─ 4. 正则解析元数据：
  │      ├─ 股票代码（证券代码 / 股票代码）
  │      ├─ 报告年份 + 报告标签（年度报告/半年度报告等）
  │      ├─ 映射为 report_period / report_type
  │      └─ 提取显式日期
  ├─ 5. 写入 financial_report 表
  └─ 6. 返回建档结果
```

### 3.2 元数据解析规则

| 字段          | 提取方式                         | 示例                 |
| ------------- | -------------------------------- | -------------------- |
| stock_code    | 正则匹配 `证券代码：000001`    | `000001`           |
| report_year   | 正则匹配 `2023 年`             | `2023`             |
| report_label  | 正则匹配报告标签                 | `年度报告`         |
| report_period | 从 `REPORT_LABEL_TO_META` 映射 | `FY`               |
| report_type   | 从 `REPORT_LABEL_TO_META` 映射 | `REPORT`           |
| report_title  | 匹配到的标题文本                 | `2023 年 年度报告` |
| report_date   | 正则匹配 `2024年4月20日`       | `2024-04-20`       |

### 3.3 报告标签映射关系

| 标签                      | report_period | report_type |
| ------------------------- | ------------- | ----------- |
| 一季度报告 / 第一季度报告 | Q1            | REPORT      |
| 二季度报告 / 第二季度报告 | Q2            | REPORT      |
| 半年度报告                | HY            | REPORT      |
| 半年度报告摘要            | HY            | SUMMARY     |
| 三季度报告 / 第三季度报告 | Q3            | REPORT      |
| 四季度报告 / 第四季度报告 | Q4            | REPORT      |
| 年度报告                  | FY            | REPORT      |
| 年度报告摘要              | FY            | SUMMARY     |

---
