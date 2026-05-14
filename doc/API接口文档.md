# API 接口文档

## 概述

- **基础 URL**：`http://localhost:8000`
- **统一响应格式**：所有接口返回 `ApiResponse` 结构体：

```json
{
  "code": 200,
  "message": "success",
  "data": { ... }
}
```

- **错误响应**：

```json
{
  "code": 40001,
  "message": "错误描述",
  "data": null
}
```

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

### 1.2 批量上传 PDF

- **POST** `/api/v1/data/upload/batch`
- **描述**：批量上传 PDF 文件，仅执行建档入库（阶段一）
- **Content-Type**：`multipart/form-data`

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| files | File[] | body | 是 | PDF 文件列表 |

### 1.3 提交单个财报解析任务

- **POST** `/api/v1/data/parse/{report_id}`
- **描述**：提交单个财报解析任务（异步后台执行）

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| report_id | int | path | 是 | 财报 ID |
| force | bool | query | 否 | 强制重新解析（包括已解析成功的），默认 `false` |

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

### 1.5 提交所有待处理解析任务

- **POST** `/api/v1/data/parse/all`
- **描述**：提交所有待处理财报的解析任务（异步后台执行）

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| limit | int | query | 否 | 最大处理数量，默认 `100` |

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

### 1.7 获取财报列表

- **GET** `/api/v1/data`
- **描述**：分页查询财报数据列表

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| page | int | query | 否 | 页码，从 1 开始，默认 `1` |
| page_size | int | query | 否 | 每页数量，默认 `10` |
| keyword | string | query | 否 | 搜索关键词 |
| status | int | query | 否 | 解析状态筛选 |
| report_type | string | query | 否 | 报告类型筛选 |
| stock_code | string | query | 否 | 股票代码筛选 |
| report_year | int | query | 否 | 报告年份筛选 |
| report_period | string | query | 否 | 报告期筛选 |

### 1.8 获取单个财报详情

- **GET** `/api/v1/data/{report_id}`
- **描述**：获取单个财报记录的详情

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| report_id | int | path | 是 | 财报记录 ID |

### 1.9 获取结构化 JSON 内容

- **GET** `/api/v1/data/{report_id}/json`
- **描述**：获取财报的结构化 JSON 文件内容

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| report_id | int | path | 是 | 财报记录 ID |

### 1.10 删除财报记录

- **DELETE** `/api/v1/data/{report_id}`
- **描述**：删除财报记录及其关联数据（事实表数据、磁盘文件）

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| report_id | int | path | 是 | 财报记录 ID |

### 1.11 导入公司基本信息

- **POST** `/api/v1/data/import-companies`
- **描述**：上传附件1 Excel 文件，导入公司基础信息
- **Content-Type**：`multipart/form-data`

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| file | File | body | 是 | Excel 文件（.xlsx 或 .xls） |

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
| question | string | body | 是 | 用户提问内容 |

**请求体示例**：

```json
{
  "session_id": "uuid-string",
  "question": "贵州茅台2023年净利润是多少？"
}
```

### 2.2 获取会话列表

- **GET** `/api/v1/chat/sessions`
- **描述**：分页获取会话列表

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| page | int | query | 否 | 页码，从 1 开始，默认 `1` |
| page_size | int | query | 否 | 每页数量，最大 100，默认 `10` |

### 2.3 获取会话历史

- **GET** `/api/v1/chat/history/{session_id}`
- **描述**：获取指定会话的消息历史记录

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| session_id | string | path | 是 | 会话 ID |

### 2.4 导出结果

- **POST** `/api/v1/chat/export`
- **描述**：导出对话结果为 Excel（result_2.xlsx）
- **Content-Type**：`application/json`

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| questions | array | body | 是 | 问题列表 |

### 2.5 关闭会话

- **PUT** `/api/v1/chat/sessions/{session_id}/close`
- **描述**：关闭指定会话

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| session_id | string | path | 是 | 会话 ID |

### 2.6 删除会话

- **DELETE** `/api/v1/chat/sessions/{session_id}`
- **描述**：删除会话及其所有消息

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| session_id | string | path | 是 | 会话 ID |

### 2.7 重命名会话

- **PUT** `/api/v1/chat/sessions/{session_id}/rename`
- **描述**：重命名指定会话
- **Content-Type**：`application/json`

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| session_id | string | path | 是 | 会话 ID |
| name | string | body | 是 | 新名称 |

**请求体示例**：

```json
{
  "name": "贵州茅台财务分析"
}
```

### 2.8 获取图表图片

- **GET** `/api/v1/chat/images/{filename}`
- **描述**：获取图表图片文件，返回图片二进制流（`image/jpeg`）

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| filename | string | path | 是 | 图片文件名 |

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

### 3.2 查询系统初始化状态

- **GET** `/api/v1/knowledge-base/init-status`
- **描述**：查询系统是否已完成初始化

### 3.3 知识库统计

- **GET** `/api/v1/knowledge-base/stats`
- **描述**：获取知识库整体统计信息（文档数、切块数、向量化进度等）

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

### 3.5 批量查询文档状态

- **POST** `/api/v1/knowledge-base/documents/status/batch`
- **描述**：根据文档 ID 列表批量查询文档状态
- **Content-Type**：`application/json`

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| document_ids | int[] | body | 是 | 文档 ID 列表 |

### 3.6 切块列表

- **GET** `/api/v1/knowledge-base/chunks`
- **描述**：分页查询知识库切块列表

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| document_id | int | query | 否 | 按文档 ID 筛选 |
| vector_status | int | query | 否 | 按向量状态筛选 |
| page | int | query | 否 | 页码，从 1 开始，默认 `1` |
| page_size | int | query | 否 | 每页数量，最大 100，默认 `20` |

### 3.7 单文档切块

- **POST** `/api/v1/knowledge-base/chunk/{document_id}`
- **描述**：提交单个文档的切块任务（异步后台执行）

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| document_id | int | path | 是 | 文档 ID |
| force | bool | query | 否 | 强制重新切块，默认 `false` |

### 3.8 批量切块

- **POST** `/api/v1/knowledge-base/chunk/batch`
- **描述**：提交批量切块任务（异步后台执行）
- **Content-Type**：`application/json`

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| document_ids | int[] | body | 是 | 文档 ID 列表 |

### 3.9 全部待处理切块

- **POST** `/api/v1/knowledge-base/chunk/all`
- **描述**：提交所有待处理文档的切块任务（异步后台执行）
- **Content-Type**：`application/json`

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| limit | int | body | 否 | 最大处理数量 |
| doc_type | string | body | 否 | 文档类型筛选 |

### 3.10 单文档向量化

- **POST** `/api/v1/knowledge-base/vectorize/{document_id}`
- **描述**：向量化单个文档的所有切块（异步后台执行）

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| document_id | int | path | 是 | 文档 ID |
| batch_size | int | query | 否 | 每批处理数量，默认 `20` |
| force | bool | query | 否 | 是否强制重试失败/已完成切块，默认 `false` |

### 3.11 批量向量化

- **POST** `/api/v1/knowledge-base/vectorize`
- **描述**：批量向量化待处理的文档切块（异步后台执行）
- **Content-Type**：`application/json`

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| batch_size | int | body | 否 | 每批处理数量 |
| force | bool | body | 否 | 是否强制重试，默认 `false` |

### 3.12 重置向量状态

- **POST** `/api/v1/knowledge-base/reset-vector-status/{document_id}`
- **描述**：重置文档的向量状态（用于取消处理中任务或重新向量化）

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| document_id | int | path | 是 | 文档 ID |
| target_status | int | query | 否 | 目标状态，默认 `0`（PENDING） |

### 3.13 知识库语义检索

- **POST** `/api/v1/knowledge-base/search`
- **描述**：知识库语义检索（调试用）
- **Content-Type**：`application/json`

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| query | string | body | 是 | 查询文本 |
| stock_code | string | body | 否 | 股票代码筛选 |
| doc_type | string | body | 否 | 文档类型筛选 |
| top_k | int | body | 否 | 返回结果数量 |

### 3.14 增量上传 PDF

- **POST** `/api/v1/knowledge-base/upload-pdf`
- **描述**：增量上传 PDF 文件，立即处理（匹配元数据 + 切块）
- **Content-Type**：`multipart/form-data`

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| pdfs | File[] | body | 是 | PDF 文件列表 |
| doc_type | string | query | 否 | 文档类型：`RESEARCH_REPORT` 或 `INDUSTRY_REPORT`，默认 `RESEARCH_REPORT` |

### 3.15 单文档上传 PDF

- **POST** `/api/v1/knowledge-base/upload-single-pdf`
- **描述**：上传单个文档的 PDF（用于文档列表中的"上传 PDF"按钮）
- **Content-Type**：`multipart/form-data`

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| document_id | int | query | 是 | 文档 ID |
| pdf_file | File | body | 是 | PDF 文件 |

### 3.16 重试失败文档

- **POST** `/api/v1/knowledge-base/retry-failed`
- **描述**：重新上传 PDF 并重试失败的文档处理
- **Content-Type**：`multipart/form-data`

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| document_id | int | query | 是 | 文档 ID |
| pdf_file | File | body | 是 | PDF 文件 |

### 3.17 查询处理进度

- **GET** `/api/v1/knowledge-base/progress`
- **描述**：查询增量处理的当前进度

---

## 四、任务二工作台（/api/v1/task2）

基于智能问数能力，对附件4中的财务问题批量执行问答。

### 4.1 获取工作台概览

- **GET** `/api/v1/task2/workspace`
- **描述**：获取当前工作台概览信息（导入状态、题目统计等）

### 4.2 导入附件4

- **POST** `/api/v1/task2/workspace/import`
- **描述**：上传并解析附件4，初始化工作台
- **Content-Type**：`multipart/form-data`

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| file | File | body | 是 | 附件4 文件 |

### 4.3 获取题目列表

- **GET** `/api/v1/task2/questions`
- **描述**：获取任务二题目列表

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| status | int | query | 否 | 状态筛选：0 待处理 / 1 回答中 / 2 已完成 / 3 失败 |

### 4.4 获取单题详情

- **GET** `/api/v1/task2/questions/{question_id}`
- **描述**：获取指定题目的详细信息

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| question_id | int | path | 是 | 题目 ID |

### 4.5 回答单题

- **POST** `/api/v1/task2/questions/{question_id}/answer`
- **描述**：执行单题回答

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| question_id | int | path | 是 | 题目 ID |

### 4.6 删除回答

- **DELETE** `/api/v1/task2/questions/{question_id}/answer`
- **描述**：删除指定题目的当前回答

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| question_id | int | path | 是 | 题目 ID |

### 4.7 重新回答

- **POST** `/api/v1/task2/questions/{question_id}/rerun`
- **描述**：删除旧结果后重新执行回答

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| question_id | int | path | 是 | 题目 ID |

### 4.8 批量回答

- **POST** `/api/v1/task2/questions/batch-answer`
- **描述**：批量回答题目

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| scope | string | query | 否 | 处理范围：`all` 全部 / `unfinished` 未完成 / `failed` 失败，默认 `unfinished` |

### 4.9 导出结果

- **POST** `/api/v1/task2/export`
- **描述**：导出任务二结果为 Excel（result_2.xlsx）

### 4.10 获取最近导出信息

- **GET** `/api/v1/task2/export/latest`
- **描述**：获取最近一次导出结果的信息

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
| question | string | body | 是 | 问题内容 |
| context | string | body | 否 | 附加上下文 |

#### 5.1.2 生成执行计划

- **POST** `/api/v1/task3/plan`
- **描述**：生成执行计划（不执行）
- **Content-Type**：`application/json`

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| question | string | body | 是 | 问题内容 |
| context | string | body | 否 | 附加上下文 |

#### 5.1.3 生成计划并执行

- **POST** `/api/v1/task3/execute`
- **描述**：生成计划并执行
- **Content-Type**：`application/json`

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| question | string | body | 是 | 问题内容 |
| context | string | body | 否 | 附加上下文 |

#### 5.1.4 验证结果

- **POST** `/api/v1/task3/verify`
- **描述**：生成计划 → 执行 → 校验，返回完整结果
- **Content-Type**：`application/json`

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| question | string | body | 是 | 问题内容 |
| context | string | body | 否 | 附加上下文 |

#### 5.1.5 导出单题结果

- **POST** `/api/v1/task3/export/single`
- **描述**：导出单个问题结果（独立模式，不依赖工作台）

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| question_id | string | query | 是 | 问题编号 |
| question | string | query | 是 | 问题内容 |

### 5.2 工作台模式

#### 5.2.1 获取工作台概览

- **GET** `/api/v1/task3/workspace`
- **描述**：获取当前工作台概览信息（导入状态、题目统计等）

#### 5.2.2 导入附件6

- **POST** `/api/v1/task3/workspace/import`
- **描述**：上传并解析附件6（.xlsx），初始化工作台
- **Content-Type**：`multipart/form-data`

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| file | File | body | 是 | 附件6 文件（.xlsx） |

#### 5.2.3 获取题目列表

- **GET** `/api/v1/task3/questions`
- **描述**：分页获取任务三题目列表

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| status | int | query | 否 | 状态筛选：0 待处理 / 1 回答中 / 2 已完成 / 3 失败 |
| page | int | query | 否 | 页码，从 1 开始，默认 `1` |
| page_size | int | query | 否 | 每页数量，默认 `10` |

#### 5.2.4 获取单题详情

- **GET** `/api/v1/task3/questions/{question_id}`
- **描述**：获取指定题目的详细信息

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| question_id | int | path | 是 | 题目 ID |

#### 5.2.5 回答单题

- **POST** `/api/v1/task3/questions/{question_id}/answer`
- **描述**：执行单题回答（规划 → 执行 → 校验 → 持久化）

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| question_id | int | path | 是 | 题目 ID |

#### 5.2.6 删除回答

- **DELETE** `/api/v1/task3/questions/{question_id}/answer`
- **描述**：删除指定题目的当前回答

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| question_id | int | path | 是 | 题目 ID |

#### 5.2.7 重新回答

- **POST** `/api/v1/task3/questions/{question_id}/rerun`
- **描述**：删除旧结果后重新执行回答

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| question_id | int | path | 是 | 题目 ID |

#### 5.2.8 批量回答

- **POST** `/api/v1/task3/questions/batch-answer`
- **描述**：批量回答题目

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| scope | string | query | 否 | 处理范围：`all` 全部 / `unfinished` 未完成 / `failed` 失败，默认 `unfinished` |

#### 5.2.9 导出工作台结果

- **POST** `/api/v1/task3/export`
- **描述**：导出工作台结果为 Excel（result_3.xlsx）

#### 5.2.10 获取最近导出信息

- **GET** `/api/v1/task3/export/latest`
- **描述**：获取最近一次导出结果的信息

---

## 六、接口汇总统计

| 模块 | 路由前缀 | 接口数量 |
|------|---------|---------|
| 数据上传处理 | `/api/v1/data` | 11 |
| 智能问数 | `/api/v1/chat` | 8 |
| 知识库管理 | `/api/v1/knowledge-base` | 17 |
| 任务二工作台 | `/api/v1/task2` | 10 |
| 任务三-增强智能问数 | `/api/v1/task3` | 15 |
| **合计** | | **61** |
