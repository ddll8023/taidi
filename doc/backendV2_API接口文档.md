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
| parse_status | int | 否 | null | 解析状态：0 待处理 / 1 成功 / 2 失败 |
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
| parse_status | int | 解析状态：0 待处理 / 1 成功 / 2 失败 |
| import_status | int | 入库状态：0 待入库 / 1 成功 / 2 失败 |
| created_at | str | 创建时间 |

---

## 三、后端处理流程说明

### 3.1 上传建档流程

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
