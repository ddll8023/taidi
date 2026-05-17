# BackendV2 API 接口文档

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
| 2004 | Token 无效 |
| 3001 | 不支持的文件格式 |
| 3002 | 文件过大 |
| 4001 | AI 服务错误 |
| 5001 | 内部错误 |
| 6001 | 密码错误 |

- **Swagger 文档**：`http://localhost:8000/docs`

---

## 一、公司基本信息导入（/api/v1/company_base_info）

上传 Excel 文件，导入上市公司基础信息（股票代码、公司名称、交易所、行业等），支持新增与按股票代码更新。

### 1.1 导入公司基本信息

- **POST** `/api/v1/company_base_info/import`
- **描述**：上传 Excel 文件（附件1），导入公司基础信息。已存在的股票代码记录将被更新，不存在的新增。
- **Content-Type**：`multipart/form-data`

| 参数 | 类型 | 位置 | 必填 | 说明 |
|------|------|------|------|------|
| file | File | body | 是 | Excel 文件（`.xlsx` 或 `.xls`） |

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
