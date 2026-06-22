# 财报分析工作台 (Financial Reports Workbench)

> **2026年（第14届）"泰迪杯"数据挖掘挑战赛 — B题：上市公司财报"智能问数"助手**

一个基于 AI 的财务报告分析与智能问答系统，支持**财报结构化解析**、**智能问数**、**知识库管理**等功能。系统将非结构化财报 PDF 转化为结构化财务数据，支持自然语言驱动的数据查询和基于语义检索的深度分析。

---

## 技术栈

### 后端

| 类别 | 技术 |
|------|------|
| 框架 | FastAPI + Uvicorn |
| 语言 | Python 3.12+ |
| 数据库 | MySQL (PyMySQL) + SQLAlchemy ORM |
| 向量数据库 | Chroma (LangChain 封装，本地文件持久化) |
| AI / LLM | LangChain, DashScope, OpenAI, Anthropic |
| 数据校验 | Pydantic / Pydantic Settings |
| 认证授权 | PyJWT + bcrypt + cryptography |
| 文件解析 | docx2txt, PyPDF, openpyxl, mutagen, python-pptx, MinerU |
| 对象存储 | Alibaba Cloud OSS (oss2) |
| 数据处理 | Pandas, Matplotlib, scikit-learn |
| 深度学习 | PyTorch, Transformers, PaddlePaddle |
| 包管理 | uv |

### 前端

| 类别 | 技术 |
|------|------|
| 框架 | Vue 3 |
| 构建工具 | Vite (端口 7388，代理 7389) |
| 状态管理 | Pinia |
| 路由 | Vue Router 4 |
| HTTP 客户端 | Axios |
| CSS 框架 | Tailwind CSS + Tailwind Typography |
| 图标 | Font Awesome (vue-fontawesome) |
| Markdown | marked + DOMPurify |

---

## 主要功能

### 1. 公司主数据管理

- 从 Excel 导入上市公司基础信息（股票代码、简称、行业分类等）
- 支持增量导入，已存在的记录自动更新
- 作为财报身份解析的主数据源

### 2. 财报数据构建

- PDF 文件单文件/批量上传与本地存储
- 自动解析财报身份（股票代码、年份、报告期、报告类型）
- LLM 智能抽取四张财务报表（资产负债表、利润表、现金流量表、核心业绩指标表）
- 四层校验（结构 → 字段 → 类型 → 业务）后 UPSERT 入库
- 支持后台异步并发解析（最多 3 并发）

### 3. 智能问数

- 自然语言驱动的财务数据查询对话
- 意图解析（公司、指标、时间范围三槽位）+ 多轮上下文继承
- SQL 自动生成（确定性模板 + LLM 回退）
- 支持 9 种派生指标计算（同比、环比、复合增长率、占比等）
- 四层 SQL 安全校验
- 查询结果可视化图表 + Markdown 渲染
- SSE 流式推送（步骤/Token/结果/错误事件）
- 会话历史管理（列表、详情、删除）

### 4. 知识库管理

- 系统初始化：从 Excel 批量导入研报元数据
- 增量 PDF 上传：按文件名匹配元数据，文本提取与按页切块
- Markdown 清洗编辑器：双栏编辑预览，保存覆写
- 文档切块（固定大小 1600 字符，重叠度 160 字符）
- 批量向量化：通过 Embedding API 将切块转为向量写入 Chroma
- 语义检索：查询文本向量化 → Chroma 相似度搜索 → 返回证据片段
- 三级状态管理（元数据状态 → 切块状态 → 向量化状态）

### 5. 深度研究（任务三）

- 问题分析与执行规划：LLM 分析后拆解为多步骤执行计划
- 6 种步骤类型：SQL 查询、派生指标计算、知识库检索、聚合统计、结果校验、答案组装
- 程序化派生指标计算（资产负债率、毛利率等），确保数值精度
- 四层结果校验（完整性 → 一致性 → 合理性 → 引用有效性）
- 导出时重新执行完整流程，确保结果反映最新数据

### 6. 任务二批量执行

- 从 Excel 导入题目列表并初始化工作台
- 单题/批量/全量自动回答，复用智能问数完整链路
- 结果导出为标准化 Excel

---

## 环境要求

- Python 3.12+
- Node.js 18+
- MySQL 8.0+
- Chroma 向量数据库（LangChain 封装，本地文件持久化，无需单独部署）

---

## 快速开始

### 后端安装与运行

```bash
# 进入后端目录（V2）
cd backendV2

# 创建虚拟环境并激活
python -m venv .venv
.venv\Scripts\activate  # Windows
# source .venv/bin/activate  # Linux/Mac

# 安装依赖（推荐使用 uv）
uv pip install -r requirements.txt

# 配置环境变量
# 编辑 .env 文件，配置数据库连接、API 密钥等

# 启动服务
uv run uvicorn app.main:app --reload --host 0.0.0.0 --port 7389
```

### 前端安装与运行

```bash
# 进入前端目录（V2）
cd frontendV2

# 安装依赖
npm install

# 启动开发服务器（端口 7388，自动代理 /api 到 7389）
npm run dev

# 构建生产版本
npm run build
```

### 访问系统

- 前端页面：http://localhost:7388
- API 服务：http://localhost:7389
- API 文档：http://localhost:7389/docs



---



## 核心模块说明

### 数据构建链路

```
公司主数据导入 → 财报 PDF 上传 → 身份解析与建档 → LLM 结构化抽取 → 四层校验 → 事实表入库
```

### 智能查询链路

```
自然语言问题 → 指代消解与意图解析 → 槽位检查（完整则 SQL 生成，缺失则追问）→ SQL 安全校验 → 执行查询 → 图表生成与回答
```

### 模块依赖关系

- **公司主数据管理** → **财报数据构建** → **智能问数** / **知识库管理**
- **智能问数** → **任务二批量执行**
- **知识库管理** → **深度研究（任务三）**
- **智能问数** + **知识库管理** → **深度研究**

---

## 配置说明

### 后端配置

在 `backendV2/` 目录下的 `.env` 文件中配置：

```env
# 数据库配置
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=admin123
MYSQL_DATABASE=financial_report

# 文件存储配置
UPLOAD_DIR=uploads
FUJIAN2_DIR=fujian2

# Chroma 向量数据库配置
CHROMA_PERSIST_DIR=chroma_data
CHUNK_SIZE=1600
CHUNK_OVERLAP=160

# AI 模型配置
CHAT_BASE_URL=适配 OpenAI API 的地址
CHAT_MODEL=对话模型名称
CHAT_API_KEY=对话模型 API 密钥
EMBEDDING_MODEL=嵌入模型名称
EMBEDDING_DIM=1024
EMBEDDING_API_KEY=嵌入模型 API 密钥

# 阿里云 OSS 配置（可选）
OSS_ACCESS_KEY_ID=your_key_id
OSS_ACCESS_KEY_SECRET=your_key_secret
OSS_BUCKET_NAME=your_bucket
OSS_ENDPOINT=your_endpoint
```

