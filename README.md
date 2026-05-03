# 财报分析工作台 (Financial Reports Workbench)

一个基于 AI 的财务报告分析与智能问答系统，支持财报结构化解析、智能问数、知识库管理等功能。

## 技术栈

### 后端

- **框架**: FastAPI
- **语言**: Python 3.12+
- **数据库**: MySQL (SQLAlchemy ORM)
- **向量数据库**: Milvus
- **AI/LLM**: LangChain, DashScope, OpenAI, Anthropic
- **其他**: PyJWT, bcrypt, OSS

### 前端

- **框架**: Vue 3
- **构建工具**: Vite
- **状态管理**: Pinia
- **路由**: Vue Router
- **样式**: Tailwind CSS
- **HTTP 客户端**: Axios

## 主要功能

### 1. 财报管理

- 财报文件上传与解析
- 结构化数据抽取（资产负债表、利润表、现金流量表等）
- 财报数据查询与可视化

### 2. 智能问数

- 基于自然语言的财报数据查询
- AI 驱动的智能问答
- 查询结果图表生成
- 会话历史管理

### 3. 知识库管理

- 文档上传与管理
- 文档切块与向量化
- 基于向量检索的知识问答

### 4. 任务工作台

- **任务二**: 批量问答处理
- **任务三**: 问题规划、执行与验证

## 环境要求

- Python 3.12+
- Node.js 18+
- MySQL 8.0+
- Milvus 向量数据库

## 快速开始

### 后端安装与运行

```bash
# 进入后端目录
cd backend

# 创建虚拟环境并激活
python -m venv .venv
.venv\Scripts\activate  # Windows
# source .venv/bin/activate  # Linux/Mac

# 安装依赖
pip install -r requirements.txt

# 配置环境变量（创建 .env 文件）
# 配置数据库连接、API密钥等

# 启动服务
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

### 前端安装与运行

```bash
# 进入前端目录
cd frontend

# 安装依赖
npm install

# 启动开发服务器
npm run dev

# 构建生产版本
npm run build
```

## 项目结构

```
code/
├── backend/                 # 后端代码
│   ├── app/
│   │   ├── api/            # API 路由层
│   │   ├── config/         # 配置文件
│   │   ├── core/           # 核心配置
│   │   ├── db/             # 数据库连接
│   │   ├── models/         # ORM 模型
│   │   ├── schemas/        # Pydantic 模型
│   │   ├── services/       # 业务逻辑层
│   │   ├── utils/          # 工具函数
│   │   └── main.py         # 应用入口
│   ├── requirements.txt    # Python 依赖
│   └── pyproject.toml      # 项目配置
│
├── frontend/               # 前端代码
│   ├── src/
│   │   ├── api/           # API 接口封装
│   │   ├── components/    # Vue 组件
│   │   ├── composables/   # 组合式函数
│   │   ├── router/        # 路由配置
│   │   ├── stores/        # Pinia 状态管理
│   │   ├── utils/         # 工具函数
│   │   ├── views/         # 页面视图
│   │   └── main.js        # 应用入口
│   ├── package.json       # NPM 配置
│   └── vite.config.js     # Vite 配置
│
└── doc/                    # 项目文档
    └── 项目结构文档.md
```

详细的项目结构说明请参阅 [项目结构文档](./doc/项目结构文档.md)。

## 核心模块说明

### 后端模块

| 模块          | 说明                                   |
| ------------- | -------------------------------------- |
| `api/`      | RESTful API 接口定义                   |
| `models/`   | 数据库模型定义（财报、聊天、知识库等） |
| `services/` | 核心业务逻辑实现                       |
| `schemas/`  | 请求/响应数据模型定义                  |
| `config/`   | 提示词配置与结构化抽取规则             |

### 前端模块

| 模块            | 说明                                     |
| --------------- | ---------------------------------------- |
| `views/`      | 页面组件（智能问数、财报管理、工作台等） |
| `components/` | 通用组件（上传弹窗、状态组件、筛选栏等） |
| `stores/`     | 状态管理（聊天、任务工作台等）           |
| `api/`        | 后端接口封装                             |

## 配置说明

### 后端配置

在 `backend/` 目录下创建 `.env` 文件：

```env
# 数据库配置
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=admin123
MYSQL_DATABASE=financial_report

# 文件存储配置
UPLOAD_DIR=uploads
fujian2_DIR=fujian2

# Milvus 配置
MILVUS_URI="http://127.0.0.1:19530"
MILVUS_COLLECTION="financial_report"
CHUNK_SIZE=1600
CHUNK_OVERLAP=160

# 模型配置
CHAT_BASE_URL=适配openai的API地址
CHAT_MODEL=对话模型名称
CHAT_API_KEY=对话模型API密钥
EMBEDDING_MODEL=嵌入模型名称
EMBEDDING_DIM=1024
EMBEDDING_API_KEY=嵌入模型API密钥


```
