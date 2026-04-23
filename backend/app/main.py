from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.schemas.common import ApiResponse, ErrorCode
from app.schemas.response import error, success
from app.api.analysis_data import router as analysis_data_router
from app.api.chat import router as chat_router
from app.api.task2 import router as task2_router
from app.api.task3 import router as task3_router
from app.api.knowledge_base import router as knowledge_base_router
from app.db.init_data import init

app = FastAPI(
    title="Backend API",
    description="Project backend service",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:7388"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# 注册路由
app.include_router(analysis_data_router)
app.include_router(chat_router)
app.include_router(task2_router)
app.include_router(task3_router)
app.include_router(knowledge_base_router)


# 全局异常处理器：捕获 HTTPException
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """将 HTTPException 转换为统一格式"""
    return JSONResponse(
        status_code=exc.status_code,
        content=error(code=exc.status_code, message=exc.detail, data=None),
    )


# 全局异常处理器：捕获所有未处理异常
@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """捕获未预期异常"""
    # 生产环境应记录日志
    print(f"未捕获异常: {exc}")
    return JSONResponse(
        status_code=500,
        content=error(
            code=ErrorCode.INTERNAL_ERROR,
            message="服务器内部错误",
            data=str(exc) if app.debug else None,
        ),
    )


@app.on_event("startup")
def startup():
    init()
    print("✅ 数据库表创建完成")


@app.get("/")
def root():
    return {"message": "HR智能助手API运行中", "docs": "/docs"}
