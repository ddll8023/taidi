"""知识库服务常量定义"""
import re

# 合法的知识库文档类型
VALID_KB_DOC_TYPES = {"RESEARCH_REPORT", "INDUSTRY_REPORT"}

# DashScope 单次批量 Embedding 最大条数
EMBEDDING_BATCH_SIZE = 25

# 可重试的异常类型（网络错误、SSL 错误等）
RETRYABLE_EXCEPTIONS = (
    ConnectionError,
    OSError,
    Exception,
)

# 文件名安全字符正则（用于标题→文件名规范化匹配）
FILENAME_UNSAFE_PATTERN = re.compile(r'[\\/:*?"<>|／∕⁄]+')
MATCH_UNDERSCORE_PATTERN = re.compile(r"\s*_\s*")
MATCH_WHITESPACE_PATTERN = re.compile(r"\s+")
