import logging
from typing import Optional


# 默认日志级别
DEFAULT_LOG_LEVEL = logging.INFO

# 日志格式：时间 [文件名:行号] [日志名] [级别] 消息
LOG_FORMAT = (
    "[%(asctime)s] [%(filename)s:%(lineno)d] [%(name)s] [%(levelname)s] %(message)s"
)
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def setup_logger(
    name: Optional[str] = None,
    level: int | str = DEFAULT_LOG_LEVEL,
    console: bool = True,
) -> logging.Logger:
    """
    配置并返回日志记录器

    Args:
        name: 日志器名称，None则返回根日志器
        level: 日志级别
        console: 是否输出到控制台

    Returns:
        配置好的 logging.Logger 实例
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # 避免重复添加处理器
    if logger.handlers:
        return logger

    # 格式化器
    formatter = logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT)

    # 控制台处理器
    if console:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger
