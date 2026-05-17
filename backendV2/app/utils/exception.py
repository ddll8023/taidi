class ServiceException(Exception):
    """服务层异常，携带错误码和消息"""

    def __init__(self, code: int, message: str):
        self.code = code
        self.message = message
        super().__init__(message)
