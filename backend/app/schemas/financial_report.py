from enum import IntEnum


# ========== 辅助类（Support）==========  # 内部 Enum、通用对象类


class ParseStatus(IntEnum):
    PENDING = 0
    SUCCESS = 1
    FAILED = 2
    PROCESSING = 3


class ReviewStatus(IntEnum):
    PENDING = 0
    APPROVED = 1
    REJECTED = 2


class ValidateStatus(IntEnum):
    PENDING = 0
    PASSED = 1
    FAILED = 2


class ImportStatus(IntEnum):
    PENDING = 0
    SUCCESS = 1
    FAILED = 2


class VectorStatus(IntEnum):
    PENDING = 0
    PROCESSING = 1
    SUCCESS = 2
    FAILED = 3
    SKIPPED = 4


# ========== 请求类（Request）==========  # 入参校验


# ========== 响应类（Response）==========  # 返回数据结构
