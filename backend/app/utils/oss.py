"""
阿里云 OSS 上传工具
"""

import oss2
from app.core.config import settings


def get_oss_bucket():
    """获取 OSS Bucket 实例"""
    auth = oss2.Auth(settings.OSS_ACCESS_KEY_ID, settings.OSS_ACCESS_KEY_SECRET)
    bucket = oss2.Bucket(auth, settings.OSS_ENDPOINT, settings.OSS_BUCKET_NAME)
    return bucket


def upload_to_oss(file_path: str, object_key: str) -> str:
    """
    上传文件到 OSS

    Args:
        file_path: 本地文件路径
        object_key: OSS 对象 key (如 "recordings/xxx.mp3")

    Returns:
        oss_url: 公网可访问的 OSS URL
    """
    bucket = get_oss_bucket()
    bucket.put_object_from_file(object_key, file_path)
    return f"https://{settings.OSS_BUCKET_NAME}.{settings.OSS_ENDPOINT}/{object_key}"


def generate_oss_url(object_key: str) -> str:
    """
    生成 OSS URL

    Args:
        object_key: OSS 对象 key

    Returns:
        oss_url: 公网可访问的 OSS URL
    """
    return f"https://{settings.OSS_BUCKET_NAME}.{settings.OSS_ENDPOINT}/{object_key}"


def extract_object_key_from_url(url: str) -> str:
    """从 OSS URL 提取 object_key"""
    if not url:
        return ""
    prefix = f"https://{settings.OSS_BUCKET_NAME}.{settings.OSS_ENDPOINT}/"
    if prefix in url:
        return url.split(prefix)[1]
    return url


def delete_from_oss(object_key: str) -> bool:
    """从 OSS 删除文件"""
    if not object_key:
        return False
    bucket = get_oss_bucket()
    bucket.delete_object(object_key)
    return True
