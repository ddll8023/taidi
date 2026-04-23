import json
import os

from fastapi import UploadFile

from app.core.config import settings


async def save_file(file: UploadFile, path: str):
    """保存文件"""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    content = await file.read()
    with open(path, "wb") as f:
        f.write(content)
    return path


def save_json(dir_path: str, file_name: str, data: dict) -> str:
    """保存结构化数据为JSON文件"""
    file_path = os.path.join(dir_path, file_name)
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return file_path
