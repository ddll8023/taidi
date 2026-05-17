from functools import lru_cache
import os
import yaml
from app.utils.file import ROOT_DIR

# 获取脚本所在目录，用于定位配置文件
SCRIPT_DIR = os.path.join(ROOT_DIR, "app", "prompts")


class PromptConfig:
    def __init__(self):
        self.prompts_dir = os.path.join(SCRIPT_DIR, "prompts")

    @property
    def get_embedding_config(self):
        """获取embedding模型提示词"""
        config_path = os.path.join(self.prompts_dir, "embedding.yaml")
        with open(config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    @property
    def get_struct_config(self):
        """获取匹配分析模型提示词"""
        config_path = os.path.join(self.prompts_dir, "struct.yaml")
        with open(config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    @property
    def get_chat_config(self):
        """获取对话模型提示词"""
        config_path = os.path.join(self.prompts_dir, "chat.yaml")
        with open(config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    @property
    def get_task3_config(self):
        """获取任务三模型提示词"""
        config_path = os.path.join(self.prompts_dir, "task3.yaml")
        with open(config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)


@lru_cache()
def get_prompt_config():
    return PromptConfig()


prompt_config = get_prompt_config()
