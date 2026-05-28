"""MinerU PDF解析工具"""

import os
import subprocess

from app.utils.file import ROOT_DIR
from app.utils.logger_config import setup_logger
import re

logger = setup_logger(__name__)


def run_mineru_parse(pdf_path: str, output_dir: str):
    """调用MinerU CLI解析PDF"""
    env = dict(os.environ)
    env["HF_HOME"] = os.path.join(ROOT_DIR, ".cache", "huggingface")
    env["HF_HUB_DISABLE_SYMLINKS_WARNING"] = "1"
    env["MINERU_MODEL_SOURCE"] = "modelscope"

    logger.info(f"调用MinerU解析: pdf_path={pdf_path} output_dir={output_dir}")
    result = subprocess.run(
        ["mineru", "-p", pdf_path, "-o", output_dir, "-b", "pipeline"],
        capture_output=True,
        text=True,
        timeout=600,
        env=env,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"MinerU 转换失败 (rc={result.returncode}): "
            f"{result.stderr.strip()[-500:]}"
        )
    logger.info(f"MinerU解析完成: pdf_path={pdf_path}")


def html_table_to_markdown(html: str):
    """HTML表格转Markdown表格"""
    rows = re.findall(r"<tr[^>]*>(.*?)</tr>", html, re.DOTALL | re.IGNORECASE)
    if not rows:
        return html

    md_rows = []
    for row_html in rows:
        cells = re.findall(
            r"<t[dh][^>]*>(.*?)</t[dh]>", row_html, re.DOTALL | re.IGNORECASE
        )
        cells = [re.sub(r"<[^>]+>", "", c).strip() for c in cells]
        md_rows.append("| " + " | ".join(cells) + " |")

    if len(md_rows) > 0:
        col_count = len(re.findall(r"<t[dh]", rows[0], re.IGNORECASE))
        md_rows.insert(1, "|" + "|".join(["---"] * col_count) + "|")

    return "\n".join(md_rows)
