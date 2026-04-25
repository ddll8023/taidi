"""导出 Milvus knowledge_chunk_embedding Collection 全量数据为 JSON 备份文件"""

import json
import os
import sys
from datetime import datetime

from pymilvus import Collection, connections

MILVUS_URI = os.getenv("MILVUS_URI", "http://127.0.0.1:19530")
COLLECTION_NAME = "knowledge_chunk_embedding"
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "milvus_backup")


def export_collection():
    print(f"[1/3] 连接 Milvus: {MILVUS_URI}")
    connections.connect(uri=MILVUS_URI)

    print(f"[2/3] 获取 Collection: {COLLECTION_NAME}")
    collection = Collection(COLLECTION_NAME)
    collection.load()

    num_entities = collection.num_entities
    print(f"      总记录数: {num_entities}")
    if num_entities == 0:
        print("      Collection 为空，无需导出。")
        return

    all_fields = [f.name for f in collection.schema.fields]
    print(f"      字段列表: {all_fields}")

    print("[3/3] 分页查询数据...")
    batch_size = 1000
    all_data = []
    offset = 0

    while offset < num_entities:
        batch = collection.query(
            expr="",
            output_fields=all_fields,
            limit=batch_size,
            offset=offset,
        )
        if not batch:
            break
        all_data.extend(batch)
        offset += len(batch)
        print(f"      已导出 {len(all_data)}/{num_entities}")

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(OUTPUT_DIR, f"{COLLECTION_NAME}_{timestamp}.json")

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)

    size_mb = os.path.getsize(output_file) / (1024 * 1024)
    print(f"\n导出完成!")
    print(f"  文件: {output_file}")
    print(f"  记录数: {len(all_data)}")
    print(f"  文件大小: {size_mb:.2f} MB")

    connections.disconnect("default")


if __name__ == "__main__":
    export_collection()
