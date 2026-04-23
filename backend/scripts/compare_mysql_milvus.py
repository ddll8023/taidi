"""
MySQL ↔ Milvus 一致性对比与清理脚本

功能：
  Step 1: 修复MySQL内部不一致（vector_status=2 但 milvus_id 为空的切块 → 回退为PENDING）
  Step 2: 从Milvus查询所有chunk_id，与MySQL交叉对比
  Step 3: 清理Milvus中重复向量（保留auto_id最大的）
  Step 4: 删除Milvus中MySQL已不存在对应的孤儿向量
  Step 5: 同步文档级vector_status

执行：cd backend && python scripts/compare_mysql_milvus.py
       加 --dry-run 仅查看不修改
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import argparse
import logging
from collections import defaultdict

from pymilvus import connections
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from app.core.config import settings
from app.db.milvus import get_kb_collection

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

DRY_RUN = False


def log_action(msg):
    prefix = "[DRY-RUN] " if DRY_RUN else "[EXEC] "
    logger.info(prefix + msg)


def get_mysql_engine():
    db_url = (
        f"mysql+pymysql://{settings.MYSQL_USER}:{settings.MYSQL_PASSWORD}"
        f"@{settings.MYSQL_HOST}:{settings.MYSQL_PORT}/{settings.MYSQL_DATABASE}"
        f"?charset=utf8mb4"
    )
    return create_engine(db_url, pool_recycle=3600)


def step1_fix_mysql_internal(engine):
    """Step 1: 修复MySQL内部不一致 — vector_status=2 但 milvus_id 为空的切块回退为PENDING"""
    logger.info("=" * 80)
    logger.info("Step 1: MySQL内部一致性修复")
    logger.info("=" * 80)

    with Session(engine) as db:
        # 统计问题数据
        result = db.execute(text(
            "SELECT COUNT(*) FROM knowledge_chunk "
            "WHERE vector_status = 2 AND milvus_id IS NULL"
        )).scalar()

        logger.info(f"发现 vector_status=2 但 milvus_id 为空的切块: {result} 条")

        if result == 0:
            logger.info("✅ MySQL内部一致性无问题，跳过")
            return 0

        if DRY_RUN:
            logger.info(f"[DRY-RUN] 将回退 {result} 条切块为 vector_status=0 (PENDING)")
            # 顺便看下涉及哪些文档
            docs = db.execute(text(
                "SELECT document_id, COUNT(*) as cnt FROM knowledge_chunk "
                "WHERE vector_status = 2 AND milvus_id IS NULL "
                "GROUP BY document_id ORDER BY cnt DESC LIMIT 10"
            )).fetchall()
            for doc_id, cnt in docs:
                logger.info(f"  文档 {doc_id}: {cnt} 条问题切块")
            return result

        db.execute(text(
            "UPDATE knowledge_chunk SET vector_status = 0, vector_error_message = NULL "
            "WHERE vector_status = 2 AND milvus_id IS NULL"
        ))
        db.commit()
        logger.info(f"✅ 已回退 {result} 条切块为 PENDING")
        return result


def step2_compare_mysql_milvus(engine):
    """Step 2: MySQL ↔ Milvus 交叉对比"""
    logger.info("=" * 80)
    logger.info("Step 2: MySQL ↔ Milvus 交叉对比")
    logger.info("=" * 80)

    # 连接 Milvus
    connections.connect(uri=settings.MILVUS_URI)
    collection = get_kb_collection()

    # 从 Milvus 分页获取所有记录（Milvus limit最大16384）
    MILVUS_PAGE_SIZE = 16384
    logger.info("查询 Milvus 全量数据（分页）...")
    milvus_records = []
    offset = 0
    while True:
        page = collection.query(
            expr="chunk_id >= 0",
            output_fields=["id", "chunk_id", "document_id", "doc_type", "vector_version"],
            limit=MILVUS_PAGE_SIZE,
            offset=offset,
        )
        milvus_records.extend(page)
        logger.info(f"  已获取 {len(milvus_records)} 条 (offset={offset})")
        if len(page) < MILVUS_PAGE_SIZE:
            break
        offset += MILVUS_PAGE_SIZE
    logger.info(f"Milvus 总向量数: {len(milvus_records)}")

    # Milvus 中 chunk_id → [记录列表]
    milvus_chunk_map = defaultdict(list)
    for r in milvus_records:
        milvus_chunk_map[r["chunk_id"]].append(r)

    milvus_chunk_ids = set(milvus_chunk_map.keys())

    # 从 MySQL 获取所有 vector_status=2 的切块
    with Session(engine) as db:
        rows = db.execute(text(
            "SELECT id, document_id, vector_status, milvus_id "
            "FROM knowledge_chunk WHERE vector_status = 2"
        )).fetchall()

    mysql_completed_ids = {r[0] for r in rows}
    mysql_milvus_id_map = {r[0]: r[3] for r in rows}

    logger.info(f"MySQL vector_status=2 切块数: {len(mysql_completed_ids)}")

    # 对比分析
    # A: MySQL标记完成但Milvus中没有对应向量
    mysql_only = mysql_completed_ids - milvus_chunk_ids
    # B: Milvus中有向量但MySQL不标记为完成
    all_mysql_ids = set()
    with Session(engine) as db:
        all_rows = db.execute(text("SELECT id FROM knowledge_chunk")).fetchall()
        all_mysql_ids = {r[0] for r in all_rows}
    milvus_only = milvus_chunk_ids - all_mysql_ids  # Milvus中存在但MySQL中不存在
    # C: Milvus中重复的chunk_id
    duplicate_chunk_ids = {cid: len(records) for cid, records in milvus_chunk_map.items() if len(records) > 1}

    logger.info("--- 对比结果 ---")
    logger.info(f"A. MySQL标记完成但Milvus无向量: {len(mysql_only)} 条")
    if mysql_only and len(mysql_only) <= 20:
        logger.info(f"   chunk_ids: {sorted(mysql_only)}")

    logger.info(f"B. Milvus有向量但MySQL已无对应切块: {len(milvus_only)} 条")
    if milvus_only and len(milvus_only) <= 20:
        logger.info(f"   chunk_ids: {sorted(milvus_only)}")

    logger.info(f"C. Milvus中重复的chunk_id: {len(duplicate_chunk_ids)} 个")
    total_dup_vectors = sum(v - 1 for v in duplicate_chunk_ids.values())
    logger.info(f"   需删除的冗余向量总数: {total_dup_vectors}")

    return {
        "milvus_records": milvus_records,
        "milvus_chunk_map": milvus_chunk_map,
        "mysql_only": mysql_only,
        "milvus_only": milvus_only,
        "duplicate_chunk_ids": duplicate_chunk_ids,
    }


def step3_clean_milvus_duplicates(collection, milvus_chunk_map, duplicate_chunk_ids):
    """Step 3: 清理Milvus中重复向量，保留auto_id最大的"""
    logger.info("=" * 80)
    logger.info("Step 3: 清理Milvus重复向量")
    logger.info("=" * 80)

    if not duplicate_chunk_ids:
        logger.info("✅ Milvus无重复向量，跳过")
        return 0

    # 计算要删除的auto_id
    delete_ids = []
    for chunk_id, records in milvus_chunk_map.items():
        if len(records) > 1:
            sorted_recs = sorted(records, key=lambda x: x["id"], reverse=True)
            for rec in sorted_recs[1:]:  # 保留最大auto_id，删除其余
                delete_ids.append(rec["id"])

    logger.info(f"需删除冗余向量: {len(delete_ids)} 条")

    if DRY_RUN:
        logger.info(f"[DRY-RUN] 将删除 {len(delete_ids)} 条Milvus冗余向量")
        return len(delete_ids)

    batch_size = 100
    deleted = 0
    for i in range(0, len(delete_ids), batch_size):
        batch = delete_ids[i:i + batch_size]
        try:
            collection.delete(expr=f"id in {batch}")
            deleted += len(batch)
            logger.info(f"  删除进度: {deleted}/{len(delete_ids)}")
        except Exception as e:
            logger.error(f"  删除批次失败: {e}")
            continue

    collection.flush()
    logger.info(f"✅ 已删除 {deleted} 条Milvus冗余向量")
    return deleted


def step4_clean_milvus_orphans(collection, milvus_only):
    """Step 4: 删除Milvus中MySQL已不存在的孤儿向量"""
    logger.info("=" * 80)
    logger.info("Step 4: 清理Milvus孤儿向量")
    logger.info("=" * 80)

    if not milvus_only:
        logger.info("✅ 无孤儿向量，跳过")
        return 0

    logger.info(f"需删除孤儿向量: {len(milvus_only)} 个chunk_id")

    if DRY_RUN:
        logger.info(f"[DRY-RUN] 将删除 {len(milvus_only)} 条Milvus孤儿向量")
        return len(milvus_only)

    chunk_id_list = sorted(milvus_only)
    batch_size = 100
    deleted = 0
    for i in range(0, len(chunk_id_list), batch_size):
        batch = chunk_id_list[i:i + batch_size]
        try:
            collection.delete(expr=f"chunk_id in {batch}")
            deleted += len(batch)
            logger.info(f"  删除进度: {deleted}/{len(chunk_id_list)}")
        except Exception as e:
            logger.error(f"  删除批次失败: {e}")
            continue

    collection.flush()
    logger.info(f"✅ 已删除 {deleted} 条Milvus孤儿向量")
    return deleted


def step5_fix_mysql_missing_vectors(engine, mysql_only):
    """Step 5: MySQL中标记完成但Milvus无向量的切块 → 回退为PENDING"""
    logger.info("=" * 80)
    logger.info("Step 5: 回退MySQL中无Milvus向量的'已完成'切块")
    logger.info("=" * 80)

    if not mysql_only:
        logger.info("✅ 无需回退，跳过")
        return 0

    chunk_id_list = sorted(mysql_only)
    logger.info(f"需回退: {len(chunk_id_list)} 条切块")

    if DRY_RUN:
        logger.info(f"[DRY-RUN] 将回退 {len(chunk_id_list)} 条切块为 vector_status=0")
        return len(chunk_id_list)

    with Session(engine) as db:
        # 分批更新避免SQL过长
        batch_size = 200
        updated = 0
        for i in range(0, len(chunk_id_list), batch_size):
            batch = chunk_id_list[i:i + batch_size]
            placeholders = ",".join(str(cid) for cid in batch)
            db.execute(text(
                f"UPDATE knowledge_chunk SET vector_status = 0, milvus_id = NULL "
                f"WHERE id IN ({placeholders})"
            ))
            updated += len(batch)
        db.commit()
        logger.info(f"✅ 已回退 {updated} 条切块为 PENDING")
        return updated


def step6_sync_document_status(engine):
    """Step 6: 同步文档级vector_status（根据切块状态重算）"""
    logger.info("=" * 80)
    logger.info("Step 6: 同步文档级vector_status")
    logger.info("=" * 80)

    with Session(engine) as db:
        # 查找所有文档及其切块状态分布
        docs = db.execute(text(
            "SELECT d.id, d.vector_status as doc_vs, "
            "  SUM(CASE WHEN c.vector_status = 0 THEN 1 ELSE 0 END) as pending_cnt, "
            "  SUM(CASE WHEN c.vector_status = 1 THEN 1 ELSE 0 END) as processing_cnt, "
            "  SUM(CASE WHEN c.vector_status = 2 THEN 1 ELSE 0 END) as completed_cnt, "
            "  SUM(CASE WHEN c.vector_status = 3 THEN 1 ELSE 0 END) as failed_cnt, "
            "  COUNT(c.id) as total_chunks "
            "FROM knowledge_document d "
            "LEFT JOIN knowledge_chunk c ON c.document_id = d.id "
            "GROUP BY d.id"
        )).fetchall()

        updates = []
        for row in docs:
            doc_id, doc_vs, pending, processing, completed, failed, total = row
            if total == 0:
                continue

            # 计算正确的文档状态
            if processing > 0:
                new_vs = 1  # PROCESSING
            elif failed > 0:
                new_vs = 3  # FAILED
            elif completed == total:
                new_vs = 2  # SUCCESS
            elif pending > 0:
                new_vs = 0  # PENDING
            else:
                continue

            if new_vs != doc_vs:
                updates.append((doc_id, doc_vs, new_vs))

        logger.info(f"文档状态不一致数: {len(updates)}")
        for doc_id, old_vs, new_vs in updates[:20]:
            vs_names = {0: "PENDING", 1: "PROCESSING", 2: "SUCCESS", 3: "FAILED", 4: "SKIPPED"}
            logger.info(f"  文档 {doc_id}: {vs_names.get(old_vs, old_vs)} → {vs_names.get(new_vs, new_vs)}")

        if not updates:
            logger.info("✅ 文档状态全部一致")
            return 0

        if DRY_RUN:
            logger.info(f"[DRY-RUN] 将更新 {len(updates)} 个文档的vector_status")
            return len(updates)

        for doc_id, old_vs, new_vs in updates:
            db.execute(text(
                f"UPDATE knowledge_document SET vector_status = {new_vs} WHERE id = {doc_id}"
            ))
        db.commit()
        logger.info(f"✅ 已更新 {len(updates)} 个文档的vector_status")
        return len(updates)


def final_summary(engine):
    """最终统计"""
    logger.info("=" * 80)
    logger.info("最终数据统计")
    logger.info("=" * 80)

    with Session(engine) as db:
        # 切块状态
        chunk_stats = db.execute(text(
            "SELECT vector_status, COUNT(*) as cnt FROM knowledge_chunk GROUP BY vector_status"
        )).fetchall()
        vs_names = {0: "PENDING", 1: "PROCESSING", 2: "COMPLETED", 3: "FAILED"}
        logger.info("--- 切块 vector_status ---")
        for vs, cnt in chunk_stats:
            logger.info(f"  {vs_names.get(vs, vs)}: {cnt}")

        # 有/无milvus_id
        milvus_stats = db.execute(text(
            "SELECT "
            "  SUM(CASE WHEN milvus_id IS NOT NULL THEN 1 ELSE 0 END) as has_milvus, "
            "  SUM(CASE WHEN milvus_id IS NULL THEN 1 ELSE 0 END) as no_milvus "
            "FROM knowledge_chunk WHERE vector_status = 2"
        )).fetchone()
        logger.info(f"--- COMPLETED切块 milvus_id ---")
        logger.info(f"  有milvus_id: {milvus_stats[0]}")
        logger.info(f"  无milvus_id: {milvus_stats[1]}")

        # 文档状态
        doc_stats = db.execute(text(
            "SELECT vector_status, COUNT(*) as cnt FROM knowledge_document GROUP BY vector_status"
        )).fetchall()
        doc_vs_names = {0: "PENDING", 1: "PROCESSING", 2: "SUCCESS", 3: "FAILED", 4: "SKIPPED"}
        logger.info("--- 文档 vector_status ---")
        for vs, cnt in doc_stats:
            logger.info(f"  {doc_vs_names.get(vs, vs)}: {cnt}")


def main():
    global DRY_RUN

    parser = argparse.ArgumentParser(description="MySQL ↔ Milvus 一致性对比与清理")
    parser.add_argument("--dry-run", action="store_true", help="仅查看不修改")
    args = parser.parse_args()
    DRY_RUN = args.dry_run

    if DRY_RUN:
        logger.info("🔍 DRY-RUN 模式：仅查看，不修改任何数据")
    else:
        logger.info("⚡ 正式模式：将执行实际修改")

    engine = get_mysql_engine()

    # Step 1: MySQL内部修复
    step1_fix_mysql_internal(engine)

    # Step 2: MySQL ↔ Milvus 对比
    cmp = step2_compare_mysql_milvus(engine)

    # Step 3: Milvus去重
    connections.connect(uri=settings.MILVUS_URI)
    collection = get_kb_collection()
    step3_clean_milvus_duplicates(collection, cmp["milvus_chunk_map"], cmp["duplicate_chunk_ids"])

    # Step 4: Milvus孤儿清理
    step4_clean_milvus_orphans(collection, cmp["milvus_only"])

    # Step 5: MySQL回退无Milvus向量的"已完成"切块
    step5_fix_mysql_missing_vectors(engine, cmp["mysql_only"])

    # Step 6: 文档级状态同步
    step6_sync_document_status(engine)

    # 最终统计
    final_summary(engine)

    logger.info("=" * 80)
    logger.info("全部完成！")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
