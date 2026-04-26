"""知识库文档管理服务"""
from app.services.knowledge_base.document import (
    register_document,
    register_and_chunk_document,
    get_document_list,
    get_chunk_list,
    extract_pdf_full_text,
    extract_pdf_pages,
    init_knowledge_base_metadata,
    init_system_from_upload,
    upload_pdf_incremental,
    upload_single_pdf_for_document,
    retry_failed_document,
    upload_single_document,
    upload_documents_batch,
)
from app.services.knowledge_base.chunk import (
    chunk_text,
    chunk_pages,
    chunk_document,
    submit_chunk_task,
    submit_batch_chunk,
    submit_all_pending_chunk,
    submit_and_run_chunk_task,
    submit_and_run_batch_chunk,
    submit_and_run_all_pending_chunk,
    run_chunk_in_background,
    run_chunk_batch_in_background,
    ChunkSubmitResult,
    BatchChunkSubmitResult,
)
from app.services.knowledge_base.vectorize import (
    vectorize_chunks,
    get_vector_version,
    vectorize_document,
    submit_vectorize_task,
    submit_batch_vectorize,
    submit_and_run_vectorize_task,
    submit_and_run_batch_vectorize,
    run_vectorize_in_background,
    run_vectorize_batch_in_background,
    get_processing_progress,
    VectorizeSubmitResult,
    BatchVectorizeSubmitResult,
)
from app.services.knowledge_base.search import (
    search_knowledge,
    search_and_format_evidence,
)
from app.services.knowledge_base.metadata import (
    get_init_status,
    get_knowledge_base_stats,
    get_documents_status_batch,
    reload_metadata_cache,
    reset_vector_status,
)
