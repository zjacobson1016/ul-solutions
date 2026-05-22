"""Vector Search index provisioning for the SharePoint ingestion gold table."""

from .models import (
    GOLD_EMBEDDING_COLUMN,
    GOLD_PRIMARY_KEY,
    GOLD_TABLE_NAME,
    default_gold_index_request,
    fq_gold_table,
)
from .vector_service import VectorSearchService

__all__ = [
    "GOLD_EMBEDDING_COLUMN",
    "GOLD_PRIMARY_KEY",
    "GOLD_TABLE_NAME",
    "VectorSearchService",
    "default_gold_index_request",
    "fq_gold_table",
]
