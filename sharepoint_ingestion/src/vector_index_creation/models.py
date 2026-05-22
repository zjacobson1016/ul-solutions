"""Request/response models and defaults for Vector Search index creation.

Column and table names match the gold SDP table defined in
``sharepoint_ingestion_etl.transformations.gold_search_chunks``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

# Must stay in sync with @dp.table(name=...) in gold_search_chunks.py
GOLD_TABLE_NAME = "gold_search_chunks"
GOLD_PRIMARY_KEY = "chunk_id"
GOLD_EMBEDDING_COLUMN = "chunk_to_embed"


def fq_gold_table(catalog: str, schema: str) -> str:
    """Fully-qualified Unity Catalog name for the pipeline gold table."""
    return f"{catalog}.{schema}.{GOLD_TABLE_NAME}"


@dataclass
class VectorSearchEndpointRequest:
    name: str
    endpoint_type: str = "STANDARD"
    wait_until_online: bool = False
    wait_timeout_seconds: int = 1800


@dataclass
class VectorSearchEndpointResponse:
    name: str
    endpoint_type: Optional[str] = None
    state: Optional[str] = None
    ready: Optional[bool] = None
    creator: Optional[str] = None


@dataclass
class VectorSearchIndexRequest:
    """Delta-sync index on ``gold_search_chunks`` (search-ready chunk rows)."""

    index_name: str
    endpoint_name: str
    source_table: str
    primary_key: str = GOLD_PRIMARY_KEY
    embedding_source_column: str = GOLD_EMBEDDING_COLUMN
    embedding_model_endpoint_name: str = "databricks-gte-large-en"
    pipeline_type: str = "TRIGGERED"


@dataclass
class VectorSearchIndexResponse:
    index_name: str
    endpoint_name: str
    primary_key: str
    index_type: str
    source_table: Optional[str] = None
    state: Optional[str] = None


def default_gold_index_request(
    catalog: str,
    schema: str,
    endpoint_name: str,
    index_name: Optional[str] = None,
) -> VectorSearchIndexRequest:
    """Build an index request targeting the SDP ``gold_search_chunks`` table."""
    return VectorSearchIndexRequest(
        index_name=index_name or f"{catalog}.{schema}.{GOLD_TABLE_NAME}_vs_index",
        endpoint_name=endpoint_name,
        source_table=fq_gold_table(catalog, schema),
    )
