"""Vector Search lifecycle via the Databricks SDK.

Creates/finds a Vector Search endpoint and a delta-sync index whose source is
the ``gold_search_chunks`` streaming table produced by
``sharepoint_ingestion_etl`` (see ``transformations/gold_search_chunks.py``).
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound, ResourceAlreadyExists
from databricks.sdk.service.vectorsearch import (
    DeltaSyncVectorIndexSpecRequest,
    EmbeddingSourceColumn,
    EndpointType,
    PipelineType,
    VectorIndexType,
)

from .models import (
    VectorSearchEndpointRequest,
    VectorSearchEndpointResponse,
    VectorSearchIndexRequest,
    VectorSearchIndexResponse,
    default_gold_index_request,
)
from .models import (
    GOLD_EMBEDDING_COLUMN,
    GOLD_PRIMARY_KEY,
)

_LOGGER = logging.getLogger(__name__)


def _endpoint_state(info) -> Optional[str]:
    state = getattr(info, "endpoint_status", None)
    if state is None:
        return None
    s = getattr(state, "state", None)
    return str(s.value) if hasattr(s, "value") else (s and str(s))


def _index_state(info) -> Optional[str]:
    status = getattr(info, "status", None)
    if status is None:
        return None
    s = getattr(status, "detailed_state", None) or getattr(status, "ready", None)
    return str(s.value) if hasattr(s, "value") else (s and str(s))


class VectorSearchService:
    """Wrap the Databricks Vector Search SDK with idempotent helpers."""

    def __init__(self, client: WorkspaceClient) -> None:
        self.w = client

    # ------------------------------------------------------------------
    # Endpoint
    # ------------------------------------------------------------------

    def get_or_create_endpoint(
        self, req: VectorSearchEndpointRequest
    ) -> VectorSearchEndpointResponse:
        try:
            endpoint_type = EndpointType(req.endpoint_type.upper())
        except ValueError as e:
            raise ValueError(
                f"Invalid endpoint_type {req.endpoint_type!r}; expected "
                "STANDARD or STORAGE_OPTIMIZED"
            ) from e

        existing = self._get_endpoint(req.name)
        if existing is None:
            _LOGGER.info(
                "Creating VS endpoint %s (%s)", req.name, endpoint_type.value
            )
            try:
                wait = self.w.vector_search_endpoints.create_endpoint(
                    name=req.name, endpoint_type=endpoint_type
                )
            except ResourceAlreadyExists:
                wait = None
        else:
            wait = None

        if req.wait_until_online:
            info = self.w.vector_search_endpoints.wait_get_endpoint_vector_search_endpoint_online(
                endpoint_name=req.name,
                timeout=timedelta(seconds=req.wait_timeout_seconds),
            )
        else:
            info = self._get_endpoint(req.name) or (
                wait.result() if wait is not None else None  # type: ignore[union-attr]
            )

        if info is None:
            raise RuntimeError(
                f"Could not retrieve VS endpoint {req.name!r} after create"
            )
        return self._endpoint_response(info)

    def get_endpoint(self, name: str) -> VectorSearchEndpointResponse:
        info = self._get_endpoint(name)
        if info is None:
            raise NotFound(f"Vector search endpoint '{name}' not found")
        return self._endpoint_response(info)

    def delete_endpoint(self, name: str) -> None:
        try:
            self.w.vector_search_endpoints.delete_endpoint(endpoint_name=name)
        except NotFound:
            pass

    def _get_endpoint(self, name: str):
        try:
            return self.w.vector_search_endpoints.get_endpoint(
                endpoint_name=name
            )
        except NotFound:
            return None

    @staticmethod
    def _endpoint_response(info) -> VectorSearchEndpointResponse:
        et = getattr(info, "endpoint_type", None)
        et_value = str(et.value) if hasattr(et, "value") else (et and str(et))
        status = getattr(info, "endpoint_status", None)
        ready = getattr(status, "ready", None)
        return VectorSearchEndpointResponse(
            name=getattr(info, "name", None),
            endpoint_type=et_value,
            state=_endpoint_state(info),
            ready=ready,
            creator=getattr(info, "creator", None),
        )

    # ------------------------------------------------------------------
    # Index (gold_search_chunks)
    # ------------------------------------------------------------------

    def get_or_create_gold_index(
        self,
        catalog: str,
        schema: str,
        endpoint_name: str,
        *,
        index_name: Optional[str] = None,
        embedding_model_endpoint_name: str = "databricks-gte-large-en",
        pipeline_type: str = "TRIGGERED",
    ) -> VectorSearchIndexResponse:
        """Create or return a delta-sync index on ``gold_search_chunks``.

        Uses ``chunk_id`` as the primary key and ``chunk_to_embed`` for
        embeddings, matching the gold table schema in the SDP pipeline.
        """
        req = default_gold_index_request(catalog, schema, endpoint_name, index_name)
        if (
            embedding_model_endpoint_name != req.embedding_model_endpoint_name
            or pipeline_type != req.pipeline_type
        ):
            req = VectorSearchIndexRequest(
                index_name=req.index_name,
                endpoint_name=req.endpoint_name,
                source_table=req.source_table,
                primary_key=GOLD_PRIMARY_KEY,
                embedding_source_column=GOLD_EMBEDDING_COLUMN,
                embedding_model_endpoint_name=embedding_model_endpoint_name,
                pipeline_type=pipeline_type,
            )
        _LOGGER.info(
            "Gold index target: %s (table=%s, pk=%s, embed_col=%s)",
            req.index_name,
            req.source_table,
            GOLD_PRIMARY_KEY,
            GOLD_EMBEDDING_COLUMN,
        )
        return self.get_or_create_index(req)

    def get_or_create_index(
        self, req: VectorSearchIndexRequest
    ) -> VectorSearchIndexResponse:
        existing = self._get_index(req.index_name)
        if existing is not None:
            _LOGGER.info("VS index %s already exists, returning", req.index_name)
            return self._index_response(existing)

        try:
            pipeline_type = PipelineType(req.pipeline_type.upper())
        except ValueError as e:
            raise ValueError(
                f"Invalid pipeline_type {req.pipeline_type!r}; expected "
                "TRIGGERED or CONTINUOUS"
            ) from e

        spec = DeltaSyncVectorIndexSpecRequest(
            source_table=req.source_table,
            embedding_source_columns=[
                EmbeddingSourceColumn(
                    name=req.embedding_source_column,
                    embedding_model_endpoint_name=req.embedding_model_endpoint_name,
                )
            ],
            pipeline_type=pipeline_type,
        )

        _LOGGER.info(
            "Creating VS index %s on %s (pipeline_type=%s)",
            req.index_name,
            req.source_table,
            pipeline_type.value,
        )
        created = self.w.vector_search_indexes.create_index(
            name=req.index_name,
            endpoint_name=req.endpoint_name,
            primary_key=req.primary_key,
            index_type=VectorIndexType.DELTA_SYNC,
            delta_sync_index_spec=spec,
        )
        return self._index_response(created)

    def get_index(self, index_name: str) -> VectorSearchIndexResponse:
        info = self._get_index(index_name)
        if info is None:
            raise NotFound(f"Vector search index '{index_name}' not found")
        return self._index_response(info)

    def delete_index(self, index_name: str) -> None:
        try:
            self.w.vector_search_indexes.delete_index(index_name=index_name)
        except NotFound:
            pass

    def sync_index(self, index_name: str) -> None:
        self.w.vector_search_indexes.sync_index(index_name=index_name)

    def _get_index(self, index_name: str):
        try:
            return self.w.vector_search_indexes.get_index(index_name=index_name)
        except NotFound:
            return None

    @staticmethod
    def _index_response(info) -> VectorSearchIndexResponse:
        index_type = getattr(info, "index_type", None)
        index_type_value = (
            str(index_type.value)
            if hasattr(index_type, "value")
            else (index_type and str(index_type))
        )
        spec = getattr(info, "delta_sync_index_spec", None)
        source_table = getattr(spec, "source_table", None) if spec else None
        return VectorSearchIndexResponse(
            index_name=getattr(info, "name", None),
            endpoint_name=getattr(info, "endpoint_name", None),
            primary_key=getattr(info, "primary_key", None),
            index_type=index_type_value or "DELTA_SYNC",
            source_table=source_table,
            state=_index_state(info),
        )
