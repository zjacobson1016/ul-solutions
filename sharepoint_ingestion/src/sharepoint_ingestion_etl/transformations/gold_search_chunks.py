"""Gold STREAMING TABLE: ai_prep_search + variant_explode over silver parsed docs."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

_SILVER_TABLE = "silver_search_chunks"
_TMP_VIEW = "_sharepoint_prepped_documents"


@dp.table(
    name="gold_search_chunks",
    comment=(
        "Gold STREAMING TABLE. Reads silver_search_chunks, applies "
        "ai_prep_search(parsed_content), and explodes search-ready chunks."
    ),
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.feature.variantType-preview": "supported",
        "quality": "gold",
    },
    cluster_by=["_ingest_date"],
)
@dp.expect_or_drop("chunk_id_not_null", "chunk_id IS NOT NULL")
@dp.expect_or_drop("chunk_to_embed_not_empty", "length(chunk_to_embed) > 0")
def gold_search_chunks():
    silver_stream = spark.readStream.table(_SILVER_TABLE)  # noqa: F821

    prepped_documents = silver_stream.withColumn(
        "result",
        F.expr("ai_prep_search(parsed_content)"),
    )

    prepped_documents.createOrReplaceTempView(_TMP_VIEW)

    return spark.sql(  # noqa: F821
        f"""
        SELECT
            chunk.value:chunk_id::STRING          AS chunk_id,
            chunk.value:chunk_position::INT       AS chunk_position,
            chunk.value:chunk_to_retrieve::STRING AS chunk_to_retrieve,
            chunk.value:chunk_to_embed::STRING    AS chunk_to_embed,
            chunk.value:pages                     AS chunk_pages,
            result:document.source_uri::STRING    AS source_uri,
            named_struct(
                'path', path,
                'modificationTime', modificationTime,
                'length', length
            )                                     AS file_metadata,
            current_timestamp()                   AS _processed_at,
            current_date()                        AS _ingest_date
        FROM {_TMP_VIEW},
             LATERAL variant_explode(result:document.contents) AS chunk
        """
    )
