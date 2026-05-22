"""Silver STREAMING TABLE: ai_parse_document over bronze PDFs."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

_BRONZE_TABLE = "sharepoint_bronze_pdfs"


@dp.table(
    name="silver_search_chunks",
    comment=(
        "Silver STREAMING TABLE. Bronze pass-through plus "
        "ai_parse_document(content) as parsed_content."
    ),
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.feature.variantType-preview": "supported",
        "quality": "silver",
    },
    cluster_by=["_ingest_date"],
)
def silver_search_chunks():
    bronze_stream = spark.readStream.table(_BRONZE_TABLE)  # noqa: F821

    return bronze_stream.withColumn(
        "parsed_content",
        F.expr("ai_parse_document(content)"),
    )
