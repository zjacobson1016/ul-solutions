"""Bronze STREAMING TABLE: incrementally ingest PDFs from SharePoint via readStream."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="sharepoint_bronze_pdfs",
    comment=(
        "Bronze STREAMING TABLE. Incrementally ingests new PDF files from "
        "SharePoint using spark.readStream with cloudFiles (Auto Loader)."
    ),
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "quality": "bronze",
    },
    cluster_by=["_ingest_date"],
)
def sharepoint_bronze_pdfs():
    sharepoint_url = spark.conf.get(  # noqa: F821
        "sharepoint_url",
        "https://7wplqh.sharepoint.com/sites/ragaas-test/Shared%20Documents/Forms/AllItems.aspx",
    )
    sharepoint_connection = spark.conf.get(  # noqa: F821
        "sharepoint_connection", "sharepoint_test"
    )
    schema_location_base = spark.conf.get("schema_location_base")  # noqa: F821

    return (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "binaryFile")
        .option("databricks.connection", sharepoint_connection)
        .option(
            "cloudFiles.schemaLocation",
            f"{schema_location_base}/sharepoint_bronze_pdfs",
        )
        .option("pathGlobFilter", "*.pdf")
        .load(sharepoint_url)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_ingest_date", F.current_date())
    )
