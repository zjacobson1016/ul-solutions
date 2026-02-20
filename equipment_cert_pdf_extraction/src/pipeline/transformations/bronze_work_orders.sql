-- ============================================================================
-- Bronze Layer: Work Orders
-- Ingests maintenance and service history from parquet files in the UC Volume.
-- ============================================================================

CREATE OR REFRESH STREAMING TABLE bronze_work_orders
AS
SELECT
  *,
  current_timestamp() AS _ingested_at,
  _metadata.file_path AS _source_file
FROM STREAM read_files(
  '${structured_data_path}/work_orders/',
  format => 'parquet'
);
