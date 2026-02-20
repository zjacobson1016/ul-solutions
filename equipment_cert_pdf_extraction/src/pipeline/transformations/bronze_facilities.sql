-- ============================================================================
-- Bronze Layer: Facilities
-- Ingests facility reference data from parquet files in the UC Volume.
-- ============================================================================

CREATE OR REFRESH STREAMING TABLE bronze_facilities
AS
SELECT
  *,
  current_timestamp() AS _ingested_at,
  _metadata.file_path AS _source_file
FROM STREAM read_files(
  '${structured_data_path}/facilities/',
  format => 'parquet'
);
