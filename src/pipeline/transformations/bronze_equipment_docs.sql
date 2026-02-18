-- ============================================================================
-- Bronze Layer: Raw PDF Document Ingestion
-- Streams binary PDF files from Unity Catalog Volume using Auto Loader
-- Preserves raw binary content for downstream AI parsing
-- ============================================================================

CREATE OR REFRESH STREAMING TABLE bronze_equipment_docs
CLUSTER BY (_ingested_at)
AS SELECT
  path                                    AS file_path,
  content                                 AS file_content,
  length                                  AS file_size_bytes,
  modificationTime                        AS file_modified_at,
  current_timestamp()                     AS _ingested_at,
  _metadata.file_path                     AS _source_file
FROM STREAM read_files(
  '${volume_path}',
  format => 'binaryFile'
)
WHERE path LIKE '%.pdf';
