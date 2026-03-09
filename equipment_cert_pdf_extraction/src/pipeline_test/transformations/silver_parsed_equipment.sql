-- ============================================================================
-- Silver Layer: AI-Parsed Equipment Document Content
-- Uses ai_parse_document() to extract structured layout from PDFs
-- Extracts text, tables, figures, and document metadata from each element
-- ============================================================================

CREATE OR REFRESH STREAMING TABLE silver_parsed_equipment
CLUSTER BY (file_name)
AS
SELECT
  file_path,
  file_size_bytes,
  _ingested_at,

  -- File metadata from parsed output
  try_cast(parsed_doc:metadata:file_metadata:file_name AS STRING) AS file_name,

  -- Concatenate all text elements into a single document text field
  concat_ws(
    '\n\n',
    transform(
      try_cast(parsed_doc:document:elements AS ARRAY<VARIANT>),
      element -> try_cast(element:content AS STRING)
    )
  ) AS document_text,

  -- Document structure metrics
  try_cast(parsed_doc:metadata:version AS STRING)               AS parse_version,
  size(try_cast(parsed_doc:document:pages AS ARRAY<VARIANT>))   AS num_pages,

  -- Count element types
  size(filter(
    try_cast(parsed_doc:document:elements AS ARRAY<VARIANT>),
    e -> try_cast(e:type AS STRING) = 'table'
  )) AS num_tables,

  size(filter(
    try_cast(parsed_doc:document:elements AS ARRAY<VARIANT>),
    e -> try_cast(e:type AS STRING) = 'figure'
  )) AS num_figures,

  size(filter(
    try_cast(parsed_doc:document:elements AS ARRAY<VARIANT>),
    e -> try_cast(e:type AS STRING) = 'text'
  )) AS num_text_blocks,

  -- Extract table HTML content separately for downstream analysis
  transform(
    filter(
      try_cast(parsed_doc:document:elements AS ARRAY<VARIANT>),
      e -> try_cast(e:type AS STRING) = 'table'
    ),
    t -> try_cast(t:content AS STRING)
  ) AS table_contents,

  -- Extract figure descriptions
  transform(
    filter(
      try_cast(parsed_doc:document:elements AS ARRAY<VARIANT>),
      e -> try_cast(e:type AS STRING) = 'figure'
    ),
    f -> try_cast(f:description AS STRING)
  ) AS figure_descriptions,

  -- Raw parsed output for debugging
  parsed_doc,

  current_timestamp() AS _parsed_at

FROM (
  SELECT
    file_path,
    file_size_bytes,
    _ingested_at,
    ai_parse_document(
      file_content,
      map('version', '2.0', 'descriptionElementTypes', '*')
    ) AS parsed_doc
  FROM STREAM bronze_equipment_docs
)
WHERE try_cast(parsed_doc:error_status AS STRING) IS NULL;
