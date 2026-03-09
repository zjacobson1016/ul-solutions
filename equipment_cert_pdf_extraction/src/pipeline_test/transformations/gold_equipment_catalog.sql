-- ============================================================================
-- Gold Layer: Equipment Certification Catalog (Materialized View)
-- Uses ai_query() to extract structured fields from parsed document text
-- Denormalized and business-ready for Genie Space and BI consumption
-- ============================================================================

CREATE OR REFRESH MATERIALIZED VIEW gold_equipment_catalog
AS
WITH ai_extracted AS (
  SELECT
    file_path,
    file_name,
    document_text,
    num_pages,
    num_tables,
    num_figures,
    _ingested_at,
    _parsed_at,
    from_json(
      ai_query(
        'databricks-meta-llama-3-3-70b-instruct',
        concat(
          'Extract these fields as a JSON object from this industrial equipment certification document. Return ONLY valid JSON, no markdown code fences or extra text. If a field is not found, use null.\n',
          'model_number examples:MD123, MD124, MD125\n',
          'rated_voltage examples:120V, 240V, 480V\n',
          'rated_watts examples:100W, 200W, 300W\n',
          'components examples:motor, transformer, relay\n',
          'Fields: model_number, rated_voltage,rated_watts,components \n\n',
          'Document:\n',
          left(document_text, 8000)
        )
      ),
      'model_number STRING, rated_voltage STRING, rated_watts STRING, components STRING'
    ) AS extracted
  FROM silver_parsed_equipment
  WHERE document_text IS NOT NULL
)
SELECT
  file_path,
  file_name,
  document_text,
  num_pages,
  num_tables,
  num_figures,
  extracted.model_number,
  extracted.rated_voltage,
  extracted.rated_watts,
  extracted.components,
  _ingested_at,
  _parsed_at
FROM ai_extracted;


