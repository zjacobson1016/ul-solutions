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
          'Fields: equipment_name, model_number, manufacturer, equipment_type, certification_id, certification_status, safety_rating, material_type, weight_kg, voltage_rating, ip_rating, operating_temp_min_c, operating_temp_max_c, compliance_standards\n\n',
          'Document:\n',
          left(document_text, 8000)
        )
      ),
      'equipment_name STRING, model_number STRING, manufacturer STRING, equipment_type STRING, certification_id STRING, certification_status STRING, safety_rating STRING, material_type STRING, weight_kg DOUBLE, voltage_rating STRING, ip_rating STRING, operating_temp_min_c DOUBLE, operating_temp_max_c DOUBLE, compliance_standards STRING'
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
  extracted.equipment_name,
  extracted.model_number,
  extracted.manufacturer,
  extracted.equipment_type,
  extracted.certification_id,
  extracted.certification_status,
  extracted.safety_rating,
  extracted.material_type,
  extracted.weight_kg,
  extracted.voltage_rating,
  extracted.ip_rating,
  extracted.operating_temp_min_c,
  extracted.operating_temp_max_c,
  extracted.compliance_standards,
  _ingested_at,
  _parsed_at
FROM ai_extracted;


-- ============================================================================
-- Gold Layer: Document Table Specifications
-- Explodes extracted HTML table content for detailed spec analysis
-- ============================================================================

CREATE OR REFRESH MATERIALIZED VIEW gold_table_specifications
AS
SELECT
  s.file_path,
  s.file_name,
  posexplode(s.table_contents) AS (table_index, table_html),
  g.equipment_name,
  g.model_number,
  g.manufacturer,
  s._parsed_at
FROM silver_parsed_equipment s
LEFT JOIN gold_equipment_catalog g
  ON s.file_path = g.file_path
WHERE size(s.table_contents) > 0;


-- ============================================================================
-- Gold Layer: Document Figure Descriptions
-- Explodes AI-generated figure descriptions for image analysis
-- ============================================================================

CREATE OR REFRESH MATERIALIZED VIEW gold_figure_descriptions
AS
SELECT
  s.file_path,
  s.file_name,
  posexplode(s.figure_descriptions) AS (figure_index, figure_description),
  g.equipment_name,
  g.model_number,
  g.manufacturer,
  s._parsed_at
FROM silver_parsed_equipment s
LEFT JOIN gold_equipment_catalog g
  ON s.file_path = g.file_path
WHERE size(s.figure_descriptions) > 0;
