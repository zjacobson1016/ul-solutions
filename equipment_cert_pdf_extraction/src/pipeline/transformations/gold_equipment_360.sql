-- ============================================================================
-- Gold Layer: Equipment 360° View
-- Joins AI-extracted certification data (unstructured) with operational
-- inventory and facility data (structured) to create a complete picture
-- of every piece of equipment across the organization.
--
-- Join strategy:
--   gold_equipment_catalog (from PDFs) is joined to equipment_inventory
--   on manufacturer + equipment_type, since model numbers in the certification
--   reports may not exactly match inventory records (different revisions, etc.)
--   Facility data is joined via facility_id on the inventory.
-- ============================================================================

CREATE OR REFRESH MATERIALIZED VIEW gold_equipment_360
AS
SELECT
  -- Inventory identifiers
  ei.asset_id,
  ei.serial_number,
  ei.model_number          AS inventory_model_number,
  ei.equipment_type,
  ei.equipment_type_code,
  ei.manufacturer,

  -- Facility context
  f.facility_id,
  f.facility_name,
  f.city,
  f.state_province,
  f.country,
  f.region,
  f.facility_type,

  -- Operational status
  ei.operational_status,
  ei.install_location,
  ei.voltage_rating,
  ei.ip_rating              AS inventory_ip_rating,
  ei.purchase_date,
  ei.purchase_price_usd,
  ei.warranty_expiration,
  ei.last_inspection_date,
  ei.next_inspection_due,

  -- Warranty status
  CASE
    WHEN ei.warranty_expiration < current_date() THEN 'Expired'
    WHEN ei.warranty_expiration < date_add(current_date(), 90) THEN 'Expiring Soon'
    ELSE 'Active'
  END AS warranty_status,

  -- Inspection status
  CASE
    WHEN ei.next_inspection_due < current_date() THEN 'Overdue'
    WHEN ei.next_inspection_due < date_add(current_date(), 30) THEN 'Due Soon'
    ELSE 'Current'
  END AS inspection_status,

  -- Certification data (from unstructured PDF pipeline)
  gc.certification_id,
  gc.certification_status,
  gc.safety_rating,
  gc.ip_rating              AS certified_ip_rating,
  gc.material_type,
  gc.weight_kg,
  gc.operating_temp_min_c,
  gc.operating_temp_max_c,
  gc.compliance_standards,
  gc.model_number           AS certified_model_number,
  gc.file_name              AS certification_document,

  -- Whether this equipment type + manufacturer combo has certification data
  CASE
    WHEN gc.certification_id IS NOT NULL THEN true
    ELSE false
  END AS has_certification,

  -- Contract data
  mc.contract_id,
  mc.contract_type,
  mc.contract_status,
  mc.sla_response_hours,
  mc.annual_value_usd       AS contract_annual_value_usd

FROM bronze_equipment_inventory ei

-- Facility enrichment
INNER JOIN bronze_facilities f
  ON ei.facility_id = f.facility_id

-- Certification enrichment (LEFT JOIN — not all inventory has been certified)
LEFT JOIN gold_equipment_catalog gc
  ON ei.manufacturer = gc.manufacturer
  AND ei.equipment_type = gc.equipment_type

-- Contract enrichment
LEFT JOIN bronze_manufacturer_contracts mc
  ON ei.manufacturer = mc.manufacturer;
