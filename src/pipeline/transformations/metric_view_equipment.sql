-- ============================================================================
-- Metric View: Equipment 360° Metrics
-- Reusable, governed business metrics spanning structured + unstructured data
-- Source: gold_equipment_360 materialized view (joins certification, inventory,
--         facilities, and contracts)
-- Used by: Genie Space "UL Solutions Equipment Catalog"
-- ============================================================================

CREATE OR REPLACE VIEW mfg_mc_se_sa.ul_solutions.equipment_certification_metrics
WITH METRICS
LANGUAGE YAML
AS $$
version: "1.1"
source: mfg_mc_se_sa.ul_solutions.gold_equipment_360
comment: "UL Solutions equipment 360 metrics — certification, inventory, facilities, and contracts"
dimensions:
  # --- From unstructured (AI-extracted certification data) ---
  - name: Equipment Type
    expr: equipment_type
    comment: "Category of industrial equipment"
  - name: Manufacturer
    expr: manufacturer
    comment: "Equipment manufacturer name"
  - name: Certification Status
    expr: certification_status
    comment: "UL certification outcome (PASS, CONDITIONAL, or NULL if uncertified)"
  - name: Certified IP Rating
    expr: certified_ip_rating
    comment: "Ingress Protection rating from certification report"
  - name: Safety Rating
    expr: safety_rating
    comment: "UL safety standard (e.g., UL 508A, UL 891)"
  - name: Material Type
    expr: material_type
    comment: "Primary construction material from certification"
  - name: Compliance Standards
    expr: compliance_standards
    comment: "Applicable compliance standards from certification"

  # --- From structured (inventory / ERP data) ---
  - name: Facility Name
    expr: facility_name
    comment: "Plant or site where equipment is deployed"
  - name: City
    expr: city
    comment: "Facility city"
  - name: Region
    expr: region
    comment: "Geographic region (North America, EMEA, APAC)"
  - name: Facility Type
    expr: facility_type
    comment: "Manufacturing Plant, Distribution Center, or R&D Laboratory"
  - name: Operational Status
    expr: operational_status
    comment: "Current equipment status (Active, Under Maintenance, Standby, Decommissioned)"
  - name: Warranty Status
    expr: warranty_status
    comment: "Warranty state (Active, Expiring Soon, Expired)"
  - name: Inspection Status
    expr: inspection_status
    comment: "Next inspection state (Current, Due Soon, Overdue)"
  - name: Voltage Rating
    expr: voltage_rating
    comment: "Rated operating voltage from inventory"
  - name: Contract Status
    expr: contract_status
    comment: "Manufacturer contract state (Active, Expiring Soon, Expired)"

measures:
  # --- Inventory measures ---
  - name: Total Assets
    expr: COUNT(*)
    comment: "Total equipment assets across all facilities"
  - name: Active Assets
    expr: COUNT_IF(operational_status = 'Active')
    comment: "Equipment currently in active operation"
  - name: Assets Under Maintenance
    expr: COUNT_IF(operational_status = 'Under Maintenance')
    comment: "Equipment currently under maintenance"
  - name: Decommissioned Assets
    expr: COUNT_IF(operational_status = 'Decommissioned')
    comment: "Equipment that has been decommissioned"

  # --- Certification measures ---
  - name: Certified Assets
    expr: COUNT_IF(has_certification = true)
    comment: "Assets with UL certification data"
  - name: Uncertified Assets
    expr: COUNT_IF(has_certification = false)
    comment: "Assets without UL certification data"
  - name: Certification Pass Count
    expr: COUNT_IF(certification_status = 'PASS')
    comment: "Assets with passing certification"
  - name: Conditional Certification Count
    expr: COUNT_IF(certification_status = 'CONDITIONAL')
    comment: "Assets with conditional certification"
  - name: Certification Pass Rate
    expr: ROUND(COUNT_IF(certification_status = 'PASS') * 100.0 / NULLIF(COUNT_IF(has_certification = true), 0), 1)
    comment: "Percentage of certified equipment that passed"

  # --- Financial measures ---
  - name: Total Purchase Value (USD)
    expr: ROUND(SUM(purchase_price_usd), 2)
    comment: "Total purchase value of equipment"
  - name: Avg Purchase Price (USD)
    expr: ROUND(AVG(purchase_price_usd), 2)
    comment: "Average equipment purchase price"
  - name: Total Contract Value (USD)
    expr: ROUND(SUM(DISTINCT contract_annual_value_usd), 2)
    comment: "Total annual manufacturer contract value"

  # --- Warranty & inspection measures ---
  - name: Expired Warranties
    expr: COUNT_IF(warranty_status = 'Expired')
    comment: "Equipment with expired warranties"
  - name: Warranties Expiring Soon
    expr: COUNT_IF(warranty_status = 'Expiring Soon')
    comment: "Equipment with warranties expiring within 90 days"
  - name: Overdue Inspections
    expr: COUNT_IF(inspection_status = 'Overdue')
    comment: "Equipment with overdue inspections"
  - name: Inspections Due Soon
    expr: COUNT_IF(inspection_status = 'Due Soon')
    comment: "Equipment with inspections due within 30 days"

  # --- Physical measures (from certification) ---
  - name: Average Weight (kg)
    expr: ROUND(AVG(weight_kg), 1)
    comment: "Average equipment weight in kilograms"
  - name: Avg Operating Temp Range (C)
    expr: ROUND(AVG(operating_temp_max_c - operating_temp_min_c), 1)
    comment: "Average operating temperature range"

  # --- Facility measures ---
  - name: Distinct Facilities
    expr: COUNT(DISTINCT facility_name)
    comment: "Number of facilities with equipment"
  - name: Distinct Manufacturers
    expr: COUNT(DISTINCT manufacturer)
    comment: "Number of unique equipment manufacturers"
$$;
