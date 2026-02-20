# Databricks notebook source
# MAGIC %md
# MAGIC # UL Solutions - Create Equipment Intelligence Metric View
# MAGIC
# MAGIC Creates a unified Databricks Metric View that joins **gold_equipment_360**
# MAGIC (inventory, facilities, certifications, contracts) with **gold_maintenance_insights**
# MAGIC (work orders, downtime, maintenance costs, risk flags) into a single governed
# MAGIC semantic model powering the Genie Space.

# COMMAND ----------

dbutils.widgets.text("catalog", "mfg_mc_se_sa", "Catalog")
dbutils.widgets.text("schema", "ul_solutions", "Schema")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

print(f"Catalog: {catalog}")
print(f"Schema:  {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the Metric View

# COMMAND ----------

metric_view_sql = f"""
CREATE OR REPLACE VIEW {catalog}.{schema}.equipment_certification_metrics
WITH METRICS
LANGUAGE YAML
AS $$
version: "1.1"
source: {catalog}.{schema}.gold_equipment_360
comment: >-
  UL Solutions unified equipment intelligence metrics — certification,
  inventory, facilities, contracts, and maintenance insights in a single
  semantic model for the Genie Space.
joins:
  - name: maintenance
    source: {catalog}.{schema}.gold_maintenance_insights
    on: maintenance.asset_id = asset_id
dimensions:
  # --- Equipment identity ---
  - name: Equipment Type
    expr: equipment_type
    comment: "Category of industrial equipment"
  - name: Manufacturer
    expr: manufacturer
    comment: "Equipment manufacturer name"
  - name: Voltage Rating
    expr: voltage_rating
    comment: "Rated operating voltage from inventory"

  # --- Certification ---
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

  # --- Facility ---
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

  # --- Operational status ---
  - name: Operational Status
    expr: operational_status
    comment: "Current equipment status (Active, Under Maintenance, Standby, Decommissioned)"
  - name: Warranty Status
    expr: warranty_status
    comment: "Warranty state (Active, Expiring Soon, Expired)"
  - name: Inspection Status
    expr: inspection_status
    comment: "Next inspection state (Current, Due Soon, Overdue)"
  - name: Contract Status
    expr: contract_status
    comment: "Manufacturer contract state (Active, Expiring Soon, Expired)"

  # --- Maintenance & risk (from gold_maintenance_insights) ---
  - name: Risk Level
    expr: maintenance.risk_level
    comment: "Computed risk flag (HIGH RISK, MEDIUM RISK, ELEVATED, NORMAL) based on certification status, open work orders, and inspection overdue"

measures:
  # --- Asset counts ---
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

  # --- Certification ---
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

  # --- Financial ---
  - name: Total Purchase Value (USD)
    expr: ROUND(SUM(purchase_price_usd), 2)
    comment: "Total purchase value of equipment"
  - name: Avg Purchase Price (USD)
    expr: ROUND(AVG(purchase_price_usd), 2)
    comment: "Average equipment purchase price"
  - name: Total Contract Value (USD)
    expr: ROUND(SUM(DISTINCT contract_annual_value_usd), 2)
    comment: "Total annual manufacturer contract value"

  # --- Warranty & inspections ---
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

  # --- Certification specs ---
  - name: Average Weight (kg)
    expr: ROUND(AVG(weight_kg), 1)
    comment: "Average equipment weight in kilograms"
  - name: Avg Operating Temp Range (C)
    expr: ROUND(AVG(operating_temp_max_c - operating_temp_min_c), 1)
    comment: "Average operating temperature range"

  # --- Cardinality ---
  - name: Distinct Facilities
    expr: COUNT(DISTINCT facility_name)
    comment: "Number of facilities with equipment"
  - name: Distinct Manufacturers
    expr: COUNT(DISTINCT manufacturer)
    comment: "Number of unique equipment manufacturers"

  # --- Work order metrics (from gold_maintenance_insights) ---
  - name: Total Work Orders
    expr: SUM(maintenance.total_work_orders)
    comment: "Total work orders across all matching assets"
  - name: Open Work Orders
    expr: SUM(maintenance.open_work_orders)
    comment: "Work orders currently open or in progress"
  - name: Completed Work Orders
    expr: SUM(maintenance.completed_work_orders)
    comment: "Work orders that have been completed"
  - name: Emergency Repairs
    expr: SUM(maintenance.emergency_repairs)
    comment: "Emergency repair work orders"
  - name: Corrective Repairs
    expr: SUM(maintenance.corrective_repairs)
    comment: "Corrective repair work orders"
  - name: Preventive Maintenance Count
    expr: SUM(maintenance.preventive_maintenance_count)
    comment: "Preventive maintenance work orders"

  # --- Maintenance cost & downtime ---
  - name: Total Maintenance Cost (USD)
    expr: ROUND(SUM(maintenance.total_maintenance_cost_usd), 2)
    comment: "Total maintenance cost (labor at $85/hr + parts) across assets"
  - name: Avg Maintenance Cost Per Asset (USD)
    expr: ROUND(AVG(maintenance.total_maintenance_cost_usd), 2)
    comment: "Average maintenance cost per asset"
  - name: Total Parts Cost (USD)
    expr: ROUND(SUM(maintenance.total_parts_cost_usd), 2)
    comment: "Total parts cost across all work orders"
  - name: Total Labor Hours
    expr: ROUND(SUM(maintenance.total_labor_hours), 1)
    comment: "Total labor hours across all work orders"
  - name: Avg Labor Hours Per Work Order
    expr: ROUND(AVG(maintenance.avg_labor_hours_per_wo), 1)
    comment: "Average labor hours per work order"
  - name: Total Downtime Hours
    expr: ROUND(SUM(maintenance.total_downtime_hours), 1)
    comment: "Total equipment downtime hours from maintenance"

  # --- Risk & priority ---
  - name: High Risk Assets
    expr: COUNT_IF(maintenance.risk_level LIKE 'HIGH RISK%')
    comment: "Assets flagged as high risk (conditional cert + open work orders)"
  - name: Medium Risk Assets
    expr: COUNT_IF(maintenance.risk_level LIKE 'MEDIUM RISK%')
    comment: "Assets flagged as medium risk (conditional cert or overdue inspection)"
  - name: Elevated Risk Assets
    expr: COUNT_IF(maintenance.risk_level LIKE 'ELEVATED%')
    comment: "Assets with elevated risk (frequent emergency repairs)"
  - name: Critical Priority Work Orders
    expr: SUM(maintenance.critical_priority_count)
    comment: "Work orders with critical priority"
  - name: High Priority Work Orders
    expr: SUM(maintenance.high_priority_count)
    comment: "Work orders with high priority"
$$
"""

spark.sql(metric_view_sql)
print(f"Metric View created: {catalog}.{schema}.equipment_certification_metrics")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify the Metric View

# COMMAND ----------

display(spark.sql(f"DESCRIBE EXTENDED {catalog}.{schema}.equipment_certification_metrics"))
