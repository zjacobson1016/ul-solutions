-- ============================================================================
-- Gold Layer: Maintenance & Certification Insights
-- Aggregates work order history per asset and enriches with certification
-- and facility data to surface maintenance patterns alongside compliance status.
--
-- Key insights this table enables:
--   - Equipment with conditional certifications AND open work orders (risk)
--   - Maintenance cost by manufacturer, facility, or equipment type
--   - Correlation between maintenance frequency and certification outcomes
--   - Overdue inspections on equipment with active certifications
-- ============================================================================

CREATE OR REFRESH MATERIALIZED VIEW gold_maintenance_insights
AS
WITH work_order_summary AS (
  SELECT
    asset_id,

    -- Counts
    COUNT(*)                                                    AS total_work_orders,
    COUNT_IF(status = 'Completed')                              AS completed_work_orders,
    COUNT_IF(status IN ('Open', 'In Progress'))                 AS open_work_orders,
    COUNT_IF(work_order_type = 'Emergency Repair')              AS emergency_repairs,
    COUNT_IF(work_order_type = 'Corrective Repair')             AS corrective_repairs,
    COUNT_IF(work_order_type = 'Preventive Maintenance')        AS preventive_maintenance_count,

    -- Costs & labor
    ROUND(COALESCE(SUM(labor_hours), 0), 1)                     AS total_labor_hours,
    ROUND(COALESCE(SUM(parts_cost_usd), 0), 2)                 AS total_parts_cost_usd,
    ROUND(COALESCE(SUM(downtime_hours), 0), 1)                  AS total_downtime_hours,
    ROUND(COALESCE(AVG(labor_hours), 0), 1)                     AS avg_labor_hours_per_wo,

    -- Recency
    MAX(CASE WHEN status = 'Completed' THEN completed_date END) AS last_completed_wo,
    MIN(CASE WHEN status = 'Open' THEN scheduled_date END)      AS next_scheduled_wo,

    -- Priority distribution
    COUNT_IF(priority = 'Critical')                             AS critical_priority_count,
    COUNT_IF(priority = 'High')                                 AS high_priority_count

  FROM bronze_work_orders
  GROUP BY asset_id
)

SELECT
  -- Asset identity
  ei.asset_id,
  ei.model_number,
  ei.equipment_type,
  ei.manufacturer,
  ei.serial_number,
  ei.operational_status,

  -- Facility
  f.facility_name,
  f.city,
  f.region,

  -- Work order metrics
  COALESCE(wo.total_work_orders, 0)             AS total_work_orders,
  COALESCE(wo.completed_work_orders, 0)         AS completed_work_orders,
  COALESCE(wo.open_work_orders, 0)              AS open_work_orders,
  COALESCE(wo.emergency_repairs, 0)             AS emergency_repairs,
  COALESCE(wo.corrective_repairs, 0)            AS corrective_repairs,
  COALESCE(wo.preventive_maintenance_count, 0)  AS preventive_maintenance_count,
  COALESCE(wo.total_labor_hours, 0)             AS total_labor_hours,
  COALESCE(wo.total_parts_cost_usd, 0)          AS total_parts_cost_usd,
  COALESCE(wo.total_downtime_hours, 0)          AS total_downtime_hours,
  COALESCE(wo.avg_labor_hours_per_wo, 0)        AS avg_labor_hours_per_wo,
  wo.last_completed_wo,
  wo.next_scheduled_wo,
  COALESCE(wo.critical_priority_count, 0)       AS critical_priority_count,
  COALESCE(wo.high_priority_count, 0)           AS high_priority_count,

  -- Total maintenance cost (labor at $85/hr + parts)
  ROUND(COALESCE(wo.total_labor_hours, 0) * 85.0 + COALESCE(wo.total_parts_cost_usd, 0), 2) AS total_maintenance_cost_usd,

  -- Certification data (from unstructured pipeline)
  gc.certification_id,
  gc.certification_status,
  gc.safety_rating,
  gc.compliance_standards,

  -- Risk flags
  CASE
    WHEN gc.certification_status = 'CONDITIONAL' AND COALESCE(wo.open_work_orders, 0) > 0
      THEN 'HIGH RISK — Conditional cert with open work orders'
    WHEN gc.certification_status = 'CONDITIONAL'
      THEN 'MEDIUM RISK — Conditional certification'
    WHEN ei.next_inspection_due < current_date()
      THEN 'MEDIUM RISK — Overdue inspection'
    WHEN COALESCE(wo.emergency_repairs, 0) >= 3
      THEN 'ELEVATED — Frequent emergency repairs'
    ELSE 'NORMAL'
  END AS risk_level,

  -- Inspection context
  ei.last_inspection_date,
  ei.next_inspection_due,
  CASE
    WHEN ei.next_inspection_due < current_date() THEN 'Overdue'
    WHEN ei.next_inspection_due < date_add(current_date(), 30) THEN 'Due Soon'
    ELSE 'Current'
  END AS inspection_status,

  -- Contract context
  mc.contract_status,
  mc.sla_response_hours

FROM bronze_equipment_inventory ei

INNER JOIN bronze_facilities f
  ON ei.facility_id = f.facility_id

LEFT JOIN work_order_summary wo
  ON ei.asset_id = wo.asset_id

LEFT JOIN gold_equipment_catalog gc
  ON ei.manufacturer = gc.manufacturer
  AND ei.equipment_type = gc.equipment_type

LEFT JOIN bronze_manufacturer_contracts mc
  ON ei.manufacturer = mc.manufacturer;
