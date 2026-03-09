SELECT
    u.usage_date,
    u.sku_name,
    u.billing_origin_product,
    u.usage_unit,
    ROUND(SUM(u.usage_quantity), 4) AS total_dbus,
    ROUND(SUM(u.usage_quantity * COALESCE(p.pricing.default, 0)), 2) AS estimated_cost_usd
FROM system.billing.usage u
LEFT JOIN system.billing.list_prices p
    ON u.sku_name = p.sku_name AND u.cloud = p.cloud AND p.price_end_time IS NULL
WHERE u.usage_date >= current_date() - 90
  AND u.usage_metadata.dlt_pipeline_id = '82618e85-2543-4d24-9134-dc30f32e2de6'
GROUP BY u.usage_date, u.sku_name, u.billing_origin_product, u.usage_unit
ORDER BY u.usage_date DESC