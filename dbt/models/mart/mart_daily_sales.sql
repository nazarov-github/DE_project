SELECT order_date::date as sales_date,
       COUNT(order_id) as order_count,
       SUM(quantity) as total_items_sold,
       SUM(item_total) as total_revenue,
       ROUND(SUM(item_total) / NULLIF(COUNT(order_id), 0), 2) as avg_order_value
FROM {{ ref("int_order_details") }}
WHERE order_status = 'Completed'
group by order_date::date