SELECT
    id,
    customer_name,
    order_total,
    error_reason,
    created_at
FROM orders
WHERE status = 'failed'
  AND created_at > '{{ cursor_value }}'
ORDER BY created_at