SELECT
    id,
    customer_name,
    amount,
    currency,
    created_at
FROM orders
WHERE amount > 1000
ORDER BY created_at DESC
