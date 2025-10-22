-- 1️⃣ Отбор заказов за последние N дней
WITH period_orders AS (
    SELECT *
    FROM orders
    WHERE datetime(date) >= datetime('now', '-' || :days || ' days')
),

-- 2️⃣ Дедупликация по external_id:
-- Если есть delivered_at — берём с максимальным delivered_at,
-- иначе — с максимальным updated_at
orders_dedup AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY external_id
                ORDER BY
                    CASE WHEN delivered_at IS NOT NULL THEN 0 ELSE 1 END,
                    datetime(COALESCE(delivered_at, updated_at)) DESC
            ) AS rn
        FROM period_orders
    )
    WHERE rn = 1
)

-- 3️⃣ Финальная выгрузка
SELECT
    od.id                         AS order_id,
    od.date,
    od.channel,
    s.name                        AS seller,
    od.external_id,
    i.sku,
    i.qty,
    i.revenue,
    i.cost,
    ROUND(i.revenue - i.cost, 2)  AS margin,
    od.status
FROM orders_dedup od
LEFT JOIN order_items i ON od.id = i.order_id
LEFT JOIN sellers s     ON od.seller_id = s.id
ORDER BY datetime(od.date), od.id, i.sku;
