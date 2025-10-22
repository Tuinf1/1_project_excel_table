-- 27–31. CTE period_orders, ranked, best (уже готово из предыдущего шага)
WITH period_orders AS (
    SELECT *
    FROM orders
    WHERE datetime(date) >= datetime('now', '-' || :days || ' days')
),
ranked AS (
    SELECT
        o.*,
        ROW_NUMBER() OVER (
            PARTITION BY external_id
            ORDER BY
                CASE WHEN delivered_at IS NOT NULL THEN 0 ELSE 1 END,
                datetime(COALESCE(delivered_at, updated_at)) DESC
        ) AS rn
    FROM period_orders o
),
best AS (
    SELECT *
    FROM ranked
    WHERE rn = 1
)

-- 32–34. Финальная выгрузка: join + расчёт margin + сортировка
SELECT
    b.id                         AS order_id,
    b.date,
    b.channel,
    s.name                       AS seller,
    b.external_id,
    i.sku,
    i.qty,
    i.revenue,
    i.cost,
    ROUND(i.revenue - i.cost, 2) AS margin,
    b.status
FROM best b
LEFT JOIN order_items i ON b.id = i.order_id
LEFT JOIN sellers s     ON b.seller_id = s.id
ORDER BY datetime(b.date), b.id, i.sku;
