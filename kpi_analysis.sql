-- =========================================================================
-- Amazon Athena SQL Queries für Fashion E-Commerce KPI Analyse
-- =========================================================================

-- 1. Kundenwertanalyse CLV mit Kohortenanalyse
WITH CustomerRevenue AS (
    SELECT
        o.userkey AS customer_id,
        SUM(o.total_price) AS total_spent,
        COUNT(DISTINCT o.order_id) AS total_orders,
        t.month AS first_order_month
    FROM fact_orders o
    JOIN dim_time t ON o.year = t.year AND o.month = t.month
    GROUP BY o.userkey, t.month
)
SELECT
    first_order_month AS "Monat",
    COUNT(customer_id) AS "Anzahl Kunden",
    ROUND(AVG(total_spent), 2) AS "Durchschnittlicher CLV (€)",
    ROUND(AVG(total_orders), 2) AS "Durchschnittliche Bestellungen pro Kunde"
FROM CustomerRevenue
GROUP BY first_order_month
ORDER BY first_order_month;

-- -------------------------------------------------------------------------
-- 2. Umsatz pro Bestellung und Kundenbindung nach Land
WITH CountrySales AS (
    SELECT
        u.country,
        ROUND(AVG(o.total_price), 2) AS avg_order_value,
        COUNT(o.order_id) AS total_orders,
        COUNT(DISTINCT o.userkey) AS unique_customers
    FROM fact_orders o
    JOIN dim_users u ON o.userkey = u.userkey
    GROUP BY u.country
),
CustomerOrderStats AS (
    SELECT
        o.userkey,
        u.country,
        COUNT(o.order_id) AS orders_per_customer
    FROM fact_orders o
    JOIN dim_users u ON o.userkey = u.userkey
    GROUP BY o.userkey, u.country
)
SELECT
    cs.country,
    cs.avg_order_value,
    cs.total_orders,
    cs.unique_customers,
    ROUND(AVG(co.orders_per_customer), 2) AS avg_orders_per_customer
FROM CountrySales cs
JOIN CustomerOrderStats co ON cs.country = co.country
GROUP BY cs.country, cs.avg_order_value, cs.total_orders, cs.unique_customers
ORDER BY avg_order_value DESC;

-- -------------------------------------------------------------------------
-- 3. Kundenwertanalyse (CLV) nach Produktkategorien
WITH CLV_Calculation AS (
    SELECT
        p.category,
        o.userkey,
        ROUND(SUM(o.total_price), 2) AS total_spent,
        COUNT(DISTINCT o.order_id) AS total_orders,
        ROUND(SUM(o.total_price)/COUNT(DISTINCT o.order_id), 2) AS avg_order_value
    FROM fact_orders o
    JOIN dim_products p ON o.productkey = p.productkey
    GROUP BY p.category, o.userkey
)
SELECT
    category,
    ROUND(AVG(total_spent), 2) AS avg_clv,
    ROUND(AVG(avg_order_value), 2) AS avg_order_value_per_segment
FROM CLV_Calculation
GROUP BY category
ORDER BY avg_clv DESC;

-- -------------------------------------------------------------------------
-- 4. Retourenquote pro Monat (Saisonalität)
WITH ReturnAnalysis AS (
    SELECT
        t.month,
        p.category,
        u.premium_member,
        COUNT(CASE WHEN LOWER(TRIM(o.order_status)) = 'returned' THEN o.order_id END) AS total_returns,
        COUNT(o.order_id) AS total_orders,
        ROUND(
            CAST(COUNT(CASE WHEN LOWER(TRIM(o.order_status)) = 'returned' THEN o.order_id END) AS DECIMAL(18,2))
            / NULLIF(CAST(COUNT(o.order_id) AS DECIMAL(18,2)), 0) * 100, 2
        ) AS return_rate
    FROM fact_orders o
    JOIN dim_products p ON o.productkey = p.productkey
    JOIN dim_users u ON o.userkey = u.userkey
    JOIN dim_time t ON o.date_id = t.date_id
    WHERE o.order_status IS NOT NULL
    GROUP BY t.month, p.category, u.premium_member
)
SELECT * FROM ReturnAnalysis
ORDER BY month, return_rate DESC;

-- -------------------------------------------------------------------------
-- 5. Marktanteil pro Kategorie basierend auf Umsatz und Rückgaben
WITH RevenueByCategory AS (
    SELECT
        p.category,
        ROUND(SUM(o.total_price), 2) AS total_revenue,
        ROUND(SUM(CASE WHEN LOWER(TRIM(o.order_status)) = 'returned' THEN o.total_price ELSE 0 END), 2) AS total_returned_revenue
    FROM fact_orders o
    JOIN dim_products p ON o.productkey = p.productkey
    WHERE o.order_status IS NOT NULL
    GROUP BY p.category
),
TotalMarketRevenue AS (
    SELECT ROUND(SUM(total_revenue), 2) AS total_market_revenue
    FROM RevenueByCategory
)
SELECT
    r.category,
    r.total_revenue,
    r.total_returned_revenue,
    ROUND((r.total_revenue / t.total_market_revenue) * 100, 2) AS market_share_percentage,
    ROUND((r.total_returned_revenue / NULLIF(r.total_revenue, 0)) * 100, 2) AS return_impact_percentage
FROM RevenueByCategory r
CROSS JOIN TotalMarketRevenue t
ORDER BY market_share_percentage DESC;

-- -------------------------------------------------------------------------
-- 6. Kundensegmentierung basierend auf Bestellwert und Häufigkeit
WITH CustomerActivity AS (
    SELECT
        o.userkey,
        COUNT(o.order_id) AS total_orders,
        ROUND(SUM(o.total_price), 2) AS total_revenue
    FROM fact_orders o
    GROUP BY o.userkey
),
RankedCustomers AS (
    SELECT
        userkey,
        total_orders,
        total_revenue,
        NTILE(5) OVER (ORDER BY total_orders DESC) AS order_segment,
        NTILE(5) OVER (ORDER BY total_revenue DESC) AS revenue_segment
    FROM CustomerActivity
),
SegmentedCustomers AS (
    SELECT
        userkey,
        CASE
            WHEN order_segment = 1 AND revenue_segment = 1 THEN 'Extrem Aktiver Top-Spender'
            WHEN order_segment <= 2 AND revenue_segment <= 2 THEN 'Sehr Aktiver Spender'
            WHEN order_segment = 3 OR revenue_segment = 3 THEN 'Mittlerer Aktivitäts-Spender'
            WHEN order_segment = 4 OR revenue_segment = 4 THEN 'Gelegentlicher Käufer'
            ELSE 'Inaktiver Kunde'
        END AS customer_segment
    FROM RankedCustomers
)
SELECT
    customer_segment,
    COUNT(userkey) AS customer_count
FROM SegmentedCustomers
GROUP BY customer_segment
ORDER BY customer_count DESC;

-- -------------------------------------------------------------------------
-- 7. Berechnung der monatlichen Wachstumsrate
WITH MonthlyOrders AS (
    SELECT
        t.year,
        t.month,
        ROUND(SUM(o.total_price), 2) AS total_revenue
    FROM fact_orders o
    JOIN dim_time t ON o.year = t.year AND o.month = t.month
    GROUP BY t.year, t.month
),
GrowthRate AS (
    SELECT
        year,
        month,
        CAST(total_revenue AS DECIMAL(18,2)) AS total_revenue,
        CAST(LAG(total_revenue, 1) OVER (ORDER BY year, month) AS DECIMAL(18,2)) AS previous_revenue,
        ROUND(
            CASE
                WHEN LAG(total_revenue, 1) OVER (ORDER BY year, month) IS NOT NULL
                AND LAG(total_revenue, 1) OVER (ORDER BY year, month) > 0
                THEN (total_revenue - LAG(total_revenue, 1) OVER (ORDER BY year, month))
                / LAG(total_revenue, 1) OVER (ORDER BY year, month) * 100
                ELSE NULL
            END, 2
        ) AS revenue_growth_rate
    FROM MonthlyOrders
)
SELECT * FROM GrowthRate;