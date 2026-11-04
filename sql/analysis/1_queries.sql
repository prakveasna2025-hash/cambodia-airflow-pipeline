-- Day 23 — SQL analysis queries
-- Run: Get-Content sql\analysis\day23_queries.sql | docker exec -i cambodia_dw_postgres psql -U airflow -d airflow

\echo ''
\echo '=== Q1: Baseline — province count and total population ==='
SELECT
    COUNT(*)                    AS province_count,
    SUM(population)             AS total_population,
    MIN(population)             AS smallest_province_pop,
    MAX(population)             AS largest_province_pop
FROM dbt_dev_marts.fact_population;

\echo ''
\echo '=== Q2: Top 5 provinces by population, with % of total ==='
WITH total AS (
    SELECT SUM(population) AS grand_total
    FROM dbt_dev_marts.fact_population
)
SELECT
    p.province_name,
    p.population,
    ROUND(100.0 * p.population / t.grand_total, 2) AS pct_of_total
FROM dbt_dev_marts.fact_population p
CROSS JOIN total t
ORDER BY p.population DESC
LIMIT 5;

\echo ''
\echo '=== Q3: Average exchange rate per year (2003-2023) ==='
SELECT
    d.year,
    COUNT(*)                                AS trading_days,
    ROUND(AVG(f.exchange_rate)::numeric, 2) AS avg_rate,
    MIN(f.exchange_rate)                    AS min_rate,
    MAX(f.exchange_rate)                    AS max_rate
FROM dbt_dev_marts.fact_exchange_rate f
JOIN dbt_dev_marts.dim_date d ON f.date_key = d.date_key
GROUP BY d.year
ORDER BY d.year;

\echo ''
\echo '=== Q4: Highest-volatility years (max - min of exchange_rate) ==='
SELECT
    d.year,
    (MAX(f.exchange_rate) - MIN(f.exchange_rate)) AS volatility
FROM dbt_dev_marts.fact_exchange_rate f
JOIN dbt_dev_marts.dim_date d ON f.date_key = d.date_key
GROUP BY d.year
ORDER BY volatility DESC
LIMIT 5;

\echo ''
\echo '=== Q5: Data completeness — days in dim_date with vs without exchange-rate data ==='
SELECT
    d.year,
    COUNT(*)                        AS days_in_dim_date,
    COUNT(f.date_key)               AS days_with_data,
    COUNT(*) - COUNT(f.date_key)    AS days_missing
FROM dbt_dev_marts.dim_date d
LEFT JOIN dbt_dev_marts.fact_exchange_rate f ON d.date_key = f.date_key
GROUP BY d.year
ORDER BY days_missing DESC, d.year
LIMIT 5;

\echo ''
\echo '=== Q6: Weekday vs weekend average exchange rate ==='
SELECT
    d.is_weekend,
    COUNT(*)                                    AS trading_days,
    ROUND(AVG(f.exchange_rate)::numeric, 2)     AS avg_rate,
    ROUND(AVG(f.spread_value)::numeric, 4)      AS avg_spread
FROM dbt_dev_marts.fact_exchange_rate f
JOIN dbt_dev_marts.dim_date d ON f.date_key = d.date_key
GROUP BY d.is_weekend
ORDER BY d.is_weekend;

\echo ''
\echo '=== Q7: Provinces above national average population ==='
WITH avg_pop AS (
    SELECT AVG(population) AS avg_population
    FROM dbt_dev_marts.fact_population
)
SELECT
    p.province_name,
    p.population,
    ROUND(a.avg_population, 0)                AS national_average,
    ROUND(p.population - a.avg_population, 0) AS above_by
FROM dbt_dev_marts.fact_population p
CROSS JOIN avg_pop a
WHERE p.population > a.avg_population
ORDER BY p.population DESC;

\echo ''
\echo '=== Q8: 30-day rolling average exchange rate (last 10 days) ==='
SELECT
    f.date,
    f.exchange_rate,
    ROUND(
        AVG(f.exchange_rate) OVER (
            ORDER BY f.date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        )::numeric, 2
    ) AS rolling_30d_avg
FROM dbt_dev_marts.fact_exchange_rate f
ORDER BY f.date DESC
LIMIT 10;