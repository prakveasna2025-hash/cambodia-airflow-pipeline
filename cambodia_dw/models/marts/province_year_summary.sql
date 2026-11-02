{{ config(materialized='table') }}

-- Summary mart: one row per (province, year).
-- Currently 2024 only → 25 rows. Multi-year data grows without schema change.
-- Designed for future multi-indicator columns (population_total, later: gdp, literacy, etc.)

SELECT
    province_key::int          AS province_key,
    province_code::text        AS province_code,
    province_name::text        AS province_name,
    year::smallint             AS year,
    population_total::bigint   AS population_total
FROM {{ ref('int_population_by_province') }}