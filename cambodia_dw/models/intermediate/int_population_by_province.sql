{{ config(materialized='view') }}

-- Intermediate: population fact joined to province dimension.
-- Reusable — any mart that needs population-by-province starts here.
-- Grain: one row per (province_key, year).

SELECT
    f.province_key::int                AS province_key,
    p.province_code::text              AS province_code,
    p.province_name::text              AS province_name,
    f.snapshot_year::smallint          AS year,
    f.population::bigint               AS population_total
FROM {{ ref('fact_population') }} f
JOIN {{ ref('dim_province') }} p
    ON f.province_key = p.province_key