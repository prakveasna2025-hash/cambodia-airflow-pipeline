-- Business rule: population must be positive.
-- Returns rows that violate the rule; test fails if any row returned.

SELECT *
FROM {{ ref('fact_population') }}
WHERE population <= 0