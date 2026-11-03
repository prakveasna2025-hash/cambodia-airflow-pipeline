-- Business rule: all exchange rate columns must be positive.
-- Returns rows that violate the rule; test fails if any row returned.

SELECT *
FROM {{ ref('fact_exchange_rate') }}
WHERE purchase <= 0
   OR sale <= 0
   OR midpoint <= 0
   OR exchange_rate <= 0