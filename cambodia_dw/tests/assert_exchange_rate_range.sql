-- Business rule: KHR/USD exchange rate must be in a plausible range.
-- Historical range is roughly 3,970-4,300. Allowing 3,000-5,000 for safety.
-- Returns rows that violate the rule; test fails if any row returned.

SELECT *
FROM {{ ref('fact_exchange_rate') }}
WHERE exchange_rate < 3000
   OR exchange_rate > 5000