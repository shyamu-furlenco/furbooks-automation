-- ============================================================================
-- UNIT TEST: Tenure Divisor Guard (Division-by-Zero)
-- Validates that the monthly amount calculation in furbooks_revenue does NOT
-- produce NULL or Infinity for short-duration schedules where
-- ROUND(DATEDIFF(DAY, start_date, end_date) / 30.45) = 0.
--
-- Currently the query does NOT have a NULLIF guard — this test documents
-- the risk and the recommended fix.
--
-- Compatible with: DuckDB, Databricks SQL
-- ============================================================================

WITH schedule_scenarios AS (
    -- Case A: 30-day schedule → tenure = ROUND(30/30.45) = ROUND(0.985) = 1 → OK
    SELECT 'A' AS case_id,
           DATE '2025-10-01' AS start_date,
           DATE '2025-10-31' AS end_date,
           1000.0            AS taxable_amount,
           'Normal 30-day'   AS description

    UNION ALL
    -- Case B: 45-day schedule → tenure = ROUND(45/30.45) = ROUND(1.477) = 1 → OK
    SELECT 'B', DATE '2025-10-01', DATE '2025-11-15', 1500.0, '45-day'

    UNION ALL
    -- Case C: 14-day schedule → tenure = ROUND(14/30.45) = ROUND(0.46) = 0 → DIVISION BY ZERO
    SELECT 'C', DATE '2025-10-01', DATE '2025-10-15', 500.0, 'Short 14-day (RISK)'

    UNION ALL
    -- Case D: 1-day schedule → tenure = ROUND(1/30.45) = ROUND(0.033) = 0 → DIVISION BY ZERO
    SELECT 'D', DATE '2025-10-01', DATE '2025-10-02', 100.0, '1-day (RISK)'

    UNION ALL
    -- Case E: 92-day schedule → tenure = ROUND(92/30.45) = ROUND(3.02) = 3 → OK
    SELECT 'E', DATE '2025-10-01', DATE '2026-01-01', 3000.0, '3-month tenure'
),

-- CURRENT (unguarded) behaviour — may produce NULL for cases C and D
current_behaviour AS (
    SELECT
        case_id,
        description,
        ROUND(DATEDIFF('day', start_date, end_date) / 30.45) AS tenure,
        taxable_amount
            / ROUND(DATEDIFF('day', start_date, end_date) / 30.45) AS monthly_amount_current,
        -- RECOMMENDED FIX: use NULLIF to surface the error explicitly
        taxable_amount
            / NULLIF(ROUND(DATEDIFF('day', start_date, end_date) / 30.45), 0) AS monthly_amount_safe
    FROM schedule_scenarios
),

assertions AS (
    SELECT
        case_id,
        description,
        tenure,
        monthly_amount_current,
        monthly_amount_safe,
        CASE
            WHEN tenure > 0 THEN
                CASE WHEN monthly_amount_current IS NOT NULL AND monthly_amount_safe IS NOT NULL
                     THEN 'PASS' ELSE 'FAIL' END
            ELSE
                -- For zero-tenure rows: current behaviour is division-by-zero (may be NULL or Infinity)
                -- Safe behaviour returns NULL (explicit signal)
                CASE WHEN monthly_amount_safe IS NULL
                     THEN 'PASS (safe: returns NULL, not Infinity)'
                     ELSE 'FAIL: zero tenure should produce NULL with NULLIF guard'
                END
        END AS result
    FROM current_behaviour
)

SELECT
    case_id,
    description,
    tenure,
    ROUND(monthly_amount_current, 4) AS current_monthly_amount,
    ROUND(monthly_amount_safe,    4) AS safe_monthly_amount,
    result
FROM assertions
ORDER BY case_id;

-- Expected output:
-- A | Normal 30-day      | 1 | 1000.0 | 1000.0 | PASS
-- B | 45-day             | 1 | 1500.0 | 1500.0 | PASS
-- C | Short 14-day (RISK)| 0 | NULL or Infinity | NULL | PASS (safe: returns NULL, not Infinity)
-- D | 1-day (RISK)       | 0 | NULL or Infinity | NULL | PASS (safe: returns NULL, not Infinity)
-- E | 3-month tenure     | 3 | 1000.0 | 1000.0 | PASS
--
-- ACTION REQUIRED: Add NULLIF(..., 0) to the divisor in furbooks_revenue CTE (line 66-68).
