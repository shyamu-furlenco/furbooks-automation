-- ============================================================================
-- UNIT TEST: Opening Revenue Boundary Conditions
-- Tests Scenario 1.1 and 1.2 of the opening_revenue CTE in isolation
-- using inline synthetic data (no external table dependencies).
--
-- Compatible with: DuckDB (local), Databricks SQL
-- Run with DuckDB: duckdb < test_opening_revenue_boundaries.sql
-- ============================================================================
-- Reference months: Month1 = 2025-10-01, Month2 = 2025-11-01
-- prev_start (Oct) = 2025-09-01
-- m_start    (Oct) = 2025-10-01
-- m_end      (Oct) = 2025-11-01
-- ============================================================================

-- Synthetic revenue rows covering all boundary scenarios
WITH synthetic_revenue AS (
    -- Case A: Scenario 1.1 — start_date in prev_month, recognised_at is NULL
    -- Expected: counted in opening_revenue
    SELECT 'A' AS case_id, 'ITEM' AS entity_type, 1 AS entity_id, 1 AS user_id,
           DATE '2025-09-15' AS start_date, DATE '2025-10-15' AS end_date,
           NULL::DATE         AS recognised_at, 100.0 AS taxable_amount

    UNION ALL
    -- Case B: Scenario 1.1 — start_date in prev_month, recognised_at before prev_start
    -- Expected: counted in opening_revenue (recognised_at before the window)
    SELECT 'B', 'ITEM', 2, 2, DATE '2025-09-20', DATE '2025-10-20',
           DATE '2025-08-01', 200.0

    UNION ALL
    -- Case C: Scenario 1.1 — start_date in prev_month, recognised_at after m_start
    -- Expected: counted in opening_revenue
    SELECT 'C', 'ITEM', 3, 3, DATE '2025-09-25', DATE '2025-10-25',
           DATE '2025-10-05', 300.0

    UNION ALL
    -- Case D: Scenario 1.2 — start_date >= m_start, recognised_at in prev_month
    -- Expected: counted in opening_revenue AND subtracted in mtp1_adjustment
    SELECT 'D', 'ITEM', 4, 4, DATE '2025-10-05', DATE '2025-11-05',
           DATE '2025-09-10', 400.0

    UNION ALL
    -- Case E: start_date in prev_month, recognised_at IN prev_month
    -- Expected: NOT in opening_revenue (was already recognised in prev month)
    SELECT 'E', 'ITEM', 5, 5, DATE '2025-09-10', DATE '2025-10-10',
           DATE '2025-09-15', 500.0

    UNION ALL
    -- Case F: start_date exactly on m_start, recognised_at = NULL
    -- Scenario 1.1: start_date NOT in prev_month → excluded from Scenario 1.1
    -- Scenario 1.2: start_date >= m_start AND recognised_at in prev_month? NO (NULL)
    -- Expected: NOT counted in opening_revenue
    SELECT 'F', 'ITEM', 6, 6, DATE '2025-10-01', DATE '2025-11-01',
           NULL::DATE, 600.0

    UNION ALL
    -- Case G: start_date exactly on prev_start (lower boundary of Scenario 1.1)
    -- Expected: counted in opening_revenue
    SELECT 'G', 'ITEM', 7, 7, DATE '2025-09-01', DATE '2025-10-01',
           NULL::DATE, 700.0

    UNION ALL
    -- Case H: start_date one day before prev_start (outside window)
    -- Expected: NOT counted in opening_revenue
    SELECT 'H', 'ITEM', 8, 8, DATE '2025-08-31', DATE '2025-09-30',
           NULL::DATE, 800.0
),

months AS (
    SELECT
        1                          AS month_num,
        DATE '2025-10-01'          AS m_start,
        DATE '2025-11-01'          AS m_end,
        DATE '2025-09-01'          AS prev_start
),

-- Replicate opening_revenue logic from the main query
opening_revenue_test AS (
    SELECT
        r.case_id,
        r.taxable_amount,
        CASE
            WHEN (
                -- Scenario 1.1
                r.start_date >= m.prev_start
                AND r.start_date <  m.m_start
                AND (r.recognised_at IS NULL
                     OR r.recognised_at <  m.prev_start
                     OR r.recognised_at >= m.m_start)
            ) OR (
                -- Scenario 1.2
                r.start_date >= m.m_start
                AND r.recognised_at >= m.prev_start
                AND r.recognised_at <  m.m_start
            )
            THEN TRUE
            ELSE FALSE
        END AS in_opening
    FROM synthetic_revenue r
    CROSS JOIN months m
),

-- Replicate mtp1_adjustment logic
mtp1_adjustment_test AS (
    SELECT
        r.case_id,
        r.taxable_amount,
        CASE
            WHEN r.start_date    >= m.m_start
             AND r.recognised_at >= m.prev_start
             AND r.recognised_at <  m.m_start
            THEN TRUE
            ELSE FALSE
        END AS in_mtp1
    FROM synthetic_revenue r
    CROSS JOIN months m
),

-- Assertions
assertions AS (
    SELECT
        o.case_id,
        o.in_opening,
        t.in_mtp1,
        CASE o.case_id
            WHEN 'A' THEN o.in_opening = TRUE  AND t.in_mtp1 = FALSE
            WHEN 'B' THEN o.in_opening = TRUE  AND t.in_mtp1 = FALSE
            WHEN 'C' THEN o.in_opening = TRUE  AND t.in_mtp1 = FALSE
            WHEN 'D' THEN o.in_opening = TRUE  AND t.in_mtp1 = TRUE   -- Scenario 1.2: both true
            WHEN 'E' THEN o.in_opening = FALSE AND t.in_mtp1 = FALSE
            WHEN 'F' THEN o.in_opening = FALSE AND t.in_mtp1 = FALSE
            WHEN 'G' THEN o.in_opening = TRUE  AND t.in_mtp1 = FALSE
            WHEN 'H' THEN o.in_opening = FALSE AND t.in_mtp1 = FALSE
            ELSE FALSE
        END AS test_passed
    FROM opening_revenue_test o
    JOIN mtp1_adjustment_test t ON o.case_id = t.case_id
)

SELECT
    case_id,
    in_opening,
    in_mtp1,
    CASE WHEN test_passed THEN 'PASS' ELSE 'FAIL' END AS result
FROM assertions
ORDER BY case_id;

-- Expected output:
-- A | true  | false | PASS
-- B | true  | false | PASS
-- C | true  | false | PASS
-- D | true  | true  | PASS   ← Scenario 1.2: opening includes it, mtp1 removes it
-- E | false | false | PASS
-- F | false | false | PASS
-- G | true  | false | PASS
-- H | false | false | PASS
