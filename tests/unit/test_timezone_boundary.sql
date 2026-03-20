-- ============================================================================
-- UNIT TEST: Timezone Boundary (UTC → IST)
-- Validates that recognised_at + INTERVAL '330 minutes' correctly shifts
-- late-UTC timestamps into the correct IST month.
--
-- The risk: a transaction recognised at 23:30 UTC on Oct 31 (last day of month)
-- maps to 05:00 IST on Nov 1 — it belongs to November in IST, not October.
-- If the query uses DATE(recognised_at) without the +330 offset, it would be
-- misassigned to October.
--
-- Compatible with: DuckDB (local), Databricks SQL
-- ============================================================================

WITH utc_timestamps AS (
    -- Case A: recognised_at = 2025-10-31 22:59 UTC → IST = 2025-11-01 04:29 → November
    SELECT 'A' AS case_id,
           TIMESTAMP '2025-10-31 22:59:00' AS recognised_at_utc,
           DATE '2025-11-01'               AS expected_ist_date,
           'November (IST)'                AS expected_month

    UNION ALL
    -- Case B: recognised_at = 2025-10-31 18:00 UTC → IST = 2025-11-01 00:30 → November
    SELECT 'B',
           TIMESTAMP '2025-10-31 18:00:00',
           DATE '2025-11-01',
           'November (IST)'

    UNION ALL
    -- Case C: recognised_at = 2025-10-31 17:29 UTC → IST = 2025-10-31 23:59 → October
    SELECT 'C',
           TIMESTAMP '2025-10-31 17:29:00',
           DATE '2025-10-31',
           'October (IST)'

    UNION ALL
    -- Case D: recognised_at = 2025-11-01 00:00 UTC → IST = 2025-11-01 05:30 → November
    SELECT 'D',
           TIMESTAMP '2025-11-01 00:00:00',
           DATE '2025-11-01',
           'November (IST)'

    UNION ALL
    -- Case E: recognised_at = 2025-09-30 18:30 UTC → IST = 2025-10-01 00:00 → October (boundary)
    SELECT 'E',
           TIMESTAMP '2025-09-30 18:30:00',
           DATE '2025-10-01',
           'October (IST)'
),

computed AS (
    SELECT
        case_id,
        expected_ist_date,
        expected_month,
        -- Correct: with UTC→IST offset
        CAST(recognised_at_utc + INTERVAL '330 minutes' AS DATE) AS actual_ist_date_correct,
        -- Incorrect: without offset (common bug)
        CAST(recognised_at_utc AS DATE)                          AS actual_date_no_offset
    FROM utc_timestamps
),

assertions AS (
    SELECT
        case_id,
        expected_ist_date,
        actual_ist_date_correct,
        actual_date_no_offset,
        actual_ist_date_correct = expected_ist_date  AS offset_correct,
        actual_date_no_offset   = expected_ist_date  AS no_offset_correct
    FROM computed
)

SELECT
    case_id,
    expected_ist_date,
    actual_ist_date_correct,
    actual_date_no_offset,
    CASE WHEN offset_correct   THEN 'PASS' ELSE 'FAIL: +330 offset produces wrong date' END AS with_offset_result,
    CASE WHEN no_offset_correct THEN 'PASS' ELSE 'FAIL (expected): raw UTC date is wrong' END AS without_offset_result
FROM assertions
ORDER BY case_id;

-- Expected output:
-- A | 2025-11-01 | 2025-11-01 | 2025-10-31 | PASS | FAIL (expected)  ← offset critical
-- B | 2025-11-01 | 2025-11-01 | 2025-10-31 | PASS | FAIL (expected)  ← offset critical
-- C | 2025-10-31 | 2025-10-31 | 2025-10-31 | PASS | PASS             ← both agree
-- D | 2025-11-01 | 2025-11-01 | 2025-11-01 | PASS | PASS             ← both agree
-- E | 2025-10-01 | 2025-10-01 | 2025-09-30 | PASS | FAIL (expected)  ← offset critical
--
-- Cases A, B, E show that omitting +330 offset misassigns transactions to the wrong month.
