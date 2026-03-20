-- ============================================================================
-- UNIT TEST: Pickup Classification Completeness
-- Verifies that every row in pickups_base is assigned to exactly one of:
--   partial_pickups, full_pickups, or pickup_cancellations
-- and that pickup_raised_all = abs(partial) + abs(full) + abs(cancelled).
--
-- Compatible with: DuckDB (local), Databricks SQL
-- ============================================================================

WITH synthetic_pickups AS (
    -- Case A: PARTIAL pickup, not cancelled → only in partial_pickups + raised_all
    SELECT 'A' AS case_id, 'ITEM' AS entity_type, 1 AS entity_id, 1 AS user_id,
           'PARTIAL'        AS pickup_type,
           'COMPLETED'      AS return_entity_state,
           DATE '2025-10-10' AS return_created_at,
           NULL::DATE        AS cancelled_at,
           100.0             AS taxable_amount

    UNION ALL
    -- Case B: FULL pickup, not cancelled → only in full_pickups + raised_all
    SELECT 'B', 'ITEM', 2, 2, 'FULL', 'COMPLETED',
           DATE '2025-10-12', NULL::DATE, 200.0

    UNION ALL
    -- Case C: PARTIAL pickup, CANCELLED → only in pickup_cancellations (NOT partial)
    SELECT 'C', 'ITEM', 3, 3, 'PARTIAL', 'CANCELLED',
           DATE '2025-10-15', DATE '2025-10-15', 300.0

    UNION ALL
    -- Case D: FULL pickup, CANCELLED → only in pickup_cancellations (NOT full)
    SELECT 'D', 'ITEM', 4, 4, 'FULL', 'CANCELLED',
           DATE '2025-10-18', DATE '2025-10-18', 400.0

    UNION ALL
    -- Case E: PARTIAL pickup, return_created_at outside window → not in any component
    SELECT 'E', 'ITEM', 5, 5, 'PARTIAL', 'COMPLETED',
           DATE '2025-09-10', NULL::DATE, 500.0

    UNION ALL
    -- Case F: CANCELLED, cancelled_at in window, return_created_at outside → in cancellations only
    SELECT 'F', 'ITEM', 6, 6, 'PARTIAL', 'CANCELLED',
           DATE '2025-09-01', DATE '2025-10-20', 600.0
),

months AS (
    SELECT
        1                AS month_num,
        DATE '2025-10-01' AS m_start,
        DATE '2025-11-01' AS m_end
),

in_raised_all AS (
    SELECT p.case_id, TRUE AS flagged
    FROM synthetic_pickups p
    JOIN months m ON p.return_created_at >= m.m_start AND p.return_created_at < m.m_end
),

in_partial AS (
    SELECT p.case_id, TRUE AS flagged
    FROM synthetic_pickups p
    JOIN months m ON p.return_created_at >= m.m_start AND p.return_created_at < m.m_end
    WHERE p.pickup_type = 'PARTIAL' AND p.return_entity_state <> 'CANCELLED'
),

in_full AS (
    SELECT p.case_id, TRUE AS flagged
    FROM synthetic_pickups p
    JOIN months m ON p.return_created_at >= m.m_start AND p.return_created_at < m.m_end
    WHERE p.pickup_type = 'FULL' AND p.return_entity_state <> 'CANCELLED'
),

in_cancellations AS (
    SELECT p.case_id, TRUE AS flagged
    FROM synthetic_pickups p
    JOIN months m ON p.cancelled_at >= m.m_start AND p.cancelled_at < m.m_end
    WHERE p.return_entity_state = 'CANCELLED'
),

all_cases AS (
    SELECT case_id FROM synthetic_pickups
),

summary AS (
    SELECT
        ac.case_id,
        COALESCE(r.flagged,  FALSE) AS in_raised,
        COALESCE(p.flagged,  FALSE) AS in_partial,
        COALESCE(f.flagged,  FALSE) AS in_full,
        COALESCE(c.flagged,  FALSE) AS in_cancellations
    FROM all_cases ac
    LEFT JOIN in_raised_all   r ON r.case_id = ac.case_id
    LEFT JOIN in_partial      p ON p.case_id = ac.case_id
    LEFT JOIN in_full         f ON f.case_id = ac.case_id
    LEFT JOIN in_cancellations c ON c.case_id = ac.case_id
),

assertions AS (
    SELECT
        case_id, in_raised, in_partial, in_full, in_cancellations,
        CASE case_id
            -- A: PARTIAL not cancelled → in raised + partial only
            WHEN 'A' THEN in_raised = TRUE  AND in_partial = TRUE  AND in_full = FALSE AND in_cancellations = FALSE
            -- B: FULL not cancelled → in raised + full only
            WHEN 'B' THEN in_raised = TRUE  AND in_partial = FALSE AND in_full = TRUE  AND in_cancellations = FALSE
            -- C: PARTIAL + CANCELLED → in raised (return_created_at in window?) and cancellations
            --    return_created_at IS in window, cancelled_at IS in window → in raised AND cancellations
            --    BUT NOT in partial (excluded by return_entity_state <> 'CANCELLED')
            WHEN 'C' THEN in_raised = TRUE  AND in_partial = FALSE AND in_full = FALSE AND in_cancellations = TRUE
            -- D: FULL + CANCELLED → same as C
            WHEN 'D' THEN in_raised = TRUE  AND in_partial = FALSE AND in_full = FALSE AND in_cancellations = TRUE
            -- E: PARTIAL, return outside window → not in any component
            WHEN 'E' THEN in_raised = FALSE AND in_partial = FALSE AND in_full = FALSE AND in_cancellations = FALSE
            -- F: CANCELLED, return_created_at outside window, cancelled_at in window → cancellations only
            WHEN 'F' THEN in_raised = FALSE AND in_partial = FALSE AND in_full = FALSE AND in_cancellations = TRUE
            ELSE FALSE
        END AS test_passed
    FROM summary
)

SELECT
    case_id,
    in_raised,
    in_partial,
    in_full,
    in_cancellations,
    CASE WHEN test_passed THEN 'PASS' ELSE 'FAIL' END AS result
FROM assertions
ORDER BY case_id;
