-- ============================================================================
-- DATA QUALITY PRE-CHECKS
-- Run these BEFORE the main revenue movement query.
-- Any FAIL result should halt execution and alert the analytics team.
-- ============================================================================
-- Parameters: :month1_start, :month2_start (e.g. '2025-10-01', '2025-11-01')
-- ============================================================================

-- ----------------------------------------------------------------------------
-- DQ-1: Revenue Recognition Schedules — Division-by-Zero Guard
-- Detects short-duration schedules where the tenure divisor rounds to 0,
-- which would produce NULL or Infinity in furbooks_revenue.taxable_amount.
-- ----------------------------------------------------------------------------
SELECT
    'DQ-1: Short-duration schedules (tenure divisor = 0)'                      AS check_name,
    COUNT(*)                                                                   AS failing_rows,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END                        AS result
FROM furlenco_silver.furbooks_evolve.revenue_recognition_schedules
WHERE vertical = 'FURLENCO_RENTAL'
  AND state NOT IN ('CANCELLED', 'INVALIDATED')
  AND ROUND(DATEDIFF(DAY, start_date, end_date) / 30.45) = 0
  AND start_date >= ADD_MONTHS(CAST(:month1_start AS DATE), -1);

-- ----------------------------------------------------------------------------
-- DQ-2: Duplicate return records
-- Detects item_ids with more than one COMPLETED return in the analysis window.
-- These are collapsed by MIN(created_at) in deduped_return_items; >1 row here
-- may indicate data anomalies (re-rental scenarios) requiring validation.
-- ----------------------------------------------------------------------------
SELECT
    'DQ-2: Items with multiple completed returns'                               AS check_name,
    COUNT(*)                                                                   AS items_with_multiple_returns,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARN: review deduped_return_items' END AS result
FROM (
    SELECT item_id, COUNT(*) AS return_count
    FROM furlenco_silver.order_management_systems_evolve.return_items
    WHERE state = 'COMPLETED'
    GROUP BY item_id
    HAVING COUNT(*) > 1
) multi_returns;

-- ----------------------------------------------------------------------------
-- DQ-3: Revenue recognitions with NULL accountable_entity_id
-- A NULL entity_id cannot join to sms_entity, silently dropping user_id.
-- ----------------------------------------------------------------------------
SELECT
    'DQ-3: Revenue recognitions with NULL entity_id'                           AS check_name,
    COUNT(*)                                                                   AS failing_rows,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END                        AS result
FROM furlenco_silver.furbooks_evolve.revenue_recognitions
WHERE vertical = 'FURLENCO_RENTAL'
  AND state NOT IN ('CANCELLED', 'INVALIDATED')
  AND accountable_entity_id IS NULL
  AND DATE(created_at + INTERVAL '330 minutes') >= CAST(:month1_start AS DATE);

-- ----------------------------------------------------------------------------
-- DQ-4: Pickup base — rows with both PARTIAL/FULL type and CANCELLED state
-- These should appear only in pickup_cancellations but the current logic
-- also allows them to appear in partial_pickups / full_pickups unless the
-- NOT CANCELLED filter is applied consistently.
-- ----------------------------------------------------------------------------
SELECT
    'DQ-4: Pickups with valid type but CANCELLED return state'                 AS check_name,
    COUNT(*)                                                                   AS ambiguous_rows,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARN: verify pickup classification' END AS result
FROM furlenco_analytics.user_defined_tables.pickup_revenue_movement_item_attachs
WHERE pickup_type IN ('PARTIAL', 'FULL')
  AND return_entity_state = 'CANCELLED'
  AND DATE(return_created_at) >= ADD_MONTHS(CAST(:month1_start AS DATE), -1);

-- ----------------------------------------------------------------------------
-- DQ-5: VAS types not covered by the categorisation CASE
-- Any vas_type falling into 'VAS Revenue - Other' should be reviewed.
-- ----------------------------------------------------------------------------
SELECT
    'DQ-5: Uncategorised VAS types'                                            AS check_name,
    vas.type                                                                   AS vas_type,
    COUNT(*)                                                                   AS row_count
FROM furlenco_silver.order_management_systems_evolve.Value_Added_Services vas
WHERE vas.state <> 'CANCELLED'
  AND vas.type NOT IN (
      'FURLENCO_CARE_PROGRAM',
      'FLEXI_CANCELLATION',
      'DELIVERY_CHARGE',
      'AC_INSTALLATION_CHARGE'
  )
GROUP BY vas.type
ORDER BY row_count DESC;
-- Expected: 0 rows (or known/accepted types listed for documentation)

-- ----------------------------------------------------------------------------
-- DQ-6: Invoice schedules with NULL number_of_invoice_cycles
-- Used as divisor in tenure_base; NULL causes NULLIF to return NULL,
-- propagating to plan transition revenue calculations.
-- ----------------------------------------------------------------------------
SELECT
    'DQ-6: Invoice schedules with NULL invoice cycle count'                    AS check_name,
    COUNT(*)                                                                   AS failing_rows,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END                        AS result
FROM furlenco_silver.furbooks_evolve.invoice_schedules
WHERE vertical = 'FURLENCO_RENTAL'
  AND state NOT IN ('INVALIDATED', 'CANCELLED', 'PENDING')
  AND accountable_entity_type IN ('ITEM', 'ATTACHMENT')
  AND external_reference_type <> 'SETTLEMENT'
  AND number_of_invoice_cycles IS NULL
  AND created_at >= '2024-06-01';

-- ----------------------------------------------------------------------------
-- DQ-7: Source table row count sanity check
-- Detects unexpectedly empty source tables (pipeline failures).
-- ----------------------------------------------------------------------------
SELECT
    'DQ-7: Source table row counts'                                            AS check_name,
    (SELECT COUNT(*) FROM furlenco_silver.furbooks_evolve.revenue_recognitions
     WHERE vertical = 'FURLENCO_RENTAL'
       AND state NOT IN ('CANCELLED', 'INVALIDATED')
       AND DATE(created_at + INTERVAL '330 minutes') >= CAST(:month1_start AS DATE)
    )                                                                          AS revenue_recognitions_count,
    (SELECT COUNT(*) FROM furlenco_silver.furbooks_evolve.revenue_recognition_schedules
     WHERE vertical = 'FURLENCO_RENTAL'
    )                                                                          AS schedules_count,
    CASE
        WHEN (SELECT COUNT(*) FROM furlenco_silver.furbooks_evolve.revenue_recognitions
              WHERE vertical = 'FURLENCO_RENTAL'
                AND state NOT IN ('CANCELLED', 'INVALIDATED')
                AND DATE(created_at + INTERVAL '330 minutes') >= CAST(:month1_start AS DATE)
             ) = 0
        THEN 'FAIL: revenue_recognitions is empty for this window'
        ELSE 'PASS'
    END                                                                        AS result;
