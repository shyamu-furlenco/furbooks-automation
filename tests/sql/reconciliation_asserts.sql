-- ============================================================================
-- RECONCILIATION ASSERTIONS
-- Append this block after the main query to validate internal consistency.
-- Run via Databricks notebook; each check returns 'PASS' or a descriptive FAIL.
-- ============================================================================
-- Usage: Replace :month1_start and :month2_start with the same values used
--        in the main revenue movement query.
-- ============================================================================

-- Reuse the final output CTE from the main query via a temp view:
--   CREATE OR REPLACE TEMP VIEW revenue_bridge AS (<main query>);

-- Then run the assertions below against revenue_bridge.

-- ----------------------------------------------------------------------------
-- CHECK 1: Gap reconciliation
-- Month 2 opening must equal Month 1 closing within a ±1% tolerance.
-- A large gap means the bridge does not fully explain the revenue movement.
-- ----------------------------------------------------------------------------
SELECT
    'CHECK 1: Gap reconciliation'                                               AS check_name,
    ROUND(m1_closing.rev, 2)                                                   AS m1_closing_rev,
    ROUND(m2_opening.rev, 2)                                                   AS m2_opening_rev,
    ROUND(gap.rev, 2)                                                          AS gap_rev,
    ROUND(ABS(gap.rev) / NULLIF(ABS(m1_closing.rev), 0) * 100, 2)             AS gap_pct,
    CASE
        WHEN ABS(gap.rev) <= 1                                                 THEN 'PASS'
        WHEN ABS(gap.rev) / NULLIF(ABS(m1_closing.rev), 0) <= 0.01            THEN 'PASS (within 1%)'
        ELSE CONCAT('FAIL: gap = ', ROUND(gap.rev, 2),
                    ' (', ROUND(ABS(gap.rev) / NULLIF(ABS(m1_closing.rev), 0) * 100, 2), '%)')
    END                                                                        AS result
FROM
    (SELECT `M1 Taxable revenue (without VAS)` AS rev
     FROM revenue_bridge WHERE component = 'Total closing Revenue')            AS m1_closing,
    (SELECT `M1 Taxable revenue (without VAS)` AS rev
     FROM revenue_bridge WHERE component = 'Opening_revenue'
     -- month2 opening = M2 column of Opening_revenue row
     -- Restate using M2 column alias:
    )                                                                          AS m2_opening,
    (SELECT `M1 Taxable revenue (without VAS)` AS rev
     FROM revenue_bridge WHERE component = 'Gap (Month1 Closing vs Month2 Opening)')
                                                                               AS gap;

-- Note: In Databricks, reference the pivot columns directly:
--   m2_opening = `M2 Taxable revenue (without VAS)` WHERE component = 'Opening_revenue'
-- Adjust column alias above to `M2 Taxable revenue (without VAS)` as needed.

-- ----------------------------------------------------------------------------
-- CHECK 2: Pickup component reconciliation
-- Total pickup raised must equal the sum of partial + full + cancelled pickups.
-- Formula: |total_raised| = |partial| + |full| + |cancellations|
-- (All three sub-components are positive; total is negative in the bridge.)
-- ----------------------------------------------------------------------------
SELECT
    'CHECK 2: Pickup component split'                                          AS check_name,
    ROUND(ABS(total_raised.rev), 2)                                           AS total_raised_abs,
    ROUND(ABS(partial.rev) + ABS(full_p.rev) + ABS(cancellations.rev), 2)    AS components_sum,
    CASE
        WHEN ABS(ABS(total_raised.rev) -
                 (ABS(partial.rev) + ABS(full_p.rev) + ABS(cancellations.rev))) <= 1
        THEN 'PASS'
        ELSE 'FAIL: pickup split does not reconcile'
    END                                                                        AS result
FROM
    (SELECT COALESCE(`M1 Taxable revenue (without VAS)`, 0) AS rev
     FROM revenue_bridge WHERE component = 'Pickup raised (Total)')            AS total_raised,
    (SELECT COALESCE(`M1 Taxable revenue (without VAS)`, 0) AS rev
     FROM revenue_bridge WHERE component = 'Partial pickup (Reduction in item count)')
                                                                               AS partial,
    (SELECT COALESCE(`M1 Taxable revenue (without VAS)`, 0) AS rev
     FROM revenue_bridge WHERE component = 'Full pickup (Reduction of Cx)')   AS full_p,
    (SELECT COALESCE(`M1 Taxable revenue (without VAS)`, 0) AS rev
     FROM revenue_bridge WHERE component = 'Pickup cancellations')             AS cancellations;

-- ----------------------------------------------------------------------------
-- CHECK 3: Discount change reconciliation
-- Discount change total must equal positive change + negative change.
-- ----------------------------------------------------------------------------
SELECT
    'CHECK 3: Discount split reconciliation'                                   AS check_name,
    ROUND(total_d.rev, 2)                                                     AS total_discount_change,
    ROUND(pos_d.rev + neg_d.rev, 2)                                           AS pos_plus_neg,
    CASE
        WHEN ABS(total_d.rev - (pos_d.rev + neg_d.rev)) <= 1                 THEN 'PASS'
        ELSE 'FAIL: discount positive + negative != total'
    END                                                                        AS result
FROM
    (SELECT COALESCE(`M1 Taxable revenue (without VAS)`, 0) AS rev
     FROM revenue_bridge WHERE component = 'Discount change')                  AS total_d,
    (SELECT COALESCE(`M1 Taxable revenue (without VAS)`, 0) AS rev
     FROM revenue_bridge WHERE component = 'Discount change - Positive')       AS pos_d,
    (SELECT COALESCE(`M1 Taxable revenue (without VAS)`, 0) AS rev
     FROM revenue_bridge WHERE component = 'Discount change - Negative')       AS neg_d;

-- ----------------------------------------------------------------------------
-- CHECK 4: Adjusted opening = Opening + MTP1 adjustment
-- Verify the adj_opening_row arithmetic matches the two source rows.
-- ----------------------------------------------------------------------------
SELECT
    'CHECK 4: Adjusted opening = Opening + MTP1'                              AS check_name,
    ROUND(opening.rev + mtp1.rev, 2)                                          AS opening_plus_mtp1,
    ROUND(adj.rev, 2)                                                         AS adjusted_opening,
    CASE
        WHEN ABS((opening.rev + mtp1.rev) - adj.rev) <= 1                    THEN 'PASS'
        ELSE 'FAIL: adjusted opening mismatch'
    END                                                                        AS result
FROM
    (SELECT COALESCE(`M1 Taxable revenue (without VAS)`, 0) AS rev
     FROM revenue_bridge WHERE component = 'Opening_revenue')                  AS opening,
    (SELECT COALESCE(`M1 Taxable revenue (without VAS)`, 0) AS rev
     FROM revenue_bridge WHERE component = 'Minimum tenure charges'
     -- Note: MTP appears twice (sort_order 2 and 10); filter by sort_order if available
    )                                                                          AS mtp1,
    (SELECT COALESCE(`M1 Taxable revenue (without VAS)`, 0) AS rev
     FROM revenue_bridge WHERE component = 'Adjusted opening')                 AS adj;

-- ----------------------------------------------------------------------------
-- CHECK 5: No NULL revenues for core components
-- Every core component must have a non-NULL M1 and M2 revenue value.
-- A NULL indicates the CTE produced no rows (possible data pipeline failure).
-- ----------------------------------------------------------------------------
SELECT
    component,
    'CHECK 5: NULL revenue check'                                              AS check_name,
    CASE
        WHEN `M1 Taxable revenue (without VAS)` IS NULL                       THEN 'FAIL: M1 revenue is NULL'
        WHEN `M2 Taxable revenue (without VAS)` IS NULL                       THEN 'FAIL: M2 revenue is NULL'
        ELSE 'PASS'
    END                                                                        AS result
FROM revenue_bridge
WHERE component IN (
    'Opening_revenue',
    'Adjusted opening',
    'Total closing Revenue'
);

-- ----------------------------------------------------------------------------
-- CHECK 6: Plan transition signs are correct
-- Positive transitions must have revenue_difference > 0.
-- Negative transitions must have revenue_difference < 0.
-- ----------------------------------------------------------------------------
SELECT
    'CHECK 6: Plan transition signs'                                           AS check_name,
    CASE
        WHEN pos.rev IS NOT NULL AND pos.rev < 0                              THEN 'FAIL: positive transition has negative revenue'
        WHEN neg.rev IS NOT NULL AND neg.rev > 0                              THEN 'FAIL: negative transition has positive revenue'
        ELSE 'PASS'
    END                                                                        AS result
FROM
    (SELECT `M1 Taxable revenue (without VAS)` AS rev
     FROM revenue_bridge WHERE component = 'Plan transition - Positive')       AS pos,
    (SELECT `M1 Taxable revenue (without VAS)` AS rev
     FROM revenue_bridge WHERE component = 'Plan transition - Negative')       AS neg;

-- ----------------------------------------------------------------------------
-- CHECK 7: Accrual change signs
-- RO Positive (DEFERRAL→ACCRUAL) should have positive revenue delta.
-- RO Negative (ACCRUAL→DEFERRAL) should have negative revenue delta.
-- ----------------------------------------------------------------------------
SELECT
    'CHECK 7: Accrual change signs'                                            AS check_name,
    CASE
        WHEN pos.rev IS NOT NULL AND pos.rev < 0                              THEN 'FAIL: RO positive has negative revenue'
        WHEN neg.rev IS NOT NULL AND neg.rev > 0                              THEN 'FAIL: RO negative has positive revenue'
        ELSE 'PASS'
    END                                                                        AS result
FROM
    (SELECT `M1 Taxable revenue (without VAS)` AS rev
     FROM revenue_bridge WHERE component = 'RO (Renewal Overdue) - Positive') AS pos,
    (SELECT `M1 Taxable revenue (without VAS)` AS rev
     FROM revenue_bridge WHERE component = 'RO (Renewal Overdue) - Negative') AS neg;
