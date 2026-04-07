%sql

-- ============================================================================
-- FURBOOKS REVENUE BRIDGE — OPTIMIZED v7 (Round 4: Databricks Perf)
-- Key optimizations:
--   R1: Single scan of revenue_recognitions via rr_base
--   R1: deleted_at IS NULL applied globally
--   R2: Dead columns/JOINs removed from furbooks_revenue
--   R2: Duplicate LATERAL VIEW EXPLODE merged into one CTE
--   R2: IST dates pre-computed in rr_base
--   R3: 4 furbooks_revenue consumers merged into 1 classified CTE
--   R3: new_deliveries + upsells share 1 scan of rental_acquition_unified
--   R3: VAS date filter tightened (no wasted previous-month rows)
--   R3: swap_items/swap_attachments UNION ALL shared between swap_base/swap_in_base
--   R3: Dead columns pruned from rr_base
--   R4: LEFT ANTI JOIN → LEFT JOIN + IS NULL (Photon compatibility)
--   R4: 6 churn CTEs → 1 conditional aggregation (5 fewer churn_joined scans)
--   R4: 3 classified CTEs → 1 conditional aggregation (2 fewer furbooks_classified scans)
--   R4: 2 accrual CTEs → 1 conditional aggregation
--   R4: vas_detail CROSS JOIN → INNER JOIN (eliminate 2x row inflation)
--   R4: gap_row scalar subqueries → CROSS JOIN pattern
-- ============================================================================

-- ============================================================================
-- SECTION 0: SHARED LOOKUPS
-- ============================================================================

WITH sms_entity AS (
    SELECT id, 'ITEM' AS entity_type, user_id, user_details_displayId
    FROM furlenco_silver.order_management_systems_evolve.items
    WHERE state <> 'CANCELLED'
    UNION ALL
    SELECT id, 'ATTACHMENT' AS entity_type, user_id, user_details_displayId
    FROM furlenco_silver.order_management_systems_evolve.attachments
    WHERE state <> 'CANCELLED'
)

-- ============================================================================
-- SECTION 1: UNIFIED BASE — single scan of revenue_recognitions + schedules
-- Dead columns pruned: id, to_be_recognised_on, sched_postTaxAmount removed.
-- ============================================================================

   , rr_base AS (
    SELECT
        rr.recognition_type,
        rr.accountable_entity_id,
        rr.accountable_entity_type,
        rr.start_date                                            AS rr_start_date,
        rr.end_date                                              AS rr_end_date,
        rr.recognised_at,
        rr.created_at,
        DATE(rr.recognised_at + INTERVAL '330 minutes')          AS recognised_at_ist,
        DATE(rr.created_at    + INTERVAL '330 minutes')          AS created_at_ist,
        rr.external_reference_type,
        rr.external_reference_id,
        rr.revenue_recognition_schedule_id,
        rr.monetary_components_taxableAmount,
        rr.monetary_components_discounts,
        rrs.monetary_components_taxableAmount                    AS sched_taxableAmount,
        rrs.start_date                                           AS sched_start_date,
        rrs.end_date                                             AS sched_end_date,
        rrs.monetary_components                                  AS sched_monetary_components,
        ROUND(DATEDIFF(DAY, rrs.start_date, rrs.end_date) / 30.45) AS sched_tenure
    FROM furlenco_silver.furbooks_evolve.revenue_recognitions AS rr
    LEFT JOIN furlenco_silver.furbooks_evolve.revenue_recognition_schedules AS rrs
        ON rrs.id = rr.revenue_recognition_schedule_id
    WHERE rr.vertical = 'FURLENCO_RENTAL'
      AND rr.state NOT IN ('CANCELLED', 'INVALIDATED')
      AND rr.deleted_at IS NULL
)

-- ============================================================================
-- SECTION 1b: FURBOOKS REVENUE (ITEM/ATTACHMENT grain)
-- Only columns used downstream: entity keys, user_id, taxable_amount,
-- start_date, recognised_at.
-- ============================================================================

   , furbooks_revenue AS (
    SELECT
        rb.accountable_entity_id,
        rb.accountable_entity_type,
        DATE(rb.rr_start_date)                                   AS start_date,
        rb.recognised_at_ist                                     AS recognised_at,
        rb.monetary_components_taxableAmount                     AS taxable_amount,
        se.user_id
    FROM rr_base rb
    LEFT JOIN sms_entity se
        ON se.id          = rb.accountable_entity_id
        AND se.entity_type = rb.accountable_entity_type
    WHERE rb.accountable_entity_type IN ('ITEM', 'ATTACHMENT')
)

-- ============================================================================
-- SECTION 2: MONTHS DRIVER
-- ============================================================================

    , months AS (
SELECT
    1 AS month_num,
    CAST(:month1_start AS DATE)                              AS m_start,
    ADD_MONTHS(CAST(:month1_start AS DATE), 1)               AS m_end,
    ADD_MONTHS(CAST(:month1_start AS DATE), -1)              AS prev_start,
    DATE_FORMAT(CAST(:month1_start AS DATE), 'MMM yyyy')     AS m_label
UNION ALL
SELECT
    2,
    CAST(:month2_start AS DATE),
    ADD_MONTHS(CAST(:month2_start AS DATE), 1),
    ADD_MONTHS(CAST(:month2_start AS DATE), -1),
    DATE_FORMAT(CAST(:month2_start AS DATE), 'MMM yyyy')
    )

-- ============================================================================
-- SECTION 3: SHARED BASE CTEs
-- ============================================================================

-- Revenue-recognition-schedule grain for plan-transition and accrual-change detection.
        , tenure_base AS (
SELECT
    rb.recognition_type                                                        AS revenue_recognition_type,
    rb.accountable_entity_id,
    rb.accountable_entity_type,
    DATE(rb.sched_start_date)                                                  AS start_date,
    DATE(rb.sched_end_date)                                                    AS end_date,
    CAST(rb.sched_taxableAmount AS DOUBLE)
    / NULLIF(rb.sched_tenure, 0)                                               AS taxableAmount,
    rb.sched_tenure                                                            AS tenure,
    rb.external_reference_type,
    rb.created_at_ist                                                          AS created_at,
    rb.sched_monetary_components                                               AS monetary_components
FROM rr_base rb
WHERE rb.accountable_entity_type IN ('ITEM', 'ATTACHMENT')
  AND rb.created_at >= '2024-06-01'
    QUALIFY ROW_NUMBER() OVER (
    PARTITION BY rb.accountable_entity_id, rb.accountable_entity_type, rb.revenue_recognition_schedule_id
    ORDER BY rb.created_at ASC
    ) = 1
    )


-- ============================================================================
-- SECTION 4: UNIFIED COMPONENT CTEs
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Classified furbooks_revenue: ONE join to months for ALL revenue components.
-- Tags each row as S1.1 (normal opening), S1.2 (MTP opening), current MTP.
-- Replaces 4 separate furbooks_revenue × months joins.
-- ----------------------------------------------------------------------------
    , furbooks_classified AS (
SELECT /*+ BROADCAST(m) */
    m.month_num,
    br.accountable_entity_id,
    br.accountable_entity_type,
    br.user_id,
    br.taxable_amount,
    -- Classify into component buckets
    CASE WHEN br.start_date >= m.prev_start AND br.start_date < m.m_start
         AND (br.recognised_at IS NULL OR br.recognised_at >= m.prev_start)
         THEN TRUE ELSE FALSE END                                                            AS is_s1_1,
    CASE WHEN br.start_date >= m.m_start
         AND br.recognised_at >= m.prev_start AND br.recognised_at < m.m_start
         THEN TRUE ELSE FALSE END                                                            AS is_s1_2,
    CASE WHEN br.recognised_at >= m.m_start AND br.recognised_at < m.m_end
         AND br.start_date >= m.m_end
         THEN TRUE ELSE FALSE END                                                            AS is_current_mtp
FROM furbooks_revenue br
    INNER JOIN months m
ON (
    -- S1.1: Normal cycles
    (    br.start_date    >= m.prev_start
    AND br.start_date    <  m.m_start
    AND (br.recognised_at IS NULL OR br.recognised_at >= m.prev_start)
    )
    OR
    -- S1.2: MTP cycles
    (   br.start_date >= m.m_start
    AND br.recognised_at >= m.prev_start
    AND br.recognised_at <  m.m_start
    )
    OR
    -- Current month MTP
    (   br.recognised_at >= m.m_start
    AND br.recognised_at <  m.m_end
    AND br.start_date    >= m.m_end
    )
    )
    )

-- Components 1, 2, 14: Opening + MTP — single pass over furbooks_classified
    , classified_components AS (
SELECT month_num,
    -- Component 1: Opening Revenue (S1.1 + S1.2)
    COUNT(DISTINCT CASE WHEN is_s1_1 OR is_s1_2
         THEN CONCAT(accountable_entity_type, '::', accountable_entity_id) END)              AS op_items,
    COUNT(DISTINCT CASE WHEN is_s1_1 OR is_s1_2 THEN user_id END)                           AS op_cx,
    SUM(CASE WHEN is_s1_1 OR is_s1_2 THEN taxable_amount::float ELSE 0 END)                 AS op_rev,
    -- Component 2: MTP Adjustment (S1.2 only, negated)
    -COUNT(DISTINCT CASE WHEN is_s1_2
         THEN CONCAT(accountable_entity_type, '::', accountable_entity_id) END)              AS mtp_adj_items,
    -COUNT(DISTINCT CASE WHEN is_s1_2 THEN user_id END)                                     AS mtp_adj_cx,
    -SUM(CASE WHEN is_s1_2 THEN taxable_amount::float ELSE 0 END)                           AS mtp_adj_rev,
    -- Component 14: Current Month MTP (negated)
    -COUNT(DISTINCT CASE WHEN is_current_mtp
         THEN CONCAT(accountable_entity_type, '::', accountable_entity_id) END)              AS cmtp_items,
    -COUNT(DISTINCT CASE WHEN is_current_mtp THEN user_id END)                               AS cmtp_cx,
    -SUM(CASE WHEN is_current_mtp THEN taxable_amount::float ELSE 0 END)                     AS cmtp_rev
FROM furbooks_classified
GROUP BY month_num
    )

-- MTP entities for anti-join (2nd and final reference to furbooks_classified)
        , mtp_current_month_entities AS (
    SELECT DISTINCT accountable_entity_id, accountable_entity_type
    FROM furbooks_classified
    WHERE is_current_mtp
    )

-- ----------------------------------------------------------------------------
-- New Deliveries + Upsells: single scan of rental_acquition_unified.
-- Pre-joined to months once; each component filters by flag.
-- ----------------------------------------------------------------------------
        , acquisition_joined AS (
SELECT /*+ BROADCAST(m) */
    m.month_num,
    f.accountable_entity_id,
    f.fur_id,
    f.taxable_amount::float                                     AS taxable_amount,
    LOWER(f.flag_based_on_Ua)                                   AS flag
FROM furlenco_analytics.user_defined_tables.rental_acquition_unified f
    JOIN months m
ON f.activation_date >= m.m_start
    AND f.activation_date <  m.m_end
WHERE LOWER(f.flag_based_on_Ua) IN ('new', 'upsell')
    )

-- Component 4: New Deliveries
        , new_deliveries AS (
SELECT month_num,
    'New deliveries (Addition of Cx)'       AS component,
    4                                       AS sort_order,
    COUNT(DISTINCT accountable_entity_id)   AS items_count,
    COUNT(DISTINCT fur_id)                  AS cx_count,
    SUM(taxable_amount)                     AS taxable_revenue
FROM acquisition_joined WHERE flag = 'new'
GROUP BY month_num
    )

-- Component 5: Upsells
        , upsells AS (
SELECT month_num,
    'Upsell (Addition in item count)'       AS component,
    5                                       AS sort_order,
    COUNT(DISTINCT accountable_entity_id)   AS items_count,
    COUNT(DISTINCT fur_id)                  AS cx_count,
    SUM(taxable_amount)                     AS taxable_revenue
FROM acquisition_joined WHERE flag = 'upsell'
GROUP BY month_num
    )

-- Pickup-date base from rental_churn_query.
-- LEFT JOIN + IS NULL excludes MTP entities (Photon-compatible).
        , churn_pickups_base AS (
SELECT
    rcq.entity_id,
    rcq.entity_type,
    rcq.user_ids,
    rcq.taxable_amount,
    rcq.churn_flag,
    rcq.transaction_type,
    rcq.payment_date,
    CAST(rcq.pickup_date AS DATE)                                                    AS pickup_date
FROM furlenco_analytics.user_defined_tables.rental_churn_query rcq
LEFT JOIN mtp_current_month_entities mtp
    ON rcq.entity_id = mtp.accountable_entity_id
    AND rcq.entity_type = mtp.accountable_entity_type
WHERE rcq.rnk = 1
  AND mtp.accountable_entity_id IS NULL
    )

-- Churn/TTO: single pre-join to months.
        , churn_joined AS (
SELECT /*+ BROADCAST(m) */
    m.month_num,
    cp.entity_id,
    cp.entity_type,
    cp.user_ids,
    cp.taxable_amount::FLOAT                                                     AS taxable_amount,
    cp.churn_flag,
    cp.transaction_type
FROM churn_pickups_base cp
    JOIN months m
ON cp.payment_date >= m.m_start
    AND cp.payment_date <  m.m_end
    )

-- Components 6-11: Churn + TTO — single pass over churn_joined
    , churn_components AS (
SELECT month_num,
    -- Component 6: Total Pickup
    -COUNT(DISTINCT CASE WHEN transaction_type = 'return_item'
         THEN CONCAT(entity_type, '::', entity_id) END)                          AS tp_items,
    -COUNT(DISTINCT CASE WHEN transaction_type = 'return_item'
         THEN user_ids END)                                                       AS tp_cx,
    -SUM(CASE WHEN transaction_type = 'return_item' THEN taxable_amount ELSE 0 END) AS tp_rev,
    -- Component 7: Partial Pickup
    -COUNT(DISTINCT CASE WHEN churn_flag = 'PARTIAL' AND transaction_type = 'return_item'
         THEN CONCAT(entity_type, '::', entity_id) END)                          AS pp_items,
    -SUM(CASE WHEN churn_flag = 'PARTIAL' AND transaction_type = 'return_item'
         THEN taxable_amount ELSE 0 END)                                          AS pp_rev,
    -- Component 8: Full Pickup
    -COUNT(DISTINCT CASE WHEN churn_flag = 'FULL' AND transaction_type = 'return_item'
         THEN CONCAT(entity_type, '::', entity_id) END)                          AS fp_items,
    -COUNT(DISTINCT CASE WHEN churn_flag = 'FULL' AND transaction_type = 'return_item'
         THEN user_ids END)                                                       AS fp_cx,
    -SUM(CASE WHEN churn_flag = 'FULL' AND transaction_type = 'return_item'
         THEN taxable_amount ELSE 0 END)                                          AS fp_rev,
    -- Component 9: TTO Total
    -COUNT(DISTINCT CASE WHEN transaction_type = 'rent_to_purchase_item'
         THEN CONCAT(entity_type, '::', entity_id) END)                          AS tto_items,
    -COUNT(DISTINCT CASE WHEN transaction_type = 'rent_to_purchase_item'
         THEN user_ids END)                                                       AS tto_cx,
    -SUM(CASE WHEN transaction_type = 'rent_to_purchase_item'
         THEN taxable_amount ELSE 0 END)                                          AS tto_rev,
    -- Component 10: TTO Partial
    -COUNT(DISTINCT CASE WHEN transaction_type = 'rent_to_purchase_item' AND churn_flag = 'PARTIAL'
         THEN CONCAT(entity_type, '::', entity_id) END)                          AS tto_pp_items,
    -SUM(CASE WHEN transaction_type = 'rent_to_purchase_item' AND churn_flag = 'PARTIAL'
         THEN taxable_amount ELSE 0 END)                                          AS tto_pp_rev,
    -- Component 11: TTO Full
    -COUNT(DISTINCT CASE WHEN transaction_type = 'rent_to_purchase_item' AND churn_flag = 'FULL'
         THEN CONCAT(entity_type, '::', entity_id) END)                          AS tto_fp_items,
    -COUNT(DISTINCT CASE WHEN transaction_type = 'rent_to_purchase_item' AND churn_flag = 'FULL'
         THEN user_ids END)                                                       AS tto_fp_cx,
    -SUM(CASE WHEN transaction_type = 'rent_to_purchase_item' AND churn_flag = 'FULL'
         THEN taxable_amount ELSE 0 END)                                          AS tto_fp_rev
FROM churn_joined
GROUP BY month_num
    )

-- ----------------------------------------------------------------------------
-- Swap entities: single UNION ALL for both SWAP_OUT and SWAP_IN.
-- Shared between swap_base and swap_in_base (halves the scan).
-- ----------------------------------------------------------------------------
        , swap_entities AS (
SELECT attachment_id AS entity_id, 'ATTACHMENT' AS entity_type, action, fulfillment_date
FROM furlenco_silver.order_management_systems_evolve.swap_attachments
WHERE state = 'FULFILLED' AND fulfillment_date IS NOT NULL AND action IN ('SWAP_OUT', 'SWAP_IN')
UNION ALL
SELECT item_id AS entity_id, 'ITEM' AS entity_type, action, fulfillment_date
FROM furlenco_silver.order_management_systems_evolve.swap_items
WHERE state = 'FULFILLED' AND fulfillment_date IS NOT NULL AND action IN ('SWAP_OUT', 'SWAP_IN')
    )

-- Swap-out base
        , swap_base AS (
SELECT
    sw.entity_id,
    sw.entity_type,
    se.user_id,
    DATE(sw.fulfillment_date + INTERVAL '330 minutes')                           AS fulfillment_date,
    rb.monetary_components_taxableAmount                                         AS taxable_amount,
    ROW_NUMBER() OVER (
    PARTITION BY sw.entity_id, sw.entity_type
    ORDER BY rb.rr_start_date DESC
    )                                                                            AS rr_rnk
FROM swap_entities sw
    LEFT JOIN sms_entity se
ON  se.id          = sw.entity_id
    AND se.entity_type = sw.entity_type
    LEFT JOIN rr_base rb
    ON  rb.accountable_entity_id   = sw.entity_id
    AND rb.accountable_entity_type = sw.entity_type
    AND rb.accountable_entity_type IN ('ITEM', 'ATTACHMENT')
    AND rb.rr_start_date          <= DATE(sw.fulfillment_date + INTERVAL '330 minutes')
WHERE sw.action = 'SWAP_OUT'
    )

-- Component 12: Swapped Out
    , swapped_out AS (
SELECT /*+ BROADCAST(m) */
    m.month_num,
    'Swapped out'                                                                AS component,
    12                                                                           AS sort_order,
    -COUNT(DISTINCT CONCAT(sb.entity_type, '::', sb.entity_id))                  AS items_count,
    -0                                                                           AS cx_count,
    -SUM(sb.taxable_amount::FLOAT)                                               AS taxable_revenue
FROM swap_base sb
    JOIN months m
ON sb.fulfillment_date >= m.m_start
    AND sb.fulfillment_date <  m.m_end
WHERE sb.rr_rnk = 1
GROUP BY m.month_num
    )

-- Swap-in base
        , swap_in_base AS (
SELECT
    sw.entity_id,
    sw.entity_type,
    se.user_id,
    DATE(sw.fulfillment_date + INTERVAL '330 minutes')                           AS fulfillment_date,
    rb.monetary_components_taxableAmount                                         AS taxable_amount,
    ROW_NUMBER() OVER (
    PARTITION BY sw.entity_id, sw.entity_type
    ORDER BY rb.rr_start_date ASC
    )                                                                            AS rr_rnk
FROM swap_entities sw
    LEFT JOIN sms_entity se
ON  se.id          = sw.entity_id
    AND se.entity_type = sw.entity_type
    LEFT JOIN rr_base rb
    ON  rb.accountable_entity_id   = sw.entity_id
    AND rb.accountable_entity_type = sw.entity_type
    AND rb.accountable_entity_type IN ('ITEM', 'ATTACHMENT')
    AND rb.rr_start_date          >= DATE(sw.fulfillment_date + INTERVAL '330 minutes')
WHERE sw.action = 'SWAP_IN'
    )

-- Component 13: Swapped In
    , swapped_in AS (
SELECT /*+ BROADCAST(m) */
    m.month_num,
    'Swapped in'                                                                 AS component,
    13                                                                           AS sort_order,
    COUNT(DISTINCT CONCAT(sb.entity_type, '::', sb.entity_id))                   AS items_count,
    0                                                                            AS cx_count,
    SUM(sb.taxable_amount::FLOAT)                                                AS taxable_revenue
FROM swap_in_base sb
    JOIN months m
ON sb.fulfillment_date >= m.m_start
    AND sb.fulfillment_date <  m.m_end
WHERE sb.rr_rnk = 1
GROUP BY m.month_num
    )


-- Penalty: reads from rr_base, uses pre-computed IST date.
        , penalty AS (
SELECT /*+ BROADCAST(m) */
    m.month_num,
    'Penalty'                                AS component,
    15                                       AS sort_order,
    COUNT(DISTINCT product_entity_id)         AS items_count,
    COUNT(DISTINCT pl.user_id)                AS cx_count,
    SUM(CAST(rb.monetary_components_taxableAmount AS DOUBLE)) AS taxable_revenue
FROM rr_base rb
    JOIN furlenco_silver.order_management_systems_evolve.penalty pl
ON rb.accountable_entity_id = pl.id
    JOIN months m
    ON rb.recognised_at_ist >= m.m_start
    AND rb.recognised_at_ist <  m.m_end
WHERE rb.accountable_entity_type = 'PENALTY'
GROUP BY m.month_num
    )

-- ----------------------------------------------------------------------------
-- Accrual Change Detection
-- ----------------------------------------------------------------------------
        , tenure_windowed AS (
SELECT
    accountable_entity_id, accountable_entity_type, start_date, end_date,
    tenure, taxableAmount,
    revenue_recognition_type,
    LAG(tenure)                   OVER w AS previous_tenure,
    LAG(start_date)               OVER w AS previous_start_date,
    LAG(end_date)                 OVER w AS previous_end_date,
    LAG(taxableAmount)            OVER w AS previous_taxableAmount,
    LAG(revenue_recognition_type) OVER w AS previous_recognition_type,
    external_reference_type, created_at, monetary_components
FROM tenure_base
    WINDOW w AS (
    PARTITION BY accountable_entity_id, accountable_entity_type
    ORDER BY start_date ASC, created_at ASC
    )
    )

        , customer_accrual_changes AS (
SELECT
    rc.accountable_entity_id, rc.accountable_entity_type,
    rc.start_date, rc.previous_start_date, rc.previous_end_date,
    rc.previous_recognition_type, rc.revenue_recognition_type AS current_recognition_type,
    se.user_id,
    rc.previous_taxableAmount::DECIMAL(10,2)                                             AS previous_month_revenue,
    rc.taxableAmount::DECIMAL(10,2)                                                      AS current_month_revenue,
    (rc.taxableAmount::DECIMAL(10,2) - rc.previous_taxableAmount::DECIMAL(10,2))         AS revenue_difference,
    rc.created_at, m.month_num
FROM tenure_windowed rc
    LEFT JOIN sms_entity se ON se.id = rc.accountable_entity_id AND se.entity_type = rc.accountable_entity_type
    JOIN months m ON rc.start_date >= m.m_start AND rc.start_date < m.m_end
WHERE rc.previous_recognition_type IS NOT NULL
  AND rc.previous_recognition_type <> rc.revenue_recognition_type
  AND (rc.revenue_recognition_type = 'ACCRUAL' OR rc.previous_recognition_type = 'ACCRUAL')
    )

-- Components 18-19: Accrual changes — single pass over customer_accrual_changes
    , accrual_components AS (
SELECT month_num,
    -- Component 18: RO Positive (ACCRUAL)
    COUNT(DISTINCT CASE WHEN current_recognition_type = 'ACCRUAL'
         THEN CONCAT(accountable_entity_type, '::', accountable_entity_id) END)  AS pos_items,
    COUNT(DISTINCT CASE WHEN current_recognition_type = 'ACCRUAL'
         THEN user_id END)                                                        AS pos_cx,
    SUM(CASE WHEN current_recognition_type = 'ACCRUAL' THEN revenue_difference ELSE 0 END) AS pos_rev,
    -- Component 19: RO Negative (DEFERRAL)
    COUNT(DISTINCT CASE WHEN current_recognition_type = 'DEFERRAL'
         THEN CONCAT(accountable_entity_type, '::', accountable_entity_id) END)  AS neg_items,
    COUNT(DISTINCT CASE WHEN current_recognition_type = 'DEFERRAL'
         THEN user_id END)                                                        AS neg_cx,
    SUM(CASE WHEN current_recognition_type = 'DEFERRAL' THEN revenue_difference ELSE 0 END) AS neg_rev
FROM customer_accrual_changes
GROUP BY month_num
    )

-- ----------------------------------------------------------------------------
-- Plan Transition + Discount Change
-- Single LATERAL VIEW EXPLODE from rr_base.
-- ----------------------------------------------------------------------------

        , discounts_exploded AS (
SELECT
    rb.accountable_entity_id,
    rb.accountable_entity_type,
    rb.revenue_recognition_schedule_id,
    rb.external_reference_type,
    DATE(rb.rr_start_date)                                             AS rr_start_date,
    DATE(rb.rr_end_date)                                               AS rr_end_date,
    rb.sched_tenure,
    d.catalogReferenceId,
    d.amount                                                           AS discount_amount
FROM rr_base rb
    LATERAL VIEW OUTER EXPLODE(
    from_json(CAST(rb.monetary_components_discounts AS STRING),
    'array<struct<amount:double,catalogReferenceId:string,code:string>>')
    ) AS d
WHERE rb.accountable_entity_type IN ('ITEM', 'ATTACHMENT')
  AND rb.created_at >= '2024-06-01'
    )

-- Plan Transition: Sum UPFRONT discounts per schedule.
        , upfront_discount_per_schedule AS (
SELECT
    e.accountable_entity_id,
    e.accountable_entity_type,
    e.revenue_recognition_schedule_id,
    e.rr_start_date                                                    AS start_date,
    e.sched_tenure                                                     AS tenure,
    SUM(COALESCE(e.discount_amount, 0))                                AS upfront_discount_amount
FROM discounts_exploded e
    JOIN furlenco_silver.godfather_evolve.discounts gd
    ON (e.catalogReferenceId = gd.id AND gd.type = 'UPFRONT') OR e.external_reference_type = 'SETTLEMENT'
GROUP BY
    e.accountable_entity_id, e.accountable_entity_type,
    e.revenue_recognition_schedule_id,
    e.rr_start_date,
    e.external_reference_type,
    e.sched_tenure
    )

        , plan_transition_windowed AS (
SELECT
    accountable_entity_id, accountable_entity_type, start_date,
    tenure,
    upfront_discount_amount,
    LAG(tenure)                  OVER w AS previous_tenure,
    LAG(upfront_discount_amount) OVER w AS previous_upfront_discount_amount,
    LAG(start_date)              OVER w AS previous_start_date
FROM upfront_discount_per_schedule
    WINDOW w AS (
    PARTITION BY accountable_entity_id, accountable_entity_type
    ORDER BY start_date ASC
    )
    )

        , customer_plan_transition AS (
SELECT
    pt.accountable_entity_id, pt.accountable_entity_type,
    pt.start_date, pt.previous_start_date,
    pt.tenure          AS current_tenure,
    pt.previous_tenure,
    se.user_id,
    (coalesce(pt.previous_upfront_discount_amount,0) - coalesce(pt.upfront_discount_amount,0)) AS revenue_difference,
    m.month_num
FROM plan_transition_windowed pt
    LEFT JOIN sms_entity se
    ON se.id = pt.accountable_entity_id AND se.entity_type = pt.accountable_entity_type
    JOIN months m
    ON pt.start_date >= m.prev_start AND pt.start_date < m.m_start
WHERE pt.previous_tenure IS NOT NULL
  AND pt.previous_tenure <> pt.tenure
    )

        , plan_transition AS (
SELECT
    month_num,
    'Plan transition'                                                                AS component,
    16                                                                               AS sort_order,
    COUNT(DISTINCT CONCAT(accountable_entity_type, '::', accountable_entity_id))     AS items_count,
    COUNT(DISTINCT user_id)                                                          AS cx_count,
    SUM(revenue_difference)                                                          AS taxable_revenue
FROM customer_plan_transition
GROUP BY month_num
    )

-- Discount Change: non-UPFRONT discounts (from same explode CTE).
        , discount_per_cycle AS (
SELECT
    e.accountable_entity_id, e.accountable_entity_type,
    e.rr_start_date AS start_date, e.rr_end_date AS end_date,
    COALESCE(SUM(e.discount_amount::DECIMAL(10,2)), 0) AS total_discount_amount
FROM discounts_exploded e
    LEFT JOIN furlenco_silver.godfather_evolve.discounts gd ON e.catalogReferenceId = gd.id
WHERE (gd.type IS NULL OR gd.type <> 'UPFRONT')
GROUP BY e.accountable_entity_id, e.accountable_entity_type, e.rr_start_date, e.rr_end_date
    )

        , discount_changes AS (
SELECT
    accountable_entity_id, accountable_entity_type, start_date, end_date,
    total_discount_amount,
    LAG(total_discount_amount)OVER (PARTITION BY accountable_entity_id, accountable_entity_type ORDER BY start_date) AS previous_discount_amount,
    LAG(start_date)           OVER (PARTITION BY accountable_entity_id, accountable_entity_type ORDER BY start_date) AS previous_start_date,
    LAG(end_date)             OVER (PARTITION BY accountable_entity_id, accountable_entity_type ORDER BY start_date) AS previous_end_date
FROM discount_per_cycle
    )

        , customer_discount_changes AS (
SELECT
    dc.accountable_entity_id, dc.accountable_entity_type,
    dc.start_date, dc.previous_start_date, dc.previous_end_date,
    se.user_id,
    dc.total_discount_amount    AS current_discount,
    dc.previous_discount_amount AS previous_discount,
    (dc.previous_discount_amount - dc.total_discount_amount) AS revenue_difference,
    m.month_num
FROM discount_changes dc
    LEFT JOIN sms_entity se ON se.id = dc.accountable_entity_id AND se.entity_type = dc.accountable_entity_type
    JOIN months m ON dc.start_date >= m.prev_start AND dc.start_date < m.m_start
WHERE dc.previous_discount_amount IS NOT NULL
  AND dc.previous_discount_amount <> dc.total_discount_amount
    )

    , discount_given AS (
SELECT
    m.month_num,
    'Discount given'                                                                          AS component,
    20                                                                                        AS sort_order,
    COUNT(DISTINCT CONCAT(dp.accountable_entity_type, '::', dp.accountable_entity_id))        AS items_count,
    COUNT(DISTINCT se.user_id)                                                                AS cx_count,
    SUM(dp.total_discount_amount)                                                             AS taxable_revenue
FROM discount_per_cycle dp
    LEFT JOIN sms_entity se ON se.id = dp.accountable_entity_id AND se.entity_type = dp.accountable_entity_type
    JOIN months m ON dp.start_date >= m.m_start AND dp.start_date < m.m_end
GROUP BY m.month_num
    )

        , discount_changes_all AS (
SELECT month_num, 'Discount change' AS component, 21 AS sort_order,
    COUNT(DISTINCT CONCAT(accountable_entity_type, '::', accountable_entity_id)) AS items_count,
    COUNT(DISTINCT user_id) AS cx_count,
    SUM(revenue_difference) AS taxable_revenue
FROM customer_discount_changes
GROUP BY month_num
    )

-- ----------------------------------------------------------------------------
-- VAS Revenue: reads from rr_base. Date filter tightened to current month only
-- (previous-month rows were generated but immediately discarded before).
-- ----------------------------------------------------------------------------
        , vas_detail AS (
SELECT /*+ BROADCAST(m) */
    rb.accountable_entity_id, rb.external_reference_id,
    rb.rr_start_date AS start_date, rb.rr_end_date AS end_date, rb.recognised_at,
    CAST(rb.monetary_components_taxableAmount AS DOUBLE) AS taxable_amount,
    vas.entity_id, vas.entity_type, vas.type AS vas_type, vas.user_id,
    m.month_num,
    CASE
    WHEN vas.type IN ('FURLENCO_CARE_PROGRAM', 'FLEXI_CANCELLATION') THEN 'VAS Revenue - Furlenco Care & Flexi'
    WHEN vas.type = 'DELIVERY_CHARGE'                                THEN 'VAS Revenue - Delivery charges'
    WHEN vas.type = 'AC_INSTALLATION_CHARGE'                         THEN 'VAS Revenue - Installation Charges'
    ELSE 'VAS Revenue - Other'
    END AS vas_category
FROM rr_base rb
    INNER JOIN months m
        ON DATE(rb.rr_start_date) >= m.m_start
        AND DATE(rb.rr_start_date) <  m.m_end
    LEFT JOIN (
    SELECT vas.*, se.user_id, se.user_details_displayId
    FROM furlenco_silver.order_management_systems_evolve.Value_Added_Services AS vas
    JOIN sms_entity se ON vas.entity_type = se.entity_type AND vas.entity_id = se.id
    WHERE vas.state <> 'CANCELLED'
    ) AS vas ON rb.accountable_entity_id = vas.id
WHERE rb.accountable_entity_type = 'VALUE_ADDED_SERVICE'
    )

    , vas_by_category AS (
SELECT
    month_num,
    vas_category                                                         AS component,
    CASE vas_category
    WHEN 'VAS Revenue - Furlenco Care & Flexi' THEN 24
    WHEN 'VAS Revenue - Delivery charges'      THEN 25
    WHEN 'VAS Revenue - Installation Charges'  THEN 26
    ELSE                                             27
    END                                                                  AS sort_order,
    COUNT(DISTINCT accountable_entity_id)                                AS items_count,
    COUNT(DISTINCT user_id)                                              AS cx_count,
    SUM(taxable_amount::float)                                                  AS taxable_revenue
FROM vas_detail
GROUP BY month_num, vas_category
    )

-- ============================================================================
-- SECTION 6: ASSEMBLY AND PIVOT
-- ============================================================================

        , all_components AS (
-- Unpack classified_components (opening, mtp_adj, current_mtp)
SELECT month_num, 'Opening_revenue' AS component, 1 AS sort_order, op_items AS items_count, op_cx AS cx_count, op_rev AS taxable_revenue FROM classified_components
UNION ALL SELECT month_num, 'Minimum tenure charges', 2, mtp_adj_items, mtp_adj_cx, mtp_adj_rev FROM classified_components
UNION ALL SELECT month_num, 'Current month MTP', 14, cmtp_items, cmtp_cx, cmtp_rev FROM classified_components
-- Acquisition (unchanged — single reference each)
UNION ALL SELECT month_num, component, sort_order, items_count, cx_count, taxable_revenue FROM new_deliveries
UNION ALL SELECT month_num, component, sort_order, items_count, cx_count, taxable_revenue FROM upsells
-- Unpack churn_components (6 components from single scan)
UNION ALL SELECT month_num, 'Total pickup (Return Request Date)', 6, tp_items, tp_cx, tp_rev FROM churn_components
UNION ALL SELECT month_num, 'Partial pickup (Reduction in item count)', 7, pp_items, 0, pp_rev FROM churn_components
UNION ALL SELECT month_num, 'Full pickup (Reduction of Cx)', 8, fp_items, fp_cx, fp_rev FROM churn_components
UNION ALL SELECT month_num, 'TTO (Total - TTO Transaction Date)', 9, tto_items, tto_cx, tto_rev FROM churn_components
UNION ALL SELECT month_num, 'TTO - Partial (Reduction in item count)', 10, tto_pp_items, 0, tto_pp_rev FROM churn_components
UNION ALL SELECT month_num, 'TTO - Full (Reduction of Cx)', 11, tto_fp_items, tto_fp_cx, tto_fp_rev FROM churn_components
-- Swaps (unchanged — single reference each)
UNION ALL SELECT month_num, component, sort_order, items_count, cx_count, taxable_revenue FROM swapped_out
UNION ALL SELECT month_num, component, sort_order, items_count, cx_count, taxable_revenue FROM swapped_in
-- Penalty (unchanged)
UNION ALL SELECT month_num, component, sort_order, items_count, cx_count, taxable_revenue FROM penalty
-- Plan transition (unchanged)
UNION ALL SELECT month_num, component, sort_order, items_count, cx_count, taxable_revenue FROM plan_transition
-- Unpack accrual_components (positive + negative from single scan)
UNION ALL SELECT month_num, 'RO (Renewal Overdue) - Positive', 18, pos_items, pos_cx, pos_rev FROM accrual_components
UNION ALL SELECT month_num, 'RO (Renewal Overdue) - Negative', 19, neg_items, neg_cx, neg_rev FROM accrual_components
-- Discounts (unchanged)
UNION ALL SELECT month_num, component, sort_order, items_count, cx_count, taxable_revenue FROM discount_given
UNION ALL SELECT month_num, component, sort_order, items_count, cx_count, taxable_revenue FROM discount_changes_all
-- VAS (unchanged)
UNION ALL SELECT month_num, component, sort_order, items_count, cx_count, taxable_revenue FROM vas_by_category
    )

        , base_wide AS (
SELECT
    component, sort_order,
    MAX(CASE WHEN month_num = 1 THEN items_count     END) AS m1_items,
    MAX(CASE WHEN month_num = 1 THEN cx_count        END) AS m1_cx,
    MAX(CASE WHEN month_num = 1 THEN taxable_revenue END) AS m1_rev,
    MAX(CASE WHEN month_num = 2 THEN items_count     END) AS m2_items,
    MAX(CASE WHEN month_num = 2 THEN cx_count        END) AS m2_cx,
    MAX(CASE WHEN month_num = 2 THEN taxable_revenue END) AS m2_rev
FROM all_components
GROUP BY component, sort_order
    )

        , opening_row AS (
SELECT
    SUM(CASE WHEN (component = 'Opening_revenue'        AND sort_order = 1)
    THEN COALESCE(m1_items, 0) ELSE 0 END) AS m1_items,
    SUM(CASE WHEN (component = 'Opening_revenue'        AND sort_order = 1)
    THEN COALESCE(m1_cx,    0) ELSE 0 END) AS m1_cx,
    SUM(CASE WHEN (component = 'Opening_revenue'        AND sort_order = 1)
    THEN COALESCE(m1_rev,   0) ELSE 0 END) AS m1_rev,
    SUM(CASE WHEN (component = 'Opening_revenue'        AND sort_order = 1)
    THEN COALESCE(m2_items, 0) ELSE 0 END) AS m2_items,
    SUM(CASE WHEN (component = 'Opening_revenue'        AND sort_order = 1)
    THEN COALESCE(m2_cx,    0) ELSE 0 END) AS m2_cx,
    SUM(CASE WHEN (component = 'Opening_revenue'        AND sort_order = 1)
    THEN COALESCE(m2_rev,   0) ELSE 0 END) AS m2_rev
FROM base_wide
WHERE (component = 'Opening_revenue'        AND sort_order = 1)

    )
    , adj_opening_row AS (
SELECT
    SUM(CASE WHEN (component = 'Opening_revenue'        AND sort_order = 1)
    OR  (component = 'Minimum tenure charges' AND sort_order = 2)
    THEN COALESCE(m1_items, 0) ELSE 0 END) AS m1_items,
    SUM(CASE WHEN (component = 'Opening_revenue'        AND sort_order = 1)
    OR  (component = 'Minimum tenure charges' AND sort_order = 2)
    THEN COALESCE(m1_cx,    0) ELSE 0 END) AS m1_cx,
    SUM(CASE WHEN (component = 'Opening_revenue'        AND sort_order = 1)
    OR  (component = 'Minimum tenure charges' AND sort_order = 2)
    THEN COALESCE(m1_rev,   0) ELSE 0 END) AS m1_rev,
    SUM(CASE WHEN (component = 'Opening_revenue'        AND sort_order = 1)
    OR  (component = 'Minimum tenure charges' AND sort_order = 2)
    THEN COALESCE(m2_items, 0) ELSE 0 END) AS m2_items,
    SUM(CASE WHEN (component = 'Opening_revenue'        AND sort_order = 1)
    OR  (component = 'Minimum tenure charges' AND sort_order = 2)
    THEN COALESCE(m2_cx,    0) ELSE 0 END) AS m2_cx,
    SUM(CASE WHEN (component = 'Opening_revenue'        AND sort_order = 1)
    OR  (component = 'Minimum tenure charges' AND sort_order = 2)
    THEN COALESCE(m2_rev,   0) ELSE 0 END) AS m2_rev
FROM base_wide
WHERE (component = 'Opening_revenue'        AND sort_order = 1)
   OR (component = 'Minimum tenure charges' AND sort_order = 2)
    )

    , closing_row AS (
SELECT
    MAX(ao.m1_items)
    + SUM(CASE WHEN bw.component IN (
    'New deliveries (Addition of Cx)',
    'Upsell (Addition in item count)',
    'Partial pickup (Reduction in item count)',
    'Full pickup (Reduction of Cx)',
    'TTO - Partial (Reduction in item count)',
    'TTO - Full (Reduction of Cx)',
    'Swapped out',
    'Swapped in'
    ) THEN COALESCE(bw.m1_items, 0) ELSE 0 END)                          AS m1_items,
    MAX(ao.m1_cx)
    + SUM(CASE WHEN bw.component IN (
    'New deliveries (Addition of Cx)',
    'Upsell (Addition in item count)',
    'Full pickup (Reduction of Cx)',
    'TTO - Full (Reduction of Cx)'
    ) THEN COALESCE(bw.m1_cx, 0) ELSE 0 END)                             AS m1_cx,
    MAX(ao.m1_rev)
    + SUM(CASE WHEN bw.component IN (
    'New deliveries (Addition of Cx)',
    'Upsell (Addition in item count)',
    'Partial pickup (Reduction in item count)',
    'Full pickup (Reduction of Cx)',
    'TTO - Partial (Reduction in item count)',
    'TTO - Full (Reduction of Cx)',
    'Swapped out',
    'Swapped in',
    'Current month MTP',
    'Penalty',
    'Plan transition',
    'Discount change'
    ) THEN COALESCE(bw.m1_rev, 0) ELSE 0 END)                            AS m1_rev,
    MAX(ao.m2_items)
    + SUM(CASE WHEN bw.component IN (
    'New deliveries (Addition of Cx)',
    'Upsell (Addition in item count)',
    'Partial pickup (Reduction in item count)',
    'Full pickup (Reduction of Cx)',
    'TTO - Partial (Reduction in item count)',
    'TTO - Full (Reduction of Cx)',
    'Swapped out',
    'Swapped in'
    ) THEN COALESCE(bw.m2_items, 0) ELSE 0 END)                          AS m2_items,
    MAX(ao.m2_cx)
    + SUM(CASE WHEN bw.component IN (
    'New deliveries (Addition of Cx)',
    'Upsell (Addition in item count)',
    'Full pickup (Reduction of Cx)',
    'TTO - Full (Reduction of Cx)'
    ) THEN COALESCE(bw.m2_cx, 0) ELSE 0 END)                             AS m2_cx,
    MAX(ao.m2_rev)
    + SUM(CASE WHEN bw.component IN (
    'New deliveries (Addition of Cx)',
    'Upsell (Addition in item count)',
    'Partial pickup (Reduction in item count)',
    'Full pickup (Reduction of Cx)',
    'TTO - Partial (Reduction in item count)',
    'TTO - Full (Reduction of Cx)',
    'Swapped out',
    'Swapped in',
    'Current month MTP',
    'Penalty',
    'Plan transition',
    'Discount change'
    ) THEN COALESCE(bw.m2_rev, 0) ELSE 0 END)                            AS m2_rev
FROM base_wide bw
    CROSS JOIN adj_opening_row ao
    )

        , gap_row AS (
SELECT
    COALESCE(o.m2_items, 0) - c.m1_items  AS gap_items,
    COALESCE(o.m2_cx,   0) - c.m1_cx     AS gap_cx,
    COALESCE(o.m2_rev,  0) - c.m1_rev    AS gap_rev
FROM opening_row o
CROSS JOIN closing_row c
    )

        , with_calculated AS (
SELECT component, sort_order, m1_items, m1_cx, m1_rev, m2_items, m2_cx, m2_rev
FROM base_wide

UNION ALL
SELECT 'Adjusted opening' AS component, 3 AS sort_order,
    m1_items, m1_cx, m1_rev, m2_items, m2_cx, m2_rev
FROM adj_opening_row

UNION ALL
SELECT 'Total closing Revenue' AS component, 31 AS sort_order,
    m1_items, m1_cx, m1_rev, m2_items, m2_cx, m2_rev
FROM closing_row

UNION ALL
SELECT 'Gap (Month1 Closing vs Month2 Opening)' AS component, 32 AS sort_order,
    gap_items AS m1_items, gap_cx AS m1_cx, gap_rev AS m1_rev,
    NULL      AS m2_items, NULL   AS m2_cx, NULL    AS m2_rev
FROM gap_row
    )

-- ============================================================================
-- FINAL OUTPUT
-- ============================================================================

SELECT
    component,
    m1_items AS `Month1 Items count`,
    m1_cx    AS `Month1 Cx count`,
    ROUND(m1_rev, 2)   AS `Month1 Taxable revenue (without VAS)`,
    m2_items AS `Month2 Items count`,
    m2_cx    AS `Month2 Cx count`,
    ROUND(m2_rev, 2)   AS `Month2 Taxable revenue (without VAS)`
FROM with_calculated
ORDER BY sort_order;
