
with sms_entity AS (
    SELECT id, 'ITEM' AS entity_type, user_id, user_details_displayId
    FROM furlenco_silver.order_management_systems_evolve.items
    WHERE state <> 'CANCELLED'
    UNION ALL
    SELECT id, 'ATTACHMENT' AS entity_type, user_id, user_details_displayId
    FROM furlenco_silver.order_management_systems_evolve.attachments
    WHERE state <> 'CANCELLED'
)

-- Fix 1: Pre-deduplicate to one row per entity before joining.
   , deduped_return_items AS (
    SELECT item_id, MIN(created_at) AS created_at
    FROM furlenco_silver.order_management_systems_evolve.return_items
    WHERE state = 'COMPLETED'
    GROUP BY item_id
)

-- Earliest completed return per attachment (mirrors deduped_return_items for ATTACHMENT entities)
   , deduped_return_attachments AS (
    SELECT attachment_id, MIN(created_at) AS created_at
    FROM furlenco_silver.order_management_systems_evolve.return_attachments
    WHERE state = 'COMPLETED'
    GROUP BY attachment_id
)

-- Core revenue grain: one row per recognition cycle with normalised amounts,
-- IST-converted timestamps, and return / schedule metadata pre-joined.
   , furbooks_revenue AS (
    SELECT
        rr.id,
        rr.recognition_type,
        rr.accountable_entity_id,
        rr.accountable_entity_type,
        rr.state,
    DATE(rr.start_date)                                      AS start_date,
    DATE(rr.end_date)                                        AS end_date,
    CAST(rrs.monetary_components_taxableAmount AS DOUBLE)/ROUND(DATEDIFF(DAY, rrs.start_date, rrs.end_date)/30.45)                    AS taxable_amount1,
    CAST(rrs.monetary_components_postTaxAmount AS DOUBLE)/ROUND(DATEDIFF(DAY, rrs.start_date, rrs.end_date)/30.45)                     AS post_tax_amount,
    ROUND(DATEDIFF(DAY, rrs.start_date, rrs.end_date)/30.45) AS tenure,
    DATE(rr.to_be_recognised_on)                             AS to_be_recognised_on,
    DATE(rr.recognised_at  + INTERVAL '330 minutes')         AS recognised_at,
    DATE(rr.created_at     + INTERVAL '330 minutes')         AS created_at,
    rr.external_reference_type,
    rr.monetary_components_taxableAmount as taxable_amount,
    rr.external_reference_id,
    rr.revenue_recognition_schedule_id,
    sms_entity.user_id,
    sms_entity.user_details_displayId,
    MIN(DATE(rr.start_date)) OVER (
    PARTITION BY rr.accountable_entity_id, rr.accountable_entity_type
    ) AS min_start_date,
-- Fix 1: uses deduped CTEs (one row per entity)
    COALESCE(
    DATE(r_item.created_at       + INTERVAL '330 minutes'),
    DATE(r_attachment.created_at + INTERVAL '330 minutes')
    ) AS return_created_at
FROM furlenco_silver.furbooks_evolve.revenue_recognitions AS rr
    LEFT JOIN deduped_return_items AS r_item
ON rr.accountable_entity_id   = r_item.item_id
    AND rr.accountable_entity_type = 'ITEM'
    LEFT JOIN deduped_return_attachments AS r_attachment
    ON rr.accountable_entity_id   = r_attachment.attachment_id
    AND rr.accountable_entity_type = 'ATTACHMENT'
    LEFT JOIN furlenco_silver.furbooks_evolve.revenue_recognition_schedules AS rrs
    ON rrs.id = rr.revenue_recognition_schedule_id
    LEFT JOIN sms_entity
    ON sms_entity.id          = rr.accountable_entity_id
    AND sms_entity.entity_type = rr.accountable_entity_type
WHERE rr.vertical = 'FURLENCO_RENTAL'
  AND rr.state NOT IN ('CANCELLED', 'INVALIDATED')
  AND rr.accountable_entity_type IN ('ITEM', 'ATTACHMENT')
    )

-- ============================================================================
-- SECTION 2: MONTHS DRIVER
-- ============================================================================

-- 2-row driver: one row per analysis month; broadcast-eligible in all joins
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

-- Revenue-recognition-schedule grain used for plan-transition and accrual-change detection.
-- Sourced from revenue_recognitions (aligned with furbooks_revenue) joined to
-- revenue_recognition_schedules for schedule-level tenure and amounts.
-- QUALIFY deduplicates to one row per (entity, schedule) — earliest recognition per schedule.
-- Secondary sort on created_at in tenure_windowed resolves ties when two schedules share a start_date.
        , tenure_base AS (
SELECT
    rr.recognition_type                                                        AS revenue_recognition_type,
    rr.accountable_entity_id,
    rr.accountable_entity_type,
    DATE(rrs.start_date)                                                       AS start_date,
    DATE(rrs.end_date)                                                         AS end_date,
    CAST(rrs.monetary_components_taxableAmount AS DOUBLE)
    / NULLIF(ROUND(DATEDIFF(DAY, rrs.start_date, rrs.end_date) / 30.45), 0) AS taxableAmount,
    ROUND(DATEDIFF(DAY, rrs.start_date, rrs.end_date) / 30.45)                AS tenure,
    rr.external_reference_type,
    DATE(rr.created_at + INTERVAL '330 minutes')                               AS created_at,
    rrs.monetary_components
FROM furlenco_silver.furbooks_evolve.revenue_recognitions rr
    LEFT JOIN furlenco_silver.furbooks_evolve.revenue_recognition_schedules rrs
ON rrs.id = rr.revenue_recognition_schedule_id
WHERE rr.vertical = 'FURLENCO_RENTAL'
  AND rr.state NOT IN ('CANCELLED', 'INVALIDATED')
  AND rr.accountable_entity_type IN ('ITEM', 'ATTACHMENT')
  AND rr.external_reference_type <> 'SETTLEMENT'
  AND rr.created_at >= '2024-06-01'
    QUALIFY ROW_NUMBER() OVER (
    PARTITION BY rr.accountable_entity_id, rr.accountable_entity_type, rr.revenue_recognition_schedule_id
    ORDER BY rr.created_at ASC
    ) = 1
    )


-- ============================================================================
-- SECTION 4: UNIFIED COMPONENT CTEs
-- Fix 2 applied throughout: COUNT(DISTINCT entity_id) → composite key.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Component 1: Opening Revenue
-- ----------------------------------------------------------------------------
    , opening_revenue AS (
SELECT /*+ BROADCAST(m) */
    m.month_num,
    'Opening_revenue'                                                                       AS component,
    1                                                                                       AS sort_order,
    COUNT(DISTINCT CONCAT(br.accountable_entity_type, '::', br.accountable_entity_id))      AS items_count,
    COUNT(DISTINCT br.user_id)                                                              AS cx_count,
    SUM(br.taxable_amount::float)                                                                  AS taxable_revenue
FROM furbooks_revenue br
    INNER JOIN months m
ON (
    -- Scenario 1.1: Normal cycles — start_date IN prev_month, recognised_at NOT IN prev_month (or NULL/FUTURE)
    (    br.start_date    >= m.prev_start
    AND br.start_date    <  m.m_start
    AND (br.recognised_at IS NULL
    -- OR br.recognised_at <  m.prev_start
    OR br.recognised_at >= m.m_start)
    )
    OR
    -- Scenario 1.2: MTP cycles — start_date NOT IN prev_month, recognised_at IN prev_month
    (   (br.start_date >= m.m_start)
    AND br.recognised_at >= m.prev_start
    AND br.recognised_at <  m.m_start
    )
    )
GROUP BY m.month_num
    )

-- ----------------------------------------------------------------------------
-- Component 2: Minimum Tenure Charges Adjustment (Opening)
-- Scenario 1.2 only: future-start cycles (start_date >= m_start) recognised
-- early in prev_month — these are subtracted to avoid double-counting opening.
-- ----------------------------------------------------------------------------
        , mtp1_adjustment AS (
SELECT /*+ BROADCAST(m) */
    m.month_num,
    'Minimum tenure charges'                                                                AS component,
    2                                                                                       AS sort_order,
    -COUNT(DISTINCT CONCAT(br.accountable_entity_type, '::', br.accountable_entity_id))     AS items_count,
    -COUNT(DISTINCT br.user_id)                                                             AS cx_count,
    -SUM(br.taxable_amount::float)                                                                 AS taxable_revenue
FROM furbooks_revenue br
    INNER JOIN months m
ON br.start_date    >= m.m_start
    AND br.recognised_at >= m.prev_start
    AND br.recognised_at <  m.m_start
GROUP BY m.month_num
    )

    -- ----------------------------------------------------------------------------
-- Component 14: Current Month MTP
-- ----------------------------------------------------------------------------
        , current_month_mtp AS (
SELECT /*+ BROADCAST(m) */
    m.month_num,
    'Current month MTP'                                                                 AS component,
    14                                                                                       AS sort_order,
    -COUNT(DISTINCT CONCAT(br.accountable_entity_type, '::', br.accountable_entity_id))      AS items_count,
    -COUNT(DISTINCT br.user_id)                                                              AS cx_count,
    -SUM(br.taxable_amount::float)                                                                  AS taxable_revenue
FROM furbooks_revenue br
    INNER JOIN months m
ON br.recognised_at >= m.m_start
    AND br.recognised_at <  m.m_end
    AND br.start_date    >= m.m_end
GROUP BY m.month_num
    )

-- ----------------------------------------------------------------------------
-- Component 4: New Deliveries
-- ----------------------------------------------------------------------------
        , new_deliveries AS (
SELECT /*+ BROADCAST(m) */
    m.month_num,
    'New deliveries (Addition of Cx)'       AS component,
    4                                       AS sort_order,
    COUNT(DISTINCT f.accountable_entity_id) AS items_count,
    COUNT(DISTINCT f.fur_id)               AS cx_count,
    SUM(f.taxable_amount::float)                    AS taxable_revenue
FROM furlenco_analytics.user_defined_tables.rental_acquition_unified f
    JOIN months m
ON f.activation_date >= m.m_start
    AND f.activation_date <  m.m_end
WHERE lower(f.flag_based_on_Ua) = 'new'
GROUP BY m.month_num
    )

-- ----------------------------------------------------------------------------
-- Component 5: Upsells
-- ----------------------------------------------------------------------------
        , upsells AS (
SELECT /*+ BROADCAST(m) */
    m.month_num,
    'Upsell (Addition in item count)'       AS component,
    5                                       AS sort_order,
    COUNT(DISTINCT f.accountable_entity_id) AS items_count,
    COUNT(DISTINCT f.fur_id)               AS cx_count,
    SUM(f.taxable_amount::float)                    AS taxable_revenue
FROM furlenco_analytics.user_defined_tables.rental_acquition_unified f
    JOIN months m
ON f.activation_date >= m.m_start
    AND f.activation_date <  m.m_end
WHERE LOWER(f.flag_based_on_Ua) = 'upsell'
GROUP BY m.month_num
    )

-- Pickup-date base from rental_churn_query.
-- rnk = 1 → most recent RR per entity (avoids fan-out across multiple RR periods).
-- CAST(pickup_date AS DATE) normalises mixed DATE/TIMESTAMP from churn query.
        , churn_pickups_base AS (
SELECT
    entity_id,
    entity_type,
    user_ids,
    taxable_amount,
    churn_flag,
    transaction_type,
    payment_date,
    CAST(pickup_date AS DATE)                                                    AS pickup_date
FROM furlenco_analytics.user_defined_tables.rental_churn_query
WHERE rnk         = 1
  and (entity_id, entity_type) not in (select accountable_entity_id, accountable_entity_type
    FROM furbooks_revenue br
    INNER JOIN months m
    ON br.recognised_at >= m.m_start
  AND br.recognised_at <  m.m_end
  AND br.start_date    >= m.m_end)

    )



-- ----------------------------------------------------------------------------
-- Component 6: Total Pickup — return_item only (Pickup date basis)
-- ----------------------------------------------------------------------------
    , total_pickups AS (
SELECT /*+ BROADCAST(m) */
    m.month_num,
    'Total pickup (Return Request Date)'                                           AS component,
    6                                                                            AS sort_order,
    -COUNT(DISTINCT CONCAT(cp.entity_type, '::', cp.entity_id))                  AS items_count,
    -COUNT(DISTINCT cp.user_ids)                                                 AS cx_count,
    -SUM(cp.taxable_amount::FLOAT)                                               AS taxable_revenue
FROM churn_pickups_base                                                          cp
    JOIN months                                                                      m
ON cp.payment_date >= m.m_start
    AND cp.payment_date <  m.m_end
WHERE cp.transaction_type = 'return_item'
GROUP BY m.month_num
    )

-- ----------------------------------------------------------------------------
-- Component 7: Partial Pickups — return_item only (Pickup date basis)
-- ----------------------------------------------------------------------------
        , partial_pickups AS (
SELECT /*+ BROADCAST(m) */
    m.month_num,
    'Partial pickup (Reduction in item count)'                                   AS component,
    7                                                                            AS sort_order,
    -COUNT(DISTINCT CONCAT(cp.entity_type, '::', cp.entity_id))                  AS items_count,
    -0                                                                           AS cx_count,
    -SUM(cp.taxable_amount::FLOAT)                                               AS taxable_revenue
FROM churn_pickups_base                                                          cp
    JOIN months                                                                      m
ON cp.payment_date >= m.m_start
    AND cp.payment_date <  m.m_end
WHERE cp.churn_flag       = 'PARTIAL'
  AND cp.transaction_type = 'return_item'
GROUP BY m.month_num
    )

-- ----------------------------------------------------------------------------
-- Component 8: Full Pickups — return_item only (Pickup date basis)
-- ----------------------------------------------------------------------------
        , full_pickups AS (
SELECT /*+ BROADCAST(m) */
    m.month_num,
    'Full pickup (Reduction of Cx)'                                              AS component,
    8                                                                            AS sort_order,
    -COUNT(DISTINCT CONCAT(cp.entity_type, '::', cp.entity_id))                  AS items_count,
    -COUNT(DISTINCT cp.user_ids)                                                 AS cx_count,
    -SUM(cp.taxable_amount::FLOAT)                                               AS taxable_revenue
FROM churn_pickups_base                                                          cp
    JOIN months                                                                      m
ON cp.payment_date >= m.m_start
    AND cp.payment_date <  m.m_end
WHERE cp.churn_flag       = 'FULL'
  AND cp.transaction_type = 'return_item'
GROUP BY m.month_num
    )

-- ----------------------------------------------------------------------------
-- Component 9: TTO Total (Pickup date basis)
-- ----------------------------------------------------------------------------
        , total_tto AS (
SELECT /*+ BROADCAST(m) */
    m.month_num,
    'TTO (Total - TTO Transaction Date)'                                            AS component,
    9                                                                            AS sort_order,
    -COUNT(DISTINCT CONCAT(cp.entity_type, '::', cp.entity_id))                  AS items_count,
    -COUNT(DISTINCT cp.user_ids)                                                 AS cx_count,
    -SUM(cp.taxable_amount::FLOAT)                                               AS taxable_revenue
FROM churn_pickups_base                                                          cp
    JOIN months                                                                      m
ON cp.payment_date >= m.m_start
    AND cp.payment_date <  m.m_end
WHERE cp.transaction_type = 'rent_to_purchase_item'
GROUP BY m.month_num
    )

-- ----------------------------------------------------------------------------
-- Component 10: TTO Partial (Pickup date basis)
-- ----------------------------------------------------------------------------
        , partial_tto AS (
SELECT /*+ BROADCAST(m) */
    m.month_num,
    'TTO - Partial (Reduction in item count)'                                    AS component,
    10                                                                           AS sort_order,
    -COUNT(DISTINCT CONCAT(cp.entity_type, '::', cp.entity_id))                  AS items_count,
    -0                                                                           AS cx_count,
    -SUM(cp.taxable_amount::FLOAT)                                               AS taxable_revenue
FROM churn_pickups_base                                                          cp
    JOIN months                                                                      m
ON cp.payment_date >= m.m_start
    AND cp.payment_date <  m.m_end
WHERE cp.transaction_type = 'rent_to_purchase_item'
  AND cp.churn_flag       = 'PARTIAL'
GROUP BY m.month_num
    )

-- ----------------------------------------------------------------------------
-- Component 11: TTO Full (Pickup date basis)
-- ----------------------------------------------------------------------------
        , full_tto AS (
SELECT /*+ BROADCAST(m) */
    m.month_num,
    'TTO - Full (Reduction of Cx)'                                               AS component,
    11                                                                           AS sort_order,
    -COUNT(DISTINCT CONCAT(cp.entity_type, '::', cp.entity_id))                  AS items_count,
    -COUNT(DISTINCT cp.user_ids)                                                 AS cx_count,
    -SUM(cp.taxable_amount::FLOAT)                                               AS taxable_revenue
FROM churn_pickups_base                                                          cp
    JOIN months                                                                      m
ON cp.payment_date >= m.m_start
    AND cp.payment_date <  m.m_end
WHERE cp.transaction_type = 'rent_to_purchase_item'
  AND cp.churn_flag       = 'FULL'
GROUP BY m.month_num
    )

-- ----------------------------------------------------------------------------
-- Swap-out base: entities physically swapped out, one row per entity per RR period.
-- rr_rnk = 1 → most recent RR on/before fulfillment_date (avoids fan-out).
-- fulfillment_date converted to IST DATE for consistent month bucketing.
-- Both swap_attachments and swap_items filtered to action = 'SWAP_OUT' + state = 'FULFILLED'.
-- ----------------------------------------------------------------------------
        , swap_base AS (
SELECT
    sw.entity_id,
    sw.entity_type,
    se.user_id,
    DATE(sw.fulfillment_date + INTERVAL '330 minutes')                           AS fulfillment_date,
    br.taxable_amount,
    DENSE_RANK() OVER (
    PARTITION BY sw.entity_id, sw.entity_type
    ORDER BY br.start_date DESC
    )                                                                            AS rr_rnk
FROM (
    SELECT attachment_id AS entity_id, 'ATTACHMENT' AS entity_type, fulfillment_date
    FROM furlenco_silver.order_management_systems_evolve.swap_attachments
    WHERE action           = 'SWAP_OUT'
    AND state            = 'FULFILLED'
    AND fulfillment_date IS NOT NULL
    UNION ALL
    SELECT item_id AS entity_id, 'ITEM' AS entity_type, fulfillment_date
    FROM furlenco_silver.order_management_systems_evolve.swap_items
    WHERE action           = 'SWAP_OUT'
    AND state            = 'FULFILLED'
    AND fulfillment_date IS NOT NULL
    ) sw
    LEFT JOIN sms_entity                                                             se
ON  se.id          = sw.entity_id
    AND se.entity_type = sw.entity_type
    LEFT JOIN furbooks_revenue                                                       br
    ON  br.accountable_entity_id   = sw.entity_id
    AND br.accountable_entity_type = sw.entity_type
    AND br.start_date             <= DATE(sw.fulfillment_date + INTERVAL '330 minutes')
    )

    -- ----------------------------------------------------------------------------
-- Component 12: Swapped Out (Fulfillment date basis)
-- cx_count = 0: customer retains subscription (swap-in replaces the item).
-- ----------------------------------------------------------------------------
    , swapped_out AS (
SELECT /*+ BROADCAST(m) */
    m.month_num,
    'Swapped out'                                                                AS component,
    12                                                                           AS sort_order,
    -COUNT(DISTINCT CONCAT(sb.entity_type, '::', sb.entity_id))                  AS items_count,
    -0                                                                           AS cx_count,
    -SUM(sb.taxable_amount::FLOAT)                                               AS taxable_revenue
FROM swap_base                                                                   sb
    JOIN months                                                                      m
ON sb.fulfillment_date >= m.m_start
    AND sb.fulfillment_date <  m.m_end
WHERE sb.rr_rnk = 1
GROUP BY m.month_num
    )

-- Swap-in base: entities physically swapped in, one row per entity per RR period.
-- rr_rnk = 1 → first RR on/after fulfillment_date (new item's opening cycle).
-- Uses ASC ordering to capture the incoming item's first revenue cycle.
        , swap_in_base AS (
SELECT
    sw.entity_id,
    sw.entity_type,
    se.user_id,
    DATE(sw.fulfillment_date + INTERVAL '330 minutes')                           AS fulfillment_date,
    br.taxable_amount,
    DENSE_RANK() OVER (
    PARTITION BY sw.entity_id, sw.entity_type
    ORDER BY br.start_date ASC
    )                                                                            AS rr_rnk
FROM (
    SELECT attachment_id AS entity_id, 'ATTACHMENT' AS entity_type, fulfillment_date
    FROM furlenco_silver.order_management_systems_evolve.swap_attachments
    WHERE action           = 'SWAP_IN'
    AND state            = 'FULFILLED'
    AND fulfillment_date IS NOT NULL
    UNION ALL
    SELECT item_id AS entity_id, 'ITEM' AS entity_type, fulfillment_date
    FROM furlenco_silver.order_management_systems_evolve.swap_items
    WHERE action           = 'SWAP_IN'
    AND state            = 'FULFILLED'
    AND fulfillment_date IS NOT NULL
    ) sw
    LEFT JOIN sms_entity                                                             se
ON  se.id          = sw.entity_id
    AND se.entity_type = sw.entity_type
    LEFT JOIN furbooks_revenue                                                       br
    ON  br.accountable_entity_id   = sw.entity_id
    AND br.accountable_entity_type = sw.entity_type
    AND br.start_date             >= DATE(sw.fulfillment_date + INTERVAL '330 minutes')
    )

    -- ----------------------------------------------------------------------------
-- Component 13: Swapped In (Fulfillment date basis)
-- cx_count = 0: existing customer receiving a replacement item (no new subscriber).
-- ----------------------------------------------------------------------------
    , swapped_in AS (
SELECT /*+ BROADCAST(m) */
    m.month_num,
    'Swapped in'                                                                 AS component,
    13                                                                           AS sort_order,
    COUNT(DISTINCT CONCAT(sb.entity_type, '::', sb.entity_id))                   AS items_count,
    0                                                                            AS cx_count,
    SUM(sb.taxable_amount::FLOAT)                                                AS taxable_revenue
FROM swap_in_base                                                                sb
    JOIN months                                                                      m
ON sb.fulfillment_date >= m.m_start
    AND sb.fulfillment_date <  m.m_end
WHERE sb.rr_rnk = 1
GROUP BY m.month_num
    )



-- ----------------------------------------------------------------------------
-- Component 11: Penalty
-- Timezone note (Fix 5): scopes by recognised_at (UTC→IST).
-- ----------------------------------------------------------------------------
        , penalty AS (
SELECT /*+ BROADCAST(m) */
    m.month_num,
    'Penalty'                                AS component,
    15                                       AS sort_order,
    COUNT(DISTINCT product_entity_id)         AS items_count,
    COUNT(DISTINCT pl.user_id)                AS cx_count,
    SUM(CAST(rr.monetary_components_taxableAmount AS DOUBLE)) AS taxable_revenue
FROM furlenco_silver.furbooks_evolve.revenue_recognitions rr
    JOIN furlenco_silver.order_management_systems_evolve.penalty pl
ON rr.accountable_entity_id = pl.id
    JOIN months m
    ON DATE(rr.recognised_at + INTERVAL '330 minutes') >= m.m_start
    AND DATE(rr.recognised_at + INTERVAL '330 minutes') <  m.m_end
WHERE rr.vertical = 'FURLENCO_RENTAL'
  AND rr.state NOT IN ('CANCELLED', 'INVALIDATED')
  AND rr.accountable_entity_type = 'PENALTY'
GROUP BY m.month_num
    )

-- ----------------------------------------------------------------------------
-- Accrual Change Detection
-- tenure_windowed computes LAG values for accrual-change detection over tenure_base.
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

-- ----------------------------------------------------------------------------
-- Component 14 & 15: Accrual Revenue Change
-- ----------------------------------------------------------------------------
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

    , accrual_positive AS (
SELECT month_num, 'RO (Renewal Overdue) - Positive' AS component, 18 AS sort_order,
    COUNT(DISTINCT CONCAT(accountable_entity_type, '::', accountable_entity_id)) AS items_count,
    COUNT(DISTINCT user_id) AS cx_count, SUM(revenue_difference) AS taxable_revenue
FROM customer_accrual_changes WHERE current_recognition_type = 'ACCRUAL'
GROUP BY month_num
    )

        , accrual_negative AS (
SELECT month_num, 'RO (Renewal Overdue) - Negative' AS component, 19 AS sort_order,
    COUNT(DISTINCT CONCAT(accountable_entity_type, '::', accountable_entity_id)) AS items_count,
    COUNT(DISTINCT user_id) AS cx_count, SUM(revenue_difference) AS taxable_revenue
FROM customer_accrual_changes WHERE current_recognition_type = 'DEFERRAL'
GROUP BY month_num
    )

-- ----------------------------------------------------------------------------
-- Component 16: Plan Transition
-- Fires only when tenure changed between consecutive schedules.
-- Revenue delta = change in UPFRONT discount (godfather type = 'UPFRONT').
-- Two-CTE pattern: Spark SQL prohibits JOIN in same FROM as LATERAL VIEW EXPLODE.
-- ----------------------------------------------------------------------------

-- Step A: One RR per schedule (earliest) — dedup before exploding discounts.
        , rr_schedule_deduped AS (
SELECT
    accountable_entity_id,
    accountable_entity_type,
    revenue_recognition_schedule_id,
    monetary_components_discounts
FROM furlenco_silver.furbooks_evolve.revenue_recognitions
WHERE vertical = 'FURLENCO_RENTAL'
  AND state NOT IN ('CANCELLED', 'INVALIDATED')
  AND accountable_entity_type IN ('ITEM', 'ATTACHMENT')
  AND external_reference_type <> 'SETTLEMENT'
  AND created_at >= '2024-06-01'
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY accountable_entity_id, accountable_entity_type, revenue_recognition_schedule_id
    ORDER BY created_at ASC
) = 1
    )

-- Step B: Explode discount array from deduped RRs (no JOINs allowed here).
        , rr_schedule_discounts_exploded AS (
SELECT
    r.accountable_entity_id,
    r.accountable_entity_type,
    r.revenue_recognition_schedule_id,
    d.catalogReferenceId,
    d.amount AS discount_amount
FROM rr_schedule_deduped r
    LATERAL VIEW OUTER EXPLODE(
    from_json(CAST(r.monetary_components_discounts AS STRING),
    'array<struct<amount:double,catalogReferenceId:string,code:string>>')
    ) AS d
    )

-- Step C: Sum UPFRONT discounts per schedule; include tenure for gate condition.
        , upfront_discount_per_schedule AS (
SELECT
    e.accountable_entity_id,
    e.accountable_entity_type,
    e.revenue_recognition_schedule_id,
    DATE(rrs.start_date)                                               AS start_date,
    ROUND(DATEDIFF(DAY, rrs.start_date, rrs.end_date) / 30.45)        AS tenure,
    SUM(COALESCE(e.discount_amount, 0))                                AS upfront_discount_amount
FROM rr_schedule_discounts_exploded e
    JOIN furlenco_silver.godfather_evolve.discounts gd
    ON e.catalogReferenceId = gd.id AND gd.type = 'UPFRONT'
    JOIN furlenco_silver.furbooks_evolve.revenue_recognition_schedules rrs
    ON rrs.id = e.revenue_recognition_schedule_id
GROUP BY
    e.accountable_entity_id, e.accountable_entity_type,
    e.revenue_recognition_schedule_id,
    DATE(rrs.start_date),
    ROUND(DATEDIFF(DAY, rrs.start_date, rrs.end_date) / 30.45)
    )

-- Step D: LAG to compare UPFRONT discount and tenure across consecutive schedules.
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

-- Step E: Apply tenure gate + join to months.
-- revenue_difference = previous_upfront_discount - current_upfront_discount.
        , customer_plan_transition AS (
SELECT
    pt.accountable_entity_id, pt.accountable_entity_type,
    pt.start_date, pt.previous_start_date,
    pt.tenure          AS current_tenure,
    pt.previous_tenure,
    se.user_id,
    (pt.previous_upfront_discount_amount - pt.upfront_discount_amount) AS revenue_difference,
    m.month_num
FROM plan_transition_windowed pt
    LEFT JOIN sms_entity se
    ON se.id = pt.accountable_entity_id AND se.entity_type = pt.accountable_entity_type
    JOIN months m
    ON pt.start_date >= m.m_start AND pt.start_date < m.m_end
WHERE pt.previous_tenure IS NOT NULL
  AND pt.previous_tenure <> pt.tenure
  AND pt.previous_upfront_discount_amount IS NOT NULL
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

-- ----------------------------------------------------------------------------
-- Discount Revenue Change
-- Excludes entities where tenure changed (those are captured in plan_transition).
-- ----------------------------------------------------------------------------
        , discount_per_cycle AS (
SELECT
    rr.accountable_entity_id, rr.accountable_entity_type,
    DATE(rr.start_date) AS start_date, DATE(rr.end_date) AS end_date,
    COALESCE(SUM(d.amount::DECIMAL(10,2)), 0) AS total_discount_amount
FROM furlenco_silver.furbooks_evolve.revenue_recognitions AS rr
    LATERAL VIEW OUTER EXPLODE(
    from_json(CAST(rr.monetary_components_discounts AS STRING),
    'array<struct<amount:double,catalogReferenceId:string,code:string>>')
    ) AS d
WHERE rr.vertical = 'FURLENCO_RENTAL'
  AND rr.state NOT IN ('CANCELLED', 'INVALIDATED')
  AND rr.accountable_entity_type IN ('ITEM', 'ATTACHMENT')
  AND rr.created_at >= '2024-06-01'
GROUP BY rr.accountable_entity_id, rr.accountable_entity_type, rr.start_date, rr.end_date
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
    JOIN months m ON dc.start_date >= m.m_start AND dc.start_date < m.m_end
WHERE dc.previous_discount_amount IS NOT NULL
  AND dc.previous_discount_amount <> dc.total_discount_amount
  -- Exclude entities where tenure changed — those are already captured in plan_transition
  AND NOT EXISTS (
        SELECT 1 FROM customer_plan_transition cpt
        WHERE cpt.accountable_entity_id   = dc.accountable_entity_id
          AND cpt.accountable_entity_type = dc.accountable_entity_type
          AND cpt.month_num               = m.month_num
  )
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

-- Discount change total (17) + breakdown by direction (18 positive, 19 negative)
-- consolidated into one CTE over a single scan of customer_discount_changes.
        , discount_changes_all AS (
SELECT month_num, 'Discount change' AS component, 21 AS sort_order,
    COUNT(DISTINCT CONCAT(accountable_entity_type, '::', accountable_entity_id)) AS items_count,
    COUNT(DISTINCT user_id) AS cx_count,
    SUM(revenue_difference) AS taxable_revenue
FROM customer_discount_changes
GROUP BY month_num
UNION ALL
SELECT month_num, 'Discount change - Positive' AS component, 22 AS sort_order,
    COUNT(DISTINCT CONCAT(accountable_entity_type, '::', accountable_entity_id)) AS items_count,
    COUNT(DISTINCT user_id) AS cx_count,
    SUM(revenue_difference) AS taxable_revenue
FROM customer_discount_changes
WHERE revenue_difference > 0
GROUP BY month_num
UNION ALL
SELECT month_num, 'Discount change - Negative' AS component, 23 AS sort_order,
    COUNT(DISTINCT CONCAT(accountable_entity_type, '::', accountable_entity_id)) AS items_count,
    COUNT(DISTINCT user_id) AS cx_count,
    SUM(revenue_difference) AS taxable_revenue
FROM customer_discount_changes
WHERE revenue_difference < 0
GROUP BY month_num
    )

-- ----------------------------------------------------------------------------
-- Component 20–23: VAS Revenue
-- ----------------------------------------------------------------------------
-- Raw VAS rows joined to SMS entity; flags current vs previous month timing
-- for downstream grouping into category-level buckets (vas_by_category).
        , vas_detail AS (
SELECT /*+ BROADCAST(m) */
    rr.accountable_entity_id, rr.external_reference_id,
    rr.start_date, rr.end_date, rr.recognised_at,
    CAST(rr.monetary_components_taxableAmount AS DOUBLE) AS taxable_amount,
    vas.entity_id, vas.entity_type, vas.type AS vas_type, vas.user_id,
    m.month_num,
    CASE
    WHEN vas.type IN ('FURLENCO_CARE_PROGRAM', 'FLEXI_CANCELLATION') THEN 'VAS Revenue - Furlenco Care & Flexi'
    WHEN vas.type = 'DELIVERY_CHARGE'                                THEN 'VAS Revenue - Delivery charges'
    WHEN vas.type = 'AC_INSTALLATION_CHARGE'                         THEN 'VAS Revenue - Installation Charges'
    ELSE 'VAS Revenue - Other'
    END AS vas_category,
    CASE
    WHEN DATE(rr.start_date) >= m.m_start    AND DATE(rr.start_date) < m.m_end    THEN 'Current month VAS'
    WHEN DATE(rr.start_date) >= m.prev_start AND DATE(rr.start_date) < m.m_start  THEN 'Previous month VAS'
    END AS vas_timing
FROM furlenco_silver.furbooks_evolve.revenue_recognitions rr
    CROSS JOIN months m
    LEFT JOIN (
    SELECT vas.*, se.user_id, se.user_details_displayId
    FROM furlenco_silver.order_management_systems_evolve.Value_Added_Services AS vas
    JOIN sms_entity se ON vas.entity_type = se.entity_type AND vas.entity_id = se.id
    WHERE vas.state <> 'CANCELLED'
    ) AS vas ON rr.accountable_entity_id = vas.id
WHERE rr.vertical = 'FURLENCO_RENTAL'
  AND rr.state NOT IN ('CANCELLED', 'INVALIDATED')
  AND rr.accountable_entity_type = 'VALUE_ADDED_SERVICE'
  AND DATE(rr.start_date) >= m.prev_start
  AND DATE(rr.start_date) <  m.m_end
    )

-- VAS aggregated by category (sort orders 20–23); previous month VAS excluded
-- from this output — it appears only in vas_detail for audit purposes.
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
WHERE vas_timing = 'Current month VAS'
GROUP BY month_num, vas_category
    )

-- ============================================================================
-- SECTION 6: ASSEMBLY AND PIVOT
-- ============================================================================

        , all_components AS (
SELECT month_num, component, sort_order, items_count, cx_count, taxable_revenue FROM opening_revenue
UNION ALL SELECT month_num, component, sort_order, items_count, cx_count, taxable_revenue FROM mtp1_adjustment
UNION ALL SELECT month_num, component, sort_order, items_count, cx_count, taxable_revenue FROM new_deliveries
UNION ALL SELECT month_num, component, sort_order, items_count, cx_count, taxable_revenue FROM upsells
UNION ALL SELECT month_num, component, sort_order, items_count, cx_count, taxable_revenue FROM total_pickups
UNION ALL SELECT month_num, component, sort_order, items_count, cx_count, taxable_revenue FROM partial_pickups
UNION ALL SELECT month_num, component, sort_order, items_count, cx_count, taxable_revenue FROM full_pickups
UNION ALL SELECT month_num, component, sort_order, items_count, cx_count, taxable_revenue FROM total_tto
UNION ALL SELECT month_num, component, sort_order, items_count, cx_count, taxable_revenue FROM partial_tto
UNION ALL SELECT month_num, component, sort_order, items_count, cx_count, taxable_revenue FROM full_tto
UNION ALL SELECT month_num, component, sort_order, items_count, cx_count, taxable_revenue FROM swapped_out
UNION ALL SELECT month_num, component, sort_order, items_count, cx_count, taxable_revenue FROM swapped_in
UNION ALL SELECT month_num, component, sort_order, items_count, cx_count, taxable_revenue FROM current_month_mtp
UNION ALL SELECT month_num, component, sort_order, items_count, cx_count, taxable_revenue FROM penalty
UNION ALL SELECT month_num, component, sort_order, items_count, cx_count, taxable_revenue FROM plan_transition
UNION ALL SELECT month_num, component, sort_order, items_count, cx_count, taxable_revenue FROM accrual_positive
UNION ALL SELECT month_num, component, sort_order, items_count, cx_count, taxable_revenue FROM accrual_negative
UNION ALL SELECT month_num, component, sort_order, items_count, cx_count, taxable_revenue FROM discount_given
UNION ALL SELECT month_num, component, sort_order, items_count, cx_count, taxable_revenue FROM discount_changes_all
UNION ALL SELECT month_num, component, sort_order, items_count, cx_count, taxable_revenue FROM vas_by_category
    )

-- Pivot: one column-set per month
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

-- Adjusted Opening = Opening + mtp1_adjustment (mtp1_adjustment is negative, so this nets out
-- Scenario 1.2 future-start cycles recognised early — leaving S1.1 only)
-- Fix: was filtering for 'MTP1' which never existed; correct filter uses sort_order to distinguish
-- mtp1_adjustment (sort_order=2) from current_month_mtp (sort_order=14), both named 'Minimum tenure charges'


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

-- Total Closing Revenue
-- Includes plan transition revenue delta (UPFRONT discount change when tenure changed).
-- Excludes accrual changes, discount changes, and VAS — these are informational only.
-- Items/cx counts are unaffected by plan changes (same subscriber, same item count).
    , closing_row AS (
SELECT
    -- Items
    -- MAX() wraps ao.* because adj_opening_row is a 1-row CTE; MAX(x) = x but satisfies Spark GROUP BY rules
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
    -- Cx
    MAX(ao.m1_cx)
    + SUM(CASE WHEN bw.component IN (
    'New deliveries (Addition of Cx)',
    'Upsell (Addition in item count)',
    'Full pickup (Reduction of Cx)',
    'TTO - Full (Reduction of Cx)'
    ) THEN COALESCE(bw.m1_cx, 0) ELSE 0 END)                             AS m1_cx,
    -- Revenue
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
    'Plan transition'
    ) THEN COALESCE(bw.m1_rev, 0) ELSE 0 END)                            AS m1_rev,
    -- Month 2 items
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
    -- Month 2 cx
    MAX(ao.m2_cx)
    + SUM(CASE WHEN bw.component IN (
    'New deliveries (Addition of Cx)',
    'Upsell (Addition in item count)',
    'Full pickup (Reduction of Cx)',
    'TTO - Full (Reduction of Cx)'
    ) THEN COALESCE(bw.m2_cx, 0) ELSE 0 END)                             AS m2_cx,
    -- Month 2 revenue
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
    'Plan transition'
    ) THEN COALESCE(bw.m2_rev, 0) ELSE 0 END)                            AS m2_rev
FROM base_wide bw
    CROSS JOIN adj_opening_row ao
    )

-- Gap = Month2 Adjusted Opening minus Month1 Closing
-- Fix: was using raw Opening_revenue (S1.1+S1.2); now uses adj_opening_row (S1.1 only, MTP-adjusted)
        , gap_row AS (
SELECT
    COALESCE((SELECT m2_items FROM opening_row), 0) -
    (SELECT m1_items FROM closing_row)  AS gap_items,
    COALESCE((SELECT m2_cx   FROM opening_row), 0) -
    (SELECT m1_cx    FROM closing_row)  AS gap_cx,
    COALESCE((SELECT m2_rev  FROM opening_row), 0) -
    (SELECT m1_rev   FROM closing_row)  AS gap_rev
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
-- Fix 4: Generic Month1/Month2 column labels (no hardcoded month names).
-- Rows 1–21: standard revenue movement bridge.
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
