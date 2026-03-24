-- ============================================================================
-- REVENUE MOVEMENT ANALYSIS: Parameterized (defaults: Oct 2025 vs Nov 2025)
-- Purpose: Explain monthly revenue changes with full audit trail
-- Owner: Analytics Team
-- Last Updated: 2026-03-16
-- ============================================================================
-- SECTION 0: CONFIGURATION  (Databricks Parameterized Query)
-- Set :month1_start and :month2_start via the widget bar (format: YYYY-MM-01).
-- All other dates and labels are derived automatically using ADD_MONTHS().
-- ============================================================================
-- doc link: https://docs.google.com/document/d/1FBKClYSvw_zk7zeS-sgSthQxOo7qpeN73-vXljkP6t0/edit?usp=sharing


-- Parameters: :month1_start and :month2_start (e.g., '2025-10-01', '2025-11-01')

-- ============================================================================
-- SECTION 1: BASE REVENUE DATA
--
-- TIMEZONE POLICY (Fix 5):
--   - start_date, end_date, to_be_recognised_on: DATE (IST-aligned, no conversion).
--   - recognised_at, created_at, updated_at: UTC TIMESTAMP.
--     Always convert: DATE(<col> + INTERVAL '330 minutes').
--   - Exception: penalty scopes by recognised_at (UTC→IST) — no billing start_date.
-- ============================================================================

-- Important Notes: Currently, Swap In Items are not coming in fb_automation. And swap outs are missing. This could be one of the reasons why the revenue is not matching

-- Unified entity lookup: ITEMs + ATTACHMENTs, excluding CANCELLED rows
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
    CAST(rrs.monetary_components_taxableAmount AS DOUBLE)/ROUND(DATEDIFF(DAY, rrs.start_date, rrs.end_date)/30.45)                    AS taxable_amount,
    CAST(rrs.monetary_components_postTaxAmount AS DOUBLE)/ROUND(DATEDIFF(DAY, rrs.start_date, rrs.end_date)/30.45)                     AS post_tax_amount,
    ROUND(DATEDIFF(DAY, rrs.start_date, rrs.end_date)/30.45) AS tenure,
    DATE(rr.to_be_recognised_on)                             AS to_be_recognised_on,
    DATE(rr.recognised_at  + INTERVAL '330 minutes')         AS recognised_at,
    DATE(rr.created_at     + INTERVAL '330 minutes')         AS created_at,
    rr.external_reference_type,
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

-- Invoice-schedule grain used for plan-transition and accrual-change detection.
-- Excludes INVALIDATED/CANCELLED/PENDING and SETTLEMENT external references.
        , tenure_base AS (
SELECT
    revenue_recognition_type,
    accountable_entity_id,
    accountable_entity_type,
    start_date,
    end_date,
    (CAST(monetary_components_taxableAmount AS DOUBLE) / NULLIF(number_of_invoice_cycles, 0)) AS taxableAmount,
    number_of_invoice_cycles                                                   AS tenure,
    state,
    external_reference_type,
    (created_at + INTERVAL '330 minutes')                                      AS created_at,
    monetary_components
FROM furlenco_silver.furbooks_evolve.invoice_schedules
WHERE vertical = 'FURLENCO_RENTAL'
  AND state NOT IN ('INVALIDATED', 'CANCELLED', 'PENDING')
  AND created_at >= '2024-06-01'
  AND accountable_entity_type IN ('ITEM', 'ATTACHMENT')
  AND external_reference_type <> 'SETTLEMENT'
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

        , pickups_base AS (
SELECT
    accountable_entity_id,
    accountable_entity_type,
    user_id,
    taxable_amount::float,
    pickup_type,
    return_entity_state,
    DATE(return_created_at) AS return_created_at,
    cancelled_at
FROM furlenco_analytics.user_defined_tables.pickup_revenue_movement_item_attachs


    )
-- ----------------------------------------------------------------------------
-- Component 6: Total Pickup Raised
-- ----------------------------------------------------------------------------
        , pickup_raised_all AS (
SELECT /*+ BROADCAST(m) */
    m.month_num,
    'Pickup raised (Total)'                                                                  AS component,
    6                                                                                        AS sort_order,
    -COUNT(DISTINCT CONCAT(pb.accountable_entity_type, '::', pb.accountable_entity_id))      AS items_count,
    -COUNT(DISTINCT pb.user_id)                                                              AS cx_count,
    -SUM(pb.taxable_amount::float::float)                                                                  AS taxable_revenue
FROM pickups_base pb
    JOIN months m
ON pb.return_created_at >= m.m_start
    AND pb.return_created_at <  m.m_end
GROUP BY m.month_num
    )

-- ----------------------------------------------------------------------------
-- Component 7: Partial Pickups
-- ----------------------------------------------------------------------------
        , partial_pickups AS (
SELECT /*+ BROADCAST(m) */
    m.month_num,
    'Partial pickup (Reduction in item count)'                                               AS component,
    7                                                                                        AS sort_order,
    -COUNT(DISTINCT CONCAT(pb.accountable_entity_type, '::', pb.accountable_entity_id))      AS items_count,
    -0                                                                                       AS cx_count,
    -SUM(pb.taxable_amount::float)                                                                  AS taxable_revenue
FROM pickups_base pb
    JOIN months m
ON pb.return_created_at >= m.m_start
    AND pb.return_created_at <  m.m_end
WHERE pb.pickup_type = 'PARTIAL'
  AND pb.return_entity_state <> 'CANCELLED'
GROUP BY m.month_num
    )

-- ----------------------------------------------------------------------------
-- Component 8: Full Pickups
-- ----------------------------------------------------------------------------
        , full_pickups AS (
SELECT /*+ BROADCAST(m) */
    m.month_num,
    'Full pickup (Reduction of Cx)'                                                          AS component,
    8                                                                                        AS sort_order,
    -COUNT(DISTINCT CONCAT(pb.accountable_entity_type, '::', pb.accountable_entity_id))      AS items_count,
    -COUNT(DISTINCT pb.user_id)                                                              AS cx_count,
    -SUM(pb.taxable_amount::float)                                                                  AS taxable_revenue
FROM pickups_base pb
    JOIN months m
ON pb.return_created_at >= m.m_start
    AND pb.return_created_at <  m.m_end
WHERE pb.pickup_type = 'FULL'
  AND pb.return_entity_state <> 'CANCELLED'
GROUP BY m.month_num
    )

-- ----------------------------------------------------------------------------
-- Component 9: Pickup Cancellations
-- ----------------------------------------------------------------------------
        , pickup_cancellations AS (
SELECT /*+ BROADCAST(m) */
    m.month_num,
    'Pickup cancellations'                                                                   AS component,
    9                                                                                        AS sort_order,
    COUNT(DISTINCT CONCAT(pb.accountable_entity_type, '::', pb.accountable_entity_id))       AS items_count,
    COUNT(DISTINCT pb.user_id)                                                               AS cx_count,
    SUM(pb.taxable_amount::float)                                                                   AS taxable_revenue
FROM pickups_base pb
    JOIN months m
ON pb.cancelled_at >= m.m_start
    AND pb.cancelled_at <  m.m_end
WHERE pb.return_entity_state = 'CANCELLED'
GROUP BY m.month_num
    )

-- ----------------------------------------------------------------------------
-- Component 10: Current Month MTP
-- ----------------------------------------------------------------------------
        , current_month_mtp AS (
SELECT /*+ BROADCAST(m) */
    m.month_num,
    'Minimum tenure charges'                                                                 AS component,
    10                                                                                       AS sort_order,
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
-- Component 11: Penalty
-- Timezone note (Fix 5): scopes by recognised_at (UTC→IST).
-- ----------------------------------------------------------------------------
        , penalty AS (
SELECT /*+ BROADCAST(m) */
    m.month_num,
    'Penalty'                                AS component,
    11                                       AS sort_order,
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
-- Component 12 & 13: Plan Transition
-- Single windowed CTE (tenure_windowed) computes all LAG values for both
-- plan-transition and accrual-change detection in one pass over tenure_base.
-- ----------------------------------------------------------------------------
        , tenure_windowed AS (
SELECT
    accountable_entity_id, accountable_entity_type, start_date, end_date,
    tenure, taxableAmount,
    revenue_recognition_type,
    LAG(tenure)                  OVER (PARTITION BY accountable_entity_id, accountable_entity_type ORDER BY start_date) AS previous_tenure,
    LAG(start_date)              OVER (PARTITION BY accountable_entity_id, accountable_entity_type ORDER BY start_date) AS previous_start_date,
    LAG(end_date)                OVER (PARTITION BY accountable_entity_id, accountable_entity_type ORDER BY start_date) AS previous_end_date,
    LAG(taxableAmount)           OVER (PARTITION BY accountable_entity_id, accountable_entity_type ORDER BY start_date) AS previous_taxableAmount,
    LAG(revenue_recognition_type)OVER (PARTITION BY accountable_entity_id, accountable_entity_type ORDER BY start_date) AS previous_recognition_type,
    external_reference_type, created_at, monetary_components
FROM tenure_base
    )

        , customer_plan_changes AS (
SELECT
    tc.accountable_entity_id, tc.accountable_entity_type,
    tc.start_date, tc.end_date,
    tc.previous_tenure, tc.tenure AS current_tenure,
    tc.previous_start_date, tc.previous_end_date,
    se.user_id, tc.external_reference_type,
    tc.previous_taxableAmount::DECIMAL(10,2)                                            AS previous_month_revenue,
    tc.taxableAmount::DECIMAL(10,2)                                                     AS current_month_revenue,
    (tc.taxableAmount::DECIMAL(10,2) - tc.previous_taxableAmount::DECIMAL(10,2))        AS revenue_difference,
    tc.created_at, m.month_num
FROM tenure_windowed tc
    LEFT JOIN sms_entity se ON se.id = tc.accountable_entity_id AND se.entity_type = tc.accountable_entity_type
    JOIN months m ON tc.start_date >= m.m_start AND tc.start_date < m.m_end
WHERE tc.previous_tenure <> tc.tenure
  AND tc.previous_tenure is not null
    )

    , plan_transition_positive AS (
SELECT month_num, 'Plan transition - Positive' AS component, 12 AS sort_order,
    COUNT(DISTINCT CONCAT(accountable_entity_type, '::', accountable_entity_id)) AS items_count,
    COUNT(DISTINCT user_id) AS cx_count, SUM(revenue_difference) AS taxable_revenue
FROM customer_plan_changes
WHERE previous_end_date IS NOT NULL AND previous_tenure IS NOT NULL AND current_tenure > previous_tenure
GROUP BY month_num
    )

        , plan_transition_negative AS (
SELECT month_num, 'Plan transition - Negative' AS component, 13 AS sort_order,
    COUNT(DISTINCT CONCAT(accountable_entity_type, '::', accountable_entity_id)) AS items_count,
    COUNT(DISTINCT user_id) AS cx_count, SUM(revenue_difference) AS taxable_revenue
FROM customer_plan_changes
WHERE previous_end_date IS NOT NULL AND previous_tenure IS NOT NULL AND current_tenure < previous_tenure
GROUP BY month_num
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
SELECT month_num, 'RO (Renewal Overdue) - Positive' AS component, 14 AS sort_order,
    COUNT(DISTINCT CONCAT(accountable_entity_type, '::', accountable_entity_id)) AS items_count,
    COUNT(DISTINCT user_id) AS cx_count, SUM(revenue_difference) AS taxable_revenue
FROM customer_accrual_changes WHERE current_recognition_type = 'ACCRUAL'
GROUP BY month_num
    )

        , accrual_negative AS (
SELECT month_num, 'RO (Renewal Overdue) - Negative' AS component, 15 AS sort_order,
    COUNT(DISTINCT CONCAT(accountable_entity_type, '::', accountable_entity_id)) AS items_count,
    COUNT(DISTINCT user_id) AS cx_count, SUM(revenue_difference) AS taxable_revenue
FROM customer_accrual_changes WHERE current_recognition_type = 'DEFERRAL'
GROUP BY month_num
    )

-- ----------------------------------------------------------------------------
-- Component 16 & 17: Discount Revenue Change
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
    )

    , discount_given AS (
SELECT
    m.month_num,
    'Discount given'                                                                          AS component,
    16                                                                                        AS sort_order,
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
SELECT month_num, 'Discount change' AS component, 17 AS sort_order,
    COUNT(DISTINCT CONCAT(accountable_entity_type, '::', accountable_entity_id)) AS items_count,
    COUNT(DISTINCT user_id) AS cx_count,
    SUM(revenue_difference) AS taxable_revenue
FROM customer_discount_changes
GROUP BY month_num
UNION ALL
SELECT month_num, 'Discount change - Positive' AS component, 18 AS sort_order,
    COUNT(DISTINCT CONCAT(accountable_entity_type, '::', accountable_entity_id)) AS items_count,
    COUNT(DISTINCT user_id) AS cx_count,
    SUM(revenue_difference) AS taxable_revenue
FROM customer_discount_changes
WHERE revenue_difference > 0
GROUP BY month_num
UNION ALL
SELECT month_num, 'Discount change - Negative' AS component, 19 AS sort_order,
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
    WHEN 'VAS Revenue - Furlenco Care & Flexi' THEN 20
    WHEN 'VAS Revenue - Delivery charges'      THEN 21
    WHEN 'VAS Revenue - Installation Charges'  THEN 22
    ELSE                                             23
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
UNION ALL SELECT month_num, component, sort_order, items_count, cx_count, taxable_revenue FROM pickup_raised_all
UNION ALL SELECT month_num, component, sort_order, items_count, cx_count, taxable_revenue FROM partial_pickups
UNION ALL SELECT month_num, component, sort_order, items_count, cx_count, taxable_revenue FROM full_pickups
UNION ALL SELECT month_num, component, sort_order, items_count, cx_count, taxable_revenue FROM pickup_cancellations
UNION ALL SELECT month_num, component, sort_order, items_count, cx_count, taxable_revenue FROM current_month_mtp
UNION ALL SELECT month_num, component, sort_order, items_count, cx_count, taxable_revenue FROM penalty
UNION ALL SELECT month_num, component, sort_order, items_count, cx_count, taxable_revenue FROM plan_transition_positive
UNION ALL SELECT month_num, component, sort_order, items_count, cx_count, taxable_revenue FROM plan_transition_negative
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

-- Adjusted Opening = Opening + MTP1 (MTP1 is negative, so this nets out
-- Scenario 1.2 future-start cycles recognised early)
        , adj_opening_row AS (
SELECT
    SUM(CASE WHEN component IN ('Opening_revenue', 'MTP1') THEN COALESCE(m1_items, 0) ELSE 0 END) AS m1_items,
    SUM(CASE WHEN component IN ('Opening_revenue', 'MTP1') THEN COALESCE(m1_cx,    0) ELSE 0 END) AS m1_cx,
    SUM(CASE WHEN component IN ('Opening_revenue', 'MTP1') THEN COALESCE(m1_rev,   0) ELSE 0 END) AS m1_rev,
    SUM(CASE WHEN component IN ('Opening_revenue', 'MTP1') THEN COALESCE(m2_items, 0) ELSE 0 END) AS m2_items,
    SUM(CASE WHEN component IN ('Opening_revenue', 'MTP1') THEN COALESCE(m2_cx,    0) ELSE 0 END) AS m2_cx,
    SUM(CASE WHEN component IN ('Opening_revenue', 'MTP1') THEN COALESCE(m2_rev,   0) ELSE 0 END) AS m2_rev
FROM base_wide
WHERE component IN ('Opening_revenue', 'MTP1')
    )

-- Total Closing Revenue
-- Intentionally excludes plan transitions (12–13), accrual changes (14–15),
-- discount changes (17–19), and VAS (20–23). These are informational movement
-- components only; the closing balance reflects headcount-driven revenue changes
-- (new deliveries, pickups, MTP, penalty) applied to the adjusted opening.
    , closing_row AS (
SELECT
    -- Items
    -- MAX() wraps ao.* because adj_opening_row is a 1-row CTE; MAX(x) = x but satisfies Spark GROUP BY rules
    MAX(ao.m1_items)
    + SUM(CASE WHEN bw.component IN (
    'New deliveries (Addition of Cx)',
    'Upsell (Addition in item count)',
    'Partial pickup (Reduction in item count)',
    'Full pickup (Reduction of Cx)'
    ) THEN COALESCE(bw.m1_items, 0) ELSE 0 END)                          AS m1_items,
    -- Cx
    MAX(ao.m1_cx)
    + SUM(CASE WHEN bw.component IN (
    'New deliveries (Addition of Cx)',
    'Upsell (Addition in item count)',
    'Full pickup (Reduction of Cx)'
    ) THEN COALESCE(bw.m1_cx, 0) ELSE 0 END)                             AS m1_cx,
    -- Revenue
    MAX(ao.m1_rev)
    + SUM(CASE WHEN bw.component IN (
    'New deliveries (Addition of Cx)',
    'Upsell (Addition in item count)',
    'Partial pickup (Reduction in item count)',
    'Full pickup (Reduction of Cx)',
    'Minimum tenure charges',
    'Penalty'
    ) THEN COALESCE(bw.m1_rev, 0) ELSE 0 END)                            AS m1_rev,
    -- Month 2 items
    MAX(ao.m2_items)
    + SUM(CASE WHEN bw.component IN (
    'New deliveries (Addition of Cx)',
    'Upsell (Addition in item count)',
    'Partial pickup (Reduction in item count)',
    'Full pickup (Reduction of Cx)'
    ) THEN COALESCE(bw.m2_items, 0) ELSE 0 END)                          AS m2_items,
    -- Month 2 cx
    MAX(ao.m2_cx)
    + SUM(CASE WHEN bw.component IN (
    'New deliveries (Addition of Cx)',
    'Upsell (Addition in item count)',
    'Full pickup (Reduction of Cx)'
    ) THEN COALESCE(bw.m2_cx, 0) ELSE 0 END)                             AS m2_cx,
    -- Month 2 revenue
    MAX(ao.m2_rev)
    + SUM(CASE WHEN bw.component IN (
    'New deliveries (Addition of Cx)',
    'Upsell (Addition in item count)',
    'Partial pickup (Reduction in item count)',
    'Full pickup (Reduction of Cx)',
    'Minimum tenure charges',
    'Penalty'
    ) THEN COALESCE(bw.m2_rev, 0) ELSE 0 END)                            AS m2_rev
FROM base_wide bw
    CROSS JOIN adj_opening_row ao
    )

-- Gap = Month2 Opening minus Month1 Closing
        , gap_row AS (
SELECT
    COALESCE((SELECT m2_items FROM base_wide WHERE component = 'Opening_revenue'), 0) -
    (SELECT m1_items FROM closing_row)  AS gap_items,
    COALESCE((SELECT m2_cx   FROM base_wide WHERE component = 'Opening_revenue'), 0) -
    (SELECT m1_cx    FROM closing_row)  AS gap_cx,
    COALESCE((SELECT m2_rev  FROM base_wide WHERE component = 'Opening_revenue'), 0) -
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
SELECT 'Total closing Revenue' AS component, 26 AS sort_order,
    m1_items, m1_cx, m1_rev, m2_items, m2_cx, m2_rev
FROM closing_row

UNION ALL
SELECT 'Gap (Month1 Closing vs Month2 Opening)' AS component, 27 AS sort_order,
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
