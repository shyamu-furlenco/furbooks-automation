
WITH 

-- ============================================================================
-- PHASE 0: NATIVE CHURN & TTO DETECTION (Grafted from Query 1)
-- Calculates true physical state and FULL/PARTIAL scope natively
-- ============================================================================

q1_base AS (
    SELECT
        items.id AS item_id,
        activation_date,
        return_items.state AS return_item_state,
        user_id,
        CASE
            WHEN return_items.created_at IS NULL AND rent_to_purchase_orders.created_at IS NULL THEN NULL
            WHEN rent_to_purchase_orders.created_at IS NULL THEN return_id
            WHEN return_items.created_at IS NULL THEN rent_to_purchase_order_id
        END AS transaction_id,
        CASE
            WHEN return_items.created_at IS NULL AND rent_to_purchase_orders.created_at IS NULL THEN CURRENT_DATE() + INTERVAL 1 DAY
            WHEN rent_to_purchase_orders.created_at IS NULL THEN return_items.created_at + INTERVAL 330 MINUTES
            WHEN return_items.created_at IS NULL THEN rent_to_purchase_orders.created_at + INTERVAL 330 MINUTES
        END AS item_transaction_date,
        NULLIF(get_json_object(CAST(rent_to_purchase_items.payment_details_payableafterpaymentoffers AS STRING), '$.byCashPreTax'), '') AS tto_pay,
        CASE
            WHEN return_items.created_at IS NULL AND rent_to_purchase_orders.created_at IS NULL THEN NULL
            WHEN rent_to_purchase_orders.created_at IS NULL THEN 'return_item'
            WHEN return_items.created_at IS NULL THEN 'rent_to_purchase_item'
        END AS transaction_type,
        return_items.updated_at + INTERVAL 330 MINUTES AS return_item_updated_at
    FROM furlenco_silver.order_management_systems_evolve.items
    LEFT JOIN furlenco_silver.order_management_systems_evolve.return_items
        ON items.id = return_items.item_id AND return_items.state != 'CANCELLED'
    LEFT JOIN furlenco_silver.order_management_systems_evolve.rent_to_purchase_items
        ON rent_to_purchase_items.item_id = items.id AND INSTR(LOWER(CAST(rent_to_purchase_items.payment_details AS STRING)), 'paid') > 0
    LEFT JOIN furlenco_silver.order_management_systems_evolve.rent_to_purchase_orders
        ON rent_to_purchase_orders.id = rent_to_purchase_items.rent_to_purchase_order_id
    WHERE items.vertical = 'FURLENCO_RENTAL'
      AND items.state != 'CANCELLED'
),

q1_user_item_cross AS (
    SELECT
        q1_base.item_id AS base_item_id,
        q1_base.activation_date AS base_activation_date,
        q1_base.user_id AS base_user_id,
        q1_base.transaction_id AS base_transaction_id,
        q1_base.item_transaction_date AS base_item_transaction_date,
        q1_base.transaction_type AS base_transaction_type,
        earlier_items.*
    FROM q1_base
    LEFT JOIN q1_base AS earlier_items
        ON earlier_items.user_id = q1_base.user_id
        AND earlier_items.activation_date < q1_base.item_transaction_date
    WHERE q1_base.transaction_id IS NOT NULL
),

q1_transaction_scope AS (
    SELECT DISTINCT
        base_transaction_id,
        base_user_id,
        base_transaction_type,
        CASE
            WHEN COUNT(DISTINCT base_item_id) = COUNT(DISTINCT CASE WHEN item_transaction_date >= base_item_transaction_date THEN item_id ELSE NULL END) 
            THEN 'FULL' ELSE 'PARTIAL'
        END AS full_or_partial_flag
    FROM q1_user_item_cross
    GROUP BY 1, 2, 3
),

q1_tto_enriched AS (
    SELECT DISTINCT
        i.id AS item_id,
        CASE
            WHEN q1_base.transaction_type = 'rent_to_purchase_item' THEN q1_base.item_transaction_date
            ELSE i.pickup_date
        END AS pickup_date,
        q1_base.item_transaction_date AS payment_date,
        q1_base.transaction_type AS transaction_type,
        transaction_id,
        full_or_partial_flag AS transaction_type_detail
    FROM furlenco_silver.order_management_systems_evolve.items AS i
    LEFT JOIN q1_base ON i.id = q1_base.item_id
    LEFT JOIN q1_transaction_scope
        ON q1_transaction_scope.base_transaction_id = q1_base.transaction_id
        AND q1_base.transaction_type = q1_transaction_scope.base_transaction_type
    WHERE transaction_id IS NOT NULL
),

q1_rental_item_base AS (
    SELECT
        i.id AS item_ids,
        i.user_id AS user_ids,
        i.activation_date AS activation_dates,
        i.composite_item_id,
        t.*
    FROM furlenco_silver.order_management_systems_evolve.items AS i
    LEFT JOIN q1_tto_enriched AS t ON i.id = t.item_id
    WHERE i.vertical = 'FURLENCO_RENTAL' AND i.state <> 'CANCELLED'
),

q1_churn_counts AS (
    SELECT
        b1.item_ids,
        COUNT(DISTINCT b2.item_ids) AS later_items_count
    FROM q1_rental_item_base AS b1
    LEFT JOIN q1_rental_item_base AS b2
        ON b1.user_ids = b2.user_ids
        AND b1.item_ids <> b2.item_ids
        AND b2.activation_dates < b1.pickup_date
        AND (b2.pickup_date IS NULL OR b2.pickup_date > b1.pickup_date)
    GROUP BY b1.item_ids
),

q1_final_tagged AS (
    SELECT
        b.*,
        CASE
            WHEN b.pickup_date IS NULL THEN 'ACTIVE'
            WHEN cc.later_items_count > 0 THEN 'PARTIAL'
            ELSE 'FULL'
        END AS churn_flag
    FROM q1_rental_item_base AS b
    JOIN q1_churn_counts AS cc ON b.item_ids = cc.item_ids
),

-- THE UNIFIED OUTPUT OF PHASE 0 (Replaces the external materialized view)
integrated_churn_query AS (
    SELECT 
        item_ids AS entity_id, 'ITEM' AS entity_type, user_ids, transaction_type, churn_flag
    FROM q1_final_tagged WHERE churn_flag <> 'ACTIVE'
    UNION ALL
    SELECT 
        at.id AS entity_id, 'ATTACHMENT' AS entity_type, ft.user_ids, ft.transaction_type, ft.churn_flag
    FROM q1_final_tagged ft
    JOIN furlenco_silver.order_management_systems_evolve.attachments at ON ft.composite_item_id = at.composite_item_id
    WHERE ft.churn_flag <> 'ACTIVE'
)

-- ============================================================================
-- PHASE 1: Revenue Recognition Base
-- ============================================================================

,rr_base AS (
    SELECT
        rr.accountable_entity_id,
        rr.accountable_entity_type,
        DATE(rr.start_date) AS cycle_start_date,
        DATE(rr.end_date) AS cycle_end_date,
        rr.recognised_at,
        DATE(rr.recognised_at + INTERVAL '330 minutes') AS recognised_at_ist,
        rr.recognition_type,
        rr.state,
        rr.monetary_components_taxableAmount,
        rrs.start_date AS sched_start_date,
        rrs.end_date AS sched_end_date
    FROM furlenco_silver.furbooks_evolve.revenue_recognitions AS rr
    LEFT JOIN furlenco_silver.furbooks_evolve.revenue_recognition_schedules AS rrs
        ON rrs.id = rr.revenue_recognition_schedule_id
    WHERE rr.vertical = 'FURLENCO_RENTAL'
      AND rr.state NOT IN ('CANCELLED', 'INVALIDATED')
      AND rr.deleted_at IS NULL
      AND rr.accountable_entity_type IN ('ITEM', 'ATTACHMENT')
      AND rr.accountable_entity_id = :accountable_entity_id
      -- 1106177
      -- 2192317
      -- 2424177
), 

-- ============================================================================
-- PHASE 1a & 1b: Tag cycles with physical returns and TTO windows
-- ============================================================================

rr_with_return_flag AS (
    SELECT
        rb.*,
        DATE(COALESCE(ri.created_at, ra.created_at) + INTERVAL '330 minutes') AS return_created_at_ist,
        COALESCE(ri.id, ra.id) AS return_item_id,
        COALESCE(ri.state, ra.state) AS return_item_state,
        CASE WHEN COALESCE(ri.id, ra.id) IS NOT NULL THEN TRUE ELSE FALSE END AS has_return_in_window
    FROM rr_base rb
    LEFT JOIN furlenco_silver.order_management_systems_evolve.return_items ri
        ON ri.item_id = rb.accountable_entity_id and rb.accountable_entity_type = 'ITEM' AND ri.state NOT IN ('CANCELLED')
        AND DATE(ri.created_at + INTERVAL '330 minutes') >= rb.cycle_start_date
        AND DATE(ri.created_at + INTERVAL '330 minutes') <= rb.cycle_end_date
    LEFT JOIN furlenco_silver.order_management_systems_evolve.return_attachments ra
        ON ra.attachment_id = rb.accountable_entity_id and rb.accountable_entity_type = 'ATTACHMENT' AND ra.state NOT IN ('CANCELLED')
        AND DATE(ra.created_at + INTERVAL '330 minutes') >= rb.cycle_start_date
        AND DATE(ra.created_at + INTERVAL '330 minutes') <= rb.cycle_end_date
)


,rr_with_tto_flag AS (
    SELECT
        rr.*,
        DATE(rto.created_at + INTERVAL '330 minutes') AS rtp_created_at_ist,
        COALESCE(rtp_i.id, rtp_a.id) AS rent_to_purchase_item_id,
        CASE WHEN rto.id IS NOT NULL THEN TRUE ELSE FALSE END AS has_rtp_in_window
    FROM rr_with_return_flag rr
    LEFT JOIN furlenco_silver.order_management_systems_evolve.rent_to_purchase_items rtp_i
        ON rtp_i.item_id = rr.accountable_entity_id and rr.accountable_entity_type = 'ITEM'
        AND INSTR(LOWER(CAST(rtp_i.payment_details AS STRING)), 'paid') > 0
    LEFT JOIN furlenco_silver.order_management_systems_evolve.rent_to_purchase_attachments rtp_a
        ON rtp_a.attachment_id = rr.accountable_entity_id and rr.accountable_entity_type = 'ATTACHMENT'
        AND INSTR(LOWER(CAST(rtp_a.payment_details AS STRING)), 'paid') > 0
    LEFT JOIN furlenco_silver.order_management_systems_evolve.rent_to_purchase_orders rto
        ON (rto.id = rtp_i.rent_to_purchase_order_id OR rto.id = rtp_a.rent_to_purchase_order_id)
        AND DATE(rto.created_at + INTERVAL '330 minutes') >= rr.cycle_start_date
        AND DATE(rto.created_at + INTERVAL '330 minutes') <= rr.cycle_end_date
        AND rto.state <> 'CANCELLED'
)
-- ============================================================================
-- PHASE 2: Enrich with Native Churn Classification
-- ============================================================================

,cycle_with_churn_detail AS (
    SELECT
        rr.*,
        rcq.churn_flag AS churn_classification,
        rcq.transaction_type AS churn_transaction_type,
        rcq.user_ids AS churn_user_id
    FROM rr_with_tto_flag rr
    -- 🔗 THE HANDSHAKE: Joins to the Phase 0 native logic instead of the external table
    LEFT JOIN integrated_churn_query rcq
        ON rcq.entity_id = rr.accountable_entity_id
        AND rcq.entity_type = rr.accountable_entity_type
        AND (rr.has_return_in_window or has_rtp_in_window)
)

-- -- ============================================================================
-- -- PHASE 2.5: Lifecycle Context & Synthetic Date Injection
-- -- ============================================================================

,cycle_lifecycle_context AS (
    SELECT 
        cwcd.*,
        MAX(cwcd.rtp_created_at_ist) OVER (
            PARTITION BY cwcd.accountable_entity_id, cwcd.accountable_entity_type
        ) AS global_tto_date
    FROM cycle_with_churn_detail cwcd
)

,cycle_with_synthetic_base AS (
    SELECT 
        *,
        SUM(CASE WHEN cycle_start_date > global_tto_date THEN 1 ELSE 0 END) OVER (
            PARTITION BY accountable_entity_id, accountable_entity_type
        ) AS future_cycles_count,
        CASE 
            WHEN cycle_start_date > global_tto_date THEN global_tto_date 
            WHEN recognised_at_ist > rtp_created_at_ist AND rtp_created_at_ist IS NOT NULL THEN rtp_created_at_ist 
            ELSE recognised_at_ist
        END AS synthetic_recognised_at_ist,
        CASE 
            WHEN cycle_start_date > global_tto_date OR (recognised_at_ist > rtp_created_at_ist AND rtp_created_at_ist IS NOT NULL) 
            THEN TRUE ELSE FALSE 
        END AS used_tto_date_for_pattern
    FROM cycle_lifecycle_context
)
-- -- ============================================================================
-- -- PHASE 2b: Generate All-Months Driver (EXPANDED TO 3 YEARS)
-- -- ============================================================================

, months_driver AS (
SELECT
    1 AS month_num,
    DATE_TRUNC('month', try_cast(:month1_start AS DATE))                                                     AS m_start,
    DATE_ADD(ADD_MONTHS(DATE_TRUNC('month',CAST(:month1_start AS DATE)), 1),-1)                 AS m_end,
    ADD_MONTHS(DATE_TRUNC('month',CAST(:month1_start AS DATE)), -1)                             AS prev_start,
    date_format(CAST(:month1_start AS DATE), 'MMM yyyy')                                        AS m_label
UNION ALL
SELECT
    2,
    DATE_TRUNC('month', :month2_start)::DATE,
    DATE_ADD(ADD_MONTHS(DATE_TRUNC('month', CAST(:month2_start AS DATE)), 1),-1),
    ADD_MONTHS(DATE_TRUNC('month', CAST(:month2_start AS DATE)), -1),
    DATE_FORMAT(CAST(:month2_start AS DATE), 'MMM yyyy')
    )
-- -- ============================================================================
-- -- PHASE 3: Detect MTP Classification (Using Synthetic Dates)
-- -- ============================================================================

,cycle_with_months_joined AS (
    SELECT
        csb.*,
        m.month_num,
        m.m_start,
        m.m_end,
        m.prev_start,
        m.m_label,
        CASE
            WHEN csb.cycle_start_date >= m.prev_start AND csb.cycle_start_date < m.m_start
             AND (csb.synthetic_recognised_at_ist IS NULL OR csb.synthetic_recognised_at_ist >= m.prev_start)
            THEN TRUE ELSE FALSE
        END AS matches_s1_1,
        CASE
            WHEN csb.cycle_start_date >= m.m_start AND csb.synthetic_recognised_at_ist >= m.prev_start AND csb.synthetic_recognised_at_ist < m.m_start
            THEN TRUE ELSE FALSE
        END AS matches_s1_2,
        CASE
            WHEN csb.synthetic_recognised_at_ist >= m.m_start AND csb.synthetic_recognised_at_ist < m.m_end
             AND (csb.cycle_start_date >= m.m_end OR (csb.cycle_start_date < m.m_end AND csb.synthetic_recognised_at_ist < csb.cycle_end_date))
            THEN TRUE ELSE FALSE
        END AS matches_current_mtp
    FROM cycle_with_synthetic_base csb
    CROSS JOIN months_driver m
)



,cycle_mtp_classification AS (
    SELECT
        *,
        CASE
            WHEN matches_current_mtp = TRUE THEN 'CURRENT_MTP'
            WHEN matches_s1_2 = TRUE THEN 'MTP'
            WHEN matches_s1_1 = TRUE THEN 'NORMAL_CYCLE'
            ELSE NULL
        END AS mtp_classification,
        m_label AS matched_month_window
    FROM cycle_with_months_joined
    WHERE matches_s1_1 OR matches_s1_2 OR matches_current_mtp  
),

cycle_with_mtp_flag AS (
    SELECT
        *, ROW_NUMBER() OVER (PARTITION BY accountable_entity_id, accountable_entity_type, cycle_start_date, cycle_end_date ORDER BY month_num ASC) AS rn
    FROM cycle_mtp_classification
),

cycle_with_mtp_flag_dedup AS (
    SELECT * EXCEPT(rn) FROM cycle_with_mtp_flag WHERE rn = 1
),

-- ============================================================================
-- PHASE 4: Synthesize Final Cycle Label (WITH NEW TTO PERSONAS)
-- ============================================================================

cycle_labels AS (
    SELECT
        *,
        CASE
            -- 1. Early TTOs (Disguised as MTPs)
            WHEN has_rtp_in_window = TRUE AND future_cycles_count > 0 THEN 'TTO_MTP_ACCELERATOR'
            
            -- 2. Late TTOs (Split by Full vs Partial)
            WHEN has_rtp_in_window = TRUE AND future_cycles_count = 0 AND churn_classification = 'FULL' THEN 'TTO_FULL'
            WHEN has_rtp_in_window = TRUE AND future_cycles_count = 0 AND churn_classification = 'PARTIAL' THEN 'TTO_PARTIAL'

            -- 3. Early Returns (MTPs)
            WHEN mtp_classification = 'CURRENT_MTP' THEN 'CURRENT_MTP'
            WHEN mtp_classification = 'MTP' THEN 'MTP'

            -- 4. Late Returns (Churns)
            WHEN has_return_in_window = TRUE AND churn_classification = 'FULL' THEN 'CHURN_FULL'
            WHEN has_return_in_window = TRUE AND churn_classification = 'PARTIAL' THEN 'CHURN_PARTIAL'
            
            ELSE 'NORMAL_CYCLE'
        END AS primary_cycle_type
    FROM cycle_with_mtp_flag_dedup
),

-- ============================================================================
-- PHASE 5: Enrich with User Context & Bifurcation
-- ============================================================================

cycle_labels_with_context AS (
    SELECT
        cl.*,
        COALESCE(i.user_id, a.user_id) AS user_id,
        COALESCE(i.order_id, a.order_id) AS order_id,
        DATE(COALESCE(i.activation_date, a.activation_date) + INTERVAL '330 minutes') AS item_activation_date,
        COALESCE(i.name, a.name) AS item_name
    FROM cycle_labels cl
    LEFT JOIN furlenco_silver.order_management_systems_evolve.items i
        ON i.id = cl.accountable_entity_id AND cl.accountable_entity_type = 'ITEM'
    LEFT JOIN furlenco_silver.order_management_systems_evolve.attachments a
        ON a.id = cl.accountable_entity_id AND cl.accountable_entity_type = 'ATTACHMENT'
),

item_lifecycle_pattern AS (
    SELECT
        accountable_entity_id,
        accountable_entity_type,
        cycle_start_date,
        cycle_end_date,
        MAX(cycle_end_date) OVER (PARTITION BY accountable_entity_id, accountable_entity_type) AS last_cycle_end_date,
        CASE
            WHEN MAX(CASE WHEN mtp_classification = 'NORMAL_CYCLE' THEN 1 ELSE 0 END) OVER (PARTITION BY accountable_entity_id, accountable_entity_type) = 0
             AND MAX(CASE WHEN mtp_classification IS NOT NULL THEN 1 ELSE 0 END) OVER (PARTITION BY accountable_entity_id, accountable_entity_type) = 1
            THEN 'EARLY_RECOGNITION'
            ELSE 'NORMAL_LIFECYCLE'
        END AS item_recognition_pattern,
        mtp_classification AS mtp_classification_synthetic,
        CASE
            WHEN has_return_in_window = TRUE
             AND MAX(cycle_end_date) OVER (PARTITION BY accountable_entity_id, accountable_entity_type) >= DATE_TRUNC('MONTH', synthetic_recognised_at_ist)
             AND MAX(cycle_end_date) OVER (PARTITION BY accountable_entity_id, accountable_entity_type) <= ADD_MONTHS(DATE_TRUNC('MONTH', synthetic_recognised_at_ist), 1)
            THEN 'CHURN_IN_RECOGNITION_MONTH'
            ELSE 'CHURN_OUTSIDE_MONTH'
        END AS churn_pattern_type,
        ROW_NUMBER() OVER (PARTITION BY accountable_entity_id, accountable_entity_type ORDER BY cycle_end_date DESC) AS cycle_position_from_end
    FROM cycle_labels_with_context
),

cycle_labels_with_bifurcation AS (
    SELECT
        cwc.*,
        ilp.last_cycle_end_date,
        ilp.item_recognition_pattern,
        ilp.churn_pattern_type,
        ilp.cycle_position_from_end,
        ilp.mtp_classification_synthetic,
        CASE WHEN ilp.cycle_position_from_end = 1 THEN TRUE ELSE FALSE END AS is_item_last_cycle
    FROM cycle_labels_with_context cwc
    LEFT JOIN item_lifecycle_pattern ilp
        ON ilp.accountable_entity_id = cwc.accountable_entity_id
        AND ilp.accountable_entity_type = cwc.accountable_entity_type
        AND ilp.cycle_start_date = cwc.cycle_start_date
        AND ilp.cycle_end_date = cwc.cycle_end_date 
)

-- ============================================================================
-- FINAL OUTPUT
-- ============================================================================

SELECT
    accountable_entity_id AS entity_id,
    accountable_entity_type AS entity_type,
    user_id,
    order_id,
    item_activation_date,
    item_name,
    cycle_start_date,
    cycle_end_date,
    recognised_at,
    recognised_at_ist,
    synthetic_recognised_at_ist,
    used_tto_date_for_pattern,
    recognition_type,
    monetary_components_taxableAmount AS cycle_taxable_amount,
    has_return_in_window,
    return_item_id,
    return_item_state,
    return_created_at_ist,
    has_rtp_in_window,
    rent_to_purchase_item_id,
    rtp_created_at_ist,
    churn_classification,
    churn_transaction_type,
    mtp_classification,
    matched_month_window,
    primary_cycle_type,
    last_cycle_end_date,
    item_recognition_pattern,
    churn_pattern_type,
    is_item_last_cycle,
    cycle_position_from_end,
    mtp_classification_synthetic,
    CURRENT_TIMESTAMP() AS extracted_at
FROM cycle_labels_with_bifurcation
--limit 50