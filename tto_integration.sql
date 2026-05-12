%sql
-- ============================================================================
-- FURBOOKS_CLASSIFIED: Full Item History with Cycle Tags (CHURN/TTO/MTP)
-- ============================================================================

-- ============================================================================
-- PHASE 0: SHARED LOOKUPS & BASE DATA
-- ============================================================================

WITH rental_churn_query AS (
    SELECT
        entity_id,
        entity_type,
        user_ids,
        activation_date,
        churn_flag,
        transaction_type,
        payment_date,
        transaction_id
    FROM furlenco_analytics.user_defined_tables.rental_churn_query
    WHERE churn_flag IN ('FULL', 'PARTIAL')  
)

-- ============================================================================
-- PHASE 1: Revenue Recognition Base
-- ============================================================================

, rr_base AS (
    SELECT
        rr.accountable_entity_id,
        rr.accountable_entity_type,
        DATE(rr.start_date)                                    AS cycle_start_date,
        DATE(rr.end_date)                                      AS cycle_end_date,
        rr.recognised_at,
        DATE(rr.recognised_at + INTERVAL '330 minutes')        AS recognised_at_ist,
        rr.recognition_type,
        rr.state,
        rr.monetary_components_taxableAmount,
        rrs.start_date                                         AS sched_start_date,
        rrs.end_date                                           AS sched_end_date
    FROM furlenco_silver.furbooks_evolve.revenue_recognitions AS rr
        LEFT JOIN furlenco_silver.furbooks_evolve.revenue_recognition_schedules AS rrs
            ON rrs.id = rr.revenue_recognition_schedule_id
    WHERE rr.vertical = 'FURLENCO_RENTAL'
      AND rr.state NOT IN ('CANCELLED', 'INVALIDATED')
      AND rr.deleted_at IS NULL
      AND rr.accountable_entity_type IN ('ITEM', 'ATTACHMENT')
)

-- ============================================================================
-- PHASE 1a: Tag cycles with return_item window matches
-- ============================================================================

,rr_with_return_flag AS (
    SELECT
        rb.*,
        
        -- [FIXED] Combine both ri and ra dates
        DATE(COALESCE(ri.created_at, ra.created_at) + INTERVAL '330 minutes') AS return_created_at_ist,
        
        -- [FIXED] Combine both IDs
        COALESCE(ri.id, ra.id)                                                AS return_item_id,
        
        -- [FIXED] Combine both states
        COALESCE(ri.state, ra.state)                                          AS return_item_state,
        
        -- [FIXED] Check if EITHER the item or attachment was returned
        CASE WHEN COALESCE(ri.id, ra.id) IS NOT NULL THEN TRUE ELSE FALSE END AS has_return_in_window
        
    FROM rr_base rb
        -- 1. Safely get the physical records
        LEFT JOIN furlenco_silver.order_management_systems_evolve.attachments att
            ON rb.accountable_entity_type = 'ATTACHMENT' AND att.id = rb.accountable_entity_id
        LEFT JOIN furlenco_silver.order_management_systems_evolve.items i
            ON (rb.accountable_entity_type = 'ITEM' AND i.id = rb.accountable_entity_id)
            OR (rb.accountable_entity_type = 'ATTACHMENT' AND i.composite_item_id = att.composite_item_id AND i.state <> 'CANCELLED')
            
        -- 2a. Join Return Items (This catches main items, and attachments where the parent item was returned)
        LEFT JOIN furlenco_silver.order_management_systems_evolve.return_items ri
            ON ri.item_id = i.id
            AND ri.state NOT IN ('CANCELLED')
            AND DATE(ri.created_at + INTERVAL '330 minutes') >= rb.cycle_start_date
            AND DATE(ri.created_at + INTERVAL '330 minutes') <= rb.cycle_end_date
        
        -- 2b. Join Return Attachments (This catches direct attachment returns)
        LEFT JOIN furlenco_silver.order_management_systems_evolve.return_attachments ra
            ON ra.attachment_id = att.id
            AND ra.state NOT IN ('CANCELLED')
            AND DATE(ra.created_at + INTERVAL '330 minutes') >= rb.cycle_start_date
            AND DATE(ra.created_at + INTERVAL '330 minutes') <= rb.cycle_end_date
)

-- ============================================================================
-- PHASE 1b: Tag cycles with rent_to_purchase window matches (TTO)
-- ============================================================================
, rr_with_tto_flag AS (
    SELECT
        rr.*,
        DATE(rto.created_at + INTERVAL '330 minutes')          AS rtp_created_at_ist,
        COALESCE(rtp_i.id, rtp_a.id)                           AS rent_to_purchase_item_id,
        CASE WHEN rto.id IS NOT NULL THEN TRUE ELSE FALSE END  AS has_rtp_in_window
    FROM rr_with_return_flag rr
        -- 1. Safely get the physical records
        LEFT JOIN furlenco_silver.order_management_systems_evolve.attachments att
            ON rr.accountable_entity_type = 'ATTACHMENT' AND att.id = rr.accountable_entity_id
        LEFT JOIN furlenco_silver.order_management_systems_evolve.items i
            ON rr.accountable_entity_type = 'ITEM' AND i.id = rr.accountable_entity_id AND i.state <> 'CANCELLED'
            
        -- 2a. Join TTO Items
        LEFT JOIN furlenco_silver.order_management_systems_evolve.rent_to_purchase_items rtp_i
            ON rtp_i.item_id = i.id
            AND rr.accountable_entity_type = 'ITEM'
            AND INSTR(LOWER(CAST(rtp_i.payment_details AS STRING)), 'paid') > 0
            
        -- 2b. [NEW] Join TTO Attachments
        LEFT JOIN furlenco_silver.order_management_systems_evolve.rent_to_purchase_attachments rtp_a
            ON rtp_a.attachment_id = att.id
            AND rr.accountable_entity_type = 'ATTACHMENT'
            AND INSTR(LOWER(CAST(rtp_a.payment_details AS STRING)), 'paid') > 0
            
        -- 3. Join TTO Orders (Linked to whichever one matched)
        LEFT JOIN furlenco_silver.order_management_systems_evolve.rent_to_purchase_orders rto
            ON (rto.id = rtp_i.rent_to_purchase_order_id OR rto.id = rtp_a.rent_to_purchase_order_id)
            AND DATE(rto.created_at + INTERVAL '330 minutes') >= rr.cycle_start_date
            AND DATE(rto.created_at + INTERVAL '330 minutes') <= rr.cycle_end_date
            AND rto.state <> 'CANCELLED'
)
-- ============================================================================
-- PHASE 2: Enrich with Churn Classification
-- ============================================================================

, cycle_with_churn_detail AS (
    SELECT
        rr.*,
        rcq.churn_flag                                         AS churn_classification,
        rcq.transaction_type                                   AS churn_transaction_type,
        rcq.user_ids                                           AS churn_user_id
    FROM rr_with_tto_flag rr
        LEFT JOIN rental_churn_query rcq
            ON rcq.entity_id = rr.accountable_entity_id
            AND rcq.entity_type = rr.accountable_entity_type
            AND rr.has_return_in_window = TRUE
)

-- ============================================================================
-- [NEW] PHASE 2.5: Lifecycle Context & Synthetic Date Injection
-- Broadcasts TTO dates to zombie cycles and counts future cycles
-- ============================================================================

, cycle_lifecycle_context AS (
    SELECT 
        cwcd.*,
        -- Count cycles occurring AFTER this specific row
        -- COUNT(cwcd.cycle_start_date) OVER (
        --     PARTITION BY cwcd.accountable_entity_id, cwcd.accountable_entity_type 
        --     ORDER BY cwcd.cycle_start_date ASC 
        --     ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING
        -- ) AS future_cycles_count,
        
        -- Broadcast the TTO date to the entire item history
        MAX(cwcd.rtp_created_at_ist) OVER (
            PARTITION BY cwcd.accountable_entity_id, cwcd.accountable_entity_type
        ) AS global_tto_date

    FROM cycle_with_churn_detail cwcd
)

, cycle_with_synthetic_base AS (
    SELECT 
        *,
        SUM(CASE WHEN cycle_start_date > global_tto_date THEN 1 ELSE 0 END) OVER (
    PARTITION BY accountable_entity_id, accountable_entity_type
) AS future_cycles_count,
        -- Generate synthetic date early so Phase 3 can use it for MTP logic
        CASE 
            WHEN cycle_start_date > global_tto_date THEN global_tto_date -- Zombie TTO Override
            WHEN recognised_at_ist > rtp_created_at_ist AND rtp_created_at_ist IS NOT NULL THEN rtp_created_at_ist -- Early override
            ELSE recognised_at_ist
        END AS synthetic_recognised_at_ist,
        
        -- Flag if we used the TTO override
        CASE 
            WHEN cycle_start_date > global_tto_date OR (recognised_at_ist > rtp_created_at_ist AND rtp_created_at_ist IS NOT NULL) 
            THEN TRUE ELSE FALSE 
        END AS used_tto_date_for_pattern
    FROM cycle_lifecycle_context
)

-- ============================================================================
-- PHASE 2b: Generate All-Months Driver
-- ============================================================================

, months_driver AS (
    WITH base_months AS (
        SELECT 0 AS month_offset UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3
        UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7
        UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10 UNION ALL SELECT 11
    )
    SELECT
        ROW_NUMBER() OVER (ORDER BY month_offset DESC) AS month_num,
        ADD_MONTHS(TRUNC(CURRENT_DATE(), 'MM'), -month_offset) AS m_start,
        ADD_MONTHS(TRUNC(CURRENT_DATE(), 'MM'), -month_offset + 1) AS m_end,
        ADD_MONTHS(TRUNC(CURRENT_DATE(), 'MM'), -month_offset - 1) AS prev_start,
        DATE_FORMAT(ADD_MONTHS(TRUNC(CURRENT_DATE(), 'MM'), -month_offset), 'MMM yyyy') AS m_label
    FROM base_months
)

-- ============================================================================
-- PHASE 3: Detect MTP Classification
-- IMPORTANT: Pointing existing logic to synthetic_recognised_at_ist
-- ============================================================================

, cycle_with_months_joined AS (
    SELECT
        csb.*,
        m.month_num,
        m.m_start,
        m.m_end,
        m.prev_start,
        m.m_label,
        -- S1.1: Normal cycles 
        CASE
            WHEN csb.cycle_start_date >= m.prev_start
             AND csb.cycle_start_date < m.m_start
             AND (csb.synthetic_recognised_at_ist IS NULL OR csb.synthetic_recognised_at_ist >= m.prev_start)
            THEN TRUE ELSE FALSE
        END AS matches_s1_1,
        -- S1.2: MTP cycles
        CASE
            WHEN csb.cycle_start_date >= m.m_start
             AND csb.synthetic_recognised_at_ist >= m.prev_start
             AND csb.synthetic_recognised_at_ist < m.m_start
            THEN TRUE ELSE FALSE
        END AS matches_s1_2,
        -- Current Month MTP
        CASE
            WHEN csb.synthetic_recognised_at_ist >= m.m_start
             AND csb.synthetic_recognised_at_ist < m.m_end
             AND (
                 csb.cycle_start_date >= m.m_end 
                 OR 
                 (csb.cycle_start_date < m.m_end AND csb.synthetic_recognised_at_ist < csb.cycle_end_date)
             )
            THEN TRUE ELSE FALSE
        END AS matches_current_mtp
    FROM cycle_with_synthetic_base csb
        CROSS JOIN months_driver m
)

, cycle_mtp_classification AS (
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
)

, cycle_with_mtp_flag AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY accountable_entity_id, accountable_entity_type, cycle_start_date, cycle_end_date
            ORDER BY month_num ASC
        ) AS rn
    FROM cycle_mtp_classification
)

, cycle_with_mtp_flag_dedup AS (
    SELECT * EXCEPT(rn)
    FROM cycle_with_mtp_flag
    WHERE rn = 1
)

-- ============================================================================
-- PHASE 4: Synthesize Final Cycle Label (WITH NEW TTO PERSONAS)
-- ============================================================================

, cycle_labels AS (
    SELECT
        *,
        CASE
            -- [NEW] SCENARIO 3: ZOMBIE TTO
            --WHEN cycle_start_date > global_tto_date THEN 'TTO_ZOMBIE_CYCLE'
            
            -- [NEW] SCENARIO 2: MID-CYCLE TTO ACCELERATOR
            WHEN has_rtp_in_window = TRUE AND future_cycles_count > 0 THEN 'TTO_MTP_ACCELERATOR'
            
            -- [NEW] SCENARIO 1: TERMINAL TTO CHURN
            WHEN has_rtp_in_window = TRUE AND future_cycles_count = 0 THEN 'TTO_CHURN_TERMINAL'

            -- Original MTP tags
            WHEN mtp_classification = 'CURRENT_MTP' THEN 'CURRENT_MTP'
            WHEN mtp_classification = 'MTP' THEN 'MTP'

            -- Original return tags
            WHEN has_return_in_window = TRUE AND churn_classification = 'FULL' THEN 'CHURN_FULL'
            WHEN has_return_in_window = TRUE AND churn_classification = 'PARTIAL' THEN 'CHURN_PARTIAL'
            
            
            ELSE 'NORMAL_CYCLE'
        END AS primary_cycle_type
    FROM cycle_with_mtp_flag_dedup
)

-- ============================================================================
-- PHASE 5: Enrich with User Context & Bifurcation
-- ============================================================================

, cycle_labels_with_context AS (
    SELECT
        cl.*,
        COALESCE(i.user_id, a.user_id)                         AS user_id,
        COALESCE(i.order_id, a.order_id)                       AS order_id,
        DATE(COALESCE(i.activation_date, a.activation_date) + INTERVAL '330 minutes') AS item_activation_date,
        COALESCE(i.name, a.name)                               AS item_name
    FROM cycle_labels cl
        -- Get context if it's an ITEM
        LEFT JOIN furlenco_silver.order_management_systems_evolve.items i
            ON i.id = cl.accountable_entity_id
            AND cl.accountable_entity_type = 'ITEM'
        -- [NEW] Get context if it's an ATTACHMENT
        LEFT JOIN furlenco_silver.order_management_systems_evolve.attachments a
            ON a.id = cl.accountable_entity_id
            AND cl.accountable_entity_type = 'ATTACHMENT'
)

, item_lifecycle_pattern AS (
    SELECT
        accountable_entity_id,
        accountable_entity_type,
        cycle_start_date,
        cycle_end_date,
        MAX(cycle_end_date) OVER (PARTITION BY accountable_entity_id, accountable_entity_type) AS last_cycle_end_date,
        
        CASE
            WHEN MAX(CASE WHEN mtp_classification = 'NORMAL_CYCLE' THEN 1 ELSE 0 END) 
                 OVER (PARTITION BY accountable_entity_id, accountable_entity_type) = 0
             AND MAX(CASE WHEN mtp_classification IS NOT NULL THEN 1 ELSE 0 END) 
                 OVER (PARTITION BY accountable_entity_id, accountable_entity_type) = 1
            THEN 'EARLY_RECOGNITION'
            ELSE 'NORMAL_LIFECYCLE'
        END AS item_recognition_pattern,
        
        mtp_classification AS mtp_classification_synthetic,
        
        CASE
            WHEN has_return_in_window = TRUE
             AND MAX(cycle_end_date) OVER (PARTITION BY accountable_entity_id, accountable_entity_type) 
                 >= DATE_TRUNC('MONTH', synthetic_recognised_at_ist)
             AND MAX(cycle_end_date) OVER (PARTITION BY accountable_entity_id, accountable_entity_type) 
                 <= ADD_MONTHS(DATE_TRUNC('MONTH', synthetic_recognised_at_ist), 1)
            THEN 'CHURN_IN_RECOGNITION_MONTH'
            ELSE 'CHURN_OUTSIDE_MONTH'
        END AS churn_pattern_type,
        
        ROW_NUMBER() OVER (
            PARTITION BY accountable_entity_id, accountable_entity_type 
            ORDER BY cycle_end_date DESC
        ) AS cycle_position_from_end
    FROM cycle_labels_with_context
)

, cycle_labels_with_bifurcation AS (
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
    accountable_entity_id                                       AS entity_id,
    accountable_entity_type                                     AS entity_type,
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
    monetary_components_taxableAmount                           AS cycle_taxable_amount,
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
    CURRENT_TIMESTAMP()                                         AS extracted_at
FROM cycle_labels_with_bifurcation
--where user_id is null
--WHERE accountable_entity_id = :accountable_entity_id
--ORDER BY user_id, entity_id, cycle_start_date;