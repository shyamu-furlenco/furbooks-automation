# Databricks notebook source
# ============================================================================
# FURBOOKS REVENUE BRIDGE — PySpark Notebook (v1)
#
# Key strategy: materialize the 5 heavy base DataFrames (sms_entity, rr_base,
# furbooks_revenue, furbooks_classified, discounts_exploded) as temporary
# Delta tables in furlenco_analytics.tmp so every downstream cell reads from
# Delta instead of re-scanning source tables. (.cache() not supported on
# serverless compute.)
#
# Cell execution order: 1 → 2 → 3 → 4 → 5 → 6 → 7 → 8 → 9 → 10 → 11 → 12 → 13 → 14 → 15
# ============================================================================

# COMMAND ----------
# =============================================================================
# CELL 1 — PARAMETERS
# Edit MONTH1_START and MONTH2_START before running.
# =============================================================================

MONTH1_START = "2026-01-01"   # Format: YYYY-MM-DD (first day of Month 1)
MONTH2_START = "2026-02-01"   # Format: YYYY-MM-DD (first day of Month 2)

print(f"Month 1 start : {MONTH1_START}")
print(f"Month 2 start : {MONTH2_START}")

# COMMAND ----------
# =============================================================================
# CELL 1b — TEMP SCHEMA SETUP
# Materialize heavy DFs as Delta tables (serverless doesn't support .cache()).
# Run-ID suffix prevents collisions on concurrent runs.
# =============================================================================

import uuid
RUN_ID = uuid.uuid4().hex[:8]
TMP_SCHEMA = "furlenco_analytics.tmp"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TMP_SCHEMA}")

TMP_SMS = f"{TMP_SCHEMA}.sms_entity_{RUN_ID}"
TMP_RR  = f"{TMP_SCHEMA}.rr_base_{RUN_ID}"
TMP_FR  = f"{TMP_SCHEMA}.furbooks_revenue_{RUN_ID}"
TMP_FC  = f"{TMP_SCHEMA}.furbooks_classified_{RUN_ID}"
TMP_DE  = f"{TMP_SCHEMA}.discounts_exploded_{RUN_ID}"
TMP_BW  = f"{TMP_SCHEMA}.base_wide_{RUN_ID}"

print(f"Temp tables will use suffix: {RUN_ID}")

# COMMAND ----------
# =============================================================================
# CELL 2 — STAGE 1: sms_entity   [MATERIALIZED → Delta]
# Union of items + attachments. Referenced 8+ times downstream.
# Caching here eliminates 8 redundant table scans.
# =============================================================================

sms_entity_df = spark.sql("""
    SELECT id, 'ITEM' AS entity_type, user_id, user_details_displayId
    FROM furlenco_silver.order_management_systems_evolve.items
    WHERE state <> 'CANCELLED'
    UNION ALL
    SELECT id, 'ATTACHMENT' AS entity_type, user_id, user_details_displayId
    FROM furlenco_silver.order_management_systems_evolve.attachments
    WHERE state <> 'CANCELLED'
""")
sms_entity_df.write.format("delta").mode("overwrite").saveAsTable(TMP_SMS)
spark.sql(f"CREATE OR REPLACE TEMP VIEW sms_entity AS SELECT * FROM {TMP_SMS}")
print(f"sms_entity materialized: {spark.table(TMP_SMS).count():,} rows")

# COMMAND ----------
# =============================================================================
# CELL 3 — STAGE 2: rr_base   [MATERIALIZED → Delta]
# revenue_recognitions LEFT JOIN schedules. Referenced 7+ times downstream
# (furbooks_revenue, tenure_base, swap_base, swap_in_base, penalty,
#  discounts_exploded, vas_detail).
# Most expensive table in the pipeline — Delta materialization eliminates 7 redundant reads.
# Date filter: created_at >= 2024-01-01 (safe margin for LAG windows).
# =============================================================================

rr_base_df = spark.sql("""
    SELECT
        rr.recognition_type,
        rr.accountable_entity_id,
        rr.accountable_entity_type,
        rr.start_date                                               AS rr_start_date,
        rr.end_date                                                 AS rr_end_date,
        rr.recognised_at,
        rr.created_at,
        DATE(rr.recognised_at + INTERVAL '330 minutes')             AS recognised_at_ist,
        DATE(rr.created_at    + INTERVAL '330 minutes')             AS created_at_ist,
        rr.external_reference_type,
        rr.external_reference_id,
        rr.revenue_recognition_schedule_id,
        rr.monetary_components_taxableAmount,
        rr.monetary_components_discounts,
        rrs.monetary_components_taxableAmount                       AS sched_taxableAmount,
        rrs.start_date                                              AS sched_start_date,
        rrs.end_date                                                AS sched_end_date,
        rrs.monetary_components                                     AS sched_monetary_components,
        ROUND(DATEDIFF(DAY, rrs.start_date, rrs.end_date) / 30.45) AS sched_tenure
    FROM furlenco_silver.furbooks_evolve.revenue_recognitions AS rr
    LEFT JOIN furlenco_silver.furbooks_evolve.revenue_recognition_schedules AS rrs
        ON rrs.id = rr.revenue_recognition_schedule_id
    WHERE rr.vertical = 'FURLENCO_RENTAL'
      AND rr.state NOT IN ('CANCELLED', 'INVALIDATED')
      AND rr.deleted_at IS NULL
      AND rr.created_at >= '2024-01-01'
""")
rr_base_df.write.format("delta").mode("overwrite").saveAsTable(TMP_RR)
spark.sql(f"CREATE OR REPLACE TEMP VIEW rr_base AS SELECT * FROM {TMP_RR}")
print(f"rr_base materialized: {spark.table(TMP_RR).count():,} rows")

# COMMAND ----------
# =============================================================================
# CELL 4 — STAGE 3: months + furbooks_revenue   [furbooks_revenue MATERIALIZED → Delta]
# months: 2 rows, tiny — no materialization needed.
# furbooks_revenue: ITEM/ATTACHMENT revenue rows, feeds furbooks_classified.
# =============================================================================

# months — inject Python params via f-string
spark.sql(f"""
    SELECT
        1                                                           AS month_num,
        CAST('{MONTH1_START}' AS DATE)                             AS m_start,
        ADD_MONTHS(CAST('{MONTH1_START}' AS DATE),  1)             AS m_end,
        ADD_MONTHS(CAST('{MONTH1_START}' AS DATE), -1)             AS prev_start,
        DATE_FORMAT(CAST('{MONTH1_START}' AS DATE), 'MMM yyyy')    AS m_label
    UNION ALL
    SELECT
        2,
        CAST('{MONTH2_START}' AS DATE),
        ADD_MONTHS(CAST('{MONTH2_START}' AS DATE),  1),
        ADD_MONTHS(CAST('{MONTH2_START}' AS DATE), -1),
        DATE_FORMAT(CAST('{MONTH2_START}' AS DATE), 'MMM yyyy')
""").createOrReplaceTempView("months")

# furbooks_revenue — CACHE (joins two already-cached DFs, feeds classified)
furbooks_revenue_df = spark.sql("""
    SELECT
        rb.accountable_entity_id,
        rb.accountable_entity_type,
        DATE(rb.rr_start_date)                AS start_date,
        rb.recognised_at_ist                  AS recognised_at,
        rb.monetary_components_taxableAmount  AS taxable_amount,
        se.user_id
    FROM rr_base rb
    LEFT JOIN sms_entity se
        ON  se.id          = rb.accountable_entity_id
        AND se.entity_type = rb.accountable_entity_type
    WHERE rb.accountable_entity_type IN ('ITEM', 'ATTACHMENT')
""")
furbooks_revenue_df.write.format("delta").mode("overwrite").saveAsTable(TMP_FR)
spark.sql(f"CREATE OR REPLACE TEMP VIEW furbooks_revenue AS SELECT * FROM {TMP_FR}")
print(f"months registered (2 rows)")
print(f"furbooks_revenue materialized: {spark.table(TMP_FR).count():,} rows")

# COMMAND ----------
# =============================================================================
# CELL 5 — STAGE 3b: tenure_base
# Deduped per (entity, schedule) — feeds accrual + plan transition pipeline.
# Uses rr_base (already cached). Not cached itself — only used once downstream.
# =============================================================================

spark.sql("""
    SELECT
        rb.recognition_type                                            AS revenue_recognition_type,
        rb.accountable_entity_id,
        rb.accountable_entity_type,
        DATE(rb.sched_start_date)                                      AS start_date,
        DATE(rb.sched_end_date)                                        AS end_date,
        CAST(rb.sched_taxableAmount AS DOUBLE)
            / NULLIF(rb.sched_tenure, 0)                               AS taxableAmount,
        rb.sched_tenure                                                AS tenure,
        rb.external_reference_type,
        rb.created_at_ist                                              AS created_at,
        rb.sched_monetary_components                                   AS monetary_components
    FROM rr_base rb
    WHERE rb.accountable_entity_type IN ('ITEM', 'ATTACHMENT')
      AND rb.created_at >= '2024-06-01'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY rb.accountable_entity_id, rb.accountable_entity_type, rb.revenue_recognition_schedule_id
        ORDER BY rb.created_at ASC
    ) = 1
""").createOrReplaceTempView("tenure_base")
print("tenure_base registered")

# COMMAND ----------
# =============================================================================
# CELL 6 — STAGE 4: furbooks_classified   [MATERIALIZED → Delta]
# Classifies each revenue row into S1.1 (normal opening), S1.2 (MTP opening),
# or current_mtp. Referenced twice: classified_components + mtp_entities.
# =============================================================================

furbooks_classified_df = spark.sql("""
    SELECT /*+ BROADCAST(m) */
        m.month_num,
        br.accountable_entity_id,
        br.accountable_entity_type,
        br.user_id,
        br.taxable_amount,
        CASE WHEN br.start_date >= m.prev_start AND br.start_date < m.m_start
                  AND (br.recognised_at IS NULL OR br.recognised_at >= m.prev_start)
             THEN TRUE ELSE FALSE END  AS is_s1_1,
        CASE WHEN br.start_date >= m.m_start
                  AND br.recognised_at >= m.prev_start AND br.recognised_at < m.m_start
             THEN TRUE ELSE FALSE END  AS is_s1_2,
        CASE WHEN br.recognised_at >= m.m_start AND br.recognised_at < m.m_end
                  AND br.start_date >= m.m_end
             THEN TRUE ELSE FALSE END  AS is_current_mtp
    FROM furbooks_revenue br
    INNER JOIN months m
        ON (
            (    br.start_date >= m.prev_start AND br.start_date < m.m_start
             AND (br.recognised_at IS NULL OR br.recognised_at >= m.prev_start))
            OR
            (    br.start_date >= m.m_start
             AND br.recognised_at >= m.prev_start AND br.recognised_at < m.m_start)
            OR
            (    br.recognised_at >= m.m_start AND br.recognised_at < m.m_end
             AND br.start_date >= m.m_end)
        )
""")
furbooks_classified_df.write.format("delta").mode("overwrite").saveAsTable(TMP_FC)
spark.sql(f"CREATE OR REPLACE TEMP VIEW furbooks_classified AS SELECT * FROM {TMP_FC}")
print(f"furbooks_classified materialized: {spark.table(TMP_FC).count():,} rows")

# Components 1, 2, 14 — single pass over cached furbooks_classified
spark.sql("""
    SELECT month_num,
        COUNT(DISTINCT CASE WHEN is_s1_1 OR is_s1_2
             THEN CONCAT(accountable_entity_type, '::', accountable_entity_id) END)   AS op_items,
        COUNT(DISTINCT CASE WHEN is_s1_1 OR is_s1_2 THEN user_id END)                 AS op_cx,
        SUM(CASE WHEN is_s1_1 OR is_s1_2 THEN CAST(taxable_amount AS FLOAT) ELSE 0 END) AS op_rev,
        -COUNT(DISTINCT CASE WHEN is_s1_2
             THEN CONCAT(accountable_entity_type, '::', accountable_entity_id) END)   AS mtp_adj_items,
        -COUNT(DISTINCT CASE WHEN is_s1_2 THEN user_id END)                            AS mtp_adj_cx,
        -SUM(CASE WHEN is_s1_2 THEN CAST(taxable_amount AS FLOAT) ELSE 0 END)          AS mtp_adj_rev,
        -COUNT(DISTINCT CASE WHEN is_current_mtp
             THEN CONCAT(accountable_entity_type, '::', accountable_entity_id) END)   AS cmtp_items,
        -COUNT(DISTINCT CASE WHEN is_current_mtp THEN user_id END)                     AS cmtp_cx,
        -SUM(CASE WHEN is_current_mtp THEN CAST(taxable_amount AS FLOAT) ELSE 0 END)   AS cmtp_rev
    FROM furbooks_classified
    GROUP BY month_num
""").createOrReplaceTempView("classified_components")

# MTP entities — used to exclude from churn (2nd reference to classified)
spark.sql("""
    SELECT DISTINCT accountable_entity_id, accountable_entity_type
    FROM furbooks_classified
    WHERE is_current_mtp
""").createOrReplaceTempView("mtp_current_month_entities")

print("classified_components + mtp_current_month_entities registered")

# COMMAND ----------
# =============================================================================
# CELL 7 — STAGE 5: Churn pipeline
# LEFT JOIN + IS NULL replaces LEFT ANTI JOIN for Photon GPU compatibility.
# churn_components aggregates all 6 churn/TTO components in a single GROUP BY.
# =============================================================================

# Exclude MTP entities from churn (Photon-compatible pattern)
spark.sql("""
    SELECT
        rcq.entity_id,
        rcq.entity_type,
        rcq.user_ids,
        rcq.taxable_amount,
        rcq.churn_flag,
        rcq.transaction_type,
        rcq.payment_date,
        CAST(rcq.pickup_date AS DATE) AS pickup_date
    FROM furlenco_analytics.user_defined_tables.rental_churn_query rcq
    LEFT JOIN mtp_current_month_entities mtp
        ON  rcq.entity_id   = mtp.accountable_entity_id
        AND rcq.entity_type = mtp.accountable_entity_type
    WHERE rcq.rnk = 1
      AND mtp.accountable_entity_id IS NULL
""").createOrReplaceTempView("churn_pickups_base")

spark.sql("""
    SELECT /*+ BROADCAST(m) */
        m.month_num,
        cp.entity_id,
        cp.entity_type,
        cp.user_ids,
        CAST(cp.taxable_amount AS FLOAT) AS taxable_amount,
        cp.churn_flag,
        cp.transaction_type
    FROM churn_pickups_base cp
    JOIN months m
        ON cp.payment_date >= m.m_start
        AND cp.payment_date <  m.m_end
""").createOrReplaceTempView("churn_joined")

# Components 6-11 in one GROUP BY pass
spark.sql("""
    SELECT month_num,
        -- Component 6: Total Pickup
        -COUNT(DISTINCT CASE WHEN transaction_type = 'return_item'
             THEN CONCAT(entity_type, '::', entity_id) END)                        AS tp_items,
        -COUNT(DISTINCT CASE WHEN transaction_type = 'return_item'
             THEN user_ids END)                                                     AS tp_cx,
        -SUM(CASE WHEN transaction_type = 'return_item' THEN taxable_amount ELSE 0 END) AS tp_rev,
        -- Component 7: Partial Pickup
        -COUNT(DISTINCT CASE WHEN churn_flag = 'PARTIAL' AND transaction_type = 'return_item'
             THEN CONCAT(entity_type, '::', entity_id) END)                        AS pp_items,
        -SUM(CASE WHEN churn_flag = 'PARTIAL' AND transaction_type = 'return_item'
             THEN taxable_amount ELSE 0 END)                                        AS pp_rev,
        -- Component 8: Full Pickup
        -COUNT(DISTINCT CASE WHEN churn_flag = 'FULL' AND transaction_type = 'return_item'
             THEN CONCAT(entity_type, '::', entity_id) END)                        AS fp_items,
        -COUNT(DISTINCT CASE WHEN churn_flag = 'FULL' AND transaction_type = 'return_item'
             THEN user_ids END)                                                     AS fp_cx,
        -SUM(CASE WHEN churn_flag = 'FULL' AND transaction_type = 'return_item'
             THEN taxable_amount ELSE 0 END)                                        AS fp_rev,
        -- Component 9: TTO Total
        -COUNT(DISTINCT CASE WHEN transaction_type = 'rent_to_purchase_item'
             THEN CONCAT(entity_type, '::', entity_id) END)                        AS tto_items,
        -COUNT(DISTINCT CASE WHEN transaction_type = 'rent_to_purchase_item'
             THEN user_ids END)                                                     AS tto_cx,
        -SUM(CASE WHEN transaction_type = 'rent_to_purchase_item'
             THEN taxable_amount ELSE 0 END)                                        AS tto_rev,
        -- Component 10: TTO Partial
        -COUNT(DISTINCT CASE WHEN transaction_type = 'rent_to_purchase_item' AND churn_flag = 'PARTIAL'
             THEN CONCAT(entity_type, '::', entity_id) END)                        AS tto_pp_items,
        -SUM(CASE WHEN transaction_type = 'rent_to_purchase_item' AND churn_flag = 'PARTIAL'
             THEN taxable_amount ELSE 0 END)                                        AS tto_pp_rev,
        -- Component 11: TTO Full
        -COUNT(DISTINCT CASE WHEN transaction_type = 'rent_to_purchase_item' AND churn_flag = 'FULL'
             THEN CONCAT(entity_type, '::', entity_id) END)                        AS tto_fp_items,
        -COUNT(DISTINCT CASE WHEN transaction_type = 'rent_to_purchase_item' AND churn_flag = 'FULL'
             THEN user_ids END)                                                     AS tto_fp_cx,
        -SUM(CASE WHEN transaction_type = 'rent_to_purchase_item' AND churn_flag = 'FULL'
             THEN taxable_amount ELSE 0 END)                                        AS tto_fp_rev
    FROM churn_joined
    GROUP BY month_num
""").createOrReplaceTempView("churn_components")

print("churn_pickups_base, churn_joined, churn_components registered")

# COMMAND ----------
# =============================================================================
# CELL 8 — STAGE 6: Acquisition (new deliveries + upsells)
# =============================================================================

spark.sql(f"""
    SELECT /*+ BROADCAST(m) */
        m.month_num,
        f.accountable_entity_id,
        f.fur_id,
        CAST(f.taxable_amount AS FLOAT) AS taxable_amount,
        LOWER(f.flag_based_on_Ua)       AS flag
    FROM furlenco_analytics.user_defined_tables.rental_acquition_unified f
    JOIN months m
        ON f.activation_date >= m.m_start
        AND f.activation_date <  m.m_end
    WHERE LOWER(f.flag_based_on_Ua) IN ('new', 'upsell')
""").createOrReplaceTempView("acquisition_joined")

spark.sql("""
    SELECT month_num,
        'New deliveries (Addition of Cx)'     AS component, 4 AS sort_order,
        COUNT(DISTINCT accountable_entity_id) AS items_count,
        COUNT(DISTINCT fur_id)                AS cx_count,
        SUM(taxable_amount)                   AS taxable_revenue
    FROM acquisition_joined WHERE flag = 'new'
    GROUP BY month_num
""").createOrReplaceTempView("new_deliveries")

spark.sql("""
    SELECT month_num,
        'Upsell (Addition in item count)'     AS component, 5 AS sort_order,
        COUNT(DISTINCT accountable_entity_id) AS items_count,
        COUNT(DISTINCT fur_id)                AS cx_count,
        SUM(taxable_amount)                   AS taxable_revenue
    FROM acquisition_joined WHERE flag = 'upsell'
    GROUP BY month_num
""").createOrReplaceTempView("upsells")

print("new_deliveries, upsells registered")

# COMMAND ----------
# =============================================================================
# CELL 9 — STAGE 7: Swaps
# swap_entities UNION ALL shared between swap_base and swap_in_base.
# Both join to cached sms_entity and rr_base.
# =============================================================================

spark.sql(f"""
    SELECT attachment_id AS entity_id, 'ATTACHMENT' AS entity_type, action, fulfillment_date
    FROM furlenco_silver.order_management_systems_evolve.swap_attachments
    WHERE state = 'FULFILLED' AND fulfillment_date IS NOT NULL AND action IN ('SWAP_OUT', 'SWAP_IN')
      AND fulfillment_date >= DATE_ADD(CAST('{MONTH1_START}' AS DATE), -60)
    UNION ALL
    SELECT item_id AS entity_id, 'ITEM' AS entity_type, action, fulfillment_date
    FROM furlenco_silver.order_management_systems_evolve.swap_items
    WHERE state = 'FULFILLED' AND fulfillment_date IS NOT NULL AND action IN ('SWAP_OUT', 'SWAP_IN')
      AND fulfillment_date >= DATE_ADD(CAST('{MONTH1_START}' AS DATE), -60)
""").createOrReplaceTempView("swap_entities")

# Swap-out: pick the most recent RR cycle before swap date
spark.sql("""
    SELECT
        sw.entity_id, sw.entity_type,
        se.user_id,
        DATE(sw.fulfillment_date + INTERVAL '330 minutes') AS fulfillment_date,
        rb.monetary_components_taxableAmount               AS taxable_amount,
        ROW_NUMBER() OVER (
            PARTITION BY sw.entity_id, sw.entity_type
            ORDER BY rb.rr_start_date DESC
        ) AS rr_rnk
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
""").createOrReplaceTempView("swap_base")

spark.sql("""
    SELECT /*+ BROADCAST(m) */
        m.month_num,
        'Swapped out' AS component, 12 AS sort_order,
        -COUNT(DISTINCT CONCAT(sb.entity_type, '::', sb.entity_id)) AS items_count,
        0                                                            AS cx_count,
        -SUM(CAST(sb.taxable_amount AS FLOAT))                       AS taxable_revenue
    FROM swap_base sb
    JOIN months m
        ON sb.fulfillment_date >= m.m_start
        AND sb.fulfillment_date <  m.m_end
    WHERE sb.rr_rnk = 1
    GROUP BY m.month_num
""").createOrReplaceTempView("swapped_out")

# Swap-in: pick the first RR cycle on or after swap date
spark.sql("""
    SELECT
        sw.entity_id, sw.entity_type,
        se.user_id,
        DATE(sw.fulfillment_date + INTERVAL '330 minutes') AS fulfillment_date,
        rb.monetary_components_taxableAmount               AS taxable_amount,
        ROW_NUMBER() OVER (
            PARTITION BY sw.entity_id, sw.entity_type
            ORDER BY rb.rr_start_date ASC
        ) AS rr_rnk
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
""").createOrReplaceTempView("swap_in_base")

spark.sql("""
    SELECT /*+ BROADCAST(m) */
        m.month_num,
        'Swapped in' AS component, 13 AS sort_order,
        COUNT(DISTINCT CONCAT(sb.entity_type, '::', sb.entity_id)) AS items_count,
        0                                                           AS cx_count,
        SUM(CAST(sb.taxable_amount AS FLOAT))                       AS taxable_revenue
    FROM swap_in_base sb
    JOIN months m
        ON sb.fulfillment_date >= m.m_start
        AND sb.fulfillment_date <  m.m_end
    WHERE sb.rr_rnk = 1
    GROUP BY m.month_num
""").createOrReplaceTempView("swapped_in")

print("swap_entities, swap_base, swapped_out, swap_in_base, swapped_in registered")

# COMMAND ----------
# =============================================================================
# CELL 10 — STAGE 8: Penalty
# =============================================================================

spark.sql("""
    SELECT /*+ BROADCAST(m) */
        m.month_num,
        'Penalty' AS component, 15 AS sort_order,
        COUNT(DISTINCT pl.product_entity_id)                         AS items_count,
        COUNT(DISTINCT pl.user_id)                                   AS cx_count,
        SUM(CAST(rb.monetary_components_taxableAmount AS DOUBLE))    AS taxable_revenue
    FROM rr_base rb
    JOIN furlenco_silver.order_management_systems_evolve.penalty pl
        ON rb.accountable_entity_id = pl.id
    JOIN months m
        ON rb.recognised_at_ist >= m.m_start
        AND rb.recognised_at_ist <  m.m_end
    WHERE rb.accountable_entity_type = 'PENALTY'
    GROUP BY m.month_num
""").createOrReplaceTempView("penalty")

print("penalty registered")

# COMMAND ----------
# =============================================================================
# CELL 11 — STAGE 9: Accrual change detection (RO positive + negative)
# tenure_windowed uses cached rr_base via tenure_base.
# accrual_components merges both directions in a single GROUP BY.
# =============================================================================

spark.sql("""
    SELECT
        accountable_entity_id, accountable_entity_type, start_date, end_date,
        tenure, taxableAmount, revenue_recognition_type,
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
""").createOrReplaceTempView("tenure_windowed")

spark.sql("""
    SELECT
        rc.accountable_entity_id, rc.accountable_entity_type,
        rc.start_date, rc.previous_start_date, rc.previous_end_date,
        rc.previous_recognition_type,
        rc.revenue_recognition_type                                                  AS current_recognition_type,
        se.user_id,
        CAST(rc.previous_taxableAmount AS DECIMAL(10,2))                             AS previous_month_revenue,
        CAST(rc.taxableAmount          AS DECIMAL(10,2))                             AS current_month_revenue,
        (CAST(rc.taxableAmount AS DECIMAL(10,2)) - CAST(rc.previous_taxableAmount AS DECIMAL(10,2))) AS revenue_difference,
        rc.created_at, m.month_num
    FROM tenure_windowed rc
    LEFT JOIN sms_entity se
        ON  se.id = rc.accountable_entity_id AND se.entity_type = rc.accountable_entity_type
    JOIN months m
        ON rc.start_date >= m.m_start AND rc.start_date < m.m_end
    WHERE rc.previous_recognition_type IS NOT NULL
      AND rc.previous_recognition_type <> rc.revenue_recognition_type
      AND (rc.revenue_recognition_type = 'ACCRUAL' OR rc.previous_recognition_type = 'ACCRUAL')
""").createOrReplaceTempView("customer_accrual_changes")

# Components 18-19: single pass over customer_accrual_changes
spark.sql("""
    SELECT month_num,
        COUNT(DISTINCT CASE WHEN current_recognition_type = 'ACCRUAL'
             THEN CONCAT(accountable_entity_type, '::', accountable_entity_id) END)  AS pos_items,
        COUNT(DISTINCT CASE WHEN current_recognition_type = 'ACCRUAL'
             THEN user_id END)                                                        AS pos_cx,
        SUM(CASE WHEN current_recognition_type = 'ACCRUAL' THEN revenue_difference ELSE 0 END) AS pos_rev,
        COUNT(DISTINCT CASE WHEN current_recognition_type = 'DEFERRAL'
             THEN CONCAT(accountable_entity_type, '::', accountable_entity_id) END)  AS neg_items,
        COUNT(DISTINCT CASE WHEN current_recognition_type = 'DEFERRAL'
             THEN user_id END)                                                        AS neg_cx,
        SUM(CASE WHEN current_recognition_type = 'DEFERRAL' THEN revenue_difference ELSE 0 END) AS neg_rev
    FROM customer_accrual_changes
    GROUP BY month_num
""").createOrReplaceTempView("accrual_components")

print("tenure_windowed, customer_accrual_changes, accrual_components registered")

# COMMAND ----------
# =============================================================================
# CELL 12 — STAGE 10: Plan Transition (uses discounts_exploded)
# discounts_exploded: LATERAL VIEW EXPLODE + from_json over rr_base.
# Referenced by both plan_transition and discount paths — registered once here.
# =============================================================================

discounts_exploded_df = spark.sql("""
    SELECT
        rb.accountable_entity_id,
        rb.accountable_entity_type,
        rb.revenue_recognition_schedule_id,
        rb.external_reference_type,
        DATE(rb.rr_start_date)  AS rr_start_date,
        DATE(rb.rr_end_date)    AS rr_end_date,
        rb.sched_tenure,
        d.catalogReferenceId,
        d.amount                AS discount_amount
    FROM rr_base rb
    LATERAL VIEW OUTER EXPLODE(
        from_json(
            CAST(rb.monetary_components_discounts AS STRING),
            'array<struct<amount:double,catalogReferenceId:string,code:string>>'
        )
    ) AS d
    WHERE rb.accountable_entity_type IN ('ITEM', 'ATTACHMENT')
      AND rb.created_at >= '2024-06-01'
""")
discounts_exploded_df.write.format("delta").mode("overwrite").saveAsTable(TMP_DE)
spark.sql(f"CREATE OR REPLACE TEMP VIEW discounts_exploded AS SELECT * FROM {TMP_DE}")
print(f"discounts_exploded materialized")

spark.sql("""
    SELECT
        e.accountable_entity_id, e.accountable_entity_type,
        e.revenue_recognition_schedule_id,
        e.rr_start_date   AS start_date,
        e.sched_tenure    AS tenure,
        SUM(COALESCE(e.discount_amount, 0)) AS upfront_discount_amount
    FROM discounts_exploded e
    JOIN furlenco_silver.godfather_evolve.discounts gd
        ON (e.catalogReferenceId = gd.id AND gd.type = 'UPFRONT')
        OR  e.external_reference_type = 'SETTLEMENT'
    GROUP BY
        e.accountable_entity_id, e.accountable_entity_type,
        e.revenue_recognition_schedule_id,
        e.rr_start_date, e.external_reference_type, e.sched_tenure
""").createOrReplaceTempView("upfront_discount_per_schedule")

spark.sql("""
    SELECT
        accountable_entity_id, accountable_entity_type, start_date, tenure,
        upfront_discount_amount,
        LAG(tenure)                  OVER w AS previous_tenure,
        LAG(upfront_discount_amount) OVER w AS previous_upfront_discount_amount,
        LAG(start_date)              OVER w AS previous_start_date
    FROM upfront_discount_per_schedule
    WINDOW w AS (
        PARTITION BY accountable_entity_id, accountable_entity_type
        ORDER BY start_date ASC
    )
""").createOrReplaceTempView("plan_transition_windowed")

spark.sql("""
    SELECT
        pt.accountable_entity_id, pt.accountable_entity_type,
        pt.start_date, pt.previous_start_date,
        pt.tenure AS current_tenure, pt.previous_tenure,
        se.user_id,
        (COALESCE(pt.previous_upfront_discount_amount, 0) - COALESCE(pt.upfront_discount_amount, 0)) AS revenue_difference,
        m.month_num
    FROM plan_transition_windowed pt
    LEFT JOIN sms_entity se
        ON se.id = pt.accountable_entity_id AND se.entity_type = pt.accountable_entity_type
    JOIN months m
        ON pt.start_date >= m.prev_start AND pt.start_date < m.m_start
    WHERE pt.previous_tenure IS NOT NULL
      AND pt.previous_tenure <> pt.tenure
""").createOrReplaceTempView("customer_plan_transition")

spark.sql("""
    SELECT month_num,
        'Plan transition' AS component, 16 AS sort_order,
        COUNT(DISTINCT CONCAT(accountable_entity_type, '::', accountable_entity_id)) AS items_count,
        COUNT(DISTINCT user_id)                                                       AS cx_count,
        SUM(revenue_difference)                                                       AS taxable_revenue
    FROM customer_plan_transition
    GROUP BY month_num
""").createOrReplaceTempView("plan_transition")

print("discounts_exploded, plan_transition registered")

# COMMAND ----------
# =============================================================================
# CELL 13 — STAGE 11: Discount changes + VAS
# =============================================================================

# Discount change
spark.sql("""
    SELECT
        e.accountable_entity_id, e.accountable_entity_type,
        e.rr_start_date AS start_date, e.rr_end_date AS end_date,
        COALESCE(SUM(CAST(e.discount_amount AS DECIMAL(10,2))), 0) AS total_discount_amount
    FROM discounts_exploded e
    LEFT JOIN furlenco_silver.godfather_evolve.discounts gd ON e.catalogReferenceId = gd.id
    WHERE gd.type IS NULL OR gd.type <> 'UPFRONT'
    GROUP BY e.accountable_entity_id, e.accountable_entity_type, e.rr_start_date, e.rr_end_date
""").createOrReplaceTempView("discount_per_cycle")

spark.sql("""
    SELECT
        accountable_entity_id, accountable_entity_type, start_date, end_date,
        total_discount_amount,
        LAG(total_discount_amount) OVER (PARTITION BY accountable_entity_id, accountable_entity_type ORDER BY start_date) AS previous_discount_amount,
        LAG(start_date)            OVER (PARTITION BY accountable_entity_id, accountable_entity_type ORDER BY start_date) AS previous_start_date,
        LAG(end_date)              OVER (PARTITION BY accountable_entity_id, accountable_entity_type ORDER BY start_date) AS previous_end_date
    FROM discount_per_cycle
""").createOrReplaceTempView("discount_changes")

spark.sql("""
    SELECT
        dc.accountable_entity_id, dc.accountable_entity_type,
        dc.start_date, dc.previous_start_date, dc.previous_end_date,
        se.user_id,
        dc.total_discount_amount    AS current_discount,
        dc.previous_discount_amount AS previous_discount,
        (dc.previous_discount_amount - dc.total_discount_amount) AS revenue_difference,
        m.month_num
    FROM discount_changes dc
    LEFT JOIN sms_entity se
        ON se.id = dc.accountable_entity_id AND se.entity_type = dc.accountable_entity_type
    JOIN months m
        ON dc.start_date >= m.prev_start AND dc.start_date < m.m_start
    WHERE dc.previous_discount_amount IS NOT NULL
      AND dc.previous_discount_amount <> dc.total_discount_amount
""").createOrReplaceTempView("customer_discount_changes")

spark.sql("""
    SELECT m.month_num,
        'Discount given' AS component, 20 AS sort_order,
        COUNT(DISTINCT CONCAT(dp.accountable_entity_type, '::', dp.accountable_entity_id)) AS items_count,
        COUNT(DISTINCT se.user_id)                                                         AS cx_count,
        SUM(dp.total_discount_amount)                                                      AS taxable_revenue
    FROM discount_per_cycle dp
    LEFT JOIN sms_entity se
        ON se.id = dp.accountable_entity_id AND se.entity_type = dp.accountable_entity_type
    JOIN months m
        ON dp.start_date >= m.m_start AND dp.start_date < m.m_end
    GROUP BY m.month_num
""").createOrReplaceTempView("discount_given")

spark.sql("""
    SELECT month_num,
        'Discount change' AS component, 21 AS sort_order,
        COUNT(DISTINCT CONCAT(accountable_entity_type, '::', accountable_entity_id)) AS items_count,
        COUNT(DISTINCT user_id)                                                       AS cx_count,
        SUM(revenue_difference)                                                       AS taxable_revenue
    FROM customer_discount_changes
    GROUP BY month_num
""").createOrReplaceTempView("discount_changes_all")

# VAS Revenue — INNER JOIN months (no CROSS JOIN row inflation)
spark.sql("""
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
        ON  DATE(rb.rr_start_date) >= m.m_start
        AND DATE(rb.rr_start_date) <  m.m_end
    LEFT JOIN (
        SELECT vas.*, se.user_id, se.user_details_displayId
        FROM furlenco_silver.order_management_systems_evolve.Value_Added_Services AS vas
        JOIN sms_entity se ON vas.entity_type = se.entity_type AND vas.entity_id = se.id
        WHERE vas.state <> 'CANCELLED'
    ) AS vas ON rb.accountable_entity_id = vas.id
    WHERE rb.accountable_entity_type = 'VALUE_ADDED_SERVICE'
""").createOrReplaceTempView("vas_detail")

spark.sql("""
    SELECT month_num,
        vas_category AS component,
        CASE vas_category
            WHEN 'VAS Revenue - Furlenco Care & Flexi' THEN 24
            WHEN 'VAS Revenue - Delivery charges'      THEN 25
            WHEN 'VAS Revenue - Installation Charges'  THEN 26
            ELSE                                            27
        END AS sort_order,
        COUNT(DISTINCT accountable_entity_id)  AS items_count,
        COUNT(DISTINCT user_id)                AS cx_count,
        SUM(CAST(taxable_amount AS FLOAT))     AS taxable_revenue
    FROM vas_detail
    GROUP BY month_num, vas_category
""").createOrReplaceTempView("vas_by_category")

print("discount_given, discount_changes_all, vas_by_category registered")

# COMMAND ----------
# =============================================================================
# CELL 14 — STAGE 12: Assembly + Pivot
# All components unioned, pivoted to wide format, closing + gap computed.
# =============================================================================

# Combine all 20+ components
spark.sql("""
    SELECT month_num, 'Opening_revenue' AS component, 1 AS sort_order, op_items AS items_count, op_cx AS cx_count, op_rev AS taxable_revenue FROM classified_components
    UNION ALL SELECT month_num, 'Minimum tenure charges', 2, mtp_adj_items, mtp_adj_cx, mtp_adj_rev FROM classified_components
    UNION ALL SELECT month_num, 'Current month MTP', 14, cmtp_items, cmtp_cx, cmtp_rev FROM classified_components
    UNION ALL SELECT month_num, component, sort_order, items_count, cx_count, taxable_revenue FROM new_deliveries
    UNION ALL SELECT month_num, component, sort_order, items_count, cx_count, taxable_revenue FROM upsells
    UNION ALL SELECT month_num, 'Total pickup (Return Request Date)',        6,  tp_items,    tp_cx,    tp_rev    FROM churn_components
    UNION ALL SELECT month_num, 'Partial pickup (Reduction in item count)',  7,  pp_items,    0,        pp_rev    FROM churn_components
    UNION ALL SELECT month_num, 'Full pickup (Reduction of Cx)',             8,  fp_items,    fp_cx,    fp_rev    FROM churn_components
    UNION ALL SELECT month_num, 'TTO (Total - TTO Transaction Date)',        9,  tto_items,   tto_cx,   tto_rev   FROM churn_components
    UNION ALL SELECT month_num, 'TTO - Partial (Reduction in item count)',   10, tto_pp_items, 0,       tto_pp_rev FROM churn_components
    UNION ALL SELECT month_num, 'TTO - Full (Reduction of Cx)',              11, tto_fp_items, tto_fp_cx, tto_fp_rev FROM churn_components
    UNION ALL SELECT month_num, component, sort_order, items_count, cx_count, taxable_revenue FROM swapped_out
    UNION ALL SELECT month_num, component, sort_order, items_count, cx_count, taxable_revenue FROM swapped_in
    UNION ALL SELECT month_num, component, sort_order, items_count, cx_count, taxable_revenue FROM penalty
    UNION ALL SELECT month_num, component, sort_order, items_count, cx_count, taxable_revenue FROM plan_transition
    UNION ALL SELECT month_num, 'RO (Renewal Overdue) - Positive', 18, pos_items, pos_cx, pos_rev FROM accrual_components
    UNION ALL SELECT month_num, 'RO (Renewal Overdue) - Negative', 19, neg_items, neg_cx, neg_rev FROM accrual_components
    UNION ALL SELECT month_num, component, sort_order, items_count, cx_count, taxable_revenue FROM discount_given
    UNION ALL SELECT month_num, component, sort_order, items_count, cx_count, taxable_revenue FROM discount_changes_all
    UNION ALL SELECT month_num, component, sort_order, items_count, cx_count, taxable_revenue FROM vas_by_category
""").createOrReplaceTempView("all_components")

# Pivot to wide format (m1 / m2 columns)
base_wide_df = spark.sql("""
    SELECT component, sort_order,
        MAX(CASE WHEN month_num = 1 THEN items_count     END) AS m1_items,
        MAX(CASE WHEN month_num = 1 THEN cx_count        END) AS m1_cx,
        MAX(CASE WHEN month_num = 1 THEN taxable_revenue END) AS m1_rev,
        MAX(CASE WHEN month_num = 2 THEN items_count     END) AS m2_items,
        MAX(CASE WHEN month_num = 2 THEN cx_count        END) AS m2_cx,
        MAX(CASE WHEN month_num = 2 THEN taxable_revenue END) AS m2_rev
    FROM all_components
    GROUP BY component, sort_order
""")
base_wide_df.write.format("delta").mode("overwrite").saveAsTable(TMP_BW)
spark.sql(f"CREATE OR REPLACE TEMP VIEW base_wide AS SELECT * FROM {TMP_BW}")
print(f"base_wide materialized: {spark.table(TMP_BW).count():,} rows")

# Opening row (Month 2 opening = next month's starting point)
spark.sql("""
    SELECT
        SUM(CASE WHEN component = 'Opening_revenue' AND sort_order = 1 THEN COALESCE(m1_items, 0) ELSE 0 END) AS m1_items,
        SUM(CASE WHEN component = 'Opening_revenue' AND sort_order = 1 THEN COALESCE(m1_cx,    0) ELSE 0 END) AS m1_cx,
        SUM(CASE WHEN component = 'Opening_revenue' AND sort_order = 1 THEN COALESCE(m1_rev,   0) ELSE 0 END) AS m1_rev,
        SUM(CASE WHEN component = 'Opening_revenue' AND sort_order = 1 THEN COALESCE(m2_items, 0) ELSE 0 END) AS m2_items,
        SUM(CASE WHEN component = 'Opening_revenue' AND sort_order = 1 THEN COALESCE(m2_cx,    0) ELSE 0 END) AS m2_cx,
        SUM(CASE WHEN component = 'Opening_revenue' AND sort_order = 1 THEN COALESCE(m2_rev,   0) ELSE 0 END) AS m2_rev
    FROM base_wide
    WHERE component = 'Opening_revenue' AND sort_order = 1
""").createOrReplaceTempView("opening_row")

# Adjusted opening = opening + MTP adjustment
spark.sql("""
    SELECT
        SUM(CASE WHEN (component = 'Opening_revenue' AND sort_order = 1)
                   OR (component = 'Minimum tenure charges' AND sort_order = 2)
                 THEN COALESCE(m1_items, 0) ELSE 0 END) AS m1_items,
        SUM(CASE WHEN (component = 'Opening_revenue' AND sort_order = 1)
                   OR (component = 'Minimum tenure charges' AND sort_order = 2)
                 THEN COALESCE(m1_cx, 0) ELSE 0 END) AS m1_cx,
        SUM(CASE WHEN (component = 'Opening_revenue' AND sort_order = 1)
                   OR (component = 'Minimum tenure charges' AND sort_order = 2)
                 THEN COALESCE(m1_rev, 0) ELSE 0 END) AS m1_rev,
        SUM(CASE WHEN (component = 'Opening_revenue' AND sort_order = 1)
                   OR (component = 'Minimum tenure charges' AND sort_order = 2)
                 THEN COALESCE(m2_items, 0) ELSE 0 END) AS m2_items,
        SUM(CASE WHEN (component = 'Opening_revenue' AND sort_order = 1)
                   OR (component = 'Minimum tenure charges' AND sort_order = 2)
                 THEN COALESCE(m2_cx, 0) ELSE 0 END) AS m2_cx,
        SUM(CASE WHEN (component = 'Opening_revenue' AND sort_order = 1)
                   OR (component = 'Minimum tenure charges' AND sort_order = 2)
                 THEN COALESCE(m2_rev, 0) ELSE 0 END) AS m2_rev
    FROM base_wide
    WHERE (component = 'Opening_revenue' AND sort_order = 1)
       OR (component = 'Minimum tenure charges' AND sort_order = 2)
""").createOrReplaceTempView("adj_opening_row")

# Closing = adjusted opening + all movement components
spark.sql("""
    SELECT
        MAX(ao.m1_items) + SUM(CASE WHEN bw.component IN (
            'New deliveries (Addition of Cx)', 'Upsell (Addition in item count)',
            'Partial pickup (Reduction in item count)', 'Full pickup (Reduction of Cx)',
            'TTO - Partial (Reduction in item count)', 'TTO - Full (Reduction of Cx)',
            'Swapped out', 'Swapped in'
        ) THEN COALESCE(bw.m1_items, 0) ELSE 0 END) AS m1_items,
        MAX(ao.m1_cx) + SUM(CASE WHEN bw.component IN (
            'New deliveries (Addition of Cx)', 'Upsell (Addition in item count)',
            'Full pickup (Reduction of Cx)', 'TTO - Full (Reduction of Cx)'
        ) THEN COALESCE(bw.m1_cx, 0) ELSE 0 END) AS m1_cx,
        MAX(ao.m1_rev) + SUM(CASE WHEN bw.component IN (
            'New deliveries (Addition of Cx)', 'Upsell (Addition in item count)',
            'Partial pickup (Reduction in item count)', 'Full pickup (Reduction of Cx)',
            'TTO - Partial (Reduction in item count)', 'TTO - Full (Reduction of Cx)',
            'Swapped out', 'Swapped in', 'Current month MTP', 'Penalty',
            'Plan transition', 'Discount change'
        ) THEN COALESCE(bw.m1_rev, 0) ELSE 0 END) AS m1_rev,
        MAX(ao.m2_items) + SUM(CASE WHEN bw.component IN (
            'New deliveries (Addition of Cx)', 'Upsell (Addition in item count)',
            'Partial pickup (Reduction in item count)', 'Full pickup (Reduction of Cx)',
            'TTO - Partial (Reduction in item count)', 'TTO - Full (Reduction of Cx)',
            'Swapped out', 'Swapped in'
        ) THEN COALESCE(bw.m2_items, 0) ELSE 0 END) AS m2_items,
        MAX(ao.m2_cx) + SUM(CASE WHEN bw.component IN (
            'New deliveries (Addition of Cx)', 'Upsell (Addition in item count)',
            'Full pickup (Reduction of Cx)', 'TTO - Full (Reduction of Cx)'
        ) THEN COALESCE(bw.m2_cx, 0) ELSE 0 END) AS m2_cx,
        MAX(ao.m2_rev) + SUM(CASE WHEN bw.component IN (
            'New deliveries (Addition of Cx)', 'Upsell (Addition in item count)',
            'Partial pickup (Reduction in item count)', 'Full pickup (Reduction of Cx)',
            'TTO - Partial (Reduction in item count)', 'TTO - Full (Reduction of Cx)',
            'Swapped out', 'Swapped in', 'Current month MTP', 'Penalty',
            'Plan transition', 'Discount change'
        ) THEN COALESCE(bw.m2_rev, 0) ELSE 0 END) AS m2_rev
    FROM base_wide bw
    CROSS JOIN adj_opening_row ao
""").createOrReplaceTempView("closing_row")

# Gap = Month2 opening minus Month1 closing
spark.sql("""
    SELECT
        COALESCE(o.m2_items, 0) - c.m1_items AS gap_items,
        COALESCE(o.m2_cx,    0) - c.m1_cx    AS gap_cx,
        COALESCE(o.m2_rev,   0) - c.m1_rev   AS gap_rev
    FROM opening_row o
    CROSS JOIN closing_row c
""").createOrReplaceTempView("gap_row")

print("Assembly complete — all_components, base_wide, closing_row, gap_row registered")

# COMMAND ----------
# =============================================================================
# CELL 15 — FINAL OUTPUT
# =============================================================================

result_df = spark.sql(f"""
    SELECT
        component,
        m1_items                    AS `{MONTH1_START[:7]} Items count`,
        m1_cx                       AS `{MONTH1_START[:7]} Cx count`,
        ROUND(m1_rev, 2)            AS `{MONTH1_START[:7]} Taxable revenue`,
        m2_items                    AS `{MONTH2_START[:7]} Items count`,
        m2_cx                       AS `{MONTH2_START[:7]} Cx count`,
        ROUND(m2_rev, 2)            AS `{MONTH2_START[:7]} Taxable revenue`
    FROM (
        SELECT component, sort_order, m1_items, m1_cx, m1_rev, m2_items, m2_cx, m2_rev
        FROM base_wide
        UNION ALL
        SELECT 'Adjusted opening', 3, m1_items, m1_cx, m1_rev, m2_items, m2_cx, m2_rev
        FROM adj_opening_row
        UNION ALL
        SELECT 'Total closing Revenue', 31, m1_items, m1_cx, m1_rev, m2_items, m2_cx, m2_rev
        FROM closing_row
        UNION ALL
        SELECT 'Gap (Month1 Closing vs Month2 Opening)', 32,
               gap_items, gap_cx, gap_rev, NULL, NULL, NULL
        FROM gap_row
    )
    ORDER BY sort_order
""")

display(result_df)

# =============================================================================
# Cleanup: drop temp Delta tables
# =============================================================================
for tbl in [TMP_SMS, TMP_RR, TMP_FR, TMP_FC, TMP_DE, TMP_BW]:
    spark.sql(f"DROP TABLE IF EXISTS {tbl}")
print("Temp Delta tables dropped")
