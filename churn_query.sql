CREATE TABLE furlenco_analytics.user_defined_tables.rental_churn_query as


WITH base AS (
    SELECT
        items.id                                                             AS item_id,
        activation_date,
        return_items.state                                                   AS return_item_state,
        user_id,
        CASE
            WHEN return_items.created_at IS NULL AND rent_to_purchase_orders.created_at IS NULL THEN NULL
            WHEN rent_to_purchase_orders.created_at IS NULL                                     THEN return_id
            WHEN return_items.created_at IS NULL                                                THEN rent_to_purchase_order_id
            END                                                                  AS transaction_id,
        CASE
            WHEN return_items.created_at IS NULL AND rent_to_purchase_orders.created_at IS NULL THEN CURRENT_DATE() + INTERVAL 1 DAY
    WHEN rent_to_purchase_orders.created_at IS NULL                                     THEN return_items.created_at + INTERVAL 330 MINUTES
    WHEN return_items.created_at IS NULL                                                THEN rent_to_purchase_orders.created_at + INTERVAL 330 MINUTES
END                                                                  AS item_transaction_date,
        NULLIF(
            get_json_object(CAST(rent_to_purchase_items.payment_details_payableafterpaymentoffers AS STRING), '$.byCashPreTax'),
            ''
        )                                                                    AS tto_pay,
                                                                
        CASE
            WHEN return_items.created_at IS NULL AND rent_to_purchase_orders.created_at IS NULL THEN NULL
            WHEN rent_to_purchase_orders.created_at IS NULL                                     THEN 'return_item'
            WHEN return_items.created_at IS NULL                                                THEN 'rent_to_purchase_item'
END                                                                  AS transaction_type,
        return_items.updated_at + INTERVAL 330 MINUTES                      AS return_item_updated_at
    FROM furlenco_silver.order_management_systems_evolve.items
    LEFT JOIN furlenco_silver.order_management_systems_evolve.return_items
        ON  items.id = return_items.item_id
        AND return_items.state != 'CANCELLED'
    LEFT JOIN furlenco_silver.order_management_systems_evolve.rent_to_purchase_items
        ON  rent_to_purchase_items.item_id = items.id
        AND INSTR(LOWER(CAST(rent_to_purchase_items.payment_details AS STRING)), 'paid') > 0
    LEFT JOIN furlenco_silver.order_management_systems_evolve.rent_to_purchase_orders
        ON rent_to_purchase_orders.id = rent_to_purchase_items.rent_to_purchase_order_id
    WHERE items.vertical = 'FURLENCO_RENTAL'
      AND items.state    != 'CANCELLED'
),

-- ✏️ Renamed: base1 → user_item_cross
-- Purpose: for each transacted item, cross-joins all items the same user
--          had active before the transaction date (used for FULL/PARTIAL detection)
user_item_cross AS (
    SELECT
        base.item_id                   AS base_item_id,
        base.activation_date           AS base_activation_date,
        base.user_id                   AS base_user_id,
        base.transaction_id            AS base_transaction_id,
        base.item_transaction_date     AS base_item_transaction_date,
        base.transaction_type          AS base_transaction_type,
        earlier_items.*                                                      -- ✏️ renamed dummy → earlier_items
    FROM base
    LEFT JOIN base AS earlier_items                                          -- ✏️ renamed dummy → earlier_items
        ON  earlier_items.user_id        = base.user_id
        AND earlier_items.activation_date < base.item_transaction_date
    WHERE base.transaction_id IS NOT NULL
),

-- ✏️ Renamed: fullorpartial → transaction_scope
-- Purpose: classifies each transaction as FULL or PARTIAL churn/return
transaction_scope AS (
    SELECT DISTINCT
        base_transaction_id,
        base_user_id,
        base_transaction_type,
        CASE
            WHEN COUNT(DISTINCT base_item_id) = COUNT(
                DISTINCT CASE
                    WHEN item_transaction_date >= base_item_transaction_date THEN item_id
                    ELSE NULL
                END
            ) THEN 'FULL'
            ELSE 'PARTIAL'
        END                                                                  AS full_or_partial_flag  -- ✏️ VAR1 → full_or_partial_flag
    FROM user_item_cross
    GROUP BY 1, 2, 3
    ORDER BY 1 DESC
),

-- ✏️ Renamed: ttto_base → tto_enriched (fixed triple-t typo; enriched with user/item detail)
tto_enriched AS (
    SELECT DISTINCT
        i.id                                                                 AS item_id,
        i.bundle_id,
        i.name                                                               AS item_name,
        return_item_state,
        i.composite_item_id,
        get_json_object(CAST(i.pricing_details AS STRING), '$.basePrice')   AS base_price,
        i.user_id,
        user_details_contactNo::STRING                                       AS contact,
        user_details_name::STRING                                                    AS user_name,
        user_details_emailId::STRING                                                 AS email_id,
        user_details_displayId::STRING                                               AS fur_id,
        i.activation_date + INTERVAL 330 MINUTES                            AS activation_date,
        CASE
            WHEN base.transaction_type = 'rent_to_purchase_item' THEN base.item_transaction_date
            ELSE i.pickup_date
        END                                                                  AS pickup_date,
        i.is_migrated_for_evolve,
        i.is_autopay_enabled,
        'NA'                                                                 AS ufdiscount_config,

        base.item_transaction_date                                           AS payment_date,
        DATE(CURRENT_DATE())                                                 AS charge_till_date,
        base.item_transaction_date                                           AS applicable_on,
        CAST(tto_pay AS FLOAT)                                               AS tto_amount,
        base.transaction_type                                                AS transaction_type,
        transaction_id,
        full_or_partial_flag                                                 AS transaction_type_detail,  -- ✏️ VAR1 → full_or_partial_flag
        DATE(CURRENT_DATE())                                                 AS updated_at,
        return_item_updated_at
    FROM furlenco_silver.order_management_systems_evolve.items              AS i
    LEFT JOIN base
        ON i.id = base.item_id
    LEFT JOIN transaction_scope                                              -- ✏️ fullorpartial → transaction_scope
        ON  transaction_scope.base_transaction_id = base.transaction_id
        AND base.transaction_type                 = transaction_scope.base_transaction_type
    WHERE transaction_id IS NOT NULL
),

-- ✏️ Renamed: base_ → rental_item_base (removed trailing underscore; more descriptive)
rental_item_base AS (
    SELECT
        i.id                                                                 AS item_ids,
        i.user_id                                                            AS user_ids,
        i.order_id,
        i.activation_date                                                    AS activation_dates,
        t.*
    FROM furlenco_silver.order_management_systems_evolve.items              AS i
    LEFT JOIN tto_enriched                                                   AS t  -- ✏️ ttto_base → tto_enriched
        ON i.id = t.item_id
    WHERE i.vertical = 'FURLENCO_RENTAL'
      AND i.state   <> 'CANCELLED'
),

churn_counts AS (
    SELECT
        b1.item_ids,
        COUNT(DISTINCT b2.item_ids)                                          AS later_items_count
    FROM rental_item_base                                                    AS b1  -- ✏️ base_ → rental_item_base
    LEFT JOIN rental_item_base                                               AS b2  -- ✏️ base_ → rental_item_base
        ON  b1.user_ids       = b2.user_ids
        AND b1.item_ids      <> b2.item_ids
        AND b2.activation_dates < b1.pickup_date
        AND (b2.pickup_date IS NULL OR b2.pickup_date > b1.pickup_date)
    GROUP BY b1.item_ids
),

final_tagged AS (
    SELECT
        b.*,
        CASE
            WHEN b.pickup_date IS NULL    THEN 'ACTIVE'
            WHEN cc.later_items_count > 0 THEN 'PARTIAL'
            ELSE 'FULL'
        END                                                                  AS churn_flag
    FROM rental_item_base                                                    AS b   -- ✏️ base_ → rental_item_base
    JOIN churn_counts                                                        AS cc
        ON b.item_ids = cc.item_ids
),

-- ✏️ Renamed: ttto_base_mv → tto_with_item_rr (fixed typo; joined with revenue_recognitions for items)
tto_with_item_rr AS (
    SELECT
        ft.*,
        rr.start_date,
        rr.end_date,
        CAST(get_json_object(CAST(monetary_components AS STRING), '$.taxableAmount') AS FLOAT)
            / ROUND(DATEDIFF(end_date, start_date) / 30.40)                AS taxable_amount,
        TRUNC(DATE_ADD(end_date, 1), 'MM')                                   AS revenue_loss_month,
        DATE_ADD(end_date, 1)                                                AS revenue_loss_day,
        recognition_type,
        DENSE_RANK() OVER(
            PARTITION BY rr.accountable_entity_id, rr.accountable_entity_type
            ORDER BY rr.start_date DESC
        )                                                                    AS rnk,
        rr.recognised_at + INTERVAL 330 MINUTES                             AS recognised_at_ist  -- ✏️ self-shadow alias → recognised_at_ist
    FROM final_tagged                                                        AS ft
    LEFT JOIN furlenco_silver.furbooks_evolve.revenue_recognitions          AS rr
        ON  ft.item_ids                   = rr.accountable_entity_id
        AND rr.accountable_entity_type    = 'ITEM'
        AND rr.state NOT IN ('CANCELLED', 'INVALIDATED')
),

attach_rr AS (
    SELECT
        ft.item_ids,
        ft.user_ids,
        ft.activation_date,
        ft.return_item_state,
        ft.base_price,
        ft.fur_id,
        ft.pickup_date,
        ft.payment_date,
        ft.tto_amount,
        ft.transaction_type,
        ft.transaction_id,
        ft.transaction_type_detail,
        ft.churn_flag,
        rr.start_date,
        rr.end_date,
        CAST(get_json_object(CAST(rr.monetary_components AS STRING), '$.taxableAmount') AS FLOAT)
            / ROUND(DATEDIFF(rr.end_date, rr.start_date) / 30.40)              AS taxable_amount,
        DENSE_RANK() OVER(
            PARTITION BY rr.accountable_entity_id, rr.accountable_entity_type
            ORDER BY rr.start_date DESC
        )                                                                        AS rnk,
        at.id                                                                    AS attachment_id
    FROM final_tagged                                                            AS ft
    JOIN furlenco_silver.order_management_systems_evolve.attachments            AS at
        ON ft.composite_item_id = at.composite_item_id
    JOIN furlenco_silver.furbooks_evolve.revenue_recognitions                   AS rr
        ON  at.id                          = rr.accountable_entity_id
        AND rr.accountable_entity_type     = 'ATTACHMENT'
        AND rr.state NOT IN ('CANCELLED', 'INVALIDATED')
    WHERE ft.churn_flag <> 'ACTIVE'
)
, final_output as (
SELECT
    item_ids                                                                     AS entity_id,
    'ITEM'                                                                       AS entity_type,
    user_ids,
    activation_date,
    return_item_state,
    base_price,
    fur_id,
    pickup_date,
    payment_date,
    tto_amount,
    transaction_type,
    transaction_id,
    transaction_type_detail,
    churn_flag,
    start_date,
    end_date,
    taxable_amount,
    rnk
FROM tto_with_item_rr
WHERE churn_flag <> 'ACTIVE'

UNION ALL

SELECT
    attachment_id                                                                AS entity_id,
    'ATTACHMENT'                                                                 AS entity_type,
    user_ids,
    activation_date,
    return_item_state,
    base_price,
    fur_id,
    pickup_date,
    payment_date,
    tto_amount,
    transaction_type,
    transaction_id,
    transaction_type_detail,
    churn_flag,
    start_date,
    end_date,
    taxable_amount,
    rnk
FROM attach_rr
)
SELECT *, current_timestamp() as refreshed_at
FROM final_output 
