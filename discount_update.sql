
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

-- CTEs to extract upfront discount per entity per schedule — used for plan-transition revenue delta.
-- Two-CTE pattern required: Spark SQL prohibits JOIN in same FROM as LATERAL VIEW EXPLODE.
        , rr_discounts_exploded AS (
SELECT
    rr.accountable_entity_id,
    rr.accountable_entity_type,
    rr.revenue_recognition_schedule_id,
    rr.created_at,
    rr.external_reference_type,
    d.catalogReferenceId,
    d.amount AS discount_amount
FROM furlenco_silver.furbooks_evolve.revenue_recognitions rr
    LATERAL VIEW OUTER EXPLODE(
    from_json(CAST(rr.monetary_components_discounts AS STRING),
    'array<struct<amount:double,catalogReferenceId:string,code:string>>')
    ) AS d
WHERE rr.vertical = 'FURLENCO_RENTAL'
  AND rr.state NOT IN ('CANCELLED', 'INVALIDATED')
  AND rr.accountable_entity_type IN ('ITEM', 'ATTACHMENT')
  AND rr.created_at >= '2024-06-01'

    )


SELECT
    e.accountable_entity_id,
    e.accountable_entity_type,
    e.revenue_recognition_schedule_id,
    SUM(COALESCE(e.discount_amount, 0)) AS upfront_discount_amount
FROM rr_discounts_exploded e
    JOIN furlenco_silver.godfather_evolve.discounts gd
ON e.catalogReferenceId = gd.id
    AND gd.type = 'UPFRONT'
WHERE 1=1
  AND e.external_reference_type <> 'SETTLEMENT'
GROUP BY e.accountable_entity_id, e.accountable_entity_type, e.revenue_recognition_schedule_id
