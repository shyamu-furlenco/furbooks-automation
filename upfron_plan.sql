with rr_discounts_exploded AS (
    SELECT
        rr.accountable_entity_id,
        rr.accountable_entity_type,
        rr.revenue_recognition_schedule_id,
        rr.created_at,
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
  AND rr.external_reference_type <> 'SETTLEMENT'
    )


SELECT
    e.accountable_entity_id,
    e.accountable_entity_type,
    e.revenue_recognition_schedule_id,
    sum(COALESCE(e.discount_amount, 0)) AS upfront_discount_amount
FROM rr_discounts_exploded e
         JOIN furlenco_silver.godfather_evolve.discounts gd
              ON e.catalogReferenceId = gd.id
-- AND gd.type = 'UPFRONT'
WHERE 1=1
  and accountable_entity_id = 1106177
GROUP BY e.accountable_entity_id, e.accountable_entity_type, e.revenue_recognition_schedule_id
 
