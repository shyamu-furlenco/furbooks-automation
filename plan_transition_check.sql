-- ============================================================================
-- Plan Transition Check — detail view for a single entity
-- Usage: set :check_entity_id, :check_entity_type, :month1_start, :month2_start
-- ============================================================================

WITH rr_schedule_deduped AS (
    SELECT
        id,
        accountable_entity_id,
        accountable_entity_type,
        revenue_recognition_schedule_id,
        start_date,
        end_date,
        external_reference_type,
        monetary_components_discounts
    FROM furlenco_silver.furbooks_evolve.revenue_recognitions
    WHERE vertical        = 'FURLENCO_RENTAL'
      AND state           NOT IN ('CANCELLED', 'INVALIDATED')
      AND accountable_entity_type IN ('ITEM', 'ATTACHMENT')
      -- AND created_at      >= '2024-06-01'
      -- Filter to the entity you want to check
      AND accountable_entity_id   = :check_entity_id
      AND accountable_entity_type = :check_entity_type
    -- QUALIFY ROW_NUMBER() OVER (
    --     PARTITION BY accountable_entity_id, accountable_entity_type, revenue_recognition_schedule_id
    --     ORDER BY created_at ASC
    -- ) = 1
)

   , rr_schedule_discounts_exploded AS (
    SELECT
        r.id,
        r.start_date,
        r.end_date,
        r.accountable_entity_id,
        r.accountable_entity_type,
        r.revenue_recognition_schedule_id,
        d.catalogReferenceId,
        r.external_reference_type,
        d.amount AS discount_amount
    FROM rr_schedule_deduped r
    LATERAL VIEW OUTER EXPLODE(
    from_json(CAST(r.monetary_components_discounts AS STRING),
    'array<struct<amount:double,catalogReferenceId:string,code:string>>')
    ) AS d
    )

   , upfront_discount_per_schedule AS (
SELECT
    e.accountable_entity_id,
    e.accountable_entity_type,
    e.revenue_recognition_schedule_id,
    e.external_reference_type,
    -- e.start_date as rr_start_date,
    -- e.end_date as rr_end_date,
    DATE(e.start_date)                                           AS start_date,
    DATE(e.end_date)                                             AS end_date,
    ROUND(DATEDIFF(DAY, rrs.start_date, rrs.end_date) / 30.45)    AS tenure,
    SUM(COALESCE(e.discount_amount, 0))                            AS upfront_discount_amount
FROM rr_schedule_discounts_exploded e
    JOIN furlenco_silver.godfather_evolve.discounts gd
ON ((e.catalogReferenceId = gd.id AND gd.type = 'UPFRONT') OR external_reference_type = 'SETTLEMENT')
    JOIN furlenco_silver.furbooks_evolve.revenue_recognition_schedules rrs
    ON rrs.id = e.revenue_recognition_schedule_id
GROUP BY
    e.accountable_entity_id, e.accountable_entity_type,
    e.revenue_recognition_schedule_id, e.external_reference_type,
    DATE(e.start_date), DATE(e.end_date),
    ROUND(DATEDIFF(DAY, rrs.start_date, rrs.end_date) / 30.45)
    )

        , months AS (
SELECT
    1                                                              AS month_num,
    CAST(:month1_start AS DATE)                                    AS m_start,
    ADD_MONTHS(CAST(:month1_start AS DATE), -1)                    AS m_prev_start,
    ADD_MONTHS(CAST(:month1_start AS DATE), 1)                     AS m_end,
    DATE_FORMAT(CAST(:month1_start AS DATE), 'MMM yyyy')           AS m_label
UNION ALL
SELECT
    2,
    CAST(:month2_start AS DATE),
    ADD_MONTHS(CAST(:month2_start AS DATE), -1),
    ADD_MONTHS(CAST(:month2_start AS DATE), 1),
    DATE_FORMAT(CAST(:month2_start AS DATE), 'MMM yyyy')
    )

        , plan_transition_windowed AS (
SELECT
    accountable_entity_id,
    accountable_entity_type,
    revenue_recognition_schedule_id,
    start_date,
    end_date,
    tenure,
    upfront_discount_amount,
    LAG(tenure)                  OVER w AS previous_tenure,
    LAG(upfront_discount_amount) OVER w AS previous_upfront_discount_amount,
    LAG(start_date)              OVER w AS previous_start_date,
    LAG(end_date)                OVER w AS previous_end_date
FROM upfront_discount_per_schedule
    WINDOW w AS (
    PARTITION BY accountable_entity_id, accountable_entity_type
    ORDER BY start_date ASC
    )
    )


SELECT
    *
     , coalesce(upfront_discount_amount,0) - coalesce(previous_upfront_discount_amount,0) AS discount_change
FROM plan_transition_windowed pt
         join months as m
              ON pt.start_date >= m.m_prev_start and pt.start_date < m.m_start
WHERE pt.previous_tenure IS NOT NULL
  AND pt.previous_tenure <> pt.tenure

