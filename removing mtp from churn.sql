        , churn_pickups_base AS (
SELECT
    entity_id,
    entity_type,
    user_ids,
    taxable_amount,
    churn_flag,
    transaction_type,
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
