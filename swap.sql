SELECT
    id,
    attachment_id   AS entity_id,
    rented_product_id,
    state,
    action,
    swap_id,
    swap_pair_id,
    swap_composite_item_id,
    pricing_details,
    payment_details,
    offers_snapshot,
    fulfillment_id,
    stock_commitment_id,
    fulfillment_date,
    selected_fulfillment_date,
    promise_date_details,
    created_at,
    updated_at,
    cancelled_at,
    cancelled_by,
    original_tenure_start_date,
    original_tenure_end_date,
    'ATTACHMENT'    AS entity_type
FROM order_management_systems_evolve.swap_attachments

UNION ALL

SELECT
    id,
    item_id         AS entity_id,
    rented_product_id,
    state,
    action,
    swap_id,
    swap_pair_id,
    swap_composite_item_id,
    pricing_details,
    payment_details,
    offers_snapshot,
    fulfillment_id,
    stock_commitment_id,
    fulfillment_date,
    selected_fulfillment_date,
    promise_date_details,
    created_at,
    updated_at,
    cancelled_at,
    cancelled_by,
    original_tenure_start_date,
    original_tenure_end_date,
    'ITEM'          AS entity_type

FROM order_management_systems_evolve.swap_items

WHERE action = 'SWAP_OUT'
  AND state  = 'FULFILLED'

    LIMIT 1048575