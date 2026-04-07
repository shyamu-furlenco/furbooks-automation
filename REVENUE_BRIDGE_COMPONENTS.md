# Furbooks Revenue Bridge — Component Definitions

## Component 1: Opening Revenue
**Sort order:** 1

**Definition:** Total revenue base carried into the month — all active RR cycles that "belong" to the previous month.

**Two scenarios (OR'd together):**
- **S1.1 (Normal):** `start_date` in previous month AND (`recognised_at` is NULL OR `recognised_at >= prev_start`)
- **S1.2 (MTP):** `start_date` in current month or later AND `recognised_at` in previous month

**Example — S1.1:**
An item has an RR cycle with `start_date = 2025-12-15`, `recognised_at = 2025-12-20`.
For Jan analysis: start_date is in Dec (prev month), recognised in Dec → **S1.1 match** → included in Jan opening.

**Example — S1.2:**
An item has `start_date = 2026-02-01`, `recognised_at = 2025-12-28`.
Start is in Feb (future), but recognised in Dec → **S1.2 match** → also included in Jan opening.

---

## Component 2: MTP Adjustment
**Sort order:** 2 · **Sign:** Negative

**Definition:** Reversal of S1.2 (MTP) cycles from opening. These future-start cycles were included in opening but shouldn't count as "normal" base — subtract them to isolate S1.1.

**Logic:** `start_date >= m_start` AND `recognised_at >= prev_start AND < m_start`

**Example:**
Same MTP cycle above (`start_date = 2026-02-01`, `recognised_at = 2025-12-28`) → subtracted here so Adjusted Opening reflects S1.1 only.

---

## Component 3: Adjusted Opening
**Sort order:** 3 · **Calculated row**

**Definition:** `Opening Revenue + MTP Adjustment` = pure S1.1 cycles only.

---

## Component 4: New Deliveries
**Sort order:** 4 · **Sign:** Positive

**Definition:** Revenue from brand new customers who got their first delivery this month.

**Source:** `rental_acquition_unified` WHERE `flag_based_on_Ua = 'new'`
**Month bucket:** `activation_date >= m_start AND < m_end`

**Example:**
Customer FUR-12345 places first-ever order. Item activated on `2026-01-10`. Flag = `'new'` → counted in Jan new deliveries.

---

## Component 5: Upsells
**Sort order:** 5 · **Sign:** Positive

**Definition:** Revenue from existing customers who added more items this month.

**Source:** `rental_acquition_unified` WHERE `flag_based_on_Ua = 'upsell'`
**Month bucket:** `activation_date >= m_start AND < m_end`

**Example:**
Customer FUR-12345 (already has a sofa) orders a bed. Bed activated `2026-01-15`. Flag = `'upsell'` → counted in Jan upsells.

---

## Component 6: Total Pickups
**Sort order:** 6 · **Sign:** Negative

**Definition:** All items that were present this month but not in next month, confirmed as returned via SMS return tables.

**How to identify a churned item (2 conditions, both must be true):**
1. **RR cycle presence check:** Entity has an RR cycle with `start_date` in current month (Month M) but does NOT have an RR cycle with `start_date` in next month (Month M+1). Both checks use `revenue_recognitions` filtered to `FURLENCO_RENTAL`, `state NOT IN ('CANCELLED','INVALIDATED')`, `entity_type IN ('ITEM','ATTACHMENT')`.
2. **Return confirmation from SMS:** The entity has a completed return — `return_items.state = 'COMPLETED'` (for ITEM) or `return_attachments.state = 'COMPLETED'` (for ATTACHMENT).

**Revenue amount:** The `taxable_amount` from the entity's last RR cycle in Month M (the cycle that was present but won't continue).

**Source tables:**
- `furbooks_evolve.revenue_recognitions` — cycle presence/absence
- `order_management_systems_evolve.return_items` / `return_attachments` — return confirmation
- `sms_entity` — for user_id (cx count)

**Example — Single item churned (Jan 2026 analysis):**
```
Entity: ITEM::5001 (Sofa), Customer FUR-100

Revenue Recognition cycles:
  Cycle 1: start_date=2025-12-01, end_date=2025-12-31, taxable_amount=1200  ← Dec cycle (prev month)
  Cycle 2: start_date=2026-01-01, end_date=2026-01-31, taxable_amount=1200  ← Jan cycle (PRESENT in Month M)
  (No Feb cycle exists)                                                      ← ABSENT in Month M+1

Return confirmation:
  return_items: item_id=5001, state='COMPLETED', created_at=2026-01-18

Result: ITEM::5001 is churned in Jan.
  items_count = -1, taxable_revenue = -1200
```

**Example — Attachment churned alongside item:**
```
Entity: ITEM::5001 (Sofa), ATTACHMENT::9001 (Sofa Cover), Customer FUR-100

ITEM::5001 RR cycles:
  Jan cycle: start_date=2026-01-01, taxable_amount=1200  ← PRESENT
  (No Feb cycle)                                          ← ABSENT → churned

ATTACHMENT::9001 RR cycles:
  Jan cycle: start_date=2026-01-01, taxable_amount=200   ← PRESENT
  (No Feb cycle)                                          ← ABSENT → churned

Return confirmation:
  return_items: item_id=5001, state='COMPLETED'
  return_attachments: attachment_id=9001, state='COMPLETED'

Result: Both entities churned in Jan.
  items_count = -2, taxable_revenue = -(1200 + 200) = -1400
```

**Edge case — Return cancelled (NOT churned):**
```
Entity: ITEM::6001 (Bed), Customer FUR-200

RR cycles:
  Jan cycle: start_date=2026-01-01, taxable_amount=900   ← PRESENT
  Feb cycle: start_date=2026-02-01, taxable_amount=900   ← PRESENT in M+1

return_items: item_id=6001, state='CANCELLED'

Result: NOT churned — Feb cycle exists AND return was cancelled. Not counted.
```

---

## Component 7: Partial Pickups
**Sort order:** 7 · **Sign:** Negative · **cx_count:** Always 0

**Definition:** Subset of total pickups (Component 6) where the customer still has at least one other entity with an RR cycle in Month M+1. The customer is NOT fully churned — they lose an item but stay in the rental base.

**How to determine PARTIAL:**
- The churned entity belongs to a customer (via `user_id`)
- That same customer has OTHER entities that DO have RR cycles with `start_date` in Month M+1
- cx_count = 0 because the customer is still active

**Example — Partial churn (Jan 2026 analysis):**
```
Customer FUR-400 has 2 items:
  ITEM::8001 (Sofa):  Jan cycle ✓, Feb cycle ✗, return_items state='COMPLETED' → CHURNED
  ITEM::8002 (Bed):   Jan cycle ✓, Feb cycle ✓                                 → STILL ACTIVE

Since ITEM::8002 still has a Feb cycle, FUR-400 is NOT fully churned.
ITEM::8001 is classified as PARTIAL pickup.

Result:
  items_count = -1
  cx_count    = 0   (customer still has the bed)
  taxable_revenue = -1200 (sofa's Jan cycle amount)
```

**Example — Partial with item + attachment (only item returned):**
```
Customer FUR-500 has:
  ITEM::8501 (Washing Machine): Jan cycle ✓, Feb cycle ✗, return COMPLETED → CHURNED
  ATTACHMENT::9501 (Stand):     Jan cycle ✓, Feb cycle ✗, return COMPLETED → CHURNED
  ITEM::8502 (Sofa):            Jan cycle ✓, Feb cycle ✓                   → STILL ACTIVE

Both ITEM::8501 and ATTACHMENT::9501 are churned, but FUR-500 still has ITEM::8502.
Both churned entities classified as PARTIAL.

Result:
  items_count = -2
  cx_count    = 0
  taxable_revenue = -(900 + 150) = -1050
```

---

## Component 8: Full Pickups
**Sort order:** 8 · **Sign:** Negative (items AND cx)

**Definition:** Subset of total pickups (Component 6) where ALL of the customer's entities are churned — no entity for that customer has an RR cycle in Month M+1. The customer exits the rental base completely.

**How to determine FULL:**
- The churned entity belongs to a customer (via `user_id`)
- That customer has NO remaining entities with RR cycles in Month M+1
- cx_count is counted (customer lost)

**Example — Full churn, single item (Jan 2026 analysis):**
```
Customer FUR-600 has 1 item:
  ITEM::9001 (Sofa): Jan cycle ✓, Feb cycle ✗, return_items state='COMPLETED' → CHURNED

FUR-600 has no other entities. ALL items churned → FULL.

Result:
  items_count = -1
  cx_count    = -1  (customer fully exited)
  taxable_revenue = -1200
```

**Example — Full churn, multiple items all returned:**
```
Customer FUR-700 has 3 entities:
  ITEM::9501 (Sofa):            Jan cycle ✓, Feb cycle ✗, return COMPLETED → CHURNED
  ITEM::9502 (Bed):             Jan cycle ✓, Feb cycle ✗, return COMPLETED → CHURNED
  ATTACHMENT::9601 (Bed Frame): Jan cycle ✓, Feb cycle ✗, return COMPLETED → CHURNED

ALL 3 entities churned, no Feb cycles for FUR-700 anywhere → FULL.

Result:
  items_count = -3
  cx_count    = -1  (one customer lost)
  taxable_revenue = -(1200 + 900 + 200) = -2300
```

**Example — Full churn but return completed in prior month:**
```
Customer FUR-800 has 1 item:
  ITEM::9701 (Table): Jan cycle ✓, Feb cycle ✗

return_items: item_id=9701, state='COMPLETED', created_at=2025-12-28
(Return was completed in Dec, but billing cycle ran through Jan)

Result: Still churned in Jan — the Jan cycle exists but Feb doesn't,
and the return is COMPLETED. The return created_at date doesn't matter;
what matters is cycle presence/absence and return state.
  items_count = -1
  cx_count    = -1
  taxable_revenue = -600
```

**Relationship between Components 6, 7, 8:**
```
Component 6 (Total Pickups) = Component 7 (Partial) + Component 8 (Full)

Total items_count  = Partial items  + Full items
Total cx_count     = 0 (Partial cx) + Full cx
Total revenue      = Partial rev    + Full rev
```

---

## Component 9: TTO Total
**Sort order:** 9 · **Sign:** Negative

**Definition:** All items that were present this month but not in next month, where the item was converted to rent-to-purchase (bought out by the customer). The item no longer generates rental revenue.

**How to identify a TTO-churned item (2 conditions, both must be true):**
1. **RR cycle presence check:** Entity has an RR cycle with `start_date` in current month (Month M) but does NOT have an RR cycle with `start_date` in next month (Month M+1). Same filters as pickups: `FURLENCO_RENTAL`, `state NOT IN ('CANCELLED','INVALIDATED')`, `entity_type IN ('ITEM','ATTACHMENT')`.
2. **TTO confirmation from SMS:** The entity has a corresponding entry in `rent_to_purchase_items` linked to a `rent_to_purchase_orders` record, confirming the item was purchased (not returned).

**Revenue amount:** The `taxable_amount` from the entity's last RR cycle in Month M.

**Source tables:**
- `furbooks_evolve.revenue_recognitions` — cycle presence/absence
- `order_management_systems_evolve.rent_to_purchase_items` / `rent_to_purchase_orders` — TTO confirmation
- `sms_entity` — for user_id (cx count)

**Example — Single item TTO (Jan 2026 analysis):**
```
Entity: ITEM::5001 (Washing Machine), Customer FUR-100

Revenue Recognition cycles:
  Cycle 1: start_date=2025-12-01, end_date=2025-12-31, taxable_amount=900   ← Dec cycle
  Cycle 2: start_date=2026-01-01, end_date=2026-01-31, taxable_amount=900   ← Jan cycle (PRESENT in Month M)
  (No Feb cycle exists)                                                       ← ABSENT in Month M+1

TTO confirmation:
  rent_to_purchase_items: item_id=5001, rent_to_purchase_order_id=3001
  rent_to_purchase_orders: id=3001, state='FULFILLED', created_at=2026-01-20

Result: ITEM::5001 is TTO-churned in Jan.
  items_count = -1, taxable_revenue = -900
```

**Example — Item + attachment both converted:**
```
Entity: ITEM::5001 (AC), ATTACHMENT::9001 (AC Stabilizer), Customer FUR-100

ITEM::5001 RR cycles:
  Jan cycle: start_date=2026-01-01, taxable_amount=1500   ← PRESENT
  (No Feb cycle)                                            ← ABSENT → TTO-churned

ATTACHMENT::9001 RR cycles:
  Jan cycle: start_date=2026-01-01, taxable_amount=200     ← PRESENT
  (No Feb cycle)                                            ← ABSENT → TTO-churned

TTO confirmation:
  rent_to_purchase_items: item_id=5001 (linked to TTO order)

Result: Both entities TTO-churned in Jan.
  items_count = -2, taxable_revenue = -(1500 + 200) = -1700
```

**Edge case — TTO order created but item still has next cycle (NOT churned):**
```
Entity: ITEM::6001 (Fridge), Customer FUR-200

RR cycles:
  Jan cycle: start_date=2026-01-01, taxable_amount=1200    ← PRESENT
  Feb cycle: start_date=2026-02-01, taxable_amount=1200    ← PRESENT in M+1

rent_to_purchase_items: item_id=6001 (TTO order in progress, not yet completed)

Result: NOT TTO-churned — Feb cycle still exists. The TTO hasn't been finalized yet.
```

---

## Component 10: TTO Partial
**Sort order:** 10 · **Sign:** Negative · **cx_count:** Always 0

**Definition:** Subset of TTO total (Component 9) where the customer still has at least one other entity with an RR cycle in Month M+1. The customer bought out some items but still rents others.

**How to determine PARTIAL:**
- The TTO-churned entity belongs to a customer (via `user_id`)
- That same customer has OTHER entities that DO have RR cycles with `start_date` in Month M+1
- cx_count = 0 because the customer is still renting

**Example — Partial TTO (Jan 2026 analysis):**
```
Customer FUR-400 has 2 items:
  ITEM::8001 (Washing Machine): Jan cycle ✓, Feb cycle ✗, TTO confirmed → TTO-CHURNED
  ITEM::8002 (Sofa):            Jan cycle ✓, Feb cycle ✓                 → STILL RENTING

Since ITEM::8002 still has a Feb cycle, FUR-400 is NOT fully churned.
ITEM::8001 is classified as PARTIAL TTO.

Result:
  items_count     = -1
  cx_count        = 0   (customer still rents the sofa)
  taxable_revenue = -900 (washing machine's Jan cycle amount)
```

**Example — Partial TTO with attachment staying:**
```
Customer FUR-500 has:
  ITEM::8501 (Fridge):        Jan cycle ✓, Feb cycle ✗, TTO confirmed   → TTO-CHURNED
  ITEM::8502 (Sofa):          Jan cycle ✓, Feb cycle ✓                   → STILL RENTING
  ATTACHMENT::9501 (Cushion): Jan cycle ✓, Feb cycle ✓                   → STILL RENTING

Only ITEM::8501 is TTO-churned. Customer keeps sofa + cushion → PARTIAL.

Result:
  items_count     = -1
  cx_count        = 0
  taxable_revenue = -1200
```

---

## Component 11: TTO Full
**Sort order:** 11 · **Sign:** Negative (items AND cx)

**Definition:** Subset of TTO total (Component 9) where ALL of the customer's entities are TTO-churned — no entity for that customer has an RR cycle in Month M+1. The customer bought everything and exits the rental base.

**How to determine FULL:**
- The TTO-churned entity belongs to a customer (via `user_id`)
- That customer has NO remaining entities with RR cycles in Month M+1
- cx_count is counted (customer lost from rental base)

**Example — Full TTO, single item (Jan 2026 analysis):**
```
Customer FUR-600 has 1 item:
  ITEM::9001 (Washing Machine): Jan cycle ✓, Feb cycle ✗, TTO confirmed → TTO-CHURNED

FUR-600 has no other entities. ALL items bought out → FULL TTO.

Result:
  items_count     = -1
  cx_count        = -1  (customer fully exited rental base)
  taxable_revenue = -900
```

**Example — Full TTO, multiple items all purchased:**
```
Customer FUR-700 has 3 entities:
  ITEM::9501 (Sofa):            Jan cycle ✓, Feb cycle ✗, TTO confirmed → TTO-CHURNED
  ITEM::9502 (Bed):             Jan cycle ✓, Feb cycle ✗, TTO confirmed → TTO-CHURNED
  ATTACHMENT::9601 (Bed Frame): Jan cycle ✓, Feb cycle ✗, TTO confirmed → TTO-CHURNED

ALL 3 entities bought out, no Feb cycles for FUR-700 → FULL TTO.

Result:
  items_count     = -3
  cx_count        = -1  (one customer lost)
  taxable_revenue = -(1200 + 900 + 200) = -2300
```

**Relationship between Components 9, 10, 11:**
```
Component 9 (TTO Total) = Component 10 (TTO Partial) + Component 11 (TTO Full)

Total items_count  = Partial items  + Full items
Total cx_count     = 0 (Partial cx) + Full cx
Total revenue      = Partial rev    + Full rev
```

---

## Component 12: Swapped Out
**Sort order:** 12 · **Sign:** Negative · **cx_count:** Always 0

**Definition:** Old items removed via swap. Revenue based on the outgoing item's most recent RR cycle before swap date.

**Source:** `swap_items` + `swap_attachments` WHERE `action = 'SWAP_OUT'`, `state = 'FULFILLED'`
**RR selection:** `DENSE_RANK() ORDER BY start_date DESC` → `rr_rnk = 1` (most recent cycle before fulfillment)
**Month bucket:** `fulfillment_date` (IST) in month

**Example:**
Customer swaps old sofa for new one. Old sofa `fulfillment_date = 2026-01-12`. Last RR cycle had `taxable_amount = 1200` → -1200 revenue. cx_count = 0 because customer keeps subscription.

---

## Component 13: Swapped In
**Sort order:** 13 · **Sign:** Positive · **cx_count:** Always 0

**Definition:** Replacement items arriving via swap. Revenue from the incoming item's first RR cycle after swap date.

**Source:** Same swap tables, `action = 'SWAP_IN'`
**RR selection:** `DENSE_RANK() ORDER BY start_date ASC` → `rr_rnk = 1` (first cycle after fulfillment)

**Example:**
New sofa arrives same day. First RR cycle after `2026-01-12` has `taxable_amount = 1500` → +1500 revenue. cx_count = 0 because this is an existing customer.

---

## Component 14: Current Month MTP
**Sort order:** 14 · **Sign:** Negative

**Definition:** Minimum tenure penalty cycles recognised this month for future periods (`start_date` beyond current month end). These are early-recognised charges for customers who broke their tenure commitment.

**Logic:** `recognised_at >= m_start AND < m_end` AND `start_date >= m_end`

**Example:**
Customer breaks tenure. RR created with `recognised_at = 2026-01-05`, `start_date = 2026-03-01`. Recognised in Jan for a March cycle → captured here.

---

## Component 15: Penalty
**Sort order:** 15 · **Sign:** Positive

**Definition:** Penalty charges (e.g., damage, missing parts) recognised this month.

**Source:** `revenue_recognitions` WHERE `entity_type = 'PENALTY'` JOIN `penalty` table
**Month bucket:** `recognised_at` (IST) in month

**Example:**
Customer returns damaged furniture. Penalty entity created, RR `recognised_at = 2026-01-14` (IST) → counted in Jan.

---

## Component 16: Plan Transition
**Sort order:** 16 · **Sign:** +/- (depends on direction)

**Definition:** Revenue impact from UPFRONT discount changes when a customer's tenure plan changes between consecutive schedules. Fires only when `previous_tenure <> current_tenure`.

**Logic chain:**
1. Explode discount JSON array from RR records
2. Keep only UPFRONT discounts (from godfather) or SETTLEMENT records
3. Sum per entity + schedule
4. LAG to compare consecutive schedules
5. Revenue difference = `previous_upfront_discount - current_upfront_discount`

**Month bucket:** `start_date >= prev_start AND < m_start` (previous month window)

**Example — Upgrade:**
Customer on 6-month plan (UPFRONT discount = Rs.500/month). Renews on 12-month plan (UPFRONT discount = Rs.800/month).
`previous_tenure = 6`, `current_tenure = 12`.
Revenue difference = `500 - 800 = -300` (higher discount means less revenue).

**Example — Downgrade:**
12-month → 6-month. Discount drops from Rs.800 to Rs.500.
Revenue difference = `800 - 500 = +300`.

---

## Component 18: RO (Renewal Overdue) — Positive
**Sort order:** 18 · **Sign:** Typically positive

**Definition:** Revenue increase when an entity's recognition type moves from DEFERRAL → ACCRUAL (customer became renewal overdue).

**Trigger:** `previous_recognition_type <> current_recognition_type` AND `current = 'ACCRUAL'`
**Revenue difference:** `current_taxableAmount - previous_taxableAmount`
**Month bucket:** `start_date >= m_start AND < m_end`

**Example:**
Customer's plan expires, they don't renew. Recognition flips from `DEFERRAL` to `ACCRUAL`. Accrual rate is typically higher → positive revenue delta.

---

## Component 19: RO (Renewal Overdue) — Negative
**Sort order:** 19 · **Sign:** Typically negative

**Definition:** Revenue decrease when an entity moves from ACCRUAL → DEFERRAL (customer renewed after being overdue).

**Trigger:** `current_recognition_type = 'DEFERRAL'` AND `previous = 'ACCRUAL'`

**Example:**
Customer was overdue, then renews. Recognition flips `ACCRUAL` → `DEFERRAL`. Lower deferred rate → negative revenue delta.

---

## Component 20: Discount Given
**Sort order:** 20 · **Informational only (does NOT flow into closing)**

**Definition:** Total non-UPFRONT discounts applied to RR cycles starting this month. UPFRONT discounts are excluded here (handled by plan transition).

**Month bucket:** `start_date >= m_start AND < m_end`

**Example:**
Customer has a loyalty discount of Rs.200/month on their Jan cycle → summed here across all entities.

---

## Component 21: Discount Change
**Sort order:** 21 · **Sign:** +/- · **Flows into closing revenue**

**Definition:** Net change in non-UPFRONT discounts between consecutive RR cycles for the same entity.

**Logic chain:**
1. Sum all non-UPFRONT discounts per entity per cycle
2. LAG to compare consecutive cycles
3. Revenue difference = `previous_discount - current_discount`

**Month bucket:** `start_date >= prev_start AND < m_start`

**Example:**
Customer had Rs.200 discount last cycle, now has Rs.350 discount this cycle.
Revenue difference = `200 - 350 = -150` (more discount → less revenue).

---

## Components 24–27: VAS Revenue
**Sign:** Positive · **Informational (does NOT flow into closing revenue column)**

| Sort Order | Category | VAS Types |
|---|---|---|
| 24 | Furlenco Care & Flexi | `FURLENCO_CARE_PROGRAM`, `FLEXI_CANCELLATION` |
| 25 | Delivery Charges | `DELIVERY_CHARGE` |
| 26 | Installation Charges | `AC_INSTALLATION_CHARGE` |
| 27 | Other | Everything else |

**Source:** `revenue_recognitions` WHERE `entity_type = 'VALUE_ADDED_SERVICE'`
**Month bucket:** `start_date` in current month only

**Example:**
Customer has Furlenco Care at Rs.99/month. RR cycle `start_date = 2026-01-01` → counted in Component 24 for Jan.

---

## Calculated Rows

### Closing Revenue (sort_order = 31)

| Metric | Formula |
|---|---|
| **Items** | Adjusted Opening + New Deliveries + Upsells + Partial Pickups + Full Pickups + TTO Partial + TTO Full + Swapped Out + Swapped In |
| **Cx** | Adjusted Opening + New Deliveries + Upsells + Full Pickups + TTO Full |
| **Revenue** | Items components + Current Month MTP + Penalty + Plan Transition + Discount Change |

### Gap (sort_order = 32)

**Formula:** `Month2 Opening Revenue - Month1 Closing Revenue`

Uses raw Opening (S1.1 + S1.2) for Month2, closing_row for Month1. Should be near zero if the bridge is complete.

---

## Component Flow Summary

```
Opening Revenue (S1.1 + S1.2)
  - MTP Adjustment (S1.2)
  = Adjusted Opening (S1.1 only)
    + New Deliveries
    + Upsells
    - Partial Pickups
    - Full Pickups
    - TTO Partial
    - TTO Full
    - Swapped Out
    + Swapped In
    - Current Month MTP
    + Penalty
    +/- Plan Transition
    +/- Discount Change
  = Closing Revenue

  Gap = Next Month Opening - This Month Closing ≈ 0

  (Informational, not in closing: RO+, RO-, Discount Given, VAS 24-27)
```
