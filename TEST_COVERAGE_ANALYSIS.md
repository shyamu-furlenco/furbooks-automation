# Test Coverage Analysis: `new_furbooks_automation_18march.sql`

## Executive Summary

The codebase currently has **zero test coverage**. The single production artifact is a 782-line
Databricks SQL query that computes a month-over-month revenue movement bridge across ~17 business
components. Given the financial nature of this output (it directly informs revenue reporting), the
absence of any automated validation is the highest-priority risk.

This document identifies the most critical gaps and proposes a concrete testing strategy.

---

## 1. Risk Map by Component

| # | CTE / Component | Risk Level | Primary Risk |
|---|----------------|-----------|--------------|
| 1 | `sms_entity` | Medium | CANCELLED exclusion silently drops legitimate rows |
| 2 | `deduped_return_items` / `deduped_return_attachments` | High | MIN(created_at) dedup may collapse legitimately separate returns |
| 3 | `furbooks_revenue` (amount calculation) | **Critical** | Division by tenure can produce `NULL` or `Infinity` when `ROUND(...)` = 0 |
| 4 | `months` driver | Medium | Incorrect `ADD_MONTHS` output silently shifts all date windows |
| 5 | `opening_revenue` (Scenarios 1.1 & 1.2) | **Critical** | Two mutually exclusive OR-branches; boundary dates can fall into both or neither |
| 6 | `mtp1_adjustment` | High | Must exactly mirror Scenario 1.2 of `opening_revenue`; drift causes double-counting |
| 7 | `new_deliveries` / `upsells` | Medium | Relies on `flag_based_on_Ua` casing (`lower()`) — values outside `'new'`/`'upsell'` are silently excluded |
| 8 | `pickups_base` → `partial_pickups` / `full_pickups` | High | `CANCELLED` return rows scoped by `cancelled_at`; a non-cancelled row with a `cancelled_at` set would be miscounted |
| 9 | `pickup_raised_all` | Low | Double cast `::float::float` is redundant but harmless |
| 10 | `current_month_mtp` | High | `br.start_date >= m.m_end` — off-by-one at month boundary |
| 11 | `penalty` | Medium | Timezone: scoped by `recognised_at` (UTC+330), not `start_date` — inconsistent with other components |
| 12 | `tenure_windowed` (LAG) | High | NULLs from LAG on first-ever schedule row; must not be confused with a legitimate "no previous plan" |
| 13 | `customer_plan_changes` / `plan_transition_positive/negative` | **Critical** | `previous_tenure <> tenure` silently excludes NULL tenure rows; revenue_difference sign direction |
| 14 | `customer_accrual_changes` / `accrual_positive/negative` | High | `ACCRUAL`→`DEFERRAL` vs `DEFERRAL`→`ACCRUAL` classification depends on `current_recognition_type` field, not the sign of the delta |
| 15 | `discount_per_cycle` (LATERAL VIEW EXPLODE) | High | Malformed JSON in `monetary_components_discounts` produces NULL rows that COALESCE silently zeroes |
| 16 | `discount_changes_all` (UNION ALL x3) | Medium | Positive + Negative sub-sets must not overlap; their union must equal the total |
| 17 | `vas_detail` / `vas_by_category` | Medium | VAS CASE mapping — unrecognised `vas_type` values fall into `'VAS Revenue - Other'` without alerting |
| 18 | `closing_row` | **Critical** | Aggregation CASE lists are manually maintained; any new component added to `all_components` must also be added here |
| 19 | `gap_row` | **Critical** | Should be near zero; a non-zero gap indicates the bridge doesn't reconcile |
| 20 | Timezone conversions (UTC→IST) | **Critical** | `+ INTERVAL '330 minutes'` applied inconsistently — `penalty` uses `recognised_at`; others use `start_date` |

---

## 2. Highest-Priority Test Areas

### 2.1 Division-by-Zero in Revenue Amount Calculation

**Location:** `furbooks_revenue` CTE, lines 66–68

```sql
CAST(rrs.monetary_components_taxableAmount AS DOUBLE)
  / ROUND(DATEDIFF(DAY, rrs.start_date, rrs.end_date) / 30.45)
```

If `DATEDIFF` < 15 days, `ROUND(.../ 30.45)` = 0, causing a division-by-zero or `NULL`.
This would silently zero-out revenue for short-duration schedules.

**Needed test:** A row where `end_date - start_date < 15 days` must produce an explicit error
or be handled via `NULLIF(..., 0)`.

---

### 2.2 Opening Revenue Boundary Overlap (Scenarios 1.1 vs 1.2)

**Location:** `opening_revenue`, lines 170–183

The two `OR` branches overlap when:
- `start_date >= m.m_start` (Scenario 1.2 condition)
- `recognised_at IS NULL` (Scenario 1.1 fallback)

A row with `start_date = m_start` and `recognised_at = NULL` satisfies **both** branches
simultaneously, causing it to be counted twice in `opening_revenue` before the `mtp1_adjustment`
subtracts it once — net result: counted once in opening AND once in MTP, overstating opening
by one entity.

**Needed tests:**
- Entity with `start_date` exactly on `m_start` and `recognised_at = NULL`
- Entity with `start_date` exactly on `prev_start` (lower boundary)
- Entity where `recognised_at` falls exactly on `m_start` (excluded from Scenario 1.1)

---

### 2.3 MTP Adjustment Must Mirror Opening Scenario 1.2

**Location:** `mtp1_adjustment` vs `opening_revenue` Scenario 1.2

`mtp1_adjustment` negates Scenario 1.2 items. If its WHERE clause ever drifts from the
Scenario 1.2 conditions in `opening_revenue`, double-counting occurs silently.

**Needed test:** Assert that `SUM(opening_revenue.taxable_revenue) + SUM(mtp1_adjustment.taxable_revenue)`
for Scenario-1.2-only rows equals zero.

---

### 2.4 Gap Row Should Reconcile to Zero

**Location:** `gap_row`, lines 731–739

The gap between Month 1 closing and Month 2 opening should be ≤ a defined tolerance (e.g. ±1%
of closing revenue) in any valid dataset. A large gap signals missing components or logic errors.

**Needed test:** Assert `ABS(gap_rev) / NULLIF(m1_rev_closing, 0) < 0.01` on historical data.

---

### 2.5 Closing Revenue Component List Completeness

**Location:** `closing_row`, lines 673–728

The CASE expression inside `closing_row` manually enumerates component names. If a new component
is added to `all_components` without updating `closing_row`, the closing total silently excludes it.

**Needed test:** Assert that every component name in `all_components` that should affect closing
revenue is present in the `closing_row` CASE lists.

---

### 2.6 Timezone Consistency

**Location:** Throughout the query; `penalty` is the explicit exception

`recognised_at + INTERVAL '330 minutes'` is applied correctly in most places, but `penalty`
scopes on `recognised_at` while other components scope on `start_date`. A transaction
recognised late on the last day of a month (UTC) could land in the wrong IST month.

**Needed tests:**
- Row recognised at `23:30 UTC` on the last day of month → should appear in next IST month
- Verify `penalty` counts match manual IST-filtered counts for known test months

---

### 2.7 Deduplication of Return Items/Attachments

**Location:** `deduped_return_items` / `deduped_return_attachments`, lines 40–53

`MIN(created_at)` keeps only the earliest return. If a physical item is returned, re-rented,
and returned again, this approach would use the first return date for all revenue recognition
rows, potentially misclassifying the second rental as already-returned.

**Needed test:** Entity with two separate completed return records at different timestamps;
assert the second rental's revenue is not incorrectly attributed to the first return date.

---

### 2.8 Pickup Type Classification

**Location:** `partial_pickups` / `full_pickups` / `pickup_cancellations`

Three mutually exclusive conditions based on `pickup_type` and `return_entity_state`:
- `PARTIAL` + not `CANCELLED`
- `FULL` + not `CANCELLED`
- Any type + `CANCELLED` (scoped on `cancelled_at`)

**Needed tests:**
- A `PARTIAL` row with `return_entity_state = 'CANCELLED'` must appear only in
  `pickup_cancellations`, not in `partial_pickups`
- A `FULL` pickup that later gets cancelled must not appear in both `full_pickups` and
  `pickup_raised_all`
- Assert `pickup_raised_all + partial_pickups + full_pickups + pickup_cancellations` accounts
  for all rows in `pickups_base` without gaps or duplicates

---

### 2.9 Discount JSON Parsing

**Location:** `discount_per_cycle`, lines 481–489

```sql
LATERAL VIEW OUTER EXPLODE(
  from_json(CAST(rr.monetary_components_discounts AS STRING),
    'array<struct<amount:double,catalogReferenceId:string,code:string>>')
) AS d
```

If `monetary_components_discounts` is NULL, `OUTER EXPLODE` returns one row with `d.amount = NULL`.
The `COALESCE(..., 0)` correctly zeroes this. But if the JSON is malformed (non-null but
unparseable), `from_json` returns NULL silently — discount is treated as zero, not flagged.

**Needed test:** Rows with malformed JSON in `monetary_components_discounts` must either raise
an error or be captured in a data quality check.

---

### 2.10 Plan Transition NULL Tenure Handling

**Location:** `customer_plan_changes`, lines 413–414

```sql
WHERE tc.previous_tenure <> tc.tenure
  AND tc.previous_tenure is not null
```

If `tc.tenure` (current) is NULL, `previous_tenure <> tenure` evaluates to `UNKNOWN` (SQL
three-valued logic), silently excluding the row. A NULL current tenure means the entity has
no active plan — this should likely be classified as a full pickup or flagged separately.

**Needed test:** Entity whose tenure becomes NULL in the current schedule should not silently
disappear from the bridge.

---

## 3. Proposed Testing Strategy

### 3.1 Framework Recommendation: DuckDB-based Unit Tests

DuckDB supports the Databricks SQL dialect closely enough to run these CTEs locally using
synthetic fixture data. This allows:
- Fast, repeatable unit tests (no Databricks cluster needed)
- CI/CD integration via GitHub Actions
- Each CTE tested in isolation using `WITH ... AS (SELECT ...)` overrides

**Tool stack:**
- `pytest` + `duckdb` Python package
- `sqlglot` for SQL parsing/linting
- Optional: `dbt` for larger-scale integration testing

### 3.2 Test Categories

#### A. Unit Tests (per-CTE)
Test each CTE in isolation with synthetic rows covering boundary conditions.
Priority order: `furbooks_revenue` → `opening_revenue` + `mtp1_adjustment` → `closing_row` → `gap_row`

#### B. Invariant / Assertion Tests (run against real data)
SQL assertions embedded as a second query after the main query:

```sql
-- Assert: gap is within 1% of closing revenue
SELECT CASE
  WHEN ABS(gap_rev) > 0.01 * ABS(m1_rev_closing) THEN 'FAIL: gap too large'
  ELSE 'PASS'
END AS reconciliation_check
FROM gap_row CROSS JOIN closing_row;

-- Assert: pickup components sum equals pickup_raised_all
SELECT CASE
  WHEN ABS(partial_rev + full_rev - total_raised_rev) > 1 THEN 'FAIL: pickup split mismatch'
  ELSE 'PASS'
END AS pickup_check;

-- Assert: discount sub-totals reconcile
SELECT CASE
  WHEN ABS(pos_rev + neg_rev - total_rev) > 1 THEN 'FAIL: discount split mismatch'
  ELSE 'PASS'
END AS discount_check;
```

#### C. Data Quality Checks (gate before query runs)
```sql
-- Assert no NULL tenures in active schedules
SELECT COUNT(*) AS null_tenures
FROM furlenco_silver.furbooks_evolve.revenue_recognition_schedules
WHERE vertical = 'FURLENCO_RENTAL'
  AND ROUND(DATEDIFF(DAY, start_date, end_date) / 30.45) = 0;
-- Expected: 0 rows
```

#### D. Regression Tests
After each query modification, compare component-level totals against a known-good baseline
(stored as a CSV or Delta table snapshot) for the same reference months.

---

## 4. Prioritized Test Implementation Backlog

| Priority | Test | Component | Effort |
|----------|------|-----------|--------|
| P0 | Gap reconciliation assert | `gap_row` | Low |
| P0 | Division-by-zero guard in amount calc | `furbooks_revenue` | Low |
| P0 | Opening Scenario 1.1 vs 1.2 boundary tests | `opening_revenue` | Medium |
| P0 | MTP1 adjustment mirrors Scenario 1.2 | `mtp1_adjustment` | Medium |
| P1 | Pickup classification completeness | `pickups_base` | Medium |
| P1 | Closing component list completeness | `closing_row` | Low |
| P1 | Timezone boundary (last-day-of-month) | `penalty`, `furbooks_revenue` | Medium |
| P1 | Discount UNION reconciliation | `discount_changes_all` | Low |
| P2 | Dedup correctness (re-rented items) | `deduped_return_items` | High |
| P2 | NULL tenure in plan transitions | `customer_plan_changes` | Medium |
| P2 | Malformed JSON discount handling | `discount_per_cycle` | Medium |
| P3 | VAS category catch-all alerting | `vas_by_category` | Low |
| P3 | Regression snapshot tests | All | High |

---

## 5. Quick Wins (Implementable This Sprint)

1. **Add `NULLIF` to the tenure divisor** to make division-by-zero explicit:
   ```sql
   CAST(rrs.monetary_components_taxableAmount AS DOUBLE)
     / NULLIF(ROUND(DATEDIFF(DAY, rrs.start_date, rrs.end_date) / 30.45), 0)
   ```

2. **Append a reconciliation block** at the end of the SQL (see `tests/sql/reconciliation_asserts.sql`).

3. **Add a data quality pre-check** that validates source table row counts and NULL rates
   before the main query runs.

4. **Tag the `pickup_raised_all` double-cast** (`::float::float`) as a known code smell
   to remove in the next refactor.
