# Feature Engineering & Derived Metrics

## Overview
This document provides comprehensive explanations of all features and metrics created in the canonical views for the Bree fintech case study. These features support funnel analysis, A/B testing, risk modeling, and business intelligence.

## Core Data Model

### Raw Tables
- **`dim_users`** - User dimension with signup, demographics, and risk data
- **`fct_transactions`** - Transaction facts with amounts, categories, and balances
- **`fct_loans`** - Loan lifecycle data with amounts, fees, and status
- **`fct_sessions`** - User session events and interactions
- **`ab_assignments`** - A/B test experiment assignments

---

## Feature Categories

## 1. User Dimension Features (`v_dim_users_clean`)

### Temporal Features
- **`signup_at_utc`** - UTC normalized signup timestamp
- **`bank_linked_at_utc`** - UTC normalized bank linkage timestamp
- **`signup_month`** - Month of signup (1-12) for cohort analysis
- **`signup_year`** - Year of signup for cohort analysis  
- **`signup_dow`** - Day of week for signup (0=Sunday) for seasonality analysis

### Banking & Onboarding Features
- **`bank_linked_flag`** - Binary indicator (1/0) if user linked their bank account
- **`days_taken_signup_bank_connect`** - Time lag between signup and bank linking in days
  - *Business Logic*: Measures onboarding friction and user engagement
  - *Calculation*: `DATEDIFF('day', signup_at_utc, bank_linked_at_utc)`

### Risk Segmentation Features
- **`risk_score_decile`** - User's baseline risk score ranked into deciles (1-10)
  - *Business Logic*: Enables risk-based analysis and policy decisions
  - *Calculation*: `NTILE(10) OVER (ORDER BY baseline_risk_score)`

---

## 2. Transaction Features (`v_fct_transactions_clean`)

### Spending Categorization
- **`spend_bucket`** - Grouped spending categories for behavioral analysis
  - *essentials*: groceries, utilities, rent, transport
  - *discretionary*: entertainment, dining
  - *other*: all other categories

### Liquidity & Balance Features
- **`neg_balance_flag`** - Binary indicator for negative account balance
- **`balance_before`** - Account balance before the transaction
- **`spend_amount_pos`** - Positive-signed outflow amounts (for aggregation)
- **`inflow_amount_pos`** - Positive-signed inflow amounts (for aggregation)

### Rolling Financial Metrics (14-day & 30-day windows)
- **`rolling_14d_spend_sum`** - Total outflows in trailing 14 days
- **`rolling_30d_spend_sum`** - Total outflows in trailing 30 days
- **`rolling_14d_inflow_sum`** - Total inflows in trailing 14 days
- **`rolling_30d_inflow_sum`** - Total inflows in trailing 30 days
- **`rolling_14d_net_cash_flow`** - Net cash flow (inflows - outflows) in trailing 14 days
- **`rolling_30d_net_cash_flow`** - Net cash flow (inflows - outflows) in trailing 30 days

### Volatility & Risk Ratios
- **`rolling_14d_outflow_to_inflow`** - Spend-to-income ratio over 14 days
  - *Business Logic*: Measures spending relative to income for affordability assessment
- **`rolling_30d_outflow_to_inflow`** - Spend-to-income ratio over 30 days
- **`rolling_14d_net_cashflow_stddev`** - Standard deviation of net cash flow (14d)
- **`rolling_30d_net_cashflow_stddev`** - Standard deviation of net cash flow (30d)
- **`vol_inflow_ratio_14d`** - Cash flow volatility normalized by income (14d)
- **`vol_inflow_ratio_30d`** - Cash flow volatility normalized by income (30d)

### Income Tracking Features
- **`last_payroll_date`** - Most recent payroll transaction date
- **`days_since_last_payroll`** - Days since last payroll (income recency)
  - *Business Logic*: Tracks income cadence for loan timing and affordability

---

## 3. Loan Features (`v_fct_loans_clean`)

### Temporal Features
- **`requested_year`**, **`requested_month`** - Loan request cohorts
- **UTC normalized timestamps** for all loan lifecycle events

### Lifecycle Binary Flags
- **`is_approved`** - Loan was approved (1/0)
- **`is_disbursed`** - Loan was disbursed (1/0)
- **`is_repaid`** - Loan was repaid (1/0)
- **`is_default`** - Loan defaulted or charged off (1/0)
- **`is_first_loan`** - User's first loan request (1/0)

### Timing Features (Process Efficiency)
- **`days_to_approve`** - Days from request to approval
- **`days_to_disburse`** - Days from approval to disbursement
- **`days_to_repay`** - Days from disbursement to repayment

### Unit Economics Features
- **`revenue`** - Total revenue per loan
  - *Calculation*: `fee + tip_amount + instant_transfer_fee`
- **`pnl`** - Profit/loss per loan
  - *Calculation*: `revenue - writeoff_amount`

### Performance Ratios
- **`revenue_to_loan`** - Revenue as % of loan amount
- **`pnl_to_loan`** - P&L as % of loan amount
- **`writeoff_to_loan`** - Writeoff as % of loan amount (loss rate)
- **`tip_to_loan`** - Tip as % of loan amount (engagement metric)

---

## 4. Session & Event Features (`v_fct_sessions_clean`)

### Funnel Event Binary Flags
- **`is_app_open`** - App open event (1/0)
- **`is_bank_linked`** - Bank link success event (1/0)
- **`is_request_start`** - Loan request started (1/0)
- **`is_request_submit`** - Loan request submitted (1/0)
- **`is_approved_event`** - Approval event (1/0)
- **`is_disbursed_event`** - Disbursement event (1/0)

### Behavioral Features
- **`event_order`** - Sequential event number per user
- **`is_first_event`** - User's first recorded event (1/0)
- **`screen_bucket`** - Screen categorization (onboarding, loan, home, other)
- **`session_duration_sec`** - Session length in seconds
- **`events_per_session`** - Event density per session

### Temporal Features
- **`event_month`**, **`event_week`** - Event timing cohorts
- **`event_dow`**, **`event_hour`** - Event timing for seasonality analysis

---

## 5. A/B Testing Features (`v_ab_assignments_clean`)

### Experiment Identification
- **`is_price_test`** - User in price experiment (1/0)
- **`is_tip_test`** - User in tip experiment (1/0)
- **`variant_norm`** - Normalized variant name (lowercase)
- **`experiment_variant`** - Combined experiment:variant identifier

### Overlap Detection
- **`assignment_order`** - Order of experiment assignment per user
- **`multi_test_user`** - User assigned to multiple experiments (1/0)

---

## 6. Analytical Views

### Funnel Analysis (`v_user_funnel_base`, `v_funnel_by_segment`)
- **User-level funnel progression** from app open → bank link → request → approval → disbursement
- **Segment-level user counts** by province, device OS, and acquisition channel
- **Raw funnel counts** for conversion rate calculation in analysis layer

### Experiment Analysis (`v_user_experiment_assignments`, `v_experiment_loan_performance`)
- **User-level experiment assignments** with overlap detection
- **Performance metrics by experiment group** (repayment rates, revenue, tips)
- **Statistical testing support** for A/B test analysis

### Risk Modeling (`v_user_risk_features`)
- **Comprehensive risk features** combining demographics, transaction patterns, and loan history
- **Historical performance indicators** (repayment rates, late payment patterns)
- **Recency features** (days since last payroll, days since first loan)

---

## Feature Engineering Principles

### 1. **Temporal Consistency**
- All timestamps normalized to UTC
- Consistent date arithmetic across features
- Rolling windows use consistent lookback periods

### 2. **Business Logic Alignment**
- Features reflect actual business processes and decisions
- Risk indicators align with underwriting criteria
- Performance metrics match business KPIs

### 3. **Statistical Robustness**
- Proper handling of NULL values and edge cases
- Appropriate aggregation methods (sums, averages, percentiles)
- Volatility measures use population standard deviation

### 4. **Analytical Flexibility**
- Features support multiple analytical use cases
- Granular features can be aggregated for different analyses
- Binary flags enable easy filtering and segmentation

---

## Data Quality Considerations

### 1. **Referential Integrity**
- All foreign keys validated across tables
- Orphaned records identified and handled

### 2. **Business Rule Validation**
- Negative amounts only in outflows
- Loan lifecycle order validation
- Reasonable value ranges for all metrics

### 3. **Completeness Checks**
- Key fields have no NULL values
- Required relationships maintained
- Data distribution checks for outliers

This feature engineering framework provides a robust foundation for comprehensive analysis of user behavior, loan performance, experiment effectiveness, and risk assessment in the Bree fintech platform.

# METRICS_2.md - Business Metrics & SQL Definitions

## Overview
This document provides SQL definitions and results for key business metrics used to monitor Bree's fintech platform performance. Each metric includes a simple explanation, calculation method, SQL script, and current results.

---

## 1. D1/D7/W1 Signup-to-Request Rate

**What it measures**: Percentage of users who request their first loan within 1 day, 7 days, or 1 week of signing up.

**How calculated**: Count users with loan requests within time window / total signups * 100

**Business importance**: Measures user activation and product-market fit

### SQL Script
```sql
WITH user_timings AS (
  SELECT u.user_id, u.signup_at_utc AS signup_at, MIN(l.requested_at_utc) AS first_request_at
  FROM v_dim_users_clean u
  LEFT JOIN v_fct_loans_clean l USING(user_id)
  GROUP BY 1,2
)
SELECT
  COUNT(*) AS total_signups,
  COUNT(CASE WHEN DATEDIFF('day', signup_at, first_request_at) <= 1 THEN 1 END) AS d1_requests,   -- includes day 0 & 1
  COUNT(CASE WHEN DATEDIFF('day', signup_at, first_request_at) <= 7 THEN 1 END) AS d7_requests,   -- true 7-day
  COUNT(CASE WHEN DATEDIFF('week', signup_at, first_request_at) <= 1 THEN 1 END) AS w1_requests,   -- align W1 covers up to 13 days
  ROUND(100.0 * COUNT(CASE WHEN DATEDIFF('day', signup_at, first_request_at) <= 1 THEN 1 END) / COUNT(*), 2) AS d1_rate_pct,
  ROUND(100.0 * COUNT(CASE WHEN DATEDIFF('day', signup_at, first_request_at) <= 7 THEN 1 END) / COUNT(*), 2) AS d7_rate_pct,
  ROUND(100.0 * COUNT(CASE WHEN DATEDIFF('week', signup_at, first_request_at) <= 1 THEN 1 END) / COUNT(*), 2) AS w1_rate_pct
FROM user_timings;
```

### Results
| total_signups | d1_requests | d7_requests | w1_requests | d1_rate_pct | d7_rate_pct | w1_rate_pct |
|---------------|-------------|-------------|-------------|-------------|-------------|-------------|
| 10,000        | 537         | 1,877       | 3,107        | 5.37%       | 18.77%      | 31.07%      |

---

## 2. Bank Link Rate

**What it measures**: Percentage of users who successfully link their bank account.

**How calculated**: Count of users with bank linked / total users * 100

**Business importance**: Critical onboarding step required for loan eligibility

### SQL Script
```sql
SELECT 
  COUNT(*) as total_users,
  SUM(bank_linked_flag) as users_linked_bank,
  ROUND(SUM(bank_linked_flag) * 100.0 / COUNT(*), 2) as bank_link_rate_pct
FROM v_dim_users_clean;
```

### Results
| total_users | users_linked_bank | bank_link_rate_pct |
|-------------|-------------------|--------------------|
| 10,000      | 7,195             | 71.95%             |

---

## 3. Approval Rate & Disbursement Rate

**What it measures**: Percentage of loan requests that get approved and percentage that get disbursed.

**How calculated**: 
- Approval rate = Approved loans / total requests * 100
- Disbursement rate = Disbursed loans / total requests * 100

**Business importance**: Measures underwriting efficiency and operational performance

### SQL Script
```sql
SELECT 
  COUNT(*) as total_loan_requests,
  SUM(is_approved) as approved_loans,
  SUM(is_disbursed) as disbursed_loans,
  ROUND(SUM(is_approved) * 100.0 / COUNT(*), 2) as approval_rate_pct,
  ROUND(SUM(is_disbursed) * 100.0 / COUNT(*), 2) as disbursement_rate_pct,
  ROUND(SUM(is_disbursed) * 100.0 / NULLIF(SUM(is_approved), 0), 2) as approval_to_disbursement_rate_pct
FROM v_fct_loans_clean;
```

### Results
| total_loan_requests | approved_loans | disbursed_loans | approval_rate_pct | disbursement_rate_pct | approval_to_disbursement_rate_pct |
|---------------------|----------------|-----------------|-------------------|-----------------------|-----------------------------------|
| 9,961               | 6,536          | 6,536           | 65.62%            | 65.62%                | 100.0%                            |

---

## 4. Repayment Rate & Default (Charge-off) Rate

**What it measures**: Percentage of disbursed loans that are repaid vs defaulted.

**How calculated**: 
- Repayment rate = Repaid loans / disbursed loans * 100
- Default rate = Defaulted loans / disbursed loans * 100

**Business importance**: Core risk metrics for loan portfolio health

### SQL Script
```sql
SELECT 
  COUNT(*) as total_disbursed_loans,
  SUM(is_repaid) as repaid_loans,
  SUM(is_default) as defaulted_loans,
  ROUND(SUM(is_repaid) * 100.0 / COUNT(*), 2) as repayment_rate_pct,
  ROUND(SUM(is_default) * 100.0 / COUNT(*), 2) as default_rate_pct
FROM v_fct_loans_clean
WHERE is_disbursed = 1;
```

### Results
| total_disbursed_loans | repaid_loans | defaulted_loans | repayment_rate_pct | default_rate_pct |
|-----------------------|--------------|-----------------|--------------------|--------------------|
| 6,536                 | 4,885        | 1,651           | 74.74%             | 25.26%             |

---

## 5. Average Loan Amount & Total Take-Rate Per Loan

**What it measures**: Average loan size and total revenue per loan (fees + tips + instant transfer fees).

**How calculated**: 
- Average loan amount = SUM(amount) / COUNT(loans)
- Average take-rate = SUM(fee + tip + instant_fee) / COUNT(loans)

**Business importance**: Unit economics and revenue per transaction

### SQL Script
```sql
SELECT 
  COUNT(*) as total_loans,
  ROUND(AVG(amount), 2) as avg_loan_amount,
  ROUND(AVG(revenue), 2) as avg_take,
  ROUND(AVG(revenue_to_loan) * 100, 2) as avg_take_rate_pct
FROM v_fct_loans_clean;
```

### Results
| total_loans | avg_loan_amount | avg_take | avg_take_rate_pct |
|-------------|-----------------|---------------------|-------------------|
| 9,961       | $141.27         | $1.03               | 0.84%             |

---

## 6. Instant Transfer Adoption Rate

**What it measures**: Percentage of disbursed loan users who pay for instant transfer (vs standard transfer).

**How calculated**: Count of loans with instant_transfer_fee > 0 / disbursed loans * 100

**Business importance**: Premium feature adoption and additional revenue stream

### SQL Script
```sql
SELECT 
  COUNT(*) as disbursed_loans,
  SUM(CASE WHEN instant_transfer_fee > 0 THEN 1 ELSE 0 END) as instant_transfer_users,
  ROUND(SUM(CASE WHEN instant_transfer_fee > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as instant_adoption_rate_pct
FROM v_fct_loans_clean
WHERE is_disbursed = 1;
```

### Results
| disbursed_loans | instant_transfer_users | instant_adoption_rate_pct |
|-----------------|------------------------|---------------------------|
| 6,536           | 959                    | 14.67%                    |

---

## 7. Revenue Per Disbursed Loan (Exclude Principal)

**What it measures**: Average revenue generated per disbursed loan from fees, tips, and instant transfer fees.

**How calculated**: SUM(fee + tip_amount + instant_transfer_fee) / COUNT(disbursed_loans)

**Business importance**: Core revenue metric excluding principal (which is repaid)

### SQL Script
```sql
SELECT 
  COUNT(*) as disbursed_loans,
  ROUND(AVG(revenue), 2) as avg_revenue_per_loan,
  ROUND(SUM(revenue), 2) as total_revenue,
  ROUND(AVG(fee), 2) as avg_fee,
  ROUND(AVG(tip_amount), 2) as avg_tip,
  ROUND(AVG(instant_transfer_fee), 2) as avg_instant_fee
FROM v_fct_loans_clean
WHERE is_disbursed = 1;
```

### Results
| disbursed_loans | avg_revenue_per_loan | total_revenue | avg_fee | avg_tip | avg_instant_fee |
|-----------------|----------------------|---------------|---------|---------|-----------------|
| 6,536           | $1.14                | $7,466.30     | $0.50   | $0.30   | $0.35           |

---

## 8. Guardrail: Basic NPS Proxy - Late Payment Rate

**What it measures**: Percentage of repaid loans that were paid more than 7 days late (customer experience proxy).

**How calculated**: Count of repaid loans with late_days > 7 / total repaid loans * 100

**Business importance**: Customer satisfaction guardrail - high late payment rates may indicate customer stress

### SQL Script
```sql
SELECT 
  COUNT(*) as repaid_loans,
  SUM(CASE WHEN late_days > 7 THEN 1 ELSE 0 END) as late_repaid_loans,
  ROUND(SUM(CASE WHEN late_days > 7 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as late_payment_rate_pct,
  ROUND(AVG(late_days), 1) as avg_late_days
FROM v_fct_loans_clean
WHERE is_repaid = 1;
```

### Results
| repaid_loans | late_repaid_loans | late_payment_rate_pct | avg_late_days |
|--------------|-------------------|-----------------------|---------------|
| 4,885        | 0                 | 0.0%                  | 1.3           |

---

## Summary Dashboard Metrics

| Metric | Value | Status |
|--------|-------|--------|
| **Activation (D7 signup-to-request)** | 18.77% | 🟡 Monitor |
| **Bank Link Rate** | 71.95% | 🟢 Good |
| **Approval Rate** | 65.62% | 🟢 Good |
| **Repayment Rate** | 74.74% | 🟢 Good |
| **Default Rate** | 25.26% | 🟡 Monitor |
| **Average Loan Amount** | $141.27 | ℹ️ Baseline |
| **Revenue per Loan** | $1.14 | ℹ️ Baseline |
| **Instant Transfer Adoption** | 14.67% | 🟡 Opportunity |
| **Late Payment Rate (NPS Proxy)** | 0.0% | 🟢 Excellent |

### Key Insights
- **Strong operational performance**: 100% approval-to-disbursement rate indicates efficient operations
- **Healthy repayment behavior**: 0% late payments (>7 days) suggests good customer experience
- **Growth opportunities**: 
  - D7 activation at 18.77% has room for improvement
  - Instant transfer adoption at 14.67% could be increased
  - Bank link rate at 71.95% is the primary funnel bottleneck
