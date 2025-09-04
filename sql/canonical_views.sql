-- ========================================================
-- CANONICAL VIEWS FOR BREE CASE STUDY
-- ========================================================
-- These views provide cleaned, enriched data for analysis
-- including funnel analysis, A/B testing, and risk modeling

-- ========================================================
-- Cleaned Dimension: Users
-- ========================================================
CREATE OR REPLACE VIEW v_dim_users_clean AS
SELECT
  u.*,

  -- standardize timestamps to UTC
  signup_at::TIMESTAMP AT TIME ZONE 'UTC' AS signup_at_utc,
  bank_linked_at::TIMESTAMP AT TIME ZONE 'UTC' AS bank_linked_at_utc,

  -- cohorting / temporal features
  DATE_PART('month', signup_at::TIMESTAMP AT TIME ZONE 'UTC') AS signup_month,
  DATE_PART('year',  signup_at::TIMESTAMP AT TIME ZONE 'UTC') AS signup_year,
  DATE_PART('dow',   signup_at::TIMESTAMP AT TIME ZONE 'UTC') AS signup_dow,

  -- banking linkage
  CASE WHEN bank_linked_at IS NOT NULL THEN 1 ELSE 0 END AS bank_linked_flag,
  DATEDIFF('day', signup_at::TIMESTAMP AT TIME ZONE 'UTC', bank_linked_at::TIMESTAMP AT TIME ZONE 'UTC')
      AS days_taken_signup_bank_connect,

  -- risk segmentation
  NTILE(10) OVER (ORDER BY baseline_risk_score) AS risk_score_decile

FROM dim_users u;


-- ========================================================
-- Cleaned Fact: Transactions
-- ========================================================
CREATE OR REPLACE VIEW v_fct_transactions_clean AS
SELECT
  t.*,

  -- ratios and volatility features
  CASE WHEN rolling_14d_inflow_sum > 0 THEN rolling_14d_spend_sum / rolling_14d_inflow_sum END AS rolling_14d_outflow_to_inflow,
  CASE WHEN rolling_30d_inflow_sum > 0 THEN rolling_30d_spend_sum / rolling_30d_inflow_sum END AS rolling_30d_outflow_to_inflow,
  CASE WHEN rolling_14d_inflow_sum > 0 THEN rolling_14d_net_cashflow_stddev / rolling_14d_inflow_sum END AS vol_inflow_ratio_14d,
  CASE WHEN rolling_30d_inflow_sum > 0 THEN rolling_30d_net_cashflow_stddev / rolling_30d_inflow_sum END AS vol_inflow_ratio_30d,

  -- payroll proximity feature
  CASE WHEN last_payroll_date IS NOT NULL
       THEN DATEDIFF('day', last_payroll_date, posted_date)
       ELSE NULL
  END AS days_since_last_payroll

FROM (
  SELECT
    f.*,

    -- spending categories bucketed
    CASE
      WHEN category IN ('groceries','utilities','rent','transport') THEN 'essentials'
      WHEN category IN ('entertainment','dining')                    THEN 'discretionary'
      ELSE 'other'
    END AS spend_bucket,

    -- normalize date
    posted_date::TIMESTAMP AT TIME ZONE 'UTC' AS posted_date_utc,

    -- liquidity stress
    CASE WHEN balance_after < 0 THEN 1 ELSE 0 END AS neg_balance_flag,
    (balance_after - amount) AS balance_before,

    -- signed amounts for inflows/outflows
    CASE WHEN direction = 'outflow' THEN -amount ELSE 0 END AS spend_amount_pos,
    CASE WHEN direction = 'inflow'  THEN  amount ELSE 0 END AS inflow_amount_pos,

    -- rolling sums (14d, 30d)
    SUM(CASE WHEN direction = 'outflow' THEN -amount ELSE 0 END)  OVER (PARTITION BY user_id ORDER BY posted_date
      RANGE BETWEEN INTERVAL '13' DAY PRECEDING AND CURRENT ROW) AS rolling_14d_spend_sum,
    SUM(CASE WHEN direction = 'outflow' THEN -amount ELSE 0 END)  OVER (PARTITION BY user_id ORDER BY posted_date
      RANGE BETWEEN INTERVAL '29' DAY PRECEDING AND CURRENT ROW) AS rolling_30d_spend_sum,
    SUM(CASE WHEN direction = 'inflow'  THEN  amount ELSE 0 END) OVER (PARTITION BY user_id ORDER BY posted_date
      RANGE BETWEEN INTERVAL '13' DAY PRECEDING AND CURRENT ROW) AS rolling_14d_inflow_sum,
    SUM(CASE WHEN direction = 'inflow'  THEN  amount ELSE 0 END) OVER (PARTITION BY user_id ORDER BY posted_date
      RANGE BETWEEN INTERVAL '29' DAY PRECEDING AND CURRENT ROW) AS rolling_30d_inflow_sum,

    -- rolling net cash flow (signed) and volatility
    SUM(CASE WHEN direction = 'inflow' THEN amount ELSE -amount END) OVER (PARTITION BY user_id ORDER BY posted_date
      RANGE BETWEEN INTERVAL '13' DAY PRECEDING AND CURRENT ROW) AS rolling_14d_net_cash_flow,
    SUM(CASE WHEN direction = 'inflow' THEN amount ELSE -amount END) OVER (PARTITION BY user_id ORDER BY posted_date
      RANGE BETWEEN INTERVAL '29' DAY PRECEDING AND CURRENT ROW) AS rolling_30d_net_cash_flow,
    STDDEV_POP(CASE WHEN direction = 'inflow' THEN amount ELSE -amount END) OVER (PARTITION BY user_id ORDER BY posted_date
      RANGE BETWEEN INTERVAL '13' DAY PRECEDING AND CURRENT ROW) AS rolling_14d_net_cashflow_stddev,
    STDDEV_POP(CASE WHEN direction = 'inflow' THEN amount ELSE -amount END) OVER (PARTITION BY user_id ORDER BY posted_date
      RANGE BETWEEN INTERVAL '29' DAY PRECEDING AND CURRENT ROW) AS rolling_30d_net_cashflow_stddev,

    -- last payroll date to track income cadence
    MAX(CASE WHEN is_payroll = 1 THEN posted_date END) OVER (PARTITION BY user_id
      ORDER BY posted_date RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_payroll_date

  FROM fct_transactions f
) t;

-- ========================================================
-- Cleaned Fact: Loans
-- ========================================================
CREATE OR REPLACE VIEW v_fct_loans_clean AS
SELECT
  l.*,

  -- cohorting
  DATE_TRUNC('year',  requested_at) AS requested_year,
  DATE_TRUNC('month', requested_at) AS requested_month,

  -- normalize timestamps
  CAST(requested_at  AS TIMESTAMP) AT TIME ZONE 'UTC' AS requested_at_utc,
  CAST(approved_at   AS TIMESTAMP) AT TIME ZONE 'UTC' AS approved_at_utc,
  CAST(disbursed_at  AS TIMESTAMP) AT TIME ZONE 'UTC' AS disbursed_at_utc,
  CAST(due_date      AS DATE)                      AS due_date_clean,
  CAST(repaid_at     AS TIMESTAMP) AT TIME ZONE 'UTC' AS repaid_at_utc,

  -- lifecycle flags
  CASE WHEN approved_at IS NOT NULL                THEN 1 ELSE 0 END AS is_approved,
  CASE WHEN disbursed_at IS NOT NULL               THEN 1 ELSE 0 END AS is_disbursed,
  CASE WHEN status = 'repaid'                      THEN 1 ELSE 0 END AS is_repaid,
  CASE WHEN status = 'default' OR chargeoff_flag=1 THEN 1 ELSE 0 END AS is_default,
  CASE WHEN requested_at = MIN(requested_at) OVER (PARTITION BY user_id) THEN 1 ELSE 0 END AS is_first_loan,

  -- lifecycle intervals (days)
  CASE WHEN approved_at  IS NOT NULL THEN DATEDIFF('day', requested_at, approved_at) END AS days_to_approve,
  CASE WHEN disbursed_at IS NOT NULL THEN DATEDIFF('day', approved_at, disbursed_at) END AS days_to_disburse,
  CASE WHEN repaid_at    IS NOT NULL THEN DATEDIFF('day', disbursed_at, repaid_at) END AS days_to_repay,

  -- simple unit economics
  (COALESCE(fee,0) + COALESCE(tip_amount,0) + COALESCE(instant_transfer_fee,0)) AS revenue,
  (COALESCE(fee,0) + COALESCE(tip_amount,0) + COALESCE(instant_transfer_fee,0) - COALESCE(writeoff_amount,0)) AS pnl,

  -- ratios (per-loan basis)
  CASE WHEN amount > 0 THEN (COALESCE(fee,0) + COALESCE(tip_amount,0) + COALESCE(instant_transfer_fee,0)) / amount END AS revenue_to_loan,
  CASE WHEN amount > 0 THEN (COALESCE(fee,0) + COALESCE(tip_amount,0) + COALESCE(instant_transfer_fee,0) - COALESCE(writeoff_amount,0)) / amount END AS pnl_to_loan,
  CASE WHEN amount > 0 THEN COALESCE(writeoff_amount,0) / amount END AS writeoff_to_loan,
  CASE WHEN amount > 0 THEN COALESCE(tip_amount,0) / amount END AS tip_to_loan

FROM fct_loans l;

-- ========================================================
-- Cleaned A/B Assignments
-- ========================================================
CREATE OR REPLACE VIEW v_ab_assignments_clean AS
SELECT
  a.*,
  -- normalize timestamp
  CAST(assigned_at AS TIMESTAMP) AT TIME ZONE 'UTC' AS assigned_at_utc,
  -- cohorting
  DATE_TRUNC('month', assigned_at) AS assigned_month,
  DATE_TRUNC('week',  assigned_at) AS assigned_week,
  DATE_PART('year',  assigned_at)  AS assigned_year,
  DATE_PART('dow',   assigned_at)  AS assigned_dow,
  -- experiment type flags
  CASE WHEN experiment_name ILIKE '%Price%' THEN 1 ELSE 0 END AS is_price_test,
  CASE WHEN experiment_name ILIKE '%Tip%'   THEN 1 ELSE 0 END AS is_tip_test,
  -- variant normalization
  LOWER(variant) AS variant_norm,
  -- user assignment order (first experiment vs later)
  ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY assigned_at) AS assignment_order,
  -- multi-test overlap flag
  CASE WHEN COUNT(*) OVER (PARTITION BY user_id) > 1 THEN 1 ELSE 0 END AS multi_test_user,
  -- helper for dashboards
  CONCAT(experiment_name, ':', variant) AS experiment_variant
FROM ab_assignments a;

-- ========================================================
-- Cleaned Fact: Sessions / Events
-- ========================================================
CREATE OR REPLACE VIEW v_fct_sessions_clean AS
SELECT
  s.*,
  -- normalize timestamp
  CAST(ts AS TIMESTAMP) AT TIME ZONE 'UTC' AS ts_utc,
  -- cohorting
  DATE_TRUNC('month', ts) AS event_month,
  DATE_TRUNC('week',  ts) AS event_week,
  DATE_PART('dow',    ts) AS event_dow,
  DATE_PART('hour',   ts) AS event_hour,
  -- funnel step flags
  CASE WHEN event_name = 'app_open'              THEN 1 ELSE 0 END AS is_app_open,
  CASE WHEN event_name = 'link_bank_success'     THEN 1 ELSE 0 END AS is_bank_linked,
  CASE WHEN event_name = 'start_advance_request' THEN 1 ELSE 0 END AS is_request_start,
  CASE WHEN event_name = 'submit_advance_request' THEN 1 ELSE 0 END AS is_request_submit,
  CASE WHEN event_name = 'approved'              THEN 1 ELSE 0 END AS is_approved_event,
  CASE WHEN event_name = 'disbursed'             THEN 1 ELSE 0 END AS is_disbursed_event,
  -- event sequencing per user
  ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY ts) AS event_order,
  CASE WHEN ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY ts) = 1
       THEN 1 ELSE 0 END AS is_first_event,
  -- screen categorization
  CASE
    WHEN screen ILIKE '%onboarding%' THEN 'onboarding'
    WHEN screen ILIKE '%loan%'       THEN 'loan'
    WHEN screen ILIKE '%home%'       THEN 'home'
    ELSE 'other'
  END AS screen_bucket,
  -- session duration (in seconds)
  (EXTRACT(EPOCH FROM (MAX(ts) OVER (PARTITION BY session_id)
                     - MIN(ts) OVER (PARTITION BY session_id)))) AS session_duration_sec,
  COUNT(*) OVER (PARTITION BY session_id) AS events_per_session
FROM fct_sessions s;

-- ========================================================
-- Canonical View: User-level Funnel Base
-- ========================================================
CREATE OR REPLACE VIEW v_user_funnel_base AS
WITH user_first_app_open AS (
  SELECT
    s.user_id,
    MIN(CASE WHEN is_app_open = 1 THEN ts_utc END) AS first_app_open_ts
  FROM v_fct_sessions_clean s
  GROUP BY s.user_id
),
user_bank_link AS (
  SELECT
    u.user_id,
    MAX(u.bank_linked_flag)  AS bank_linked_flag,
    MIN(u.bank_linked_at_utc) AS bank_link_ts
  FROM v_dim_users_clean u
  GROUP BY u.user_id
),
user_loan_steps AS (
  SELECT
    l.user_id,
    MIN(l.requested_at_utc)   AS first_request_ts,
    MIN(l.approved_at_utc)    AS first_approved_ts,
    MIN(l.disbursed_at_utc)   AS first_disbursed_ts,
    MAX(l.is_approved)        AS any_approved,
    MAX(l.is_disbursed)       AS any_disbursed
  FROM v_fct_loans_clean l
  GROUP BY l.user_id
),
seg AS (
  SELECT
    u.user_id,
    u.province,
    u.device_os,
    u.acquisition_channel,
    u.signup_month,
    u.signup_year
  FROM v_dim_users_clean u
)
SELECT
  s.user_id,
  s.province,
  s.device_os,
  s.acquisition_channel,
  s.signup_month,
  s.signup_year,

  -- funnel timestamps
  a.first_app_open_ts,
  b.bank_link_ts,
  l.first_request_ts,
  l.first_approved_ts,
  l.first_disbursed_ts,

  -- step flags (1/0)
  CASE WHEN a.first_app_open_ts IS NOT NULL THEN 1 ELSE 0 END AS did_app_open,
  COALESCE(b.bank_linked_flag, 0)                             AS did_bank_link,
  CASE WHEN l.first_request_ts   IS NOT NULL THEN 1 ELSE 0 END AS did_request,
  CASE WHEN l.first_approved_ts  IS NOT NULL OR l.any_approved  = 1 THEN 1 ELSE 0 END AS did_approve,
  CASE WHEN l.first_disbursed_ts IS NOT NULL OR l.any_disbursed = 1 THEN 1 ELSE 0 END AS did_disburse

FROM seg s
LEFT JOIN user_first_app_open a ON s.user_id = a.user_id
LEFT JOIN user_bank_link      b ON s.user_id = b.user_id
LEFT JOIN user_loan_steps     l ON s.user_id = l.user_id;

-- ========================================================
-- Canonical View: Funnel by Segment (counts + conversion)
-- Depends on: v_user_funnel_base
-- ========================================================
CREATE OR REPLACE VIEW v_funnel_by_segment AS
WITH agg AS (
  SELECT
    province,
    device_os,
    acquisition_channel,
    signup_month,
    signup_year,

    COUNT(DISTINCT user_id)                                                     AS total_users,
    COUNT(DISTINCT CASE WHEN first_app_open_ts  IS NOT NULL THEN user_id END)   AS app_open_users,
    COUNT(DISTINCT CASE WHEN did_bank_link      = 1         THEN user_id END)   AS bank_linked_users,
    COUNT(DISTINCT CASE WHEN first_request_ts   IS NOT NULL THEN user_id END)   AS requested_users,
    COUNT(DISTINCT CASE WHEN first_approved_ts  IS NOT NULL THEN user_id END)   AS approved_users,
    COUNT(DISTINCT CASE WHEN first_disbursed_ts IS NOT NULL THEN user_id END)   AS disbursed_users
  FROM v_user_funnel_base
  GROUP BY 1,2,3,4,5
)
SELECT
  province,
  device_os,
  acquisition_channel,
  signup_month,
  signup_year,

  -- step counts
  total_users,
  app_open_users,
  bank_linked_users,
  requested_users,
  approved_users,
  disbursed_users

FROM agg
ORDER BY 1,2,3;

-- ========================================================
-- A/B Test Analysis Views
-- ========================================================

-- User-level experiment assignments with overlap detection
CREATE OR REPLACE VIEW v_user_experiment_assignments AS
WITH user_experiments AS (
  SELECT 
    user_id,
    MAX(CASE WHEN experiment_name = 'PriceTest_2025Q2' THEN variant END) as price_variant,
    MAX(CASE WHEN experiment_name = 'TipPrompt_2025Q2' THEN variant END) as tip_variant,
    COUNT(DISTINCT experiment_name) as num_experiments
  FROM v_ab_assignments_clean
  GROUP BY user_id
)
SELECT 
  u.*,
  CASE WHEN num_experiments > 1 THEN 1 ELSE 0 END as is_overlapping_user,
  COALESCE(price_variant, 'not_assigned') as price_test_group,
  COALESCE(tip_variant, 'not_assigned') as tip_test_group,
  CONCAT(COALESCE(price_variant, 'none'), '_', COALESCE(tip_variant, 'none')) as combined_variant
FROM user_experiments u;

-- Loan performance by experiment groups
CREATE OR REPLACE VIEW v_experiment_loan_performance AS
SELECT
  e.price_test_group,
  e.tip_test_group,
  e.is_overlapping_user,
  COUNT(DISTINCT l.user_id) as users_with_loans,
  COUNT(l.loan_id) as total_loans,
  AVG(l.amount) as avg_loan_amount,
  AVG(l.revenue) as avg_revenue_per_loan,
  AVG(l.tip_amount) as avg_tip_amount,
  SUM(CASE WHEN l.status = 'repaid' THEN 1 ELSE 0 END) * 1.0 / COUNT(l.loan_id) as repayment_rate,
  SUM(CASE WHEN l.status = 'default' THEN 1 ELSE 0 END) * 1.0 / COUNT(l.loan_id) as default_rate,
  AVG(l.pnl) as avg_pnl_per_loan
FROM v_user_experiment_assignments e
LEFT JOIN v_fct_loans_clean l ON e.user_id = l.user_id
WHERE l.loan_id IS NOT NULL
GROUP BY 1,2,3;

-- ========================================================
-- Risk Modeling Features
-- ========================================================

-- User-level risk features for modeling (updated for experiments)
CREATE OR REPLACE VIEW v_user_risk_features AS
WITH txn AS (
  SELECT
    user_id,
    SUM(CASE WHEN direction='inflow'  THEN amount ELSE 0 END) AS total_inflows,
    SUM(CASE WHEN direction='outflow' THEN amount ELSE 0 END) AS total_outflows,
    COUNT(*)                          AS txn_count,
    AVG(amount)                       AS avg_txn_amount,
    STDDEV(amount)                    AS txn_amount_volatility,
    AVG(balance_after)                 AS avg_balance,
    MIN(balance_after)                 AS min_balance,
    SUM(CASE WHEN balance_after < 0 THEN 1 ELSE 0 END) AS neg_balance_days,
    COUNT(DISTINCT category)          AS unique_spend_categories
  FROM fct_transactions
  GROUP BY user_id
)
SELECT
  u.user_id,
  u.province,
  u.device_os,
  u.acquisition_channel,
  u.baseline_risk_score,
  u.risk_score_decile,

  -- Transaction features
  t.total_inflows,
  t.total_outflows,
  t.txn_count,
  CASE WHEN t.total_inflows > 0 THEN t.total_outflows*1.0/t.total_inflows END AS outflow_inflow_ratio,
  t.avg_balance,
  t.min_balance,
  t.neg_balance_days,
  t.unique_spend_categories,
  t.txn_amount_volatility,

FROM v_dim_users_clean u
LEFT JOIN txn t ON u.user_id = t.user_id;

-- ========================================================
-- Experiment Analysis Views
-- ========================================================

-- Loans enriched with experiment assignments and user features
CREATE OR REPLACE VIEW v_loans_with_experiments AS
SELECT
  l.loan_id,
  l.user_id,
  l.amount,
  l.revenue,
  l.tip_amount,
  l.pnl,
  l.is_default,
  l.is_repaid,
  l.is_disbursed,
  l.disbursed_at_utc,      
  e.price_test_group,
  e.tip_test_group,
  e.combined_variant,
  u.risk_score_decile,
  u.province,
  u.device_os,
  u.acquisition_channel
FROM v_fct_loans_clean l
JOIN v_user_experiment_assignments e ON l.user_id = e.user_id
JOIN v_dim_users_clean u ON l.user_id = u.user_id;

-- Daily experiment assignment tracking
CREATE OR REPLACE VIEW v_experiment_daily_assignments AS
SELECT
  assigned_at::DATE AS date,
  experiment_name,
  variant,
  COUNT(DISTINCT user_id) AS users_assigned
FROM v_ab_assignments_clean
GROUP BY 1,2,3;

-- Funnel analysis by experiment groups
CREATE OR REPLACE VIEW v_funnel_by_experiment AS
SELECT
  e.price_test_group,
  e.tip_test_group,
  COUNT(DISTINCT u.user_id) AS total_users,
  SUM(did_app_open)   AS app_open_users,
  SUM(did_bank_link)  AS bank_linked_users,
  SUM(did_request)    AS requested_users,
  SUM(did_approve)    AS approved_users,
  SUM(did_disburse)   AS disbursed_users
FROM v_user_funnel_base u
JOIN v_user_experiment_assignments e ON u.user_id = e.user_id
GROUP BY 1,2;

-- Loan tip analysis features
CREATE OR REPLACE VIEW v_loan_tip_features AS
SELECT
  loan_id,
  user_id,
  CASE WHEN tip_amount > 0 THEN 1 ELSE 0 END AS tip_taken,
  tip_amount,
  amount,
  CASE WHEN amount > 0 THEN tip_amount/amount END AS tip_to_principal
FROM v_fct_loans_clean;

-- ========================================================
-- Historical Loan Performance (Previous Loans Only)
-- ========================================================
CREATE OR REPLACE VIEW v_historical_loan_performance AS
SELECT 
  current_loan.user_id,
  current_loan.loan_id,
  COUNT(prev_loan.loan_id) AS prev_loan_count,
  CASE 
    WHEN COUNT(prev_loan.loan_id) > 0 
    THEN SUM(CASE WHEN prev_loan.status = 'repaid' THEN 1 ELSE 0 END) * 1.0 / COUNT(prev_loan.loan_id)
    ELSE NULL 
  END AS hist_repay_rate,
  CASE 
    WHEN COUNT(prev_loan.loan_id) > 0 
    THEN SUM(CASE WHEN prev_loan.status = 'default' THEN 1 ELSE 0 END) * 1.0 / COUNT(prev_loan.loan_id)
    ELSE null 
  END AS hist_default_rate,
  CASE 
    WHEN COUNT(prev_loan.loan_id) > 0 
    THEN SUM(CASE WHEN prev_loan.status = 'default' THEN 1 ELSE 0 END)
    ELSE null 
  END AS hist_default_count,
  CASE 
    WHEN COUNT(prev_loan.loan_id) > 0 
    THEN avg(prev_loan.late_days)
    ELSE null 
  END AS hist_avg_late_days,
  CASE 
    WHEN COUNT(prev_loan.loan_id) > 0 
    THEN sum(case when prev_loan.late_days > 0 then 1 else 0 end)
    ELSE null 
  END AS hist_avg_late_days_count
FROM v_fct_loans_clean current_loan
LEFT JOIN v_fct_loans_clean prev_loan 
  ON current_loan.user_id = prev_loan.user_id 
  AND prev_loan.requested_at_utc < current_loan.requested_at_utc
  AND prev_loan.is_disbursed = 1  -- Only consider disbursed loans
GROUP BY current_loan.user_id, current_loan.loan_id;

-- Experiment loans with risk features
CREATE OR REPLACE VIEW v_experiment_loans_risk AS
SELECT
  l.*,
  e.price_test_group,
  e.tip_test_group,
  r.risk_score_decile,
  r.outflow_inflow_ratio,
  hlp.hist_repay_rate,
  hlp.hist_default_rate,
  hlp.hist_default_count
FROM v_fct_loans_clean l
JOIN v_user_experiment_assignments e ON l.user_id = e.user_id
JOIN v_user_risk_features r ON l.user_id = r.user_id
LEFT JOIN v_historical_loan_performance hlp ON l.loan_id = hlp.loan_id;

-- ========================================================
-- For Risk Modeling
-- ========================================================

-- Per-day aggregates + end-of-day balance proxy (uses your spend_bucket)
CREATE OR REPLACE VIEW v_txn_daily AS
WITH t AS (
  SELECT
    user_id,
    CAST(posted_date_utc AS DATE) AS d,
    direction,
    amount,
    category,
    spend_bucket,
    balance_after,
    ROW_NUMBER() OVER (
      PARTITION BY user_id, CAST(posted_date_utc AS DATE)
      ORDER BY posted_date_utc DESC
    ) AS rn_day_last
  FROM v_fct_transactions_clean
)
SELECT
  user_id,
  d AS txn_date,
  SUM(CASE WHEN direction='inflow'  THEN amount ELSE 0 END) AS inflow_sum,
  SUM(CASE WHEN direction='outflow' THEN -amount ELSE 0 END) AS outflow_sum,
  SUM(CASE WHEN category='rent' THEN -amount ELSE 0 END)     AS rent_outflow,
  SUM(CASE WHEN spend_bucket='essentials'    THEN -amount ELSE 0 END) AS essentials_outflow,
  SUM(CASE WHEN spend_bucket='discretionary' THEN -amount ELSE 0 END) AS discretionary_outflow,
  MIN(balance_after) AS min_balance_day,
  MAX(CASE WHEN rn_day_last=1 THEN balance_after END) AS eod_balance
FROM t
GROUP BY 1,2;

CREATE OR REPLACE VIEW v_payroll_history AS
SELECT user_id, CAST(posted_date_utc AS TIMESTAMP) AS payroll_ts
FROM v_fct_transactions_clean
WHERE is_payroll = 1;

CREATE OR REPLACE VIEW v_risk_features_at_approval AS
WITH loans AS (
  SELECT l.loan_id, l.user_id, l.amount, CAST(l.approved_at_utc AS TIMESTAMP) AS anchor_ts
  FROM v_fct_loans_clean l
  WHERE l.is_approved = 1 AND l.approved_at_utc IS NOT NULL
),

-- 30d window on transactions
w30 AS (
  SELECT lo.loan_id, tx.direction, tx.amount, tx.category
  FROM loans lo
  JOIN v_fct_transactions_clean tx
    ON tx.user_id = lo.user_id
   AND tx.posted_date_utc >= lo.anchor_ts - INTERVAL 30 DAY
   AND tx.posted_date_utc <  lo.anchor_ts
),

-- 14d / 30d windows on daily rollups
w14_day AS (
  SELECT lo.loan_id, d.*
  FROM loans lo
  JOIN v_txn_daily d
    ON d.user_id = lo.user_id
   AND d.txn_date >= CAST(lo.anchor_ts AS DATE) - INTERVAL 14 DAY
   AND d.txn_date <  CAST(lo.anchor_ts AS DATE)
),
w30_day AS (
  SELECT lo.loan_id, d.*
  FROM loans lo
  JOIN v_txn_daily d
    ON d.user_id = lo.user_id
   AND d.txn_date >= CAST(lo.anchor_ts AS DATE) - INTERVAL 30 DAY
   AND d.txn_date <  CAST(lo.anchor_ts AS DATE)
),

-- Payroll context
payroll_ctx AS (
  SELECT
    lo.loan_id,
    MAX(CASE WHEN p.payroll_ts < lo.anchor_ts THEN p.payroll_ts END) AS last_payroll_ts
  FROM loans lo
  LEFT JOIN v_payroll_history p
    ON p.user_id = lo.user_id
   AND p.payroll_ts >= lo.anchor_ts - INTERVAL 120 DAY
   AND p.payroll_ts <  lo.anchor_ts
  GROUP BY lo.loan_id
),
payroll_gaps AS (
  SELECT
    lo.loan_id,
    p.payroll_ts AS ts,
    LEAD(p.payroll_ts) OVER (PARTITION BY lo.loan_id ORDER BY p.payroll_ts) AS next_ts
  FROM loans lo
  JOIN v_payroll_history p
    ON p.user_id = lo.user_id
   AND p.payroll_ts >= lo.anchor_ts - INTERVAL 180 DAY
   AND p.payroll_ts <  lo.anchor_ts
),
payroll_freq AS (
  SELECT loan_id, MEDIAN(date_diff('day', ts, next_ts)) AS median_payroll_gap_days
  FROM payroll_gaps
  WHERE next_ts IS NOT NULL
  GROUP BY loan_id
),

-- 30d aggregates (txn grain)
agg_30 AS (
  SELECT
    loan_id,
    SUM(CASE WHEN direction='inflow'  THEN amount ELSE 0 END)  AS inflow_sum_30d,
    SUM(CASE WHEN direction='outflow' THEN -amount ELSE 0 END) AS outflow_sum_30d,
    SUM(CASE WHEN category='rent' THEN -amount ELSE 0 END)     AS rent_sum_30d,
    CASE
      WHEN AVG(CASE WHEN direction='inflow' THEN amount END) IS NOT NULL
      THEN STDDEV(CASE WHEN direction='inflow' THEN amount END)
           / NULLIF(AVG(CASE WHEN direction='inflow' THEN amount END), 0)
    END AS inflow_cv_30d
  FROM w30
  GROUP BY loan_id
),

-- 14d daily rollups
agg_14 AS (
  SELECT
    loan_id,
    AVG(eod_balance) AS avg_eod_balance_14d,
    MIN(min_balance_day) AS min_balance_14d,
    SUM(CASE WHEN min_balance_day < 0 THEN 1 ELSE 0 END) AS overdraft_days_14d,
    SUM(inflow_sum)  AS inflow_sum_14d,
    SUM(outflow_sum) AS outflow_sum_14d,
    SUM(essentials_outflow)    AS essentials_outflow_14d,
    SUM(discretionary_outflow) AS discretionary_outflow_14d,
    STDDEV(inflow_sum - outflow_sum) AS net_cf_sd_daily_14d
  FROM w14_day
  GROUP BY loan_id
),

-- 30d daily rollups
agg_30_day AS (
  SELECT
    loan_id,
    AVG(eod_balance) AS avg_eod_balance_30d,
    SUM(CASE WHEN min_balance_day < 0 THEN 1 ELSE 0 END) AS overdraft_days_30d,
    SUM(essentials_outflow)    AS essentials_outflow_30d,
    SUM(discretionary_outflow) AS discretionary_outflow_30d,
    STDDEV(inflow_sum - outflow_sum) AS net_cf_sd_daily_30d
  FROM w30_day
  GROUP BY loan_id
)

SELECT
  lo.loan_id,
  lo.user_id,
  lo.amount,
  lo.anchor_ts,

  -- Income cadence & stability
  pf.median_payroll_gap_days,
  CASE
    WHEN pf.median_payroll_gap_days BETWEEN 6 AND 8  THEN 'weekly'
    WHEN pf.median_payroll_gap_days BETWEEN 12 AND 17 THEN 'biweekly'
    WHEN pf.median_payroll_gap_days BETWEEN 26 AND 35 THEN 'monthly'
    ELSE 'irregular'
  END AS payroll_frequency_bucket,
  date_diff('day', pc.last_payroll_ts, lo.anchor_ts) AS days_since_last_payroll,
  a30.inflow_cv_30d,
  a30.inflow_sum_30d AS total_inflow_30d,

  -- Liquidity & balances
  a14.avg_eod_balance_14d,
  a14.min_balance_14d,
  a14.overdraft_days_14d,
  a30d.avg_eod_balance_30d,
  a30d.overdraft_days_30d,

  -- Totals & base ratios
  a14.inflow_sum_14d,
  a14.outflow_sum_14d,
  a30.outflow_sum_30d AS total_outflow_30d,
  CASE WHEN a14.inflow_sum_14d > 0 THEN a14.outflow_sum_14d * 1.0 / NULLIF(a14.inflow_sum_14d, 0) END AS outflow_to_inflow_14d,
  CASE WHEN a30.inflow_sum_30d > 0 THEN a30.outflow_sum_30d * 1.0 / NULLIF(a30.inflow_sum_30d, 0) END AS outflow_to_inflow_30d,

  -- Expense pressure & cushion (14d + 30d, denominated by outflow)
  CASE WHEN a30.outflow_sum_30d > 0 THEN a30.rent_sum_30d * 1.0 / NULLIF(a30.outflow_sum_30d, 0) END AS rent_share_30d,
  CASE WHEN a14.outflow_sum_14d > 0 THEN a14.essentials_outflow_14d    * 1.0 / NULLIF(a14.outflow_sum_14d, 0) END AS essentials_share_14d,
  CASE WHEN a14.outflow_sum_14d > 0 THEN a14.discretionary_outflow_14d * 1.0 / NULLIF(a14.outflow_sum_14d, 0) END AS discretionary_share_14d,
  CASE WHEN a30.outflow_sum_30d > 0 THEN a30d.essentials_outflow_30d    * 1.0 / NULLIF(a30.outflow_sum_30d, 0) END AS essentials_share_30d,
  CASE WHEN a30.outflow_sum_30d > 0 THEN a30d.discretionary_outflow_30d * 1.0 / NULLIF(a30.outflow_sum_30d, 0) END AS discretionary_share_30d,

  -- Momentum (net_cf / inflow)
  CASE WHEN a14.inflow_sum_14d > 0
       THEN (a14.inflow_sum_14d - a14.outflow_sum_14d) * 1.0 / NULLIF(a14.inflow_sum_14d, 0)
  END AS net_cf_momentum_14d,
  CASE WHEN a30.inflow_sum_30d > 0
       THEN (a30.inflow_sum_30d - a30.outflow_sum_30d) * 1.0 / NULLIF(a30.inflow_sum_30d, 0)
  END AS net_cf_momentum_30d,

  -- Volatility (daily net-cf sd / inflow)
  CASE WHEN a14.inflow_sum_14d > 0 THEN a14.net_cf_sd_daily_14d * 1.0 / NULLIF(a14.inflow_sum_14d, 0) END AS inflow_volatility_14d,
  CASE WHEN a30.inflow_sum_30d > 0 THEN a30d.net_cf_sd_daily_30d * 1.0 / NULLIF(a30.inflow_sum_30d, 0) END AS inflow_volatility_30d,

  -- Balance ratios
  CASE WHEN a14.outflow_sum_14d > 0 THEN a14.avg_eod_balance_14d * 1.0 / NULLIF(a14.outflow_sum_14d, 0) END AS balance_to_outflow_14d,
  CASE WHEN a30.outflow_sum_30d > 0 THEN a30d.avg_eod_balance_30d * 1.0 / NULLIF(a30.outflow_sum_30d, 0) END AS balance_to_outflow_30d,
  CASE WHEN a14.inflow_sum_14d  > 0 THEN a14.avg_eod_balance_14d * 1.0 / NULLIF(a14.inflow_sum_14d, 0)  END AS balance_to_inflow_14d,
  CASE WHEN a30.inflow_sum_30d  > 0 THEN a30d.avg_eod_balance_30d * 1.0 / NULLIF(a30.inflow_sum_30d, 0)  END AS balance_to_inflow_30d

FROM loans lo
LEFT JOIN payroll_ctx   pc   ON lo.loan_id = pc.loan_id
LEFT JOIN payroll_freq  pf   ON lo.loan_id = pf.loan_id
LEFT JOIN agg_30        a30  ON lo.loan_id = a30.loan_id
LEFT JOIN agg_14        a14  ON lo.loan_id = a14.loan_id
LEFT JOIN agg_30_day    a30d ON lo.loan_id = a30d.loan_id;

CREATE OR REPLACE VIEW v_default_label_30d AS
SELECT
  l.loan_id,
  l.user_id,
  CASE
    WHEN l.is_disbursed = 1
     AND l.is_default = 1
    THEN 1 ELSE 0
  END AS default_30d
FROM v_fct_loans_clean l
WHERE l.is_disbursed = 1;

CREATE OR REPLACE VIEW v_canonical_risk_model AS
SELECT
  l.loan_id,
  l.user_id,

  -- Loan context (approval-time only)
  l.amount AS loan_amount,
  l.is_first_loan,
  l.requested_at_utc,
  l.approved_at_utc,
  l.disbursed_at_utc ,
  l.due_date_clean ,
  l.repaid_at_utc ,
  l.tip_amount,
  l.fee, l.instant_transfer_fee, l.status ,  l.autopay_enrolled,

  -- User / acquisition / baseline
  u.device_os,
  u.province,
  u.acquisition_channel,
  u.signup_month,
  u.baseline_risk_score,
  u.risk_score_decile,
  u.payroll_frequency,
  u.fico_band,

  -- Approval-time risk features
  rfa.median_payroll_gap_days,
  rfa.payroll_frequency_bucket,
  rfa.days_since_last_payroll,
  rfa.inflow_cv_30d,
  rfa.total_inflow_30d,
  rfa.avg_eod_balance_14d,
  rfa.min_balance_14d,
  rfa.overdraft_days_14d,
  rfa.avg_eod_balance_30d,
  rfa.overdraft_days_30d,
  rfa.inflow_sum_14d,
  rfa.outflow_sum_14d,
  rfa.total_outflow_30d,
  rfa.outflow_to_inflow_14d,
  rfa.outflow_to_inflow_30d,
  rfa.rent_share_30d,
  rfa.essentials_share_14d,
  rfa.discretionary_share_14d,
  rfa.essentials_share_30d,
  rfa.discretionary_share_30d,
  rfa.net_cf_momentum_14d,
  rfa.net_cf_momentum_30d,
  rfa.inflow_volatility_14d,
  rfa.inflow_volatility_30d,
  rfa.balance_to_outflow_14d,
  rfa.balance_to_outflow_30d,
  rfa.balance_to_inflow_14d,
  rfa.balance_to_inflow_30d,

  -- Historical loan performance (previous loans only)
  hlp.hist_repay_rate,
  hlp.hist_default_rate,
  hlp.hist_default_count,
  hlp.hist_avg_late_days,
  hlp.hist_avg_late_days_count,

  -- Label
  dl.default_30d
FROM v_fct_loans_clean l
JOIN v_dim_users_clean u                  ON l.user_id = u.user_id
LEFT JOIN v_user_risk_features urf        ON l.user_id = urf.user_id
LEFT JOIN v_risk_features_at_approval rfa ON l.loan_id = rfa.loan_id
LEFT JOIN v_default_label_30d dl          ON l.loan_id = dl.loan_id
LEFT JOIN v_historical_loan_performance hlp ON l.loan_id = hlp.loan_id
WHERE l.status IN ('repaid', 'default');
