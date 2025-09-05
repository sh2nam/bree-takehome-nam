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
  f.*,

  -- normalized timestamp
  f.posted_date::TIMESTAMP AT TIME ZONE 'UTC' AS posted_date_utc,

  -- signed helpers (do NOT aggregate here)
  CASE WHEN f.direction = 'outflow' THEN -f.amount ELSE 0 END AS spend_amount_pos,
  CASE WHEN f.direction = 'inflow'  THEN  f.amount ELSE 0 END  AS inflow_amount_pos,

  -- liquidity flags (atomic)
  CASE WHEN f.balance_after < 0 THEN 1 ELSE 0 END AS neg_balance_flag,
  (f.balance_after - f.amount) AS balance_before,

  -- spend bucket (simple, auditable)
  CASE
    WHEN f.category IN ('groceries','utilities','rent','transport') THEN 'essentials'
    WHEN f.category IN ('entertainment','dining')                    THEN 'discretionary'
    ELSE 'other'
  END AS spend_bucket

FROM fct_transactions f;

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

-- ========================================================
-- Risk Model
-- ========================================================

-- Transactions for risk model
CREATE OR REPLACE VIEW v_fct_transactions_for_risk AS
SELECT
  t.user_id,
  t.txn_id,
  t.posted_date_utc,

  /* =========================
     14-DAY WINDOWS (t-13..t)
     ========================= */
  -- Sums
  SUM(t.inflow_amount_pos)
    OVER (PARTITION BY t.user_id ORDER BY t.posted_date_utc
          RANGE BETWEEN INTERVAL '13' DAY PRECEDING AND CURRENT ROW) AS inflow_sum_14d,
  SUM(t.spend_amount_pos)
    OVER (PARTITION BY t.user_id ORDER BY t.posted_date_utc
          RANGE BETWEEN INTERVAL '13' DAY PRECEDING AND CURRENT ROW) AS spend_sum_14d,
  SUM(CASE WHEN t.spend_bucket='essentials' THEN t.spend_amount_pos ELSE 0 END)
    OVER (PARTITION BY t.user_id ORDER BY t.posted_date_utc
          RANGE BETWEEN INTERVAL '13' DAY PRECEDING AND CURRENT ROW) AS essentials_spend_sum_14d,
  SUM(CASE WHEN t.category='rent' THEN t.spend_amount_pos ELSE 0 END)
    OVER (PARTITION BY t.user_id ORDER BY t.posted_date_utc
          RANGE BETWEEN INTERVAL '13' DAY PRECEDING AND CURRENT ROW) AS rent_spend_sum_14d,

  -- Counts
  SUM(CASE WHEN t.neg_balance_flag=1 THEN 1 ELSE 0 END)
    OVER (PARTITION BY t.user_id ORDER BY t.posted_date_utc
          RANGE BETWEEN INTERVAL '13' DAY PRECEDING AND CURRENT ROW) AS neg_txn_count_14d,
  COUNT(*) OVER (PARTITION BY t.user_id ORDER BY t.posted_date_utc
                 RANGE BETWEEN INTERVAL '13' DAY PRECEDING AND CURRENT ROW) AS txn_count_14d,

  -- Volatility (σ) & daily mean
  STDDEV_POP(t.inflow_amount_pos)
    OVER (PARTITION BY t.user_id ORDER BY t.posted_date_utc
          RANGE BETWEEN INTERVAL '13' DAY PRECEDING AND CURRENT ROW) AS inflow_std_14d,
  (SUM(t.inflow_amount_pos)
     OVER (PARTITION BY t.user_id ORDER BY t.posted_date_utc
           RANGE BETWEEN INTERVAL '13' DAY PRECEDING AND CURRENT ROW)) / 14.0 AS inflow_mean_14d,

  STDDEV_POP(t.balance_after)
    OVER (PARTITION BY t.user_id ORDER BY t.posted_date_utc
          RANGE BETWEEN INTERVAL '13' DAY PRECEDING AND CURRENT ROW) AS bal_std_14d,
  AVG(NULLIF(t.balance_after,0))
    OVER (PARTITION BY t.user_id ORDER BY t.posted_date_utc
          RANGE BETWEEN INTERVAL '13' DAY PRECEDING AND CURRENT ROW) AS bal_mean_14d,

  /* Payroll proximity (default 1000 if none yet) */
  COALESCE(
    DATEDIFF(
      'day',
      MAX(CASE WHEN t.is_payroll=1 THEN t.posted_date_utc END)
        OVER (PARTITION BY t.user_id ORDER BY t.posted_date_utc
              RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
      t.posted_date_utc
    ),
    1000
  ) AS days_since_last_payroll,

  /* =========================
     30-DAY WINDOWS (t-29..t)
     ========================= */
  -- Sums
  SUM(t.inflow_amount_pos)
    OVER (PARTITION BY t.user_id ORDER BY t.posted_date_utc
          RANGE BETWEEN INTERVAL '29' DAY PRECEDING AND CURRENT ROW) AS inflow_sum_30d,
  SUM(t.spend_amount_pos)
    OVER (PARTITION BY t.user_id ORDER BY t.posted_date_utc
          RANGE BETWEEN INTERVAL '29' DAY PRECEDING AND CURRENT ROW) AS spend_sum_30d,
  SUM(CASE WHEN t.spend_bucket='essentials' THEN t.spend_amount_pos ELSE 0 END)
    OVER (PARTITION BY t.user_id ORDER BY t.posted_date_utc
          RANGE BETWEEN INTERVAL '29' DAY PRECEDING AND CURRENT ROW) AS essentials_spend_sum_30d,
  SUM(CASE WHEN t.category='rent' THEN t.spend_amount_pos ELSE 0 END)
    OVER (PARTITION BY t.user_id ORDER BY t.posted_date_utc
          RANGE BETWEEN INTERVAL '29' DAY PRECEDING AND CURRENT ROW) AS rent_spend_sum_30d,

  -- Counts
  SUM(CASE WHEN t.neg_balance_flag=1 THEN 1 ELSE 0 END)
    OVER (PARTITION BY t.user_id ORDER BY t.posted_date_utc
          RANGE BETWEEN INTERVAL '29' DAY PRECEDING AND CURRENT ROW) AS neg_txn_count_30d,
  COUNT(*) OVER (PARTITION BY t.user_id ORDER BY t.posted_date_utc
                 RANGE BETWEEN INTERVAL '29' DAY PRECEDING AND CURRENT ROW) AS txn_count_30d,

  -- Volatility (σ) & daily mean
  STDDEV_POP(t.inflow_amount_pos)
    OVER (PARTITION BY t.user_id ORDER BY t.posted_date_utc
          RANGE BETWEEN INTERVAL '29' DAY PRECEDING AND CURRENT ROW) AS inflow_std_30d,
  (SUM(t.inflow_amount_pos)
     OVER (PARTITION BY t.user_id ORDER BY t.posted_date_utc
           RANGE BETWEEN INTERVAL '29' DAY PRECEDING AND CURRENT ROW)) / 30.0 AS inflow_mean_30d,

  STDDEV_POP(t.balance_after)
    OVER (PARTITION BY t.user_id ORDER BY t.posted_date_utc
          RANGE BETWEEN INTERVAL '29' DAY PRECEDING AND CURRENT ROW) AS bal_std_30d,
  AVG(NULLIF(t.balance_after,0))
    OVER (PARTITION BY t.user_id ORDER BY t.posted_date_utc
          RANGE BETWEEN INTERVAL '29' DAY PRECEDING AND CURRENT ROW) AS bal_mean_30d

FROM v_fct_transactions_clean t;

/* ---------- User-level prior loan performance ---------- */
CREATE OR REPLACE VIEW v_user_prior_loan_perf AS
WITH cur AS (
  SELECT loan_id, user_id, approved_at_utc
  FROM v_fct_loans_clean
),
-- Prior APPROVED loans (strictly before current approval)
prior_approved AS (
  SELECT
    c.loan_id       AS cur_loan_id,
    p.*
  FROM cur c
  JOIN v_fct_loans_clean p
    ON p.user_id = c.user_id
   AND p.approved_at_utc < c.approved_at_utc   -- strictly prior approved loans
),
-- Prior UNAPPROVED loans requested before the current approval
prior_unapproved AS (
  SELECT
    c.loan_id       AS cur_loan_id,
    p.*
  FROM cur c
  JOIN v_fct_loans_clean p
    ON p.user_id = c.user_id
   AND p.requested_at_utc < c.approved_at_utc  -- requested before current approval
   AND COALESCE(p.is_approved,0) = 0
)
SELECT
  c.loan_id,

  /* any prior approved loan exists */
  CASE WHEN COUNT(pa.loan_id) > 0 THEN 1 ELSE 0 END AS prior_loan_flag,

  /* average late days across prior loans (use field directly) */
  AVG(CASE WHEN pa.is_disbursed = 1 THEN pa.late_days END) AS prior_avg_days_late,

  /* average prior disbursed amount */
  AVG(CASE WHEN pa.is_disbursed = 1 THEN pa.amount END)  AS prior_avg_amount,

  /* average prior revenue/amount among disbursed */
  AVG(CASE WHEN pa.is_disbursed = 1 AND pa.amount > 0
           THEN (pa.revenue / pa.amount) END)            AS prior_avg_revenue_to_loan,

  /* prior tip take rate among disbursed */
  AVG(CASE WHEN pa.is_disbursed = 1
           THEN CASE WHEN COALESCE(pa.tip_amount,0) > 0 THEN 1 ELSE 0 END
      END)::DOUBLE                                       AS prior_tip_take_rate,

  /* counts of prior loans by approval status */
  COUNT(pa.loan_id)                                      AS prior_approved_loans_count,
  COUNT(pu.loan_id)                                      AS prior_unapproved_loans_count

FROM cur c
LEFT JOIN prior_approved  pa ON pa.cur_loan_id = c.loan_id
LEFT JOIN prior_unapproved pu ON pu.cur_loan_id = c.loan_id
GROUP BY c.loan_id;


-- ========================================================
-- Risk Modeling Base (derived in txn_snapshot)
-- ========================================================
CREATE OR REPLACE VIEW v_risk_model_base AS
WITH base AS (
  SELECT
    l.loan_id,
    l.user_id,
    l.amount,
    l.approved_at_utc,
    l.disbursed_at_utc,
    l.due_date_clean,
    l.is_default,
    l.is_repaid,

    -- user segments & baselines
    u.province,
    u.device_os,
    u.acquisition_channel,
    u.baseline_risk_score,
    u.payroll_frequency,

    -- prior default flag (strictly before this approval)
    CASE WHEN EXISTS (
      SELECT 1
      FROM v_fct_loans_clean p
      WHERE p.user_id = l.user_id
        AND p.approved_at_utc < l.approved_at_utc
        AND p.is_default = 1
    ) THEN 1 ELSE 0 END AS prior_loan_default_flag
  FROM v_fct_loans_clean l
  JOIN v_dim_users_clean u USING (user_id)
  WHERE l.approved_at_utc IS NOT NULL
),

/* ---------- Latest txn snapshot BEFORE approval ---------- */
txn_snapshot AS (
  SELECT * FROM (
    SELECT
      b.loan_id,
      r.posted_date_utc,

      /* atomic 14d */
      r.inflow_sum_14d, r.spend_sum_14d,
      r.essentials_spend_sum_14d, r.rent_spend_sum_14d,
      r.txn_count_14d, r.neg_txn_count_14d,
      r.inflow_mean_14d, r.inflow_std_14d,
      r.bal_mean_14d,   r.bal_std_14d,

      /* atomic 30d */
      r.inflow_sum_30d, r.spend_sum_30d,
      r.essentials_spend_sum_30d, r.rent_spend_sum_30d,
      r.txn_count_30d, r.neg_txn_count_30d,
      r.inflow_mean_30d, r.inflow_std_30d,
      r.bal_mean_30d,   r.bal_std_30d,

      r.days_since_last_payroll,

      /* derived */
      CASE WHEN r.spend_sum_14d<>0 THEN r.rent_spend_sum_14d       / NULLIF(r.spend_sum_14d,0) END AS rent_share_outflows_14d,
      CASE WHEN r.spend_sum_14d<>0 THEN r.essentials_spend_sum_14d / NULLIF(r.spend_sum_14d,0) END AS essentials_share_14d,
      CASE WHEN r.spend_sum_30d<>0 THEN r.rent_spend_sum_30d       / NULLIF(r.spend_sum_30d,0) END AS rent_share_outflows_30d,
      CASE WHEN r.spend_sum_30d<>0 THEN r.essentials_spend_sum_30d / NULLIF(r.spend_sum_30d,0) END AS essentials_share_30d,

      (r.inflow_sum_14d + r.spend_sum_14d) AS net_cashflow_14d,
      (r.inflow_sum_30d + r.spend_sum_30d) AS net_cashflow_30d,

      CASE WHEN ABS(r.inflow_sum_14d + r.spend_sum_14d) > 0
           THEN r.inflow_std_14d / ABS(r.inflow_sum_14d + r.spend_sum_14d) END AS inflow_vol_to_netcashflow_14d,
      CASE WHEN ABS(r.inflow_sum_14d + r.spend_sum_14d) > 0
           THEN r.bal_std_14d   / ABS(r.inflow_sum_14d + r.spend_sum_14d) END AS bal_vol_to_netcashflow_14d,
      CASE WHEN ABS(r.inflow_sum_30d + r.spend_sum_30d) > 0
           THEN r.inflow_std_30d / ABS(r.inflow_sum_30d + r.spend_sum_30d) END AS inflow_vol_to_netcashflow_30d,
      CASE WHEN ABS(r.inflow_sum_30d + r.spend_sum_30d) > 0
           THEN r.bal_std_30d   / ABS(r.inflow_sum_30d + r.spend_sum_30d) END AS bal_vol_to_netcashflow_30d,

      CASE WHEN r.spend_sum_14d<>0 THEN r.inflow_sum_14d / NULLIF(r.spend_sum_14d,0) END AS cashin_to_cashout_14d,
      CASE WHEN r.spend_sum_30d<>0 THEN r.inflow_sum_30d / NULLIF(r.spend_sum_30d,0) END AS cashin_to_cashout_30d,
      CASE WHEN r.txn_count_14d>0 THEN r.neg_txn_count_14d * 1.0 / NULLIF(r.txn_count_14d,0) END AS overdraft_txshare_14d,
      CASE WHEN r.txn_count_30d>0 THEN r.neg_txn_count_30d * 1.0 / NULLIF(r.txn_count_30d,0) END AS overdraft_txshare_30d,

      ROW_NUMBER() OVER (PARTITION BY b.loan_id ORDER BY r.posted_date_utc DESC) AS rn
    FROM base b
    JOIN v_fct_transactions_for_risk r
      ON r.user_id = b.user_id
     AND r.posted_date_utc < b.approved_at_utc
  ) s
  WHERE rn = 1
)

SELECT
  b.loan_id, b.user_id, b.amount, b.is_default,
  b.approved_at_utc, b.disbursed_at_utc, b.due_date_clean,

  b.province, b.device_os, b.acquisition_channel,
  b.baseline_risk_score, b.payroll_frequency,

  -- prior performance
  p.prior_loan_flag,
  b.prior_loan_default_flag,
  p.prior_avg_days_late,
  p.prior_avg_amount,
  p.prior_avg_revenue_to_loan,
  p.prior_tip_take_rate,
  p.prior_approved_loans_count,
  p.prior_unapproved_loans_count,

  -- flag for txn availability
  CASE WHEN s.loan_id IS NULL THEN 0 ELSE 1 END AS txn_info_found,

  -- atomic txn features
  s.inflow_sum_14d, s.spend_sum_14d, s.essentials_spend_sum_14d, s.rent_spend_sum_14d,
  s.txn_count_14d,  s.neg_txn_count_14d, s.inflow_mean_14d, s.inflow_std_14d, s.bal_mean_14d, s.bal_std_14d,
  s.inflow_sum_30d, s.spend_sum_30d, s.essentials_spend_sum_30d, s.rent_spend_sum_30d,
  s.txn_count_30d,  s.neg_txn_count_30d, s.inflow_mean_30d, s.inflow_std_30d, s.bal_mean_30d, s.bal_std_30d,
  s.days_since_last_payroll,

  -- derived txn features
  s.rent_share_outflows_14d, s.essentials_share_14d,
  s.rent_share_outflows_30d, s.essentials_share_30d,
  s.net_cashflow_14d,        s.net_cashflow_30d,
  s.inflow_vol_to_netcashflow_14d, s.bal_vol_to_netcashflow_14d,
  s.inflow_vol_to_netcashflow_30d, s.bal_vol_to_netcashflow_30d,
  s.cashin_to_cashout_14d, s.cashin_to_cashout_30d,
  s.overdraft_txshare_14d, s.overdraft_txshare_30d

FROM base b
LEFT JOIN v_user_prior_loan_perf p ON p.loan_id = b.loan_id
LEFT JOIN txn_snapshot s           ON s.loan_id = b.loan_id;
