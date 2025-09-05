-- ========================================================
-- DATA QUALITY CHECKS FOR BREE CANONICAL VIEWS
-- ========================================================
-- Comprehensive data quality validation including:
-- 1. Row count reconciliations
-- 2. NOT NULL expectations for keys
-- 3. Referential integrity (orphan detection)
-- 4. Distribution checks and business rule validations

-- ========================================================
-- 1. ROW COUNT RECONCILIATIONS
-- ========================================================

-- Base table row counts
CREATE OR REPLACE VIEW dq_row_counts AS
SELECT 
  'dim_users' as table_name,
  COUNT(*) as row_count,
  COUNT(DISTINCT user_id) as unique_keys
FROM dim_users
UNION ALL
SELECT 
  'fct_transactions' as table_name,
  COUNT(*) as row_count,
  COUNT(DISTINCT txn_id) as unique_keys
FROM fct_transactions
UNION ALL
SELECT 
  'fct_loans' as table_name,
  COUNT(*) as row_count,
  COUNT(DISTINCT loan_id) as unique_keys
FROM fct_loans
UNION ALL
SELECT 
  'fct_sessions' as table_name,
  COUNT(*) as row_count,
  COUNT(DISTINCT event_id) as unique_keys
FROM fct_sessions
UNION ALL
SELECT 
  'ab_assignments' as table_name,
  COUNT(*) as row_count,
  COUNT(DISTINCT assignment_id) as unique_keys
FROM ab_assignments;

-- Canonical view row counts
CREATE OR REPLACE VIEW dq_canonical_view_counts AS
SELECT 
  'v_dim_users_clean' as view_name,
  COUNT(*) as row_count,
  COUNT(DISTINCT user_id) as unique_keys
FROM v_dim_users_clean
UNION ALL
SELECT 
  'v_fct_transactions_clean' as view_name,
  COUNT(*) as row_count,
  COUNT(DISTINCT txn_id) as unique_keys
FROM v_fct_transactions_clean
UNION ALL
SELECT 
  'v_fct_loans_clean' as view_name,
  COUNT(*) as row_count,
  COUNT(DISTINCT loan_id) as unique_keys
FROM v_fct_loans_clean
UNION ALL
SELECT 
  'v_ab_assignments_clean' as view_name,
  COUNT(*) as row_count,
  COUNT(DISTINCT assignment_id) as unique_keys
FROM v_ab_assignments_clean
UNION ALL
SELECT 
  'v_fct_sessions_clean' as view_name,
  COUNT(*) as row_count,
  COUNT(DISTINCT event_id) as unique_keys
FROM v_fct_sessions_clean
UNION ALL
SELECT 
  'v_user_funnel_base' as view_name,
  COUNT(*) as row_count,
  COUNT(DISTINCT user_id) as unique_keys
FROM v_user_funnel_base
UNION ALL
SELECT 
  'v_funnel_by_segment' as view_name,
  COUNT(*) as row_count,
  0 as unique_keys  -- aggregated view
FROM v_funnel_by_segment
UNION ALL
SELECT 
  'v_user_experiment_assignments' as view_name,
  COUNT(*) as row_count,
  COUNT(DISTINCT user_id) as unique_keys
FROM v_user_experiment_assignments
UNION ALL
SELECT 
  'v_loans_with_experiments' as view_name,
  COUNT(*) as row_count,
  COUNT(DISTINCT loan_id) as unique_keys
FROM v_loans_with_experiments
UNION ALL
SELECT 
  'v_fct_transactions_for_risk' as view_name,
  COUNT(*) as row_count,
  COUNT(DISTINCT txn_id) as unique_keys
FROM v_fct_transactions_for_risk
UNION ALL
SELECT 
  'v_user_prior_loan_perf' as view_name,
  COUNT(*) as row_count,
  COUNT(DISTINCT loan_id) as unique_keys
FROM v_user_prior_loan_perf
UNION ALL
SELECT 
  'v_risk_model_base' as view_name,
  COUNT(*) as row_count,
  COUNT(DISTINCT loan_id) as unique_keys
FROM v_risk_model_base;

-- Row count reconciliation report (base tables vs clean views)
CREATE OR REPLACE VIEW dq_row_count_reconciliation AS
WITH base_counts AS (
  SELECT 
    table_name,
    row_count as base_count,
    unique_keys as base_unique_keys
  FROM dq_row_counts
),
clean_view_counts AS (
  SELECT 
    CASE 
      WHEN view_name = 'v_dim_users_clean' THEN 'dim_users'
      WHEN view_name = 'v_fct_transactions_clean' THEN 'fct_transactions'
      WHEN view_name = 'v_fct_loans_clean' THEN 'fct_loans'
      WHEN view_name = 'v_fct_sessions_clean' THEN 'fct_sessions'
      WHEN view_name = 'v_ab_assignments_clean' THEN 'ab_assignments'
    END as table_name,
    row_count as view_count,
    unique_keys as view_unique_keys
  FROM dq_canonical_view_counts
  WHERE view_name IN ('v_dim_users_clean', 'v_fct_transactions_clean', 'v_fct_loans_clean', 'v_fct_sessions_clean', 'v_ab_assignments_clean')
)
SELECT 
  COALESCE(b.table_name, v.table_name) as table_name,
  b.base_count,
  v.view_count,
  b.base_unique_keys,
  v.view_unique_keys,
  CASE 
    WHEN b.base_count = v.view_count THEN 'PASS'
    ELSE 'FAIL'
  END as row_count_check,
  CASE 
    WHEN b.base_unique_keys = v.view_unique_keys THEN 'PASS'
    ELSE 'FAIL'
  END as unique_key_check
FROM base_counts b
FULL OUTER JOIN clean_view_counts v ON b.table_name = v.table_name;

-- ========================================================
-- 2. NOT NULL EXPECTATIONS FOR KEYS
-- ========================================================

CREATE OR REPLACE VIEW dq_null_key_checks AS
-- Users table key checks
SELECT 
  'v_dim_users_clean' as table_name,
  'user_id' as key_column,
  COUNT(*) as total_rows,
  COUNT(*) - COUNT(user_id) as null_count,
  CASE WHEN COUNT(*) - COUNT(user_id) = 0 THEN 'PASS' ELSE 'FAIL' END as null_check
FROM v_dim_users_clean
UNION ALL
-- Transactions table key checks
SELECT 
  'v_fct_transactions_clean' as table_name,
  'txn_id' as key_column,
  COUNT(*) as total_rows,
  COUNT(*) - COUNT(txn_id) as null_count,
  CASE WHEN COUNT(*) - COUNT(txn_id) = 0 THEN 'PASS' ELSE 'FAIL' END as null_check
FROM v_fct_transactions_clean
UNION ALL
SELECT 
  'v_fct_transactions_clean' as table_name,
  'user_id' as key_column,
  COUNT(*) as total_rows,
  COUNT(*) - COUNT(user_id) as null_count,
  CASE WHEN COUNT(*) - COUNT(user_id) = 0 THEN 'PASS' ELSE 'FAIL' END as null_check
FROM v_fct_transactions_clean
UNION ALL
-- Loans table key checks
SELECT 
  'v_fct_loans_clean' as table_name,
  'loan_id' as key_column,
  COUNT(*) as total_rows,
  COUNT(*) - COUNT(loan_id) as null_count,
  CASE WHEN COUNT(*) - COUNT(loan_id) = 0 THEN 'PASS' ELSE 'FAIL' END as null_check
FROM v_fct_loans_clean
UNION ALL
SELECT 
  'v_fct_loans_clean' as table_name,
  'user_id' as key_column,
  COUNT(*) as total_rows,
  COUNT(*) - COUNT(user_id) as null_count,
  CASE WHEN COUNT(*) - COUNT(user_id) = 0 THEN 'PASS' ELSE 'FAIL' END as null_check
FROM v_fct_loans_clean
UNION ALL
-- Sessions table key checks
SELECT 
  'v_fct_sessions_clean' as table_name,
  'event_id' as key_column,
  COUNT(*) as total_rows,
  COUNT(*) - COUNT(event_id) as null_count,
  CASE WHEN COUNT(*) - COUNT(event_id) = 0 THEN 'PASS' ELSE 'FAIL' END as null_check
FROM v_fct_sessions_clean
UNION ALL
SELECT 
  'v_fct_sessions_clean' as table_name,
  'user_id' as key_column,
  COUNT(*) as total_rows,
  COUNT(*) - COUNT(user_id) as null_count,
  CASE WHEN COUNT(*) - COUNT(user_id) = 0 THEN 'PASS' ELSE 'FAIL' END as null_check
FROM v_fct_sessions_clean
UNION ALL
-- A/B assignments key checks
SELECT 
  'v_ab_assignments_clean' as table_name,
  'assignment_id' as key_column,
  COUNT(*) as total_rows,
  COUNT(*) - COUNT(assignment_id) as null_count,
  CASE WHEN COUNT(*) - COUNT(assignment_id) = 0 THEN 'PASS' ELSE 'FAIL' END as null_check
FROM v_ab_assignments_clean
UNION ALL
SELECT 
  'v_ab_assignments_clean' as table_name,
  'user_id' as key_column,
  COUNT(*) as total_rows,
  COUNT(*) - COUNT(user_id) as null_count,
  CASE WHEN COUNT(*) - COUNT(user_id) = 0 THEN 'PASS' ELSE 'FAIL' END as null_check
FROM v_ab_assignments_clean;

-- ========================================================
-- 3. REFERENTIAL INTEGRITY (ORPHAN DETECTION)
-- ========================================================

CREATE OR REPLACE VIEW dq_referential_integrity AS
-- Orphaned transactions (user_id not in users table)
SELECT 
  'transactions_orphans' as check_name,
  COUNT(*) as orphan_count,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as integrity_check
FROM v_fct_transactions_clean t
LEFT JOIN v_dim_users_clean u ON t.user_id = u.user_id
WHERE u.user_id IS NULL
UNION ALL
-- Orphaned loans (user_id not in users table)
SELECT 
  'loans_orphans' as check_name,
  COUNT(*) as orphan_count,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as integrity_check
FROM v_fct_loans_clean l
LEFT JOIN v_dim_users_clean u ON l.user_id = u.user_id
WHERE u.user_id IS NULL
UNION ALL
-- Orphaned sessions (user_id not in users table)
SELECT 
  'sessions_orphans' as check_name,
  COUNT(*) as orphan_count,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as integrity_check
FROM v_fct_sessions_clean s
LEFT JOIN v_dim_users_clean u ON s.user_id = u.user_id
WHERE u.user_id IS NULL
UNION ALL
-- Orphaned A/B assignments (user_id not in users table)
SELECT 
  'ab_assignments_orphans' as check_name,
  COUNT(*) as orphan_count,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as integrity_check
FROM v_ab_assignments_clean a
LEFT JOIN v_dim_users_clean u ON a.user_id = u.user_id
WHERE u.user_id IS NULL;

-- ========================================================
-- 4. DISTRIBUTION CHECKS & ANALYTICAL VALIDATIONS
-- ========================================================

-- Transaction amount and direction validations
CREATE OR REPLACE VIEW dq_transaction_validations AS
-- Negative amounts should only be in outflows
SELECT 
  'negative_amounts_in_inflows' as check_name,
  COUNT(*) as violation_count,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as validation_check
FROM v_fct_transactions_clean
WHERE direction = 'inflow' AND amount < 0
UNION ALL
-- Positive amounts should not be in outflows (unless zero)
SELECT 
  'positive_amounts_in_outflows' as check_name,
  COUNT(*) as violation_count,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as validation_check
FROM v_fct_transactions_clean
WHERE direction = 'outflow' AND amount > 0
UNION ALL
-- Zero amounts check
SELECT 
  'zero_amounts' as check_name,
  COUNT(*) as violation_count,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARN' END as validation_check
FROM v_fct_transactions_clean
WHERE amount = 0
UNION ALL
-- Extreme amounts (> $10,000)
SELECT 
  'extreme_amounts' as check_name,
  COUNT(*) as violation_count,
  CASE WHEN COUNT(*) < (SELECT COUNT(*) * 0.01 FROM v_fct_transactions_clean) THEN 'PASS' ELSE 'WARN' END as validation_check
FROM v_fct_transactions_clean
WHERE ABS(amount) > 10000;

-- Loan amount and status validations
CREATE OR REPLACE VIEW dq_loan_validations AS
-- Loan amounts should be positive
SELECT 
  'negative_loan_amounts' as check_name,
  COUNT(*) as violation_count,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as validation_check
FROM v_fct_loans_clean
WHERE amount <= 0
UNION ALL
-- Approved loans should have approval timestamp
SELECT 
  'approved_without_timestamp' as check_name,
  COUNT(*) as violation_count,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as validation_check
FROM v_fct_loans_clean
WHERE is_approved = 1 AND approved_at_utc IS NULL
UNION ALL
-- Disbursed loans should have disbursement timestamp
SELECT 
  'disbursed_without_timestamp' as check_name,
  COUNT(*) as violation_count,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as validation_check
FROM v_fct_loans_clean
WHERE is_disbursed = 1 AND disbursed_at_utc IS NULL
UNION ALL
-- Repaid loans should have repayment timestamp
SELECT 
  'repaid_without_timestamp' as check_name,
  COUNT(*) as violation_count,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as validation_check
FROM v_fct_loans_clean
WHERE is_repaid = 1 AND repaid_at_utc IS NULL
UNION ALL
-- Loan lifecycle order validation (requested < approved < disbursed)
SELECT 
  'invalid_loan_lifecycle_order' as check_name,
  COUNT(*) as violation_count,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as validation_check
FROM v_fct_loans_clean
WHERE (approved_at_utc IS NOT NULL AND approved_at_utc < requested_at_utc)
   OR (disbursed_at_utc IS NOT NULL AND approved_at_utc IS NOT NULL AND disbursed_at_utc < approved_at_utc)
   OR (disbursed_at_utc IS NOT NULL AND disbursed_at_utc < requested_at_utc);

-- User signup and bank linking validations
CREATE OR REPLACE VIEW dq_user_validations AS
-- Bank linked timestamp should be after signup
SELECT 
  'bank_linked_before_signup' as check_name,
  COUNT(*) as violation_count,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as validation_check
FROM v_dim_users_clean
WHERE bank_linked_at_utc IS NOT NULL 
  AND bank_linked_at_utc < signup_at_utc
UNION ALL
-- Risk score should be between 0 and 1
SELECT 
  'invalid_risk_scores' as check_name,
  COUNT(*) as violation_count,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as validation_check
FROM v_dim_users_clean
WHERE baseline_risk_score < 0 OR baseline_risk_score > 1
UNION ALL
-- Days to bank connect should be non-negative
SELECT 
  'negative_days_to_bank_connect' as check_name,
  COUNT(*) as violation_count,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as validation_check
FROM v_dim_users_clean
WHERE days_taken_signup_bank_connect < 0;

-- Session and event validations
CREATE OR REPLACE VIEW dq_session_validations AS
-- Session duration should be non-negative
SELECT 
  'negative_session_duration' as check_name,
  COUNT(*) as violation_count,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as validation_check
FROM v_fct_sessions_clean
WHERE session_duration_sec < 0
UNION ALL
-- Events per session should be positive
SELECT 
  'zero_events_per_session' as check_name,
  COUNT(*) as violation_count,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARN' END as validation_check
FROM v_fct_sessions_clean
WHERE events_per_session <= 0;

-- A/B test assignment validations
CREATE OR REPLACE VIEW dq_ab_test_validations AS
-- Assignment timestamp should be reasonable (not in future, not too old)
SELECT 
  'future_assignment_dates' as check_name,
  COUNT(*) as violation_count,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as validation_check
FROM v_ab_assignments_clean
WHERE assigned_at_utc > CURRENT_TIMESTAMP
UNION ALL
-- Experiment variants should be valid
SELECT 
  'invalid_experiment_variants' as check_name,
  COUNT(*) as violation_count,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARN' END as validation_check
FROM v_ab_assignments_clean
WHERE variant_norm NOT IN ('control', 'treatment', 'variant_a', 'variant_b', 'low', 'high', 'enabled', 'disabled');

-- Distribution checks
CREATE OR REPLACE VIEW dq_distribution_checks AS
-- Check user distribution by province
SELECT 
  'user_province_distribution' as check_name,
  province,
  COUNT(*) as count,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage
FROM v_dim_users_clean
GROUP BY province
ORDER BY count DESC;

-- Risk model base data coverage
CREATE OR REPLACE VIEW dq_risk_model_coverage AS
SELECT 
  'Total approved loans' as metric,
  COUNT(*) as count
FROM v_risk_model_base
UNION ALL
SELECT 
  'Loans with transaction data' as metric,
  COUNT(*) as count
FROM v_risk_model_base
WHERE txn_info_found = 1
UNION ALL
SELECT 
  'Loans with prior loan history' as metric,
  COUNT(*) as count
FROM v_risk_model_base
WHERE prior_loan_flag = 1
UNION ALL
SELECT 
  'Default loans' as metric,
  COUNT(*) as count
FROM v_risk_model_base
WHERE is_default = 1
UNION ALL
SELECT 
  'Disbursed loans' as metric,
  COUNT(*) as count
FROM v_risk_model_base
WHERE disbursed_at_utc IS NOT NULL;

-- Funnel analysis data quality
CREATE OR REPLACE VIEW dq_funnel_completeness AS
SELECT 
  'Users with app open events' as stage,
  COUNT(*) as count
FROM v_user_funnel_base
WHERE first_app_open_ts IS NOT NULL
UNION ALL
SELECT 
  'Users who linked bank' as stage,
  COUNT(*) as count
FROM v_user_funnel_base
WHERE did_bank_link = 1
UNION ALL
SELECT 
  'Users who requested loans' as stage,
  COUNT(*) as count
FROM v_user_funnel_base
WHERE first_request_ts IS NOT NULL
UNION ALL
SELECT 
  'Users who got approved' as stage,
  COUNT(*) as count
FROM v_user_funnel_base
WHERE first_approved_ts IS NOT NULL;

-- Business rule validations
CREATE OR REPLACE VIEW dq_business_rules AS
-- Check for negative loan amounts
SELECT 
  'loans_negative_amounts' as rule_name,
  COUNT(*) as violation_count,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as rule_check
FROM v_fct_loans_clean
WHERE amount <= 0
UNION ALL
-- Check for future loan request dates
SELECT 
  'loans_future_request_dates' as rule_name,
  COUNT(*) as violation_count,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as rule_check
FROM v_fct_loans_clean
WHERE requested_at_utc > CURRENT_TIMESTAMP
UNION ALL
-- Check for disbursed loans without disbursement dates
SELECT 
  'disbursed_loans_no_disbursement_date' as rule_name,
  COUNT(*) as violation_count,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as rule_check
FROM v_fct_loans_clean
WHERE is_disbursed = 1 AND disbursed_at_utc IS NULL
UNION ALL
-- Check for repaid loans without repayment dates
SELECT 
  'repaid_loans_no_repayment_date' as rule_name,
  COUNT(*) as violation_count,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as rule_check
FROM v_fct_loans_clean
WHERE is_repaid = 1 AND repaid_at_utc IS NULL
UNION ALL
-- Check for transactions with zero amounts
SELECT 
  'transactions_zero_amounts' as rule_name,
  COUNT(*) as violation_count,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as rule_check
FROM v_fct_transactions_clean
WHERE amount = 0
UNION ALL
-- Check for users with future signup dates
SELECT 
  'users_future_signup_dates' as rule_name,
  COUNT(*) as violation_count,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as rule_check
FROM v_dim_users_clean
WHERE signup_at_utc > CURRENT_TIMESTAMP
UNION ALL
-- Check for experiment assignment integrity
SELECT 
  'experiment_assignments_future_dates' as rule_name,
  COUNT(*) as violation_count,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as rule_check
FROM v_ab_assignments_clean
WHERE assigned_at_utc > CURRENT_TIMESTAMP;

-- ========================================================
-- SUMMARY DATA QUALITY REPORT
-- ========================================================

CREATE OR REPLACE VIEW dq_summary_report AS
WITH all_checks AS (
  SELECT 'Row Count Reconciliation' as check_category, 
         row_count_check as status,
         COUNT(*) as check_count
  FROM dq_row_count_reconciliation
  GROUP BY row_count_check
  
  UNION ALL
  
  SELECT 'Unique Key Reconciliation' as check_category,
         unique_key_check as status,
         COUNT(*) as check_count
  FROM dq_row_count_reconciliation
  GROUP BY unique_key_check
  
  UNION ALL
  
  SELECT 'Null Key Checks' as check_category,
         null_check as status,
         COUNT(*) as check_count
  FROM dq_null_key_checks
  GROUP BY null_check
  
  UNION ALL
  
  SELECT 'Referential Integrity' as check_category,
         integrity_check as status,
         COUNT(*) as check_count
  FROM dq_referential_integrity
  GROUP BY integrity_check
  
  UNION ALL
  
  SELECT 'Business Rules' as check_category,
         rule_check as status,
         COUNT(*) as check_count
  FROM dq_business_rules
  GROUP BY rule_check
)
SELECT 
  check_category,
  status,
  check_count,
  CASE 
    WHEN status = 'PASS' THEN '✓'
    WHEN status = 'FAIL' THEN '✗'
    ELSE '?'
  END as status_icon
FROM all_checks
ORDER BY check_category, status;

-- Quick data quality dashboard
CREATE OR REPLACE VIEW dq_dashboard AS
SELECT 
  'CANONICAL VIEWS' as section,
  'Total Views Created' as metric,
  COUNT(*) as value
FROM dq_canonical_view_counts
UNION ALL
SELECT 
  'DATA COVERAGE' as section,
  'Risk Model Coverage %' as metric,
  ROUND(100.0 * SUM(CASE WHEN txn_info_found = 1 THEN 1 ELSE 0 END) / COUNT(*), 1) as value
FROM v_risk_model_base
UNION ALL
SELECT 
  'DATA QUALITY' as section,
  'Failed Checks' as metric,
  SUM(CASE WHEN status = 'FAIL' THEN check_count ELSE 0 END) as value
FROM dq_summary_report
UNION ALL
SELECT 
  'DATA QUALITY' as section,
  'Passed Checks' as metric,
  SUM(CASE WHEN status = 'PASS' THEN check_count ELSE 0 END) as value
FROM dq_summary_report
ORDER BY section, metric;

