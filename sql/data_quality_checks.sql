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

-- Canonical view row counts (should match base tables)
CREATE OR REPLACE VIEW dq_view_row_counts AS
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
  'v_fct_sessions_clean' as view_name,
  COUNT(*) as row_count,
  COUNT(DISTINCT event_id) as unique_keys
FROM v_fct_sessions_clean
UNION ALL
SELECT 
  'v_ab_assignments_clean' as view_name,
  COUNT(*) as row_count,
  COUNT(DISTINCT assignment_id) as unique_keys
FROM v_ab_assignments_clean;

-- Row count reconciliation report
CREATE OR REPLACE VIEW dq_row_count_reconciliation AS
WITH base_counts AS (
  SELECT 
    REPLACE(table_name, 'fct_', '') as entity,
    REPLACE(table_name, 'dim_', '') as entity_clean,
    row_count as base_count,
    unique_keys as base_unique_keys
  FROM dq_row_counts
),
view_counts AS (
  SELECT 
    REPLACE(REPLACE(view_name, 'v_fct_', ''), '_clean', '') as entity,
    REPLACE(REPLACE(REPLACE(view_name, 'v_dim_', ''), 'v_', ''), '_clean', '') as entity_clean,
    row_count as view_count,
    unique_keys as view_unique_keys
  FROM dq_view_row_counts
)
SELECT 
  COALESCE(b.entity_clean, v.entity_clean) as entity,
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
FULL OUTER JOIN view_counts v ON b.entity_clean = v.entity_clean;

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
-- 4. DISTRIBUTION CHECKS & BUSINESS RULE VALIDATIONS
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

-- ========================================================
-- 5. SUMMARY DATA QUALITY REPORT
-- ========================================================

CREATE OR REPLACE VIEW dq_summary_report AS
SELECT 
  'Row Count Reconciliation' as check_category,
  SUM(CASE WHEN row_count_check = 'FAIL' OR unique_key_check = 'FAIL' THEN 1 ELSE 0 END) as failed_checks,
  COUNT(*) as total_checks,
  CASE WHEN SUM(CASE WHEN row_count_check = 'FAIL' OR unique_key_check = 'FAIL' THEN 1 ELSE 0 END) = 0 
       THEN 'PASS' ELSE 'FAIL' END as category_status
FROM dq_row_count_reconciliation
UNION ALL
SELECT 
  'NULL Key Checks' as check_category,
  SUM(CASE WHEN null_check = 'FAIL' THEN 1 ELSE 0 END) as failed_checks,
  COUNT(*) as total_checks,
  CASE WHEN SUM(CASE WHEN null_check = 'FAIL' THEN 1 ELSE 0 END) = 0 
       THEN 'PASS' ELSE 'FAIL' END as category_status
FROM dq_null_key_checks
UNION ALL
SELECT 
  'Referential Integrity' as check_category,
  SUM(CASE WHEN integrity_check = 'FAIL' THEN 1 ELSE 0 END) as failed_checks,
  COUNT(*) as total_checks,
  CASE WHEN SUM(CASE WHEN integrity_check = 'FAIL' THEN 1 ELSE 0 END) = 0 
       THEN 'PASS' ELSE 'FAIL' END as category_status
FROM dq_referential_integrity
UNION ALL
SELECT 
  'Transaction Validations' as check_category,
  SUM(CASE WHEN validation_check = 'FAIL' THEN 1 ELSE 0 END) as failed_checks,
  COUNT(*) as total_checks,
  CASE WHEN SUM(CASE WHEN validation_check = 'FAIL' THEN 1 ELSE 0 END) = 0 
       THEN 'PASS' ELSE 'FAIL' END as category_status
FROM dq_transaction_validations
UNION ALL
SELECT 
  'Loan Validations' as check_category,
  SUM(CASE WHEN validation_check = 'FAIL' THEN 1 ELSE 0 END) as failed_checks,
  COUNT(*) as total_checks,
  CASE WHEN SUM(CASE WHEN validation_check = 'FAIL' THEN 1 ELSE 0 END) = 0 
       THEN 'PASS' ELSE 'FAIL' END as category_status
FROM dq_loan_validations
UNION ALL
SELECT 
  'User Validations' as check_category,
  SUM(CASE WHEN validation_check = 'FAIL' THEN 1 ELSE 0 END) as failed_checks,
  COUNT(*) as total_checks,
  CASE WHEN SUM(CASE WHEN validation_check = 'FAIL' THEN 1 ELSE 0 END) = 0 
       THEN 'PASS' ELSE 'FAIL' END as category_status
FROM dq_user_validations
UNION ALL
SELECT 
  'Session Validations' as check_category,
  SUM(CASE WHEN validation_check = 'FAIL' THEN 1 ELSE 0 END) as failed_checks,
  COUNT(*) as total_checks,
  CASE WHEN SUM(CASE WHEN validation_check = 'FAIL' THEN 1 ELSE 0 END) = 0 
       THEN 'PASS' ELSE 'FAIL' END as category_status
FROM dq_session_validations
UNION ALL
SELECT 
  'A/B Test Validations' as check_category,
  SUM(CASE WHEN validation_check = 'FAIL' THEN 1 ELSE 0 END) as failed_checks,
  COUNT(*) as total_checks,
  CASE WHEN SUM(CASE WHEN validation_check = 'FAIL' THEN 1 ELSE 0 END) = 0 
       THEN 'PASS' ELSE 'FAIL' END as category_status
FROM dq_ab_test_validations;

-- ========================================================
-- 6. RISK MODEL VALIDATIONS
-- ========================================================

-- Risk model row count reconciliation
CREATE OR REPLACE VIEW dq_risk_row_counts AS
SELECT 
  'canonical_vs_approved_loans' as check_name,
  ABS((SELECT COUNT(*) FROM v_canonical_risk_model) - (SELECT COUNT(*) FROM v_fct_loans_clean WHERE is_approved = 1)) as violation_count,
  CASE WHEN ABS((SELECT COUNT(*) FROM v_canonical_risk_model) - (SELECT COUNT(*) FROM v_fct_loans_clean WHERE is_approved = 1)) = 0 
       THEN 'PASS' ELSE 'FAIL' END as validation_check
UNION ALL
SELECT 
  'labels_vs_disbursed_loans' as check_name,
  ABS((SELECT COUNT(*) FROM v_default_label_30d) - (SELECT COUNT(*) FROM v_fct_loans_clean WHERE is_disbursed = 1)) as violation_count,
  CASE WHEN ABS((SELECT COUNT(*) FROM v_default_label_30d) - (SELECT COUNT(*) FROM v_fct_loans_clean WHERE is_disbursed = 1)) = 0 
       THEN 'PASS' ELSE 'FAIL' END as validation_check;

-- Risk model timestamp validations
CREATE OR REPLACE VIEW dq_risk_timestamp_validations AS
SELECT 
  'bad_approval_ordering' as check_name,
  COUNT(*) as violation_count,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as validation_check
FROM v_fct_loans_clean
WHERE requested_at_utc IS NOT NULL
  AND approved_at_utc IS NOT NULL
  AND requested_at_utc > approved_at_utc
UNION ALL
SELECT 
  'bad_disburse_ordering' as check_name,
  COUNT(*) as violation_count,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as validation_check
FROM v_fct_loans_clean
WHERE approved_at_utc IS NOT NULL
  AND disbursed_at_utc IS NOT NULL
  AND approved_at_utc > disbursed_at_utc
UNION ALL
SELECT 
  'negative_days_since_payroll' as check_name,
  COUNT(*) as violation_count,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as validation_check
FROM v_canonical_risk_model
WHERE days_since_last_payroll < 0;

-- Risk model ratio validations
CREATE OR REPLACE VIEW dq_risk_ratio_validations AS
SELECT 
  'invalid_spending_ratios' as check_name,
  (SUM(CASE WHEN rent_share_30d < 0 OR rent_share_30d > 1 THEN 1 ELSE 0 END) +
   SUM(CASE WHEN essentials_share_14d < 0 OR essentials_share_14d > 1 THEN 1 ELSE 0 END) +
   SUM(CASE WHEN discretionary_share_14d < 0 OR discretionary_share_14d > 1 THEN 1 ELSE 0 END)) as violation_count,
  CASE WHEN (SUM(CASE WHEN rent_share_30d < 0 OR rent_share_30d > 1 THEN 1 ELSE 0 END) +
             SUM(CASE WHEN essentials_share_14d < 0 OR essentials_share_14d > 1 THEN 1 ELSE 0 END) +
             SUM(CASE WHEN discretionary_share_14d < 0 OR discretionary_share_14d > 1 THEN 1 ELSE 0 END)) = 0 
       THEN 'PASS' ELSE 'FAIL' END as validation_check
FROM v_canonical_risk_model;

-- Risk model distribution checks
CREATE OR REPLACE VIEW dq_risk_distribution_checks AS
SELECT 
  'extreme_outflow_to_inflow' as check_name,
  COUNT(*) as violation_count,
  CASE WHEN COUNT(*) < 10 THEN 'PASS' ELSE 'WARN' END as validation_check
FROM v_canonical_risk_model
WHERE outflow_to_inflow_14d > 5 OR outflow_to_inflow_30d > 5
UNION ALL
SELECT 
  'negative_volatility' as check_name,
  COUNT(*) as violation_count,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as validation_check
FROM v_canonical_risk_model
WHERE inflow_volatility_14d < 0 OR inflow_volatility_30d < 0
UNION ALL
SELECT 
  'non_binary_default_labels' as check_name,
  COUNT(*) as violation_count,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as validation_check
FROM v_default_label_30d
WHERE default_30d NOT IN (0,1);

-- ========================================================
-- 7. UPDATED SUMMARY DATA QUALITY REPORT
-- ========================================================

-- Use original summary report to avoid column name issues
CREATE OR REPLACE VIEW dq_summary_report_extended AS
SELECT * FROM dq_summary_report
UNION ALL
SELECT 
  'Risk Model Row Counts' as check_category,
  SUM(CASE WHEN validation_check = 'FAIL' THEN 1 ELSE 0 END) as failed_checks,
  COUNT(*) as total_checks,
  CASE WHEN SUM(CASE WHEN validation_check = 'FAIL' THEN 1 ELSE 0 END) = 0 
       THEN 'PASS' ELSE 'FAIL' END as category_status
FROM dq_risk_row_counts
UNION ALL
SELECT 
  'Risk Model Timestamps' as check_category,
  SUM(CASE WHEN validation_check = 'FAIL' THEN 1 ELSE 0 END) as failed_checks,
  COUNT(*) as total_checks,
  CASE WHEN SUM(CASE WHEN validation_check = 'FAIL' THEN 1 ELSE 0 END) = 0 
       THEN 'PASS' ELSE 'FAIL' END as category_status
FROM dq_risk_timestamp_validations
UNION ALL
SELECT 
  'Risk Model Ratios' as check_category,
  SUM(CASE WHEN validation_check = 'FAIL' THEN 1 ELSE 0 END) as failed_checks,
  COUNT(*) as total_checks,
  CASE WHEN SUM(CASE WHEN validation_check = 'FAIL' THEN 1 ELSE 0 END) = 0 
       THEN 'PASS' ELSE 'FAIL' END as category_status
FROM dq_risk_ratio_validations
UNION ALL
SELECT 
  'Risk Model Distributions' as check_category,
  SUM(CASE WHEN validation_check = 'FAIL' THEN 1 ELSE 0 END) as failed_checks,
  COUNT(*) as total_checks,
  CASE WHEN SUM(CASE WHEN validation_check = 'FAIL' THEN 1 ELSE 0 END) = 0 
       THEN 'PASS' ELSE 'FAIL' END as category_status
FROM dq_risk_distribution_checks;
