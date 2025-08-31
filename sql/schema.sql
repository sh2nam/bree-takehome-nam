-- users
CREATE TABLE dim_users (
  user_id INTEGER PRIMARY KEY,
  signup_at TIMESTAMP,
  province TEXT,
  device_os TEXT,
  acquisition_channel TEXT,
  bank_linked_at TIMESTAMP,
  payroll_frequency TEXT,
  baseline_risk_score DOUBLE,
  fico_band TEXT
);

-- sessions
CREATE TABLE fct_sessions (
  event_id TEXT PRIMARY KEY,
  user_id INTEGER,
  session_id TEXT,
  ts TIMESTAMP,
  event_name TEXT,
  screen TEXT,
  properties_json TEXT
);

-- transactions
CREATE TABLE fct_transactions (
  txn_id TEXT PRIMARY KEY,
  user_id INTEGER,
  posted_date DATE,
  amount DOUBLE,
  direction TEXT,
  mcc TEXT,
  category TEXT,
  balance_after DOUBLE,
  is_payroll INTEGER
);

-- loans
CREATE TABLE fct_loans (
  loan_id TEXT PRIMARY KEY,
  user_id INTEGER,
  requested_at TIMESTAMP,
  approved_at TIMESTAMP,
  disbursed_at TIMESTAMP,
  due_date DATE,
  repaid_at TIMESTAMP,
  amount DOUBLE,
  fee DOUBLE,
  tip_amount DOUBLE,
  instant_transfer_fee DOUBLE,
  status TEXT,
  late_days INTEGER,
  chargeoff_flag INTEGER,
  autopay_enrolled INTEGER,
  principal_repaid DOUBLE,
  writeoff_amount DOUBLE,
  price_variant TEXT,
  tip_variant TEXT
);

-- assignments
CREATE TABLE ab_assignments (
  assignment_id TEXT PRIMARY KEY,
  user_id INTEGER,
  experiment_name TEXT,
  variant TEXT,
  assigned_at TIMESTAMP
);
