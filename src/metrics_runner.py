#!/usr/bin/env python3
"""
Business Metrics Runner
Executes SQL queries from METRICS.md and displays results in a formatted way.
"""

import duckdb
import pandas as pd
from datetime import datetime
import sys
import os

def connect_db():
    """Connect to the DuckDB database."""
    try:
        conn = duckdb.connect('bree_case_study.db')
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        sys.exit(1)

def format_currency(value):
    """Format numeric values as currency."""
    if pd.isna(value):
        return "N/A"
    return f"${value:,.2f}"

def format_percentage(value):
    """Format numeric values as percentage."""
    if pd.isna(value):
        return "N/A"
    return f"{value:.2f}%"

def run_metric_query(conn, metric_name, sql_query, description=""):
    """Run a single metric query and display formatted results."""
    print(f"\n{'='*80}")
    print(f"📊 {metric_name}")
    print(f"{'='*80}")
    if description:
        print(f"📝 {description}")
        print()
    
    try:
        result = conn.execute(sql_query).fetchdf()
        print("📋 Results:")
        print(result.to_string(index=False))
        print()
        return result
    except Exception as e:
        print(f"❌ Error executing query: {e}")
        return None

def main():
    """Main function to run all business metrics."""
    print("🚀 Bree Business Metrics Dashboard")
    print(f"📅 Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    conn = connect_db()
    
    # Metric 1: D1/D7/W1 Signup-to-Request Rate
    metric_1_sql = """
    WITH user_timings AS (
      SELECT u.user_id, u.signup_at_utc AS signup_at, MIN(l.requested_at_utc) AS first_request_at
      FROM v_dim_users_clean u
      LEFT JOIN v_fct_loans_clean l USING(user_id)
      GROUP BY 1,2
    )
    SELECT
      COUNT(*) AS total_signups,
      COUNT(CASE WHEN DATEDIFF('day', signup_at, first_request_at) <= 1 THEN 1 END) AS d1_requests,
      COUNT(CASE WHEN DATEDIFF('day', signup_at, first_request_at) <= 7 THEN 1 END) AS d7_requests,
      COUNT(CASE WHEN DATEDIFF('week', signup_at, first_request_at) <= 1 THEN 1 END) AS w1_requests,
      ROUND(100.0 * COUNT(CASE WHEN DATEDIFF('day', signup_at, first_request_at) <= 1 THEN 1 END) / COUNT(*), 2) AS d1_rate_pct,
      ROUND(100.0 * COUNT(CASE WHEN DATEDIFF('day', signup_at, first_request_at) <= 7 THEN 1 END) / COUNT(*), 2) AS d7_rate_pct,
      ROUND(100.0 * COUNT(CASE WHEN DATEDIFF('week', signup_at, first_request_at) <= 1 THEN 1 END) / COUNT(*), 2) AS w1_rate_pct
    FROM user_timings;
    """
    
    run_metric_query(
        conn, 
        "1. D1/D7/W1 Signup-to-Request Rate",
        metric_1_sql,
        "Measures user activation and product-market fit"
    )
    
    # Metric 2: Bank Link Rate
    metric_2_sql = """
    SELECT 
      COUNT(*) as total_users,
      SUM(bank_linked_flag) as users_linked_bank,
      ROUND(SUM(bank_linked_flag) * 100.0 / COUNT(*), 2) as bank_link_rate_pct
    FROM v_dim_users_clean;
    """
    
    run_metric_query(
        conn,
        "2. Bank Link Rate", 
        metric_2_sql,
        "Critical onboarding step required for loan eligibility"
    )
    
    # Metric 3: Approval Rate & Disbursement Rate
    metric_3_sql = """
    SELECT 
      COUNT(*) as total_loan_requests,
      SUM(is_approved) as approved_loans,
      SUM(is_disbursed) as disbursed_loans,
      ROUND(SUM(is_approved) * 100.0 / COUNT(*), 2) as approval_rate_pct,
      ROUND(SUM(is_disbursed) * 100.0 / COUNT(*), 2) as disbursement_rate_pct,
      ROUND(SUM(is_disbursed) * 100.0 / NULLIF(SUM(is_approved), 0), 2) as approval_to_disbursement_rate_pct
    FROM v_fct_loans_clean;
    """
    
    run_metric_query(
        conn,
        "3. Approval Rate & Disbursement Rate",
        metric_3_sql,
        "Measures underwriting efficiency and operational performance"
    )
    
    # Metric 4: Repayment Rate & Default Rate
    metric_4_sql = """
    SELECT 
      COUNT(*) as total_disbursed_loans,
      SUM(is_repaid) as repaid_loans,
      SUM(is_default) as defaulted_loans,
      ROUND(SUM(is_repaid) * 100.0 / COUNT(*), 2) as repayment_rate_pct,
      ROUND(SUM(is_default) * 100.0 / COUNT(*), 2) as default_rate_pct
    FROM v_fct_loans_clean
    WHERE is_disbursed = 1;
    """
    
    run_metric_query(
        conn,
        "4. Repayment Rate & Default Rate",
        metric_4_sql,
        "Core risk metrics for loan portfolio health"
    )
    
    # Metric 5: Average Loan Amount & Take-Rate
    metric_5_sql = """
    SELECT 
      COUNT(*) as total_loans,
      ROUND(AVG(amount), 2) as avg_loan_amount,
      ROUND(AVG(revenue), 2) as avg_take,
      ROUND(AVG(revenue_to_loan) * 100, 2) as avg_take_rate_pct
    FROM v_fct_loans_clean;
    """
    
    run_metric_query(
        conn,
        "5. Average Loan Amount & Total Take-Rate Per Loan",
        metric_5_sql,
        "Unit economics and revenue per transaction"
    )
    
    # Metric 6: Instant Transfer Adoption Rate
    metric_6_sql = """
    SELECT 
      COUNT(*) as disbursed_loans,
      SUM(CASE WHEN instant_transfer_fee > 0 THEN 1 ELSE 0 END) as instant_transfer_users,
      ROUND(SUM(CASE WHEN instant_transfer_fee > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as instant_adoption_rate_pct
    FROM v_fct_loans_clean
    WHERE is_disbursed = 1;
    """
    
    run_metric_query(
        conn,
        "6. Instant Transfer Adoption Rate",
        metric_6_sql,
        "Premium feature adoption and additional revenue stream"
    )
    
    # Metric 7: Revenue Per Disbursed Loan
    metric_7_sql = """
    SELECT 
      COUNT(*) as disbursed_loans,
      ROUND(AVG(revenue), 2) as avg_revenue_per_loan,
      ROUND(SUM(revenue), 2) as total_revenue,
      ROUND(AVG(fee), 2) as avg_fee,
      ROUND(AVG(tip_amount), 2) as avg_tip,
      ROUND(AVG(instant_transfer_fee), 2) as avg_instant_fee
    FROM v_fct_loans_clean
    WHERE is_disbursed = 1;
    """
    
    run_metric_query(
        conn,
        "7. Revenue Per Disbursed Loan (Exclude Principal)",
        metric_7_sql,
        "Core revenue metric excluding principal (which is repaid)"
    )
    
    # Metric 8: Late Payment Rate (NPS Proxy)
    metric_8_sql = """
    SELECT 
      COUNT(*) as repaid_loans,
      SUM(CASE WHEN late_days > 7 THEN 1 ELSE 0 END) as late_repaid_loans,
      ROUND(SUM(CASE WHEN late_days > 7 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as late_payment_rate_pct,
      ROUND(AVG(late_days), 1) as avg_late_days
    FROM v_fct_loans_clean
    WHERE is_repaid = 1;
    """
    
    run_metric_query(
        conn,
        "8. Guardrail: Basic NPS Proxy - Late Payment Rate",
        metric_8_sql,
        "Customer satisfaction guardrail - high late payment rates may indicate customer stress"
    )
    
    # Summary Dashboard
    print(f"\n{'='*80}")
    print("📊 SUMMARY DASHBOARD")
    print(f"{'='*80}")
    
    # Get key metrics for summary
    try:
        # D7 rate
        d7_result = conn.execute(metric_1_sql).fetchdf()
        d7_rate = d7_result['d7_rate_pct'].iloc[0] if not d7_result.empty else 0
        
        # Bank link rate
        bank_result = conn.execute(metric_2_sql).fetchdf()
        bank_rate = bank_result['bank_link_rate_pct'].iloc[0] if not bank_result.empty else 0
        
        # Approval rate
        approval_result = conn.execute(metric_3_sql).fetchdf()
        approval_rate = approval_result['approval_rate_pct'].iloc[0] if not approval_result.empty else 0
        
        # Repayment rate
        repay_result = conn.execute(metric_4_sql).fetchdf()
        repay_rate = repay_result['repayment_rate_pct'].iloc[0] if not repay_result.empty else 0
        default_rate = repay_result['default_rate_pct'].iloc[0] if not repay_result.empty else 0
        
        # Revenue metrics
        revenue_result = conn.execute(metric_7_sql).fetchdf()
        avg_revenue = revenue_result['avg_revenue_per_loan'].iloc[0] if not revenue_result.empty else 0
        
        # Average loan amount
        loan_amount_result = conn.execute(metric_5_sql).fetchdf()
        avg_loan_amount = loan_amount_result['avg_loan_amount'].iloc[0] if not loan_amount_result.empty else 0
        
        # Instant transfer
        instant_result = conn.execute(metric_6_sql).fetchdf()
        instant_rate = instant_result['instant_adoption_rate_pct'].iloc[0] if not instant_result.empty else 0
        
        # Late payment
        late_result = conn.execute(metric_8_sql).fetchdf()
        late_rate = late_result['late_payment_rate_pct'].iloc[0] if not late_result.empty else 0
        
        def get_status(metric, value, thresholds):
            """Get status emoji based on thresholds."""
            if value >= thresholds.get('good', 0):
                return '🟢 Good'
            elif value >= thresholds.get('monitor', 0):
                return '🟡 Monitor'
            else:
                return '🔴 Needs Attention'
        
        print(f"{'Metric':<35} {'Value':<15} {'Status'}")
        print("-" * 65)
        print(f"{'Activation (D7 signup-to-request)':<35} {d7_rate:.2f}%{'':<9} {get_status('d7', d7_rate, {'good': 25, 'monitor': 15})}")
        print(f"{'Bank Link Rate':<35} {bank_rate:.2f}%{'':<9} {get_status('bank', bank_rate, {'good': 70, 'monitor': 60})}")
        print(f"{'Approval Rate':<35} {approval_rate:.2f}%{'':<9} {get_status('approval', approval_rate, {'good': 60, 'monitor': 50})}")
        print(f"{'Repayment Rate':<35} {repay_rate:.2f}%{'':<9} {get_status('repay', repay_rate, {'good': 70, 'monitor': 60})}")
        print(f"{'Default Rate':<35} {default_rate:.2f}%{'':<9} {'🟡 Monitor' if default_rate < 30 else '🔴 High'}")
        print(f"{'Average Loan Amount':<35} ${avg_loan_amount:.2f}{'':<9} ℹ️ Baseline")
        print(f"{'Revenue per Loan':<35} ${avg_revenue:.2f}{'':<9} ℹ️ Baseline")
        print(f"{'Instant Transfer Adoption':<35} {instant_rate:.2f}%{'':<9} {'🟢 Good' if instant_rate > 20 else '🟡 Opportunity'}")
        print(f"{'Late Payment Rate (NPS Proxy)':<35} {late_rate:.2f}%{'':<9} {'🟢 Excellent' if late_rate < 5 else '🟡 Monitor'}")
        
    except Exception as e:
        print(f"❌ Error generating summary: {e}")
    
    # Close connection
    conn.close()
    print(f"\n{'='*80}")
    print("✅ Metrics analysis complete!")
    print(f"{'='*80}")

if __name__ == "__main__":
    main()
