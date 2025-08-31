#!/usr/bin/env python3
"""Test script to verify dashboard data loading works"""

import duckdb
import pandas as pd

def test_dashboard_queries():
    """Test the queries used in the dashboard"""
    conn = duckdb.connect('bree_case_study.db')
    
    # Test funnel query
    funnel_query = """
    WITH user_funnel AS (
        SELECT 
            u.user_id,
            u.province,
            u.device_os,
            u.acquisition_channel,
            u.signup_month,
            u.signup_at_utc,
            u.bank_linked_flag,
            CASE WHEN l.user_id IS NOT NULL THEN 1 ELSE 0 END AS requested_loan,
            CASE WHEN l.is_approved = 1 THEN 1 ELSE 0 END AS approved_loan,
            CASE WHEN l.is_disbursed = 1 THEN 1 ELSE 0 END AS disbursed_loan,
            CASE WHEN l.is_repaid = 1 THEN 1 ELSE 0 END AS repaid_loan,
            CASE WHEN l.is_default = 1 THEN 1 ELSE 0 END AS defaulted_loan
        FROM v_dim_users_clean u
        LEFT JOIN (
            SELECT user_id, 
                   MAX(is_approved) as is_approved,
                   MAX(is_disbursed) as is_disbursed, 
                   MAX(is_repaid) as is_repaid,
                   MAX(is_default) as is_default
            FROM v_fct_loans_clean 
            GROUP BY user_id
        ) l ON u.user_id = l.user_id
    )
    SELECT * FROM user_funnel LIMIT 5
    """
    
    # Test experiment query
    experiment_query = """
    SELECT 
        l.loan_id,
        l.user_id,
        l.amount,
        l.tip_amount,
        l.revenue,
        l.is_disbursed,
        l.status,
        e.price_test_group,
        e.tip_test_group,
        CASE WHEN l.tip_amount > 0 THEN 1 ELSE 0 END as tip_taken,
        CASE WHEN l.instant_transfer_fee > 0 THEN 1 ELSE 0 END as instant_transfer_used
    FROM v_fct_loans_clean l
    JOIN v_user_experiment_assignments e ON l.user_id = e.user_id
    WHERE l.is_disbursed = 1
    LIMIT 5
    """
    
    try:
        print("Testing funnel query...")
        funnel_df = conn.execute(funnel_query).fetchdf()
        print(f"✓ Funnel query successful: {len(funnel_df)} rows")
        print(f"Columns: {list(funnel_df.columns)}")
        
        print("\nTesting experiment query...")
        experiment_df = conn.execute(experiment_query).fetchdf()
        print(f"✓ Experiment query successful: {len(experiment_df)} rows")
        print(f"Columns: {list(experiment_df.columns)}")
        
        print("\n✓ All dashboard queries work correctly!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    
    finally:
        conn.close()

if __name__ == "__main__":
    test_dashboard_queries()
