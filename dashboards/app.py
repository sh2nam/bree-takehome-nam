#!/usr/bin/env python3
"""
Bree Analytics Dashboard
A Streamlit app for funnel analysis and experiment readouts
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import duckdb
import numpy as np
from datetime import datetime, timedelta

# Page config
st.set_page_config(
    page_title="Bree Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

@st.cache_data
def load_data():
    """Load data from DuckDB database"""
    conn = duckdb.connect('bree_case_study.db')
    
    # Funnel data
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
    SELECT * FROM user_funnel
    """
    
    # Experiment data
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
    """
    
    funnel_df = conn.execute(funnel_query).fetchdf()
    experiment_df = conn.execute(experiment_query).fetchdf()
    
    conn.close()
    return funnel_df, experiment_df

def create_funnel_chart(df, title="User Funnel"):
    """Create funnel visualization"""
    funnel_metrics = {
        'Signups': len(df),
        'Bank Linked': df['bank_linked_flag'].sum(),
        'Loan Requested': df['requested_loan'].sum(),
        'Loan Approved': df['approved_loan'].sum(),
        'Loan Disbursed': df['disbursed_loan'].sum(),
        'Loan Repaid': df['repaid_loan'].sum()
    }
    
    # Calculate conversion rates
    stages = list(funnel_metrics.keys())
    values = list(funnel_metrics.values())
    
    # Create funnel chart
    fig = go.Figure(go.Funnel(
        y = stages,
        x = values,
        textposition = "inside",
        textinfo = "value+percent initial",
        opacity = 0.65,
        marker = {"color": ["deepskyblue", "lightsalmon", "tan", "teal", "silver", "gold"]},
        connector = {"line": {"color": "royalblue", "dash": "dot", "width": 3}}
    ))
    
    fig.update_layout(
        title=title,
        height=500,
        font_size=12
    )
    
    return fig, funnel_metrics

def main():
    st.title("📊 Bree Analytics Dashboard")
    st.markdown("---")
    
    # Load data
    with st.spinner("Loading data..."):
        funnel_df, experiment_df = load_data()
    
    # Sidebar filters
    st.sidebar.header("🔍 Filters")
    
    # Province filter
    provinces = ['All'] + sorted(funnel_df['province'].unique().tolist())
    selected_province = st.sidebar.selectbox("Province", provinces)
    
    # Device OS filter
    devices = ['All'] + sorted(funnel_df['device_os'].unique().tolist())
    selected_device = st.sidebar.selectbox("Device OS", devices)
    
    # Acquisition channel filter
    channels = ['All'] + sorted(funnel_df['acquisition_channel'].unique().tolist())
    selected_channel = st.sidebar.selectbox("Acquisition Channel", channels)
    
    # Signup cohort filter
    cohorts = ['All'] + sorted(funnel_df['signup_month'].unique().tolist())
    selected_cohort = st.sidebar.selectbox("Signup Cohort", cohorts)
    
    # Apply filters to funnel data
    filtered_df = funnel_df.copy()
    
    if selected_province != 'All':
        filtered_df = filtered_df[filtered_df['province'] == selected_province]
    if selected_device != 'All':
        filtered_df = filtered_df[filtered_df['device_os'] == selected_device]
    if selected_channel != 'All':
        filtered_df = filtered_df[filtered_df['acquisition_channel'] == selected_channel]
    if selected_cohort != 'All':
        filtered_df = filtered_df[filtered_df['signup_month'] == selected_cohort]
    
    # Apply same filters to experiment data
    filtered_experiment_df = experiment_df.copy()
    
    # Get user IDs from filtered funnel data to filter experiments
    filtered_user_ids = set(filtered_df['user_id'].tolist())
    filtered_experiment_df = filtered_experiment_df[filtered_experiment_df['user_id'].isin(filtered_user_ids)]
    
    # Main dashboard
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.header("🔄 User Funnel Analysis")
        
        if len(filtered_df) > 0:
            funnel_fig, funnel_metrics = create_funnel_chart(filtered_df, "Filtered User Funnel")
            st.plotly_chart(funnel_fig, use_container_width=True)
        else:
            st.warning("No data available for selected filters")
    
    with col2:
        st.header("📈 Key Metrics")
        
        if len(filtered_df) > 0:
            # Calculate conversion rates
            total_users = len(filtered_df)
            bank_link_rate = (filtered_df['bank_linked_flag'].sum() / total_users * 100) if total_users > 0 else 0
            request_rate = (filtered_df['requested_loan'].sum() / total_users * 100) if total_users > 0 else 0
            approval_rate = (filtered_df['approved_loan'].sum() / filtered_df['requested_loan'].sum() * 100) if filtered_df['requested_loan'].sum() > 0 else 0
            
            st.metric("Total Users", f"{total_users:,}")
            st.metric("Bank Link Rate", f"{bank_link_rate:.1f}%")
            st.metric("Request Rate", f"{request_rate:.1f}%")
            st.metric("Approval Rate", f"{approval_rate:.1f}%")
    
    st.markdown("---")
    
    # Experiment Analysis Section
    st.header("🧪 Experiment Analysis")
    
    # Summary statistics
    st.subheader("Statistics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_experiments = len(filtered_experiment_df)
        st.metric("Total Experiment Loans", f"{total_experiments:,}")
    
    with col2:
        avg_revenue = filtered_experiment_df['revenue'].mean() if len(filtered_experiment_df) > 0 else 0
        st.metric("Average Revenue per Loan", f"${avg_revenue:.2f}")
    
    with col3:
        overall_tip_rate = (filtered_experiment_df['tip_taken'].sum() / len(filtered_experiment_df) * 100) if len(filtered_experiment_df) > 0 else 0
        st.metric("Overall Tip Take Rate", f"{overall_tip_rate:.1f}%")
    
    with col4:
        avg_loan_amount = filtered_experiment_df['amount'].mean() if len(filtered_experiment_df) > 0 else 0
        st.metric("Average Loan Amount", f"${avg_loan_amount:.2f}")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("💰 Tip Test Results")
        
        # Tip take rate by variant
        tip_analysis = filtered_experiment_df.groupby('tip_test_group').agg({
            'loan_id': 'count',
            'tip_taken': 'sum',
            'tip_amount': 'mean',
            'revenue': 'mean'
        }).reset_index()
        
        tip_analysis['tip_take_rate'] = (tip_analysis['tip_taken'] / tip_analysis['loan_id'] * 100)
        tip_analysis.columns = ['Variant', 'Total Loans', 'Tips Taken', 'Avg Tip Amount', 'Avg Revenue', 'Tip Take Rate %']
        
        st.dataframe(tip_analysis, use_container_width=True)
        
        # Tip take rate chart
        fig_tip = px.bar(
            tip_analysis, 
            x='Variant', 
            y='Tip Take Rate %',
            title="Tip Take Rate by Variant",
            color='Variant'
        )
        st.plotly_chart(fig_tip, use_container_width=True)
    
    with col2:
        st.subheader("💵 Price Test Results")
        
        # Revenue analysis by price variant
        price_analysis = filtered_experiment_df.groupby('price_test_group').agg({
            'loan_id': 'count',
            'revenue': ['mean', 'sum'],
            'amount': 'mean'
        }).reset_index()
        
        price_analysis.columns = ['Variant', 'Total Loans', 'Avg Revenue', 'Total Revenue', 'Avg Loan Amount']
        
        st.dataframe(price_analysis, use_container_width=True)
        
        # Revenue per loan chart
        fig_price = px.bar(
            price_analysis, 
            x='Variant', 
            y='Avg Revenue',
            title="Average Revenue per Loan by Price Variant",
            color='Variant'
        )
        st.plotly_chart(fig_price, use_container_width=True)
    
    # Combined experiment view
    st.subheader("📊 Combined Experiment Matrix")
    
    # Create matrix of tip variant vs price variant
    matrix_data = filtered_experiment_df.groupby(['tip_test_group', 'price_test_group']).agg({
        'loan_id': 'count',
        'revenue': 'mean',
        'tip_taken': lambda x: (x.sum() / len(x) * 100)
    }).reset_index()
    
    matrix_pivot = matrix_data.pivot(index='tip_test_group', columns='price_test_group', values='revenue')
    
    # Calculate range for better color scaling
    min_val = matrix_pivot.min().min()
    max_val = matrix_pivot.max().max()
    mid_val = (min_val + max_val) / 2
    
    fig_matrix = px.imshow(
        matrix_pivot,
        title="Average Revenue by Tip Variant (rows) × Price Variant (columns)",
        color_continuous_scale="RdYlGn",
        aspect="auto",
        text_auto=True,
        zmin=min_val,
        zmax=max_val
    )
    
    # Update colorbar and layout for better granularity
    fig_matrix.update_layout(
        coloraxis_colorbar=dict(
            title="Revenue ($)",
            tickformat=".3f",
            nticks=10
        )
    )
    
    # Add value annotations for clarity
    fig_matrix.update_traces(
        texttemplate="%{z:.3f}",
        textfont={"size": 14, "color": "black"},
        textfont_color="black"
    )
    
    st.plotly_chart(fig_matrix, use_container_width=True)

if __name__ == "__main__":
    main()
