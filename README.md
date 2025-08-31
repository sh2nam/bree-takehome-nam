# Bree Analytics Case Study

## Project Overview
This project demonstrates comprehensive data science capabilities for a fintech lending platform, including data engineering, funnel analysis, A/B testing, risk modeling, and interactive dashboards. The analysis covers user acquisition, loan performance, experiment evaluation, and predictive risk modeling.

## Project Structure
```
/data           # Generated CSV datasets
/notebooks      # Exploratory data analysis and experimentation
/src            # Reusable Python modules and SQL utilities
/dashboards     # Streamlit application for interactive visualization
/sql            # SQL queries for models, metrics, and data validation
/reports        # Data quality reports and validation outputs
README.md       # Project setup and execution guide
METRICS.md      # Canonical metric definitions and SQL
EXPERIMENTS.md  # Experiment design and analysis documentation
MODELING.md     # Feature engineering, training, and evaluation details
AI_LOG.md       # AI tool usage and methodology
```

## Setup Instructions

### Prerequisites
- Python 3.8+
- Required packages (see requirements.txt)

### Installation
```bash
pip install -r requirements.txt
```

### Data Generation (Optional - CSV files included)
```bash
python src/generate_data.py
```

### Database Setup (Required)
```bash
python src/duckdb_pipeline.py
```

### Running Analysis
```bash
jupyter notebook notebooks/
```

### Dashboard
```bash
streamlit run dashboards/app.py
```

## Quick Start
1. Install dependencies: `pip install -r requirements.txt`
2. **Create database from CSV files**: `python src/duckdb_pipeline.py`
3. Run exploratory analysis in notebooks: `jupyter notebook notebooks/`
4. Launch interactive dashboard: `streamlit run dashboards/app.py`

**Note**: The CSV data files are included in the repository, but you must run the DuckDB pipeline to create the database before the notebooks will work.

## File Descriptions

### Notebooks (`/notebooks/`)

- **`2_funnel_growth_deep_dive.ipynb`** - Comprehensive funnel analysis examining user conversion rates from signup to loan repayment. Includes segment analysis, drop-off identification, and P75 benchmark calculations for growth opportunities.

- **`3_experimentation.ipynb`** - A/B testing analysis for overlapping experiments (PriceTest and TipPrompt). Covers experimental design validation, statistical significance testing, and business impact assessment.

- **`4_model_v2.ipynb`** - Risk modeling notebook featuring comprehensive feature engineering, model training with LASSO regression, and evaluation of loan default prediction performance.

### Source Code (`/src/`)

- **`constants.py`** - Configuration constants and shared parameters used across the project
- **`data_quality_runner.py`** - Automated data validation and quality checks with JSON report generation
- **`data_reader.py`** - Utilities for reading and processing CSV data files
- **`duckdb_pipeline.py`** - Main ETL pipeline that loads CSV data into DuckDB and creates canonical views
- **`generate_bree_synthetic_data.py`** - Synthetic data generator for users, sessions, transactions, loans, and experiments
- **`metrics_runner.py`** - SQL query execution engine for canonical metrics and KPI calculations
- **`test_dashboard.py`** - Test script to verify dashboard data loading and query functionality

### Dashboards (`/dashboards/`)

- **`app.py`** - Interactive Streamlit dashboard featuring funnel analysis with filters (province, device OS, acquisition channel, signup cohort) and experiment readouts (tip take-rate and revenue by variant)

## Key Decisions Made

### Data Generation
- **Modified `generate_bree_synthetic_data.py`**: Fixed datetime type conversion issues in the loan generation logic
- **Specific fix applied**:
  - **Line 192**: Removed unnecessary `datetime.fromisoformat()` call since `disbursed_at` was already a datetime object
  - **Original error**: `TypeError: fromisoformat: argument must be str` 
  - **Solution**: Changed `datetime.fromisoformat(disbursed_at)` to simply `disbursed_at` for date arithmetic

## Results Summary

### Key Findings

#### Funnel Analysis
- **Overall conversion rates**: 72% bank linking, 31% loan requests, 66% approval rate, 75% repayment rate
- **Provincial differences**: Ontario leads with highest user volume, but conversion rates vary significantly by region
- **Channel performance**: Organic and referral channels show strongest conversion, paid channels have higher acquisition costs
- **Device insights**: iOS users demonstrate higher engagement and conversion rates compared to Android

#### Experiment Results
- **Price Test (A vs B)**: Variant B shows higher revenue per loan but lower approval rates
- **Tip Prompt Test**: Persuasive variant increases tip take-rate by ~15% compared to control
- **Combined impact**: Optimal revenue achieved with Price B + Persuasive tip combination

#### Risk Modeling
- **Model performance**: AUC ~0.58, indicating limited predictive power beyond baseline risk score
- **Feature importance**: Baseline risk score dominates, engineered features add minimal value
- **Business constraints**: Unable to achieve ≤15% default rate while maintaining ≥30% approval rate
- **Profitability**: Current model insufficient for profitable lending decisions

### Data Quality
- **40 automated checks** across 12 categories, all passing
- **Data integrity**: 100% referential integrity maintained across 920K+ transactions
- **Pipeline reliability**: Robust ETL with comprehensive validation and error handling

### Interactive Dashboard
- **Real-time filtering** by province, device OS, acquisition channel, and signup cohort
- **Experiment visualization** with enhanced color granularity for better insights
- **Responsive design** with integrated funnel and experiment analysis
