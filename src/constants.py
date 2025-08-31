"""Configuration constants for the Bree data pipeline."""

import os
from pathlib import Path

# Base paths using pathlib for better cross-platform compatibility
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
SQL_DIR = PROJECT_ROOT / "sql"

# Table configuration mapping DuckDB table names to CSV files
TABLE_CONFIG = {
    "dim_users": {
        "csv_file": "users.csv",
        "description": "User dimension table with demographics and risk profiles"
    },
    "fct_sessions": {
        "csv_file": "sessions.csv", 
        "description": "Session events fact table for funnel analysis"
    },
    "fct_transactions": {
        "csv_file": "transactions.csv",
        "description": "Financial transactions fact table"
    },
    "fct_loans": {
        "csv_file": "loans.csv",
        "description": "Loan lifecycle fact table"
    },
    "ab_assignments": {
        "csv_file": "ab_assignments.csv",
        "description": "A/B test assignment table"
    }
}

# Data quality settings
DUPLICATE_HANDLING = {
    "transactions.csv": {
        "id_column": "txn_id",
        "strategy": "append_index"
    }
}
