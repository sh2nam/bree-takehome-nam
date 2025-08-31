"""Data loading utilities for CSV files with data quality handling."""

import logging
from pathlib import Path
from typing import Dict, Optional

import pandas as pd

from constants import DATA_DIR, DUPLICATE_HANDLING

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


class DataQualityError(Exception):
    """Custom exception for data quality issues."""
    pass


def handle_duplicates(filename: str, dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Handle duplicate records based on configuration.
    
    Args:
        filename: Name of the CSV file being processed
        dataframe: DataFrame to check for duplicates
        
    Returns:
        DataFrame with duplicates handled
        
    Raises:
        DataQualityError: If duplicates cannot be resolved
    """
    if filename not in DUPLICATE_HANDLING:
        return dataframe
    
    config = DUPLICATE_HANDLING[filename]
    id_column = config["id_column"]
    strategy = config["strategy"]
    
    if id_column not in dataframe.columns:
        logger.warning(f"ID column '{id_column}' not found in {filename}")
        return dataframe
    
    duplicate_mask = dataframe[id_column].duplicated(keep=False)
    
    if not duplicate_mask.any():
        logger.info(f"✓ No duplicates found in {filename}")
        return dataframe
    
    duplicate_count = duplicate_mask.sum()
    logger.warning(f"Found {duplicate_count} duplicate {id_column}s in {filename}")
    
    if strategy == "append_index":
        # Create unique IDs by appending row index
        dataframe.loc[duplicate_mask, id_column] = (
            dataframe.loc[duplicate_mask, id_column].astype(str) + 
            "-dup-" + 
            dataframe.loc[duplicate_mask].index.astype(str)
        )
        logger.info(f"✓ Fixed duplicates in {filename} by appending row indices")
    else:
        raise DataQualityError(f"Unknown duplicate handling strategy: {strategy}")
    
    return dataframe


def load_csv_files(data_directory: Optional[Path] = None) -> Dict[str, pd.DataFrame]:
    """
    Load all CSV files from the specified directory.
    
    Args:
        data_directory: Directory containing CSV files (defaults to DATA_DIR)
        
    Returns:
        Dictionary mapping filenames to DataFrames
        
    Raises:
        FileNotFoundError: If data directory doesn't exist
        DataQualityError: If critical data quality issues are found
    """
    if data_directory is None:
        data_directory = DATA_DIR
    
    if not data_directory.exists():
        raise FileNotFoundError(f"Data directory not found: {data_directory}")
    
    csv_files = list(data_directory.glob("*.csv"))
    
    if not csv_files:
        logger.warning(f"No CSV files found in {data_directory}")
        return {}
    
    logger.info(f"Loading {len(csv_files)} CSV files from {data_directory}")
    
    datasets = {}
    failed_files = []
    
    for csv_file in csv_files:
        filename = csv_file.name
        logger.info(f"Reading {filename}...")
        
        try:
            dataframe = pd.read_csv(csv_file)
            dataframe = handle_duplicates(filename, dataframe)
            datasets[filename] = dataframe
            
            logger.info(f"✓ Loaded {filename}: {len(dataframe):,} rows, {len(dataframe.columns)} columns")
            
        except Exception as e:
            logger.error(f"✗ Failed to load {filename}: {e}")
            failed_files.append(filename)
            continue
    
    if failed_files:
        logger.warning(f"Failed to load {len(failed_files)} files: {failed_files}")
    
    logger.info(f"Successfully loaded {len(datasets)}/{len(csv_files)} datasets")
    return datasets


def print_data_summary(datasets: Dict[str, pd.DataFrame]) -> None:
    """
    Print summary statistics for loaded datasets.
    
    Args:
        datasets: Dictionary of DataFrames from load_csv_files()
    """
    if not datasets:
        logger.warning("No datasets to summarize")
        return
    
    print("\n" + "="*70)
    print("DATA SUMMARY")
    print("="*70)
    
    total_rows = 0
    for filename, dataframe in datasets.items():
        rows, cols = dataframe.shape
        total_rows += rows
        memory_mb = dataframe.memory_usage(deep=True).sum() / 1024 / 1024
        print(f"{filename:<25} | {rows:>8,} rows | {cols:>2} cols | {memory_mb:>6.1f} MB")
    
    print("-" * 70)
    print(f"{'TOTAL':<25} | {total_rows:>8,} rows")
    print("="*70)


def validate_data_schema(datasets: Dict[str, pd.DataFrame]) -> bool:
    """
    Validate that loaded datasets have expected columns and data types.
    
    Args:
        datasets: Dictionary of DataFrames to validate
        
    Returns:
        True if all validations pass, False otherwise
    """
    # Expected schemas - could be moved to constants
    expected_schemas = {
        "users.csv": ["user_id", "signup_at", "province", "device_os"],
        "transactions.csv": ["txn_id", "user_id", "amount", "category"],
        "loans.csv": ["loan_id", "user_id", "amount", "status"],
        "sessions.csv": ["event_id", "user_id", "event_name"],
        "ab_assignments.csv": ["assignment_id", "user_id", "experiment_name"]
    }
    
    validation_passed = True
    
    for filename, expected_cols in expected_schemas.items():
        if filename not in datasets:
            logger.warning(f"Expected dataset {filename} not found")
            continue
            
        dataframe = datasets[filename]
        missing_cols = set(expected_cols) - set(dataframe.columns)
        
        if missing_cols:
            logger.error(f"Missing columns in {filename}: {missing_cols}")
            validation_passed = False
        else:
            logger.info(f"✓ Schema validation passed for {filename}")
    
    return validation_passed


if __name__ == '__main__':
    # Load and validate data
    try:
        data = load_csv_files()
        print_data_summary(data)
        
        # Validate schemas
        if validate_data_schema(data):
            logger.info("✓ All schema validations passed")
        else:
            logger.error("✗ Schema validation failed")
        
        # Show sample data
        print("\nSample data preview:")
        for filename, dataframe in data.items():
            print(f"\n{filename}:")
            print(dataframe.head(2).to_string(index=False))
            
    except Exception as e:
        logger.error(f"Data loading failed: {e}")
        raise
