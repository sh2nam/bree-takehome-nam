"""DuckDB database loader for Bree case study data pipeline."""

import logging
from pathlib import Path
from typing import Dict, Optional

import duckdb
import pandas as pd

from constants import SQL_DIR, TABLE_CONFIG
from data_reader import load_csv_files

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


class DatabaseError(Exception):
    """Custom exception for database operations."""
    pass


class DuckDBLoader:
    """Handles loading CSV data into DuckDB with proper schema management."""
    
    def __init__(self, database_path: str = 'bree_case_study.db'):
        """
        Initialize DuckDB loader.
        
        Args:
            database_path: Path to database file or ':memory:' for in-memory database
        """
        self.database_path = database_path
        self.connection: Optional[duckdb.DuckDBPyConnection] = None
        
    def connect(self) -> duckdb.DuckDBPyConnection:
        """Create and return DuckDB connection."""
        if self.connection is None:
            self.connection = duckdb.connect(self.database_path)
            logger.info(f"✓ Connected to DuckDB database: {self.database_path}")
        return self.connection
    
    def execute_sql_file(self, sql_file_path: Path, description: str = "SQL script") -> None:
        """
        Execute SQL statements from a file.
        
        Args:
            sql_file_path: Path to SQL file
            description: Description for logging
            
        Raises:
            DatabaseError: If SQL execution fails
        """
        if not sql_file_path.exists():
            logger.warning(f"SQL file not found: {sql_file_path}, skipping {description}")
            return
            
        try:
            with open(sql_file_path, "r") as f:
                sql_content = f.read()
            
            # Split and execute individual statements
            statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
            
            conn = self.connect()
            for statement in statements:
                if statement:
                    conn.execute(statement)
            
            logger.info(f"✓ Executed {description}: {len(statements)} statements")
            
        except Exception as e:
            error_msg = f"Failed to execute {description}: {e}"
            logger.error(error_msg)
            raise DatabaseError(error_msg) from e
    
    def drop_existing_schema(self) -> None:
        """Drop existing database schema if drop script exists."""
        logger.info("Dropping existing schemas...")
        drop_schema_path = SQL_DIR / "drop_schema.sql"
        self.execute_sql_file(drop_schema_path, "drop schema")
    
    def create_database_schema(self) -> None:
        """Create database schema from SQL file."""
        logger.info("Creating database schema...")
        schema_path = SQL_DIR / "schema.sql"
        
        if not schema_path.exists():
            raise DatabaseError(f"Schema file not found: {schema_path}")
            
        self.execute_sql_file(schema_path, "create schema")
    
    def create_analytical_views(self) -> None:
        """Create analytical views if views script exists."""
        logger.info("Creating analytical views...")
        views_path = SQL_DIR / "canonical_views.sql"
        self.execute_sql_file(views_path, "create views")
    
    def get_connection(self) -> duckdb.DuckDBPyConnection:
        """Get the DuckDB connection for direct querying."""
        return self.connect()
    
    def load_pipeline(self) -> duckdb.DuckDBPyConnection:
        """Convenience method that runs the full pipeline and returns connection."""
        return self.load_all_data()
    
    def load_table_data(self, datasets: Dict[str, pd.DataFrame]) -> int:
        """
        Load CSV data into DuckDB tables.
        
        Args:
            datasets: Dictionary mapping CSV filenames to DataFrames
            
        Returns:
            Number of tables successfully loaded
        """
        logger.info("Loading data into tables...")
        
        conn = self.connect()
        tables_loaded = 0
        
        for table_name, config in TABLE_CONFIG.items():
            csv_filename = config["csv_file"]
            
            if csv_filename not in datasets:
                logger.error(f"CSV file {csv_filename} not found for table {table_name}")
                continue
                
            try:
                dataframe = datasets[csv_filename]
                
                # Register DataFrame temporarily for DuckDB
                conn.register("temp_dataframe", dataframe)
                
                # Insert data into table
                insert_query = f"INSERT INTO {table_name} SELECT * FROM temp_dataframe"
                conn.execute(insert_query)
                
                # Verify insertion
                count_result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
                row_count = count_result[0] if count_result else 0
                
                logger.info(f"✓ Loaded {table_name}: {row_count:,} rows from {csv_filename}")
                tables_loaded += 1
                
            except Exception as e:
                logger.error(f"✗ Failed to load {table_name}: {e}")
                continue
        
        return tables_loaded
    
    def load_all_data(self, data_directory: Optional[Path] = None) -> duckdb.DuckDBPyConnection:
        """
        Complete data loading pipeline: read CSVs, create schema, load data.
        
        Args:
            data_directory: Directory containing CSV files
            
        Returns:
            DuckDB connection with loaded data
            
        Raises:
            DatabaseError: If critical steps fail
        """
        logger.info("="*70)
        logger.info("STARTING DUCKDB DATA LOADING PIPELINE")
        logger.info("="*70)
        
        try:
            # Step 1: Load CSV files
            logger.info("Step 1: Loading CSV files...")
            datasets = load_csv_files(data_directory)
            
            if not datasets:
                raise DatabaseError("No datasets loaded from CSV files")
            
            # Step 2: Drop existing schema
            logger.info("Step 2: Dropping existing schema...")
            self.drop_existing_schema()
            
            # Step 3: Create database schema
            logger.info("Step 3: Creating database schema...")
            self.create_database_schema()
            
            # Step 4: Load table data
            logger.info("Step 4: Loading table data...")
            tables_loaded = self.load_table_data(datasets)
            
            # Step 5: Create analytical views
            logger.info("Step 5: Creating analytical views...")
            self.create_analytical_views()
            
            total_tables = len(TABLE_CONFIG)
            logger.info(f"✓ Pipeline complete! {tables_loaded}/{total_tables} tables loaded successfully")
            logger.info("="*70)
            
            return self.connect()
            
        except Exception as e:
            logger.error(f"Data loading pipeline failed: {e}")
            raise
    
    def verify_data_integrity(self) -> Dict[str, int]:
        """
        Verify data was loaded correctly by checking table row counts.
        
        Returns:
            Dictionary mapping table names to row counts
        """
        logger.info("Verifying data integrity...")
        
        conn = self.connect()
        table_counts = {}
        
        print("\n" + "="*70)
        print("DATA VERIFICATION")
        print("="*70)
        
        for table_name in TABLE_CONFIG.keys():
            try:
                result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
                count = result[0] if result else 0
                table_counts[table_name] = count
                print(f"{table_name:<25} | {count:>8,} rows")
                
            except Exception as e:
                logger.error(f"Error verifying {table_name}: {e}")
                table_counts[table_name] = -1
                print(f"{table_name:<25} | ERROR: {e}")
        
        print("="*70)
        return table_counts
    
    def show_sample_data(self, limit: int = 3) -> None:
        """
        Display sample data from each table.
        
        Args:
            limit: Number of sample rows to show per table
        """
        conn = self.connect()
        
        print(f"\n" + "="*70)
        print(f"SAMPLE DATA (first {limit} rows)")
        print("="*70)
        
        for table_name in TABLE_CONFIG.keys():
            try:
                print(f"\n{table_name}:")
                result = conn.execute(f"SELECT * FROM {table_name} LIMIT {limit}").fetchdf()
                print(result.to_string(index=False))
                
            except Exception as e:
                logger.error(f"Error querying {table_name}: {e}")
    
    def run_data_quality_checks(self) -> bool:
        """
        Run basic data quality checks on loaded tables.
        
        Returns:
            True if all checks pass, False otherwise
        """
        logger.info("Running data quality checks...")
        
        conn = self.connect()
        checks_passed = True
        
        quality_checks = [
            ("Users have valid signup dates", "SELECT COUNT(*) FROM dim_users WHERE signup_at IS NULL"),
            ("Transactions have valid amounts", "SELECT COUNT(*) FROM fct_transactions WHERE amount IS NULL"),
            ("Loans have valid user references", """
                SELECT COUNT(*) FROM fct_loans l 
                LEFT JOIN dim_users u ON l.user_id = u.user_id 
                WHERE u.user_id IS NULL
            """),
            ("Sessions have valid user references", """
                SELECT COUNT(*) FROM fct_sessions s 
                LEFT JOIN dim_users u ON s.user_id = u.user_id 
                WHERE u.user_id IS NULL
            """)
        ]
        
        for check_name, query in quality_checks:
            try:
                result = conn.execute(query).fetchone()
                invalid_count = result[0] if result else 0
                
                if invalid_count > 0:
                    logger.warning(f"✗ {check_name}: {invalid_count} invalid records")
                    checks_passed = False
                else:
                    logger.info(f"✓ {check_name}: passed")
                    
            except Exception as e:
                logger.error(f"✗ {check_name}: check failed - {e}")
                checks_passed = False
        
        return checks_passed
    
    def run_comprehensive_data_quality_checks(self) -> Dict:
        """
        Run comprehensive data quality checks using the dedicated DQ runner.
        
        Returns:
            Data quality report dictionary
        """
        from data_quality_runner import DataQualityRunner
        
        logger.info("Running comprehensive data quality validation suite...")
        
        # Use the same database path as this loader
        dq_runner = DataQualityRunner(self.database_path)
        
        try:
            # Run the full DQ suite without saving files (return report only)
            report = dq_runner.run_full_data_quality_suite(save_reports=False)
            return report
        except Exception as e:
            logger.error(f"Comprehensive data quality checks failed: {e}")
            return {"overall_status": "ERROR", "error": str(e)}


def main():
    """Main function to run the complete data loading pipeline."""
    try:
        # Initialize loader
        loader = DuckDBLoader()
        
        # Load all data
        connection = loader.load_all_data()
        
        # Verify data integrity
        table_counts = loader.verify_data_integrity()
        
        # Show sample data
        loader.show_sample_data()
        
        # Run data quality checks
        if loader.run_data_quality_checks():
            logger.info("✓ All data quality checks passed")
        else:
            logger.warning("⚠ Some data quality checks failed")
        
        # Run basic analytical queries
        logger.info("\nRunning basic analytical queries...")
        
        # User distribution by province
        result = connection.execute("""
            SELECT province, COUNT(*) as user_count 
            FROM dim_users 
            GROUP BY province 
            ORDER BY user_count DESC
            LIMIT 5
        """).fetchdf()
        print("\nTop 5 provinces by user count:")
        print(result.to_string(index=False))
        
        # Loan status distribution
        result = connection.execute("""
            SELECT status, COUNT(*) as loan_count,
                   ROUND(AVG(amount), 2) as avg_amount
            FROM fct_loans 
            GROUP BY status 
            ORDER BY loan_count DESC
        """).fetchdf()
        print("\nLoan status distribution:")
        print(result.to_string(index=False))
        
        logger.info("✓ DuckDB data loading pipeline completed successfully!")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise


if __name__ == '__main__':
    main()
