"""
Data Quality Check Runner for Bree Case Study
Executes comprehensive data quality validations and generates reports
"""

import duckdb
import pandas as pd
from pathlib import Path
import json
from datetime import datetime
from typing import Dict, List, Tuple
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataQualityRunner:
    def __init__(self, db_path: str = "bree_case_study.db"):
        """Initialize data quality runner with database connection"""
        self.db_path = db_path
        self.conn = None
        self.results = {}
        
    def connect(self):
        """Establish database connection"""
        try:
            self.conn = duckdb.connect(self.db_path)
            # Set memory management settings to handle large queries
            self.conn.execute("PRAGMA max_temp_directory_size='20GiB'")
            self.conn.execute("PRAGMA memory_limit='8GB'")
            logger.info(f"Connected to database: {self.db_path}")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    def load_sql_script(self, script_path: str) -> str:
        """Load SQL script from file"""
        try:
            with open(script_path, 'r') as f:
                return f.read()
        except Exception as e:
            logger.error(f"Failed to load SQL script {script_path}: {e}")
            raise
    
    def execute_dq_checks(self):
        """Execute all data quality check views"""
        if not self.conn:
            self.connect()
        
        # Load and execute data quality checks SQL
        sql_script_path = Path(__file__).parent.parent / "sql" / "data_quality_checks.sql"
        logger.info(f"Loading data quality checks from: {sql_script_path}")
        
        dq_sql = self.load_sql_script(sql_script_path)
        
        try:
            # Execute the entire DQ script to create views
            self.conn.execute(dq_sql)
            logger.info("Data quality check views created successfully")
        except Exception as e:
            logger.error(f"Failed to create data quality views: {e}")
            raise
    
    def run_check_category(self, view_name: str, category_name: str) -> pd.DataFrame:
        """Run a specific category of data quality checks"""
        try:
            query = f"SELECT * FROM {view_name}"
            df = self.conn.execute(query).fetchdf()
            logger.info(f"Executed {category_name}: {len(df)} checks")
            return df
        except Exception as e:
            logger.error(f"Failed to run {category_name}: {e}")
            return pd.DataFrame()
    
    def generate_detailed_report(self) -> Dict:
        """Generate comprehensive data quality report"""
        logger.info("Generating detailed data quality report...")
        
        report = {
            "execution_timestamp": datetime.now().isoformat(),
            "database_path": self.db_path,
            "categories": {}
        }
        
        # Define check categories and their corresponding views
        check_categories = {
            "row_count_reconciliation": "dq_row_count_reconciliation",
            "null_key_checks": "dq_null_key_checks", 
            "referential_integrity": "dq_referential_integrity",
            "transaction_validations": "dq_transaction_validations",
            "loan_validations": "dq_loan_validations",
            "user_validations": "dq_user_validations",
            "session_validations": "dq_session_validations",
            "ab_test_validations": "dq_ab_test_validations",
            "risk_row_counts": "dq_risk_row_counts",
            "risk_timestamp_validations": "dq_risk_timestamp_validations",
            "risk_ratio_validations": "dq_risk_ratio_validations",
            "risk_distribution_checks": "dq_risk_distribution_checks"
        }
        
        # Execute each category
        for category, view_name in check_categories.items():
            df = self.run_check_category(view_name, category)
            if not df.empty:
                report["categories"][category] = {
                    "total_checks": len(df),
                    "failed_checks": len(df[df.iloc[:, -1] == 'FAIL']) if len(df.columns) > 0 else 0,
                    "warned_checks": len(df[df.iloc[:, -1] == 'WARN']) if len(df.columns) > 0 else 0,
                    "passed_checks": len(df[df.iloc[:, -1] == 'PASS']) if len(df.columns) > 0 else 0,
                    "details": df.to_dict('records')
                }
        
        # Get summary report with fallback
        try:
            # Try extended summary first
            summary_df = self.conn.execute("SELECT * FROM dq_summary_report_extended").fetchdf()
            report["summary"] = summary_df.to_dict('records')
            
            # Calculate overall status
            total_failed = summary_df['failed_checks'].sum()
            report["overall_status"] = "PASS" if total_failed == 0 else "FAIL"
            report["total_failed_checks"] = int(total_failed)
            report["total_check_categories"] = len(summary_df)
            
        except Exception as e:
            logger.warning(f"Extended summary failed ({e}), falling back to basic summary")
            try:
                # Fallback to original summary
                summary_df = self.conn.execute("SELECT * FROM dq_summary_report").fetchdf()
                report["summary"] = summary_df.to_dict('records')
                
                # Calculate overall status from categories
                total_failed = 0
                for category_data in report["categories"].values():
                    total_failed += category_data.get("failed_checks", 0)
                
                report["overall_status"] = "PASS" if total_failed == 0 else "FAIL"
                report["total_failed_checks"] = int(total_failed)
                report["total_check_categories"] = len(report["categories"])
                
            except Exception as e2:
                logger.error(f"Failed to generate any summary report: {e2}")
                # Generate summary from categories data
                total_failed = 0
                for category_data in report["categories"].values():
                    total_failed += category_data.get("failed_checks", 0)
                
                report["summary"] = []
                report["overall_status"] = "PASS" if total_failed == 0 else "FAIL"
                report["total_failed_checks"] = int(total_failed)
                report["total_check_categories"] = len(report["categories"])
        
        return report
    
    def print_summary_report(self, report: Dict):
        """Print a formatted summary of data quality results"""
        print("\n" + "="*80)
        print("DATA QUALITY SUMMARY REPORT")
        print("="*80)
        print(f"Execution Time: {report['execution_timestamp']}")
        print(f"Database: {report['database_path']}")
        print(f"Overall Status: {report['overall_status']}")
        print(f"Total Failed Checks: {report['total_failed_checks']}")
        print(f"Check Categories: {report['total_check_categories']}")
        
        if report.get('summary'):
            print("\nCATEGORY BREAKDOWN:")
            print("-" * 80)
            for category in report['summary']:
                status_icon = "✅" if category['category_status'] == 'PASS' else "❌"
                print(f"{status_icon} {category['check_category']:<35} | "
                      f"Failed: {category['failed_checks']:>2} | "
                      f"Total: {category['total_checks']:>2} | "
                      f"Status: {category['category_status']}")
        else:
            # Fallback: show categories from detailed report
            print("\nCATEGORY BREAKDOWN:")
            print("-" * 80)
            for category_name, category_data in report['categories'].items():
                failed = category_data.get('failed_checks', 0)
                total = category_data.get('total_checks', 0)
                status = 'PASS' if failed == 0 else 'FAIL'
                status_icon = "✅" if status == 'PASS' else "❌"
                print(f"{status_icon} {category_name.replace('_', ' ').title():<35} | "
                      f"Failed: {failed:>2} | "
                      f"Total: {total:>2} | "
                      f"Status: {status}")
        
        # Print detailed failures
        failed_details = []
        for category_name, category_data in report['categories'].items():
            for detail in category_data['details']:
                # Check if this is a failed check (different views have different column structures)
                if any(col for col in detail.values() if col == 'FAIL'):
                    failed_details.append({
                        'category': category_name,
                        'detail': detail
                    })
        
        if failed_details:
            print(f"\nFAILED CHECKS DETAILS ({len(failed_details)} total):")
            print("-" * 80)
            for failure in failed_details[:10]:  # Show first 10 failures
                print(f"❌ {failure['category']}: {failure['detail']}")
            
            if len(failed_details) > 10:
                print(f"... and {len(failed_details) - 10} more failures")
        
        print("\n" + "="*80)
    
    def save_report(self, report: Dict, output_path: str = None):
        """Save detailed report to JSON file"""
        if output_path is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            reports_dir = Path(__file__).parent.parent / "reports"
            reports_dir.mkdir(exist_ok=True)
            output_path = reports_dir / f"data_quality_report_{timestamp}.json"
        
        try:
            with open(output_path, 'w') as f:
                json.dump(report, f, indent=2, default=str)
            logger.info(f"Detailed report saved to: {output_path}")
            return output_path
        except Exception as e:
            logger.error(f"Failed to save report: {e}")
            return None
    
    def export_failed_checks_csv(self, report: Dict, output_path: str = None):
        """Export failed checks to CSV for further analysis"""
        if output_path is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            reports_dir = Path(__file__).parent.parent / "reports"
            reports_dir.mkdir(exist_ok=True)
            output_path = reports_dir / f"failed_checks_{timestamp}.csv"
        
        failed_records = []
        for category_name, category_data in report['categories'].items():
            for detail in category_data['details']:
                # Add category info to each record
                record = {'category': category_name, **detail}
                # Check if this record represents a failure
                if any(col for col in detail.values() if col == 'FAIL'):
                    failed_records.append(record)
        
        if failed_records:
            try:
                df = pd.DataFrame(failed_records)
                df.to_csv(output_path, index=False)
                logger.info(f"Failed checks exported to: {output_path}")
                return output_path
            except Exception as e:
                logger.error(f"Failed to export CSV: {e}")
                return None
        else:
            logger.info("No failed checks to export")
            return None
    
    def run_full_data_quality_suite(self, save_reports: bool = True) -> Dict:
        """Run complete data quality validation suite"""
        logger.info("Starting full data quality validation suite...")
        
        try:
            # Execute all DQ checks
            self.execute_dq_checks()
            
            # Generate comprehensive report
            report = self.generate_detailed_report()
            
            # Print summary to console
            self.print_summary_report(report)
            
            # Save reports if requested
            if save_reports:
                json_path = self.save_report(report)
                csv_path = self.export_failed_checks_csv(report)
                
                report["output_files"] = {
                    "json_report": json_path,
                    "failed_checks_csv": csv_path
                }
            
            logger.info("Data quality validation suite completed successfully")
            return report
            
        except Exception as e:
            logger.error(f"Data quality suite failed: {e}")
            raise
        finally:
            if self.conn:
                self.conn.close()
                logger.info("Database connection closed")

def main():
    """Main execution function"""
    # Initialize and run data quality checks
    dq_runner = DataQualityRunner()
    
    try:
        report = dq_runner.run_full_data_quality_suite(save_reports=True)
        
        # Return exit code based on results
        if report["overall_status"] == "PASS":
            print("\n🎉 All data quality checks passed!")
            return 0
        else:
            print(f"\n⚠️  Data quality issues detected: {report['total_failed_checks']} failed checks")
            return 1
            
    except Exception as e:
        print(f"\n💥 Data quality suite execution failed: {e}")
        return 2

if __name__ == "__main__":
    exit(main())
