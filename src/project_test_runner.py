#!/usr/bin/env python3
"""
Project Test Runner
Validates all key components of the Bree case study project to ensure nothing breaks.
"""

import os
import sys
import subprocess
import duckdb
import pandas as pd
from datetime import datetime
import traceback

class ProjectTester:
    def __init__(self):
        self.project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.results = []
        self.total_tests = 0
        self.passed_tests = 0
        
    def log_test(self, test_name, status, message="", details=""):
        """Log test results."""
        self.total_tests += 1
        if status == "PASS":
            self.passed_tests += 1
            print(f"✅ {test_name}")
        elif status == "FAIL":
            print(f"❌ {test_name}: {message}")
            if details:
                print(f"   Details: {details}")
        elif status == "WARN":
            print(f"⚠️  {test_name}: {message}")
        
        self.results.append({
            'test': test_name,
            'status': status,
            'message': message,
            'details': details
        })
    
    def test_database_connection(self):
        """Test DuckDB database connection and basic queries."""
        try:
            conn = duckdb.connect(os.path.join(self.project_root, 'bree_case_study.db'))
            
            # Test basic connection
            result = conn.execute("SELECT 1 as test").fetchone()
            if result[0] == 1:
                self.log_test("Database Connection", "PASS")
            else:
                self.log_test("Database Connection", "FAIL", "Unexpected result from test query")
                return False
            
            conn.close()
            return True
        except Exception as e:
            self.log_test("Database Connection", "FAIL", str(e))
            return False
    
    def test_canonical_views(self):
        """Test that all canonical views exist and are queryable."""
        expected_views = [
            'v_dim_users_clean',
            'v_fct_transactions_clean', 
            'v_fct_loans_clean',
            'v_fct_sessions_clean',
            'v_ab_assignments_clean',
            'v_user_funnel_base',
            'v_funnel_by_segment',
            'v_user_experiment_assignments',
            'v_loans_with_experiments'
        ]
        
        try:
            conn = duckdb.connect(os.path.join(self.project_root, 'bree_case_study.db'))
            
            for view in expected_views:
                try:
                    result = conn.execute(f"SELECT COUNT(*) FROM {view}").fetchone()
                    if result[0] >= 0:  # Any count is fine, including 0
                        self.log_test(f"View: {view}", "PASS")
                    else:
                        self.log_test(f"View: {view}", "FAIL", "Negative count returned")
                except Exception as e:
                    self.log_test(f"View: {view}", "FAIL", str(e))
            
            conn.close()
            
        except Exception as e:
            self.log_test("Canonical Views Test", "FAIL", f"Database connection failed: {str(e)}")
    
    def test_duckdb_pipeline(self):
        """Test the DuckDB pipeline script."""
        try:
            pipeline_path = os.path.join(self.project_root, 'src', 'duckdb_pipeline.py')
            if not os.path.exists(pipeline_path):
                self.log_test("DuckDB Pipeline", "FAIL", "Pipeline script not found")
                return
            
            # Test that the script can be imported without errors
            sys.path.insert(0, os.path.join(self.project_root, 'src'))
            try:
                import duckdb_pipeline
                self.log_test("DuckDB Pipeline Import", "PASS")
            except Exception as e:
                self.log_test("DuckDB Pipeline Import", "FAIL", str(e))
            
        except Exception as e:
            self.log_test("DuckDB Pipeline", "FAIL", str(e))
    
    def test_data_quality_runner(self):
        """Test the data quality runner."""
        try:
            dq_path = os.path.join(self.project_root, 'src', 'data_quality_runner.py')
            if not os.path.exists(dq_path):
                self.log_test("Data Quality Runner", "FAIL", "Script not found")
                return
            
            # Test import
            try:
                import data_quality_runner
                self.log_test("Data Quality Runner Import", "PASS")
            except Exception as e:
                self.log_test("Data Quality Runner Import", "FAIL", str(e))
            
        except Exception as e:
            self.log_test("Data Quality Runner", "FAIL", str(e))
    
    def test_metrics_runner(self):
        """Test the metrics runner script."""
        try:
            metrics_path = os.path.join(self.project_root, 'src', 'metrics_runner.py')
            if not os.path.exists(metrics_path):
                self.log_test("Metrics Runner", "FAIL", "Script not found")
                return
            
            # Test that key metrics queries work
            conn = duckdb.connect(os.path.join(self.project_root, 'bree_case_study.db'))
            
            # Test a simple metrics query
            test_query = """
            SELECT 
              COUNT(*) as total_users,
              SUM(bank_linked_flag) as users_linked_bank
            FROM v_dim_users_clean;
            """
            
            result = conn.execute(test_query).fetchdf()
            if not result.empty and result['total_users'].iloc[0] > 0:
                self.log_test("Metrics Runner Query", "PASS")
            else:
                self.log_test("Metrics Runner Query", "FAIL", "No data returned")
            
            conn.close()
            
        except Exception as e:
            self.log_test("Metrics Runner", "FAIL", str(e))
    
    def test_notebooks_structure(self):
        """Test that notebook files exist and are readable."""
        notebooks_dir = os.path.join(self.project_root, 'notebooks')
        
        if not os.path.exists(notebooks_dir):
            self.log_test("Notebooks Directory", "FAIL", "Directory not found")
            return
        
        expected_notebooks = [
            '2_funnel_growth_deep_dive.ipynb',
            '3_experimentation.ipynb'
        ]
        
        for notebook in expected_notebooks:
            notebook_path = os.path.join(notebooks_dir, notebook)
            if os.path.exists(notebook_path):
                try:
                    # Test that it's a valid JSON file (notebooks are JSON)
                    import json
                    with open(notebook_path, 'r') as f:
                        json.load(f)
                    self.log_test(f"Notebook Structure: {notebook}", "PASS")
                except Exception as e:
                    self.log_test(f"Notebook Structure: {notebook}", "FAIL", f"Invalid JSON: {str(e)}")
            else:
                self.log_test(f"Notebook Structure: {notebook}", "FAIL", "File not found")
    
    def test_notebook_execution(self):
        """Execute notebook cells and catch errors."""
        try:
            import nbformat
            from nbconvert.preprocessors import ExecutePreprocessor
        except ImportError:
            self.log_test("Notebook Execution", "WARN", "nbformat/nbconvert not available - install with: pip install nbformat nbconvert")
            return
        
        notebooks_dir = os.path.join(self.project_root, 'notebooks')
        expected_notebooks = [
            '2_funnel_growth_deep_dive.ipynb',
            '3_experimentation.ipynb'
        ]
        
        for notebook_name in expected_notebooks:
            notebook_path = os.path.join(notebooks_dir, notebook_name)
            
            if not os.path.exists(notebook_path):
                self.log_test(f"Notebook Execution: {notebook_name}", "FAIL", "File not found")
                continue
            
            try:
                # Read the notebook
                with open(notebook_path, 'r') as f:
                    nb = nbformat.read(f, as_version=4)
                
                # Create executor with timeout
                ep = ExecutePreprocessor(timeout=300, kernel_name='python3')
                
                # Execute the notebook
                try:
                    ep.preprocess(nb, {'metadata': {'path': notebooks_dir}})
                    self.log_test(f"Notebook Execution: {notebook_name}", "PASS")
                    
                except Exception as exec_error:
                    # Get more detailed error information
                    error_msg = str(exec_error)
                    if hasattr(exec_error, 'ename'):
                        error_msg = f"{exec_error.ename}: {exec_error.evalue}"
                    
                    self.log_test(f"Notebook Execution: {notebook_name}", "FAIL", 
                                f"Execution error: {error_msg}")
                    
            except Exception as e:
                self.log_test(f"Notebook Execution: {notebook_name}", "FAIL", 
                            f"Failed to read/parse notebook: {str(e)}")
    
    def test_notebook_cells_individually(self):
        """Test individual notebook cells for more granular error reporting."""
        try:
            import nbformat
            import sys
            from io import StringIO
            from contextlib import redirect_stdout, redirect_stderr
        except ImportError:
            self.log_test("Individual Cell Testing", "WARN", "nbformat not available")
            return
        
        notebooks_dir = os.path.join(self.project_root, 'notebooks')
        expected_notebooks = [
            '2_funnel_growth_deep_dive.ipynb',
            '3_experimentation.ipynb'
        ]
        
        for notebook_name in expected_notebooks:
            notebook_path = os.path.join(notebooks_dir, notebook_name)
            
            if not os.path.exists(notebook_path):
                continue
            
            try:
                with open(notebook_path, 'r') as f:
                    nb = nbformat.read(f, as_version=4)
                
                code_cells = [cell for cell in nb.cells if cell.cell_type == 'code']
                
                if len(code_cells) == 0:
                    self.log_test(f"Cell Analysis: {notebook_name}", "WARN", "No code cells found")
                    continue
                
                error_cells = []
                for i, cell in enumerate(code_cells):
                    # Basic syntax check
                    try:
                        compile(cell.source, f"<cell_{i}>", "exec")
                    except SyntaxError as e:
                        error_cells.append(f"Cell {i+1}: Syntax error - {str(e)}")
                    except Exception as e:
                        error_cells.append(f"Cell {i+1}: Compile error - {str(e)}")
                
                if error_cells:
                    self.log_test(f"Cell Analysis: {notebook_name}", "FAIL", 
                                f"Found {len(error_cells)} problematic cells", 
                                "; ".join(error_cells[:3]))  # Show first 3 errors
                else:
                    self.log_test(f"Cell Analysis: {notebook_name}", "PASS", 
                                f"All {len(code_cells)} code cells passed syntax check")
                    
            except Exception as e:
                self.log_test(f"Cell Analysis: {notebook_name}", "FAIL", str(e))
    
    def test_sql_files(self):
        """Test SQL files can be read and contain expected content."""
        sql_dir = os.path.join(self.project_root, 'sql')
        
        if not os.path.exists(sql_dir):
            self.log_test("SQL Directory", "FAIL", "Directory not found")
            return
        
        expected_sql_files = [
            'schema.sql',
            'canonical_views.sql',
            'data_quality_checks.sql'
        ]
        
        for sql_file in expected_sql_files:
            sql_path = os.path.join(sql_dir, sql_file)
            if os.path.exists(sql_path):
                try:
                    with open(sql_path, 'r') as f:
                        content = f.read()
                    if len(content) > 0:
                        self.log_test(f"SQL File: {sql_file}", "PASS")
                    else:
                        self.log_test(f"SQL File: {sql_file}", "WARN", "File is empty")
                except Exception as e:
                    self.log_test(f"SQL File: {sql_file}", "FAIL", str(e))
            else:
                self.log_test(f"SQL File: {sql_file}", "FAIL", "File not found")
    
    def test_data_files(self):
        """Test that data files exist and are readable."""
        data_dir = os.path.join(self.project_root, 'data')
        
        if not os.path.exists(data_dir):
            self.log_test("Data Directory", "FAIL", "Directory not found")
            return
        
        expected_data_files = [
            'ab_assignments.csv',
            'loans.csv', 
            'sessions.csv',
            'transactions.csv',
            'users.csv'
        ]
        
        for data_file in expected_data_files:
            data_path = os.path.join(data_dir, data_file)
            if os.path.exists(data_path):
                try:
                    df = pd.read_csv(data_path, nrows=1)  # Just read first row to test
                    self.log_test(f"Data File: {data_file}", "PASS")
                except Exception as e:
                    self.log_test(f"Data File: {data_file}", "FAIL", str(e))
            else:
                self.log_test(f"Data File: {data_file}", "FAIL", "File not found")
    
    def test_project_structure(self):
        """Test overall project structure."""
        expected_dirs = ['src', 'sql', 'data', 'notebooks', 'reports']
        
        for directory in expected_dirs:
            dir_path = os.path.join(self.project_root, directory)
            if os.path.exists(dir_path) and os.path.isdir(dir_path):
                self.log_test(f"Directory: {directory}", "PASS")
            else:
                self.log_test(f"Directory: {directory}", "FAIL", "Directory not found")
    
    def run_all_tests(self):
        """Run all tests and generate summary report."""
        print("🚀 Bree Case Study - Project Test Runner")
        print(f"📅 Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)
        
        # Run all test categories
        print("\n📁 Testing Project Structure...")
        self.test_project_structure()
        
        print("\n💾 Testing Database Connection...")
        db_connected = self.test_database_connection()
        
        if db_connected:
            print("\n🔍 Testing Canonical Views...")
            self.test_canonical_views()
            
            print("\n📊 Testing Metrics Runner...")
            self.test_metrics_runner()
        else:
            print("⚠️  Skipping database-dependent tests due to connection failure")
        
        print("\n🐍 Testing Python Scripts...")
        self.test_duckdb_pipeline()
        self.test_data_quality_runner()
        
        print("\n📓 Testing Notebooks...")
        self.test_notebooks_structure()
        
        print("\n🔍 Testing Notebook Cell Syntax...")
        self.test_notebook_cells_individually()
        
        print("\n▶️ Testing Notebook Execution...")
        self.test_notebook_execution()
        
        print("\n📄 Testing SQL Files...")
        self.test_sql_files()
        
        print("\n📊 Testing Data Files...")
        self.test_data_files()
        
        # Generate summary
        print("\n" + "=" * 80)
        print("📋 TEST SUMMARY")
        print("=" * 80)
        
        success_rate = (self.passed_tests / self.total_tests * 100) if self.total_tests > 0 else 0
        
        print(f"Total Tests: {self.total_tests}")
        print(f"Passed: {self.passed_tests}")
        print(f"Failed: {self.total_tests - self.passed_tests}")
        print(f"Success Rate: {success_rate:.1f}%")
        
        if success_rate >= 90:
            print("🟢 Project Status: HEALTHY")
        elif success_rate >= 70:
            print("🟡 Project Status: NEEDS ATTENTION")
        else:
            print("🔴 Project Status: CRITICAL ISSUES")
        
        # Show failed tests
        failed_tests = [r for r in self.results if r['status'] == 'FAIL']
        if failed_tests:
            print(f"\n❌ Failed Tests ({len(failed_tests)}):")
            for test in failed_tests:
                print(f"   • {test['test']}: {test['message']}")
        
        print(f"\n✅ Test run completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)
        
        return success_rate >= 70  # Return True if project is healthy

def main():
    """Main function to run all tests."""
    tester = ProjectTester()
    success = tester.run_all_tests()
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
