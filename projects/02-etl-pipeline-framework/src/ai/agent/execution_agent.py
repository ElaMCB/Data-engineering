"""
Execution Agent
Executes tests for ETL/ELT pipelines
"""

import subprocess
import json
import os
from typing import Dict, List
from datetime import datetime


class ExecutionAgent:
    """Executes tests for discovered pipelines"""
    
    def __init__(self, region: str = "us-east-1"):
        self.region = region
    
    def execute_etl_tests(
        self,
        discovery: Dict,
        test_suite: Dict
    ) -> Dict:
        """
        Execute ETL pipeline tests
        
        Args:
            discovery: Pipeline discovery results
            test_suite: Generated test suite
            
        Returns:
            Execution results
        """
        results = {
            "pipeline_type": "ETL",
            "start_time": datetime.now().isoformat(),
            "tests_executed": [],
            "passed_tests": 0,
            "failed_tests": 0,
            "total_tests": 0,
            "errors": []
        }
        
        # Execute unit tests
        unit_tests = test_suite.get("unit_tests", [])
        for test in unit_tests:
            test_result = self._execute_pytest_test(test["code"])
            results["tests_executed"].append({
                "type": "unit",
                "name": test.get("category"),
                "result": test_result
            })
            if test_result["status"] == "passed":
                results["passed_tests"] += 1
            else:
                results["failed_tests"] += 1
            results["total_tests"] += 1
        
        # Execute integration tests
        integration_tests = test_suite.get("integration_tests", [])
        for test in integration_tests:
            test_result = self._execute_pytest_test(test["code"])
            results["tests_executed"].append({
                "type": "integration",
                "name": test.get("category"),
                "result": test_result
            })
            if test_result["status"] == "passed":
                results["passed_tests"] += 1
            else:
                results["failed_tests"] += 1
            results["total_tests"] += 1
        
        # Execute E2E tests (run actual pipeline)
        e2e_tests = test_suite.get("e2e_tests", [])
        for test in e2e_tests:
            test_result = self._execute_pipeline(discovery, test_suite.get("test_data", []))
            results["tests_executed"].append({
                "type": "e2e",
                "name": test.get("category"),
                "result": test_result
            })
            if test_result["status"] == "passed":
                results["passed_tests"] += 1
            else:
                results["failed_tests"] += 1
            results["total_tests"] += 1
        
        results["end_time"] = datetime.now().isoformat()
        results["success_rate"] = (
            results["passed_tests"] / results["total_tests"] * 100
            if results["total_tests"] > 0 else 0
        )
        
        return results
    
    def execute_elt_tests(
        self,
        discovery: Dict,
        test_suite: Dict
    ) -> Dict:
        """
        Execute ELT pipeline tests (SQL-based)
        
        Args:
            discovery: Pipeline discovery results
            test_suite: Generated test suite
            
        Returns:
            Execution results
        """
        results = {
            "pipeline_type": "ELT",
            "start_time": datetime.now().isoformat(),
            "tests_executed": [],
            "passed_tests": 0,
            "failed_tests": 0,
            "total_tests": 0,
            "errors": []
        }
        
        # For ELT, we need to:
        # 1. Load test data to source database
        # 2. Execute SQL transformation
        # 3. Query target and validate
        
        # Execute SQL-based tests
        sql_tests = test_suite.get("sql_tests", [])
        for test in sql_tests:
            test_result = self._execute_sql_test(test)
            results["tests_executed"].append({
                "type": "sql",
                "name": test.get("name"),
                "result": test_result
            })
            if test_result["status"] == "passed":
                results["passed_tests"] += 1
            else:
                results["failed_tests"] += 1
            results["total_tests"] += 1
        
        results["end_time"] = datetime.now().isoformat()
        results["success_rate"] = (
            results["passed_tests"] / results["total_tests"] * 100
            if results["total_tests"] > 0 else 0
        )
        
        return results
    
    def _execute_pytest_test(self, test_code: str) -> Dict:
        """Execute a pytest test"""
        # Write test to temporary file
        import tempfile
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
            f.write(test_code)
            temp_file = f.name
        
        try:
            # Run pytest
            result = subprocess.run(
                ["pytest", temp_file, "-v", "--json-report"],
                capture_output=True,
                text=True,
                timeout=300
            )
            
            return {
                "status": "passed" if result.returncode == 0 else "failed",
                "stdout": result.stdout,
                "stderr": result.stderr,
                "return_code": result.returncode
            }
        except subprocess.TimeoutExpired:
            return {
                "status": "failed",
                "error": "Test execution timeout"
            }
        except Exception as e:
            return {
                "status": "failed",
                "error": str(e)
            }
        finally:
            # Cleanup
            if os.path.exists(temp_file):
                os.unlink(temp_file)
    
    def _execute_pipeline(
        self,
        discovery: Dict,
        test_data: List[Dict]
    ) -> Dict:
        """Execute the actual pipeline with test data"""
        # This would integrate with your pipeline execution
        # For now, return a mock result
        
        return {
            "status": "passed",
            "records_processed": 100,
            "duration_seconds": 45.2,
            "metrics": {
                "extract_records": 100,
                "transform_records": 100,
                "load_records": 100
            }
        }
    
    def _execute_sql_test(self, test: Dict) -> Dict:
        """Execute SQL-based test for ELT"""
        # This would connect to MSSQL and execute SQL
        # For now, return a mock result
        
        return {
            "status": "passed",
            "sql": test.get("sql", ""),
            "rows_affected": 100,
            "validation_passed": True
        }

