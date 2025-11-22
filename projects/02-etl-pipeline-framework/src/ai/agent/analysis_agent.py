"""
Analysis Agent
Analyzes test results and suggests fixes
"""

import boto3
import json
from typing import Dict, List
from datetime import datetime


class AnalysisAgent:
    """Analyzes test execution results"""
    
    def __init__(self, region: str = "us-east-1"):
        self.region = region
        self.bedrock = boto3.client('bedrock-runtime', region_name=region)
    
    def analyze_results(
        self,
        execution_results: Dict,
        pipeline_type: str
    ) -> Dict:
        """
        Analyze test execution results
        
        Args:
            execution_results: Results from test execution
            pipeline_type: 'ETL' or 'ELT'
            
        Returns:
            Analysis results with suggestions
        """
        analysis = {
            "pipeline_type": pipeline_type,
            "overall_status": "passed" if execution_results.get("failed_tests", 0) == 0 else "failed",
            "success_rate": execution_results.get("success_rate", 0),
            "failures": [],
            "warnings": [],
            "suggestions": [],
            "root_causes": []
        }
        
        # Analyze failures
        for test in execution_results.get("tests_executed", []):
            if test.get("result", {}).get("status") == "failed":
                failure_analysis = self._analyze_failure(test, pipeline_type)
                analysis["failures"].append(failure_analysis)
        
        # Root cause analysis
        if analysis["failures"]:
            analysis["root_causes"] = self._identify_root_causes(analysis["failures"])
        
        # Generate suggestions
        analysis["suggestions"] = self._generate_suggestions(
            analysis,
            pipeline_type
        )
        
        return analysis
    
    def _analyze_failure(self, test: Dict, pipeline_type: str) -> Dict:
        """Analyze a specific test failure"""
        result = test.get("result", {})
        error = result.get("error") or result.get("stderr", "")
        
        failure = {
            "test_name": test.get("name"),
            "test_type": test.get("type"),
            "error_message": error,
            "pattern": self._identify_failure_pattern(error),
            "severity": self._assess_severity(error),
            "suggested_fix": None
        }
        
        # Generate fix suggestion using AI
        if error:
            failure["suggested_fix"] = self._suggest_fix(error, pipeline_type)
        
        return failure
    
    def _identify_failure_pattern(self, error: str) -> str:
        """Identify common failure patterns"""
        error_lower = error.lower()
        
        patterns = {
            "connection_error": ["connection", "timeout", "network", "refused"],
            "data_quality": ["null", "missing", "invalid", "type error"],
            "schema_mismatch": ["schema", "column", "does not exist"],
            "transformation_error": ["transform", "calculation", "formula"],
            "performance": ["timeout", "slow", "memory"]
        }
        
        for pattern, keywords in patterns.items():
            if any(keyword in error_lower for keyword in keywords):
                return pattern
        
        return "unknown"
    
    def _assess_severity(self, error: str) -> str:
        """Assess severity of failure"""
        error_lower = error.lower()
        
        if any(word in error_lower for word in ["critical", "fatal", "crash"]):
            return "critical"
        elif any(word in error_lower for word in ["error", "failed", "exception"]):
            return "high"
        elif any(word in error_lower for word in ["warning", "deprecated"]):
            return "medium"
        else:
            return "low"
    
    def _suggest_fix(self, error: str, pipeline_type: str) -> str:
        """Use AI to suggest a fix"""
        prompt = f"""
        A {pipeline_type} pipeline test failed with this error:
        
        {error}
        
        Suggest a specific fix. Be concise and actionable.
        """
        
        try:
            response = self.bedrock.invoke_model(
                modelId="anthropic.claude-v2",
                body=json.dumps({
                    "prompt": prompt,
                    "max_tokens_to_sample": 500,
                    "temperature": 0.3
                })
            )
            
            result = json.loads(response['body'].read())
            return result.get('completion', 'No suggestion available')
        except Exception:
            return "Manual review required"
    
    def _identify_root_causes(self, failures: List[Dict]) -> List[str]:
        """Identify root causes from failures"""
        patterns = {}
        
        for failure in failures:
            pattern = failure.get("pattern", "unknown")
            patterns[pattern] = patterns.get(pattern, 0) + 1
        
        # Sort by frequency
        sorted_patterns = sorted(patterns.items(), key=lambda x: x[1], reverse=True)
        
        return [pattern for pattern, count in sorted_patterns[:3]]  # Top 3
    
    def _generate_suggestions(
        self,
        analysis: Dict,
        pipeline_type: str
    ) -> List[str]:
        """Generate improvement suggestions"""
        suggestions = []
        
        # Based on success rate
        success_rate = analysis.get("success_rate", 100)
        if success_rate < 80:
            suggestions.append(
                f"Low success rate ({success_rate}%). Review test cases and data quality."
            )
        
        # Based on failure patterns
        root_causes = analysis.get("root_causes", [])
        if "connection_error" in root_causes:
            suggestions.append(
                "Connection errors detected. Check database connectivity and network configuration."
            )
        
        if "schema_mismatch" in root_causes:
            suggestions.append(
                "Schema mismatches found. Review schema evolution and versioning strategy."
            )
        
        if "data_quality" in root_causes:
            suggestions.append(
                "Data quality issues detected. Enhance data validation rules."
            )
        
        # Pipeline-specific suggestions
        if pipeline_type == "ELT":
            suggestions.append(
                "For ELT pipelines, consider adding SQL validation tests."
            )
        else:
            suggestions.append(
                "For ETL pipelines, consider adding transformation unit tests."
            )
        
        return suggestions

