"""
Core Data Quality Validator
Validates data against defined rules and returns comprehensive results
"""

from typing import List, Dict, Any, Callable, Optional
from dataclasses import dataclass, field
from datetime import datetime
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, when, count, isnan, isnull
import structlog

logger = structlog.get_logger()


@dataclass
class ValidationRule:
    """Represents a single validation rule"""
    name: str
    description: str
    validator_func: Callable
    severity: str = "error"  # error, warning, info
    enabled: bool = True


@dataclass
class ValidationResult:
    """Results of validation execution"""
    rule_name: str
    passed: bool
    failed_count: int
    total_count: int
    pass_rate: float
    failed_records: Optional[DataFrame] = None
    error_message: Optional[str] = None
    execution_time: float = 0.0
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())


class QualityValidator:
    """
    Core data quality validation engine
    
    Supports:
    - Multiple validation rules
    - Spark DataFrame validation
    - Detailed error reporting
    - Performance metrics
    """
    
    def __init__(self, spark: Optional[SparkSession] = None):
        """
        Initialize Quality Validator
        
        Args:
            spark: Optional SparkSession instance
        """
        self.spark = spark
        self.rules: List[ValidationRule] = []
        self.logger = logger.bind(component="QualityValidator")
    
    def add_rule(
        self,
        name: str,
        description: str,
        validator_func: Callable,
        severity: str = "error",
        enabled: bool = True
    ):
        """
        Add a validation rule
        
        Args:
            name: Rule name
            description: Rule description
            validator_func: Function that takes DataFrame and returns (passed, failed_df, error_msg)
            severity: Rule severity (error, warning, info)
            enabled: Whether rule is enabled
        """
        rule = ValidationRule(
            name=name,
            description=description,
            validator_func=validator_func,
            severity=severity,
            enabled=enabled
        )
        self.rules.append(rule)
        self.logger.info("Added validation rule", rule_name=name)
    
    def validate(self, df: DataFrame) -> Dict[str, Any]:
        """
        Validate DataFrame against all rules
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Dictionary with validation results
        """
        start_time = datetime.now()
        self.logger.info("Starting validation", rules_count=len(self.rules))
        
        total_records = df.count()
        results = []
        overall_passed = True
        
        for rule in self.rules:
            if not rule.enabled:
                continue
            
            rule_start = datetime.now()
            self.logger.info("Executing rule", rule_name=rule.name)
            
            try:
                # Execute validator function
                passed, failed_df, error_msg = rule.validator_func(df)
                
                failed_count = failed_df.count() if failed_df else 0
                pass_rate = ((total_records - failed_count) / total_records * 100) if total_records > 0 else 100.0
                
                rule_result = ValidationResult(
                    rule_name=rule.name,
                    passed=passed,
                    failed_count=failed_count,
                    total_count=total_records,
                    pass_rate=pass_rate,
                    failed_records=failed_df,
                    error_message=error_msg,
                    execution_time=(datetime.now() - rule_start).total_seconds(),
                    timestamp=datetime.now().isoformat()
                )
                
                results.append(rule_result)
                
                if not passed and rule.severity == "error":
                    overall_passed = False
                
                self.logger.info(
                    "Rule execution completed",
                    rule_name=rule.name,
                    passed=passed,
                    failed_count=failed_count,
                    pass_rate=pass_rate
                )
                
            except Exception as e:
                self.logger.error("Rule execution failed", rule_name=rule.name, error=str(e), exc_info=True)
                
                rule_result = ValidationResult(
                    rule_name=rule.name,
                    passed=False,
                    failed_count=total_records,
                    total_count=total_records,
                    pass_rate=0.0,
                    error_message=f"Validation error: {str(e)}",
                    execution_time=(datetime.now() - rule_start).total_seconds(),
                    timestamp=datetime.now().isoformat()
                )
                results.append(rule_result)
                overall_passed = False
        
        total_duration = (datetime.now() - start_time).total_seconds()
        
        # Calculate summary statistics
        total_failed = sum(r.failed_count for r in results)
        overall_pass_rate = ((total_records - total_failed) / total_records * 100) if total_records > 0 else 100.0
        
        summary = {
            "overall_passed": overall_passed,
            "total_records": total_records,
            "total_failed": total_failed,
            "overall_pass_rate": overall_pass_rate,
            "rules_executed": len(results),
            "rules_passed": sum(1 for r in results if r.passed),
            "rules_failed": sum(1 for r in results if not r.passed),
            "execution_time": total_duration,
            "timestamp": datetime.now().isoformat(),
            "results": [
                {
                    "rule_name": r.rule_name,
                    "passed": r.passed,
                    "failed_count": r.failed_count,
                    "pass_rate": r.pass_rate,
                    "error_message": r.error_message,
                    "execution_time": r.execution_time
                }
                for r in results
            ]
        }
        
        self.logger.info(
            "Validation completed",
            overall_passed=overall_passed,
            pass_rate=overall_pass_rate,
            duration=total_duration
        )
        
        return summary
    
    def get_failed_records(self, df: DataFrame) -> DataFrame:
        """
        Get all records that failed any validation rule
        
        Args:
            df: Original DataFrame
            
        Returns:
            DataFrame with failed records
        """
        validation_results = self.validate(df)
        
        failed_dfs = []
        for result in validation_results["results"]:
            if not result["passed"] and result.get("failed_records"):
                failed_dfs.append(result["failed_records"])
        
        if not failed_dfs:
            # Return empty DataFrame with same schema
            return df.limit(0)
        
        # Union all failed records
        combined = failed_dfs[0]
        for failed_df in failed_dfs[1:]:
            combined = combined.unionByName(failed_df, allowMissingColumns=True)
        
        # Remove duplicates
        return combined.distinct()

