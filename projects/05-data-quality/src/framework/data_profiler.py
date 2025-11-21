"""
Data Profiler
Generates comprehensive data profiles and statistics
"""

from typing import Dict, Any, List
from dataclasses import dataclass
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    count, countDistinct, min, max, mean, stddev,
    col, isnan, isnull, when
)
import structlog

logger = structlog.get_logger()


@dataclass
class ColumnProfile:
    """Profile statistics for a single column"""
    column_name: str
    data_type: str
    total_count: int
    null_count: int
    null_percentage: float
    distinct_count: int
    distinct_percentage: float
    min_value: Any = None
    max_value: Any = None
    mean_value: Any = None
    stddev_value: Any = None


@dataclass
class ProfileResult:
    """Complete data profile results"""
    total_records: int
    total_columns: int
    column_profiles: List[ColumnProfile]
    timestamp: str


class DataProfiler:
    """
    Generates comprehensive data profiles
    
    Features:
    - Column-level statistics
    - Null value analysis
    - Distinct value counts
    - Min/max/mean/stddev for numeric columns
    """
    
    def __init__(self, spark: SparkSession = None):
        """
        Initialize Data Profiler
        
        Args:
            spark: Optional SparkSession instance
        """
        self.spark = spark
        self.logger = logger.bind(component="DataProfiler")
    
    def profile(self, df: DataFrame, columns: List[str] = None) -> ProfileResult:
        """
        Generate profile for DataFrame
        
        Args:
            df: DataFrame to profile
            columns: Optional list of columns to profile (default: all)
            
        Returns:
            ProfileResult with statistics
        """
        self.logger.info("Starting data profiling")
        
        total_records = df.count()
        
        if columns is None:
            columns = df.columns
        
        column_profiles = []
        
        for col_name in columns:
            self.logger.debug("Profiling column", column=col_name)
            
            # Get data type
            data_type = str(df.schema[col_name].dataType)
            
            # Basic counts
            null_count = df.filter(
                col(col_name).isNull() | isnan(col(col_name))
            ).count()
            null_percentage = (null_count / total_records * 100) if total_records > 0 else 0.0
            
            distinct_count = df.select(col_name).distinct().count()
            distinct_percentage = (distinct_count / total_records * 100) if total_records > 0 else 0.0
            
            # Numeric statistics (if applicable)
            min_val = None
            max_val = None
            mean_val = None
            stddev_val = None
            
            if data_type in ["IntegerType", "LongType", "DoubleType", "DecimalType", "FloatType"]:
                try:
                    stats = df.select(
                        min(col(col_name)).alias("min"),
                        max(col(col_name)).alias("max"),
                        mean(col(col_name)).alias("mean"),
                        stddev(col(col_name)).alias("stddev")
                    ).collect()[0]
                    
                    min_val = stats["min"]
                    max_val = stats["max"]
                    mean_val = stats["mean"]
                    stddev_val = stats["stddev"]
                except Exception as e:
                    self.logger.warning("Could not compute numeric stats", column=col_name, error=str(e))
            
            profile = ColumnProfile(
                column_name=col_name,
                data_type=data_type,
                total_count=total_records,
                null_count=null_count,
                null_percentage=null_percentage,
                distinct_count=distinct_count,
                distinct_percentage=distinct_percentage,
                min_value=min_val,
                max_value=max_val,
                mean_value=mean_val,
                stddev_value=stddev_val
            )
            
            column_profiles.append(profile)
        
        from datetime import datetime
        result = ProfileResult(
            total_records=total_records,
            total_columns=len(columns),
            column_profiles=column_profiles,
            timestamp=datetime.now().isoformat()
        )
        
        self.logger.info("Data profiling completed", columns=len(columns))
        
        return result
    
    def summary(self, profile: ProfileResult) -> Dict[str, Any]:
        """
        Generate summary from profile
        
        Args:
            profile: ProfileResult instance
            
        Returns:
            Dictionary with summary statistics
        """
        summary = {
            "total_records": profile.total_records,
            "total_columns": profile.total_columns,
            "columns_with_nulls": sum(1 for p in profile.column_profiles if p.null_count > 0),
            "columns_with_high_null_rate": sum(
                1 for p in profile.column_profiles if p.null_percentage > 10.0
            ),
            "columns_with_low_distinctness": sum(
                1 for p in profile.column_profiles if p.distinct_percentage < 1.0
            ),
            "column_details": [
                {
                    "column": p.column_name,
                    "data_type": p.data_type,
                    "null_percentage": round(p.null_percentage, 2),
                    "distinct_count": p.distinct_count,
                    "min": p.min_value,
                    "max": p.max_value,
                    "mean": p.mean_value
                }
                for p in profile.column_profiles
            ]
        }
        
        return summary

