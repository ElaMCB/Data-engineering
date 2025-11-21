"""
Anomaly Detector
Detects anomalies and outliers in data
"""

from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, abs as spark_abs, mean, stddev
import structlog

logger = structlog.get_logger()


@dataclass
class AnomalyResult:
    """Results of anomaly detection"""
    column_name: str
    anomaly_count: int
    anomaly_percentage: float
    method: str
    threshold: float
    anomalous_records: DataFrame


class AnomalyDetector:
    """
    Detects anomalies in data using statistical methods
    
    Methods:
    - Z-score (standard deviations from mean)
    - IQR (Interquartile Range)
    - Custom thresholds
    """
    
    def __init__(self, spark: SparkSession = None):
        """
        Initialize Anomaly Detector
        
        Args:
            spark: Optional SparkSession instance
        """
        self.spark = spark
        self.logger = logger.bind(component="AnomalyDetector")
    
    def detect_anomalies(
        self,
        df: DataFrame,
        columns: List[str],
        method: str = "zscore",
        threshold: float = 3.0
    ) -> List[AnomalyResult]:
        """
        Detect anomalies in specified columns
        
        Args:
            df: DataFrame to analyze
            columns: List of column names to check
            method: Detection method ("zscore", "iqr", "threshold")
            threshold: Threshold value for detection
            
        Returns:
            List of AnomalyResult objects
        """
        self.logger.info("Starting anomaly detection", columns=columns, method=method)
        
        results = []
        
        for col_name in columns:
            try:
                if method == "zscore":
                    result = self._detect_zscore_anomalies(df, col_name, threshold)
                elif method == "iqr":
                    result = self._detect_iqr_anomalies(df, col_name, threshold)
                elif method == "threshold":
                    result = self._detect_threshold_anomalies(df, col_name, threshold)
                else:
                    self.logger.warning("Unknown method", method=method)
                    continue
                
                if result:
                    results.append(result)
                    
            except Exception as e:
                self.logger.error("Anomaly detection failed", column=col_name, error=str(e))
        
        self.logger.info("Anomaly detection completed", anomalies_found=len(results))
        
        return results
    
    def _detect_zscore_anomalies(
        self,
        df: DataFrame,
        column_name: str,
        zscore_threshold: float = 3.0
    ) -> Optional[AnomalyResult]:
        """Detect anomalies using Z-score method"""
        try:
            # Calculate mean and stddev
            stats = df.select(
                mean(col(column_name)).alias("mean"),
                stddev(col(column_name)).alias("stddev")
            ).collect()[0]
            
            mean_val = stats["mean"]
            stddev_val = stats["stddev"]
            
            if stddev_val is None or stddev_val == 0:
                return None
            
            # Calculate Z-score and find anomalies
            zscore = spark_abs((col(column_name) - mean_val) / stddev_val)
            anomalous_df = df.filter(zscore > zscore_threshold)
            anomaly_count = anomalous_df.count()
            total_count = df.count()
            anomaly_percentage = (anomaly_count / total_count * 100) if total_count > 0 else 0.0
            
            return AnomalyResult(
                column_name=column_name,
                anomaly_count=anomaly_count,
                anomaly_percentage=anomaly_percentage,
                method="zscore",
                threshold=zscore_threshold,
                anomalous_records=anomalous_df
            )
        except Exception as e:
            self.logger.error("Z-score detection failed", column=column_name, error=str(e))
            return None
    
    def _detect_iqr_anomalies(
        self,
        df: DataFrame,
        column_name: str,
        iqr_multiplier: float = 1.5
    ) -> Optional[AnomalyResult]:
        """Detect anomalies using IQR (Interquartile Range) method"""
        try:
            from pyspark.sql.functions import percentile_approx
            
            # Calculate quartiles
            q1 = df.select(percentile_approx(col(column_name), 0.25)).collect()[0][0]
            q3 = df.select(percentile_approx(col(column_name), 0.75)).collect()[0][0]
            
            if q1 is None or q3 is None:
                return None
            
            iqr = q3 - q1
            lower_bound = q1 - (iqr_multiplier * iqr)
            upper_bound = q3 + (iqr_multiplier * iqr)
            
            # Find anomalies
            anomalous_df = df.filter(
                (col(column_name) < lower_bound) | (col(column_name) > upper_bound)
            )
            anomaly_count = anomalous_df.count()
            total_count = df.count()
            anomaly_percentage = (anomaly_count / total_count * 100) if total_count > 0 else 0.0
            
            return AnomalyResult(
                column_name=column_name,
                anomaly_count=anomaly_count,
                anomaly_percentage=anomaly_percentage,
                method="iqr",
                threshold=iqr_multiplier,
                anomalous_records=anomalous_df
            )
        except Exception as e:
            self.logger.error("IQR detection failed", column=column_name, error=str(e))
            return None
    
    def _detect_threshold_anomalies(
        self,
        df: DataFrame,
        column_name: str,
        threshold: float
    ) -> Optional[AnomalyResult]:
        """Detect anomalies using custom threshold"""
        # This is a placeholder - would need specific threshold logic
        # For now, detect values > threshold
        anomalous_df = df.filter(col(column_name) > threshold)
        anomaly_count = anomalous_df.count()
        total_count = df.count()
        anomaly_percentage = (anomaly_count / total_count * 100) if total_count > 0 else 0.0
        
        return AnomalyResult(
            column_name=column_name,
            anomaly_count=anomaly_count,
            anomaly_percentage=anomaly_percentage,
            method="threshold",
            threshold=threshold,
            anomalous_records=anomalous_df
        )

