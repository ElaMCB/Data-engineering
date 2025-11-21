"""
Data Transformer
Transforms extracted data according to business rules
"""

from typing import Dict, Any, List, Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, when, trim, upper, lower, regexp_replace,
    to_date, to_timestamp, coalesce, lit, current_timestamp, expr
)
from src.framework.base_pipeline import Transformer
import structlog

logger = structlog.get_logger()


class DataTransformer(Transformer):
    """
    Transforms data according to business rules
    
    Features:
    - Data cleaning and standardization
    - Type conversions
    - Data validation
    - Column renaming
    - Data enrichment
    """
    
    def __init__(
        self,
        spark: SparkSession,
        transformations: Optional[List[Dict[str, Any]]] = None,
        column_mappings: Optional[Dict[str, str]] = None,
        data_quality_rules: Optional[List[Dict[str, Any]]] = None
    ):
        """
        Initialize Data Transformer
        
        Args:
            spark: SparkSession instance
            transformations: List of transformation rules
            column_mappings: Column rename mappings
            data_quality_rules: Data quality validation rules
        """
        self.spark = spark
        self.transformations = transformations or []
        self.column_mappings = column_mappings or {}
        self.data_quality_rules = data_quality_rules or []
        self.logger = logger.bind(component="DataTransformer")
    
    def transform(self, data: DataFrame) -> DataFrame:
        """
        Transform data
        
        Args:
            data: Input DataFrame
            
        Returns:
            Transformed DataFrame
        """
        self.logger.info("Starting data transformation")
        
        df = data
        
        # Apply column mappings (renaming)
        if self.column_mappings:
            for old_name, new_name in self.column_mappings.items():
                if old_name in df.columns:
                    df = df.withColumnRenamed(old_name, new_name)
                    self.logger.debug("Renamed column", old=old_name, new=new_name)
        
        # Apply custom transformations
        for transformation in self.transformations:
            df = self._apply_transformation(df, transformation)
        
        # Apply standard data cleaning
        df = self._apply_standard_cleaning(df)
        
        # Apply data quality rules
        if self.data_quality_rules:
            df = self._apply_quality_rules(df)
        
        # Add metadata columns
        df = df.withColumn("etl_timestamp", current_timestamp())
        df = df.withColumn("etl_batch_id", lit(self._generate_batch_id()))
        
        record_count = df.count()
        self.logger.info("Data transformation completed", record_count=record_count)
        
        return df
    
    def _apply_transformation(self, df: DataFrame, transformation: Dict[str, Any]) -> DataFrame:
        """Apply a single transformation rule"""
        transform_type = transformation.get("type")
        
        if transform_type == "cast":
            column = transformation["column"]
            target_type = transformation["target_type"]
            df = df.withColumn(column, col(column).cast(target_type))
        
        elif transform_type == "replace":
            column = transformation["column"]
            old_value = transformation.get("old_value")
            new_value = transformation.get("new_value")
            df = df.withColumn(
                column,
                when(col(column) == old_value, new_value).otherwise(col(column))
            )
        
        elif transform_type == "default_value":
            column = transformation["column"]
            default = transformation["default"]
            df = df.withColumn(
                column,
                coalesce(col(column), lit(default))
            )
        
        elif transform_type == "custom_expr":
            column = transformation["column"]
            expression = transformation["expression"]
            df = df.withColumn(column, expr(expression))
        
        return df
    
    def _apply_standard_cleaning(self, df: DataFrame) -> DataFrame:
        """Apply standard data cleaning operations"""
        # Clean string columns
        string_columns = [field.name for field in df.schema.fields 
                         if str(field.dataType) == "StringType()"]
        
        for col_name in string_columns:
            # Trim whitespace
            df = df.withColumn(col_name, trim(col(col_name)))
            # Remove extra spaces
            df = df.withColumn(
                col_name,
                regexp_replace(col(col_name), "\\s+", " ")
            )
        
        return df
    
    def _apply_quality_rules(self, df: DataFrame) -> DataFrame:
        """Apply data quality validation rules"""
        for rule in self.data_quality_rules:
            rule_type = rule.get("type")
            
            if rule_type == "not_null":
                column = rule["column"]
                df = df.filter(col(column).isNotNull())
            
            elif rule_type == "value_range":
                column = rule["column"]
                min_val = rule.get("min")
                max_val = rule.get("max")
                if min_val is not None:
                    df = df.filter(col(column) >= min_val)
                if max_val is not None:
                    df = df.filter(col(column) <= max_val)
            
            elif rule_type == "allowed_values":
                column = rule["column"]
                allowed = rule["allowed_values"]
                df = df.filter(col(column).isin(allowed))
        
        return df
    
    def _generate_batch_id(self) -> str:
        """Generate batch ID for tracking"""
        from datetime import datetime
        return datetime.now().strftime("%Y%m%d%H%M%S")

