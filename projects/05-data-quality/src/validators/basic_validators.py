"""
Basic Data Quality Validators
Common validation rules for data quality
"""

from typing import Tuple, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when, isnan, isnull


class BasicValidators:
    """Collection of basic validation functions"""
    
    @staticmethod
    def not_null(column_name: str) -> Tuple[str, str, callable]:
        """
        Validate that column is not null
        
        Args:
            column_name: Name of column to validate
            
        Returns:
            Tuple of (name, description, validator_function)
        """
        def validator(df: DataFrame) -> Tuple[bool, DataFrame, Optional[str]]:
            null_count = df.filter(col(column_name).isNull() | isnan(col(column_name))).count()
            failed_df = df.filter(col(column_name).isNull() | isnan(col(column_name)))
            passed = null_count == 0
            error_msg = f"{null_count} records have null values in {column_name}" if not passed else None
            return passed, failed_df, error_msg
        
        return (
            f"not_null_{column_name}",
            f"Column {column_name} must not be null",
            validator
        )
    
    @staticmethod
    def value_range(column_name: str, min_val: Optional[float] = None, max_val: Optional[float] = None) -> Tuple[str, str, callable]:
        """
        Validate that column values are within range
        
        Args:
            column_name: Name of column to validate
            min_val: Minimum allowed value
            max_val: Maximum allowed value
            
        Returns:
            Tuple of (name, description, validator_function)
        """
        def validator(df: DataFrame) -> Tuple[bool, DataFrame, Optional[str]]:
            conditions = []
            if min_val is not None:
                conditions.append(col(column_name) < min_val)
            if max_val is not None:
                conditions.append(col(column_name) > max_val)
            
            if not conditions:
                return True, df.limit(0), None
            
            condition = conditions[0]
            for cond in conditions[1:]:
                condition = condition | cond
            
            failed_df = df.filter(condition)
            failed_count = failed_df.count()
            passed = failed_count == 0
            
            range_str = f"[{min_val}, {max_val}]" if min_val and max_val else f">= {min_val}" if min_val else f"<= {max_val}"
            error_msg = f"{failed_count} records have values outside range {range_str} in {column_name}" if not passed else None
            
            return passed, failed_df, error_msg
        
        desc = f"Column {column_name} must be"
        if min_val and max_val:
            desc += f" between {min_val} and {max_val}"
        elif min_val:
            desc += f" >= {min_val}"
        elif max_val:
            desc += f" <= {max_val}"
        
        return (
            f"value_range_{column_name}",
            desc,
            validator
        )
    
    @staticmethod
    def allowed_values(column_name: str, allowed_values: list) -> Tuple[str, str, callable]:
        """
        Validate that column values are in allowed list
        
        Args:
            column_name: Name of column to validate
            allowed_values: List of allowed values
            
        Returns:
            Tuple of (name, description, validator_function)
        """
        def validator(df: DataFrame) -> Tuple[bool, DataFrame, Optional[str]]:
            failed_df = df.filter(~col(column_name).isin(allowed_values))
            failed_count = failed_df.count()
            passed = failed_count == 0
            error_msg = f"{failed_count} records have invalid values in {column_name}. Allowed: {allowed_values}" if not passed else None
            return passed, failed_df, error_msg
        
        return (
            f"allowed_values_{column_name}",
            f"Column {column_name} must be one of: {allowed_values}",
            validator
        )
    
    @staticmethod
    def unique(column_name: str) -> Tuple[str, str, callable]:
        """
        Validate that column values are unique
        
        Args:
            column_name: Name of column to validate
            
        Returns:
            Tuple of (name, description, validator_function)
        """
        def validator(df: DataFrame) -> Tuple[bool, DataFrame, Optional[str]]:
            total_count = df.count()
            unique_count = df.select(column_name).distinct().count()
            passed = total_count == unique_count
            
            if not passed:
                # Find duplicate records
                duplicates = df.groupBy(column_name).count().filter(col("count") > 1)
                duplicate_values = duplicates.select(column_name).collect()
                duplicate_list = [row[column_name] for row in duplicate_values]
                failed_df = df.filter(col(column_name).isin(duplicate_list))
                error_msg = f"Found {total_count - unique_count} duplicate values in {column_name}: {duplicate_list[:10]}"
            else:
                failed_df = df.limit(0)
                error_msg = None
            
            return passed, failed_df, error_msg
        
        return (
            f"unique_{column_name}",
            f"Column {column_name} must have unique values",
            validator
        )
    
    @staticmethod
    def regex_match(column_name: str, pattern: str) -> Tuple[str, str, callable]:
        """
        Validate that column values match regex pattern
        
        Args:
            column_name: Name of column to validate
            pattern: Regex pattern to match
            
        Returns:
            Tuple of (name, description, validator_function)
        """
        def validator(df: DataFrame) -> Tuple[bool, DataFrame, Optional[str]]:
            from pyspark.sql.functions import regexp_extract
            failed_df = df.filter(regexp_extract(col(column_name), pattern, 0) == "")
            failed_count = failed_df.count()
            passed = failed_count == 0
            error_msg = f"{failed_count} records don't match pattern {pattern} in {column_name}" if not passed else None
            return passed, failed_df, error_msg
        
        return (
            f"regex_match_{column_name}",
            f"Column {column_name} must match pattern: {pattern}",
            validator
        )

