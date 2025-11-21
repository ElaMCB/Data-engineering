"""
Business Logic Validators
Generic business rule validators
"""

from typing import Tuple, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, abs as spark_abs


class BusinessValidators:
    """Collection of business logic validation functions"""
    
    @staticmethod
    def referential_integrity(
        foreign_key_column: str,
        reference_table: DataFrame,
        reference_key_column: str
    ) -> Tuple[str, str, callable]:
        """
        Validate referential integrity (foreign key exists in reference table)
        
        Args:
            foreign_key_column: Column name in main table
            reference_table: Reference DataFrame
            reference_key_column: Key column in reference table
            
        Returns:
            Tuple of (name, description, validator_function)
        """
        def validator(df: DataFrame) -> Tuple[bool, DataFrame, Optional[str]]:
            # Get valid keys from reference table
            valid_keys = reference_table.select(reference_key_column).distinct()
            
            # Find records with invalid foreign keys
            failed_df = df.join(
                valid_keys,
                df[foreign_key_column] == valid_keys[reference_key_column],
                "left_anti"
            )
            
            failed_count = failed_df.count()
            passed = failed_count == 0
            error_msg = f"{failed_count} records have {foreign_key_column} not found in reference table" if not passed else None
            return passed, failed_df, error_msg
        
        return (
            f"referential_integrity_{foreign_key_column}",
            f"{foreign_key_column} must exist in reference table",
            validator
        )
    
    @staticmethod
    def calculated_field_consistency(
        calculated_column: str,
        formula: str,
        tolerance: float = 0.01
    ) -> Tuple[str, str, callable]:
        """
        Validate calculated field matches formula (e.g., total = quantity × price)
        
        Args:
            calculated_column: Name of calculated column
            formula: Formula expression (e.g., "quantity * price_per_unit")
            tolerance: Allowed difference for rounding
            
        Returns:
            Tuple of (name, description, validator_function)
        """
        def validator(df: DataFrame) -> Tuple[bool, DataFrame, Optional[str]]:
            from pyspark.sql import functions as F
            
            # Evaluate formula
            calculated_value = F.expr(formula)
            difference = spark_abs(col(calculated_column) - calculated_value)
            
            failed_df = df.filter(difference > tolerance)
            failed_count = failed_df.count()
            passed = failed_count == 0
            error_msg = f"{failed_count} records have {calculated_column} inconsistent with formula: {formula}" if not passed else None
            return passed, failed_df, error_msg
        
        return (
            f"calculated_field_{calculated_column}",
            f"{calculated_column} must match formula: {formula}",
            validator
        )
    
    @staticmethod
    def date_range_valid(
        date_column: str,
        min_date: Optional[str] = None,
        max_date: Optional[str] = None
    ) -> Tuple[str, str, callable]:
        """
        Validate date is within valid range
        
        Args:
            date_column: Name of date column
            min_date: Minimum allowed date (ISO format)
            max_date: Maximum allowed date (ISO format)
            
        Returns:
            Tuple of (name, description, validator_function)
        """
        def validator(df: DataFrame) -> Tuple[bool, DataFrame, Optional[str]]:
            from pyspark.sql.functions import to_date
            
            conditions = []
            if min_date:
                conditions.append(to_date(col(date_column)) < to_date(lit(min_date)))
            if max_date:
                from pyspark.sql.functions import lit
                conditions.append(to_date(col(date_column)) > to_date(lit(max_date)))
            
            if not conditions:
                return True, df.limit(0), None
            
            combined = conditions[0]
            for cond in conditions[1:]:
                combined = combined | cond
            
            failed_df = df.filter(combined)
            failed_count = failed_df.count()
            passed = failed_count == 0
            
            range_str = f"[{min_date}, {max_date}]" if min_date and max_date else f">= {min_date}" if min_date else f"<= {max_date}"
            error_msg = f"{failed_count} records have {date_column} outside range {range_str}" if not passed else None
            return passed, failed_df, error_msg
        
        from pyspark.sql.functions import lit
        desc = f"{date_column} must be"
        if min_date and max_date:
            desc += f" between {min_date} and {max_date}"
        elif min_date:
            desc += f" >= {min_date}"
        elif max_date:
            desc += f" <= {max_date}"
        
        return (
            f"date_range_{date_column}",
            desc,
            validator
        )

