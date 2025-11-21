"""
Oil & Gas Specific Validators
Domain-specific validation rules for oil & gas carrier ticket transactions
"""

from typing import Tuple, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, abs as spark_abs, when, datediff, current_date, to_date


class OilGasValidators:
    """Collection of oil & gas specific validation functions"""
    
    # Valid fuel types
    VALID_FUEL_TYPES = [
        "Crude Oil",
        "Gasoline",
        "Diesel",
        "Jet Fuel",
        "Liquefied Natural Gas",
        "Liquefied Petroleum Gas",
        "Heavy Fuel Oil",
        "Marine Gas Oil",
        "Bunker Fuel"
    ]
    
    # Valid quantity units
    VALID_QUANTITY_UNITS = [
        "Barrels",
        "Metric Tons",
        "Cubic Meters",
        "Gallons",
        "Liters",
        "Pounds"
    ]
    
    # Valid currencies
    VALID_CURRENCIES = ["USD", "EUR", "GBP", "JPY", "CNY"]
    
    # Valid transaction statuses
    VALID_STATUSES = [
        "Pending",
        "Completed",
        "Cancelled",
        "Refunded",
        "In Transit",
        "Delivered"
    ]
    
    @staticmethod
    def valid_fuel_type() -> Tuple[str, str, callable]:
        """
        Validate fuel type is in allowed list
        
        Returns:
            Tuple of (name, description, validator_function)
        """
        def validator(df: DataFrame) -> Tuple[bool, DataFrame, Optional[str]]:
            failed_df = df.filter(~col("fuel_type").isin(OilGasValidators.VALID_FUEL_TYPES))
            failed_count = failed_df.count()
            passed = failed_count == 0
            error_msg = f"{failed_count} records have invalid fuel_type. Allowed: {OilGasValidators.VALID_FUEL_TYPES}" if not passed else None
            return passed, failed_df, error_msg
        
        return (
            "valid_fuel_type",
            f"Fuel type must be one of: {OilGasValidators.VALID_FUEL_TYPES}",
            validator
        )
    
    @staticmethod
    def quantity_range() -> Tuple[str, str, callable]:
        """
        Validate quantity is within reasonable range for oil & gas
        
        Returns:
            Tuple of (name, description, validator_function)
        """
        def validator(df: DataFrame) -> Tuple[bool, DataFrame, Optional[str]]:
            # Reasonable range: 0.01 to 5,000,000 (very large tanker)
            failed_df = df.filter(
                (col("quantity") < 0.01) | (col("quantity") > 5000000)
            )
            failed_count = failed_df.count()
            passed = failed_count == 0
            error_msg = f"{failed_count} records have quantity outside valid range [0.01, 5000000]" if not passed else None
            return passed, failed_df, error_msg
        
        return (
            "quantity_range",
            "Quantity must be between 0.01 and 5,000,000 units",
            validator
        )
    
    @staticmethod
    def price_per_unit_range() -> Tuple[str, str, callable]:
        """
        Validate price per unit is within reasonable range
        
        Returns:
            Tuple of (name, description, validator_function)
        """
        def validator(df: DataFrame) -> Tuple[bool, DataFrame, Optional[str]]:
            # Reasonable range: $0.01 to $10,000 per unit
            failed_df = df.filter(
                (col("price_per_unit") < 0.01) | (col("price_per_unit") > 10000)
            )
            failed_count = failed_df.count()
            passed = failed_count == 0
            error_msg = f"{failed_count} records have price_per_unit outside valid range [0.01, 10000]" if not passed else None
            return passed, failed_df, error_msg
        
        return (
            "price_per_unit_range",
            "Price per unit must be between $0.01 and $10,000",
            validator
        )
    
    @staticmethod
    def quantity_price_consistency() -> Tuple[str, str, callable]:
        """
        Validate that total_amount = quantity × price_per_unit (within tolerance)
        
        Returns:
            Tuple of (name, description, validator_function)
        """
        def validator(df: DataFrame) -> Tuple[bool, DataFrame, Optional[str]]:
            tolerance = 0.01  # Allow $0.01 difference for rounding
            calculated_total = col("quantity") * col("price_per_unit")
            difference = spark_abs(col("total_amount") - calculated_total)
            
            failed_df = df.filter(difference > tolerance)
            failed_count = failed_df.count()
            passed = failed_count == 0
            error_msg = f"{failed_count} records have total_amount inconsistent with quantity × price_per_unit" if not passed else None
            return passed, failed_df, error_msg
        
        return (
            "quantity_price_consistency",
            "Total amount must equal quantity × price_per_unit (within $0.01 tolerance)",
            validator
        )
    
    @staticmethod
    def valid_quantity_unit() -> Tuple[str, str, callable]:
        """
        Validate quantity unit is valid
        
        Returns:
            Tuple of (name, description, validator_function)
        """
        def validator(df: DataFrame) -> Tuple[bool, DataFrame, Optional[str]]:
            failed_df = df.filter(~col("quantity_unit").isin(OilGasValidators.VALID_QUANTITY_UNITS))
            failed_count = failed_df.count()
            passed = failed_count == 0
            error_msg = f"{failed_count} records have invalid quantity_unit. Allowed: {OilGasValidators.VALID_QUANTITY_UNITS}" if not passed else None
            return passed, failed_df, error_msg
        
        return (
            "valid_quantity_unit",
            f"Quantity unit must be one of: {OilGasValidators.VALID_QUANTITY_UNITS}",
            validator
        )
    
    @staticmethod
    def valid_currency() -> Tuple[str, str, callable]:
        """
        Validate currency code is valid
        
        Returns:
            Tuple of (name, description, validator_function)
        """
        def validator(df: DataFrame) -> Tuple[bool, DataFrame, Optional[str]]:
            failed_df = df.filter(~col("currency").isin(OilGasValidators.VALID_CURRENCIES))
            failed_count = failed_df.count()
            passed = failed_count == 0
            error_msg = f"{failed_count} records have invalid currency. Allowed: {OilGasValidators.VALID_CURRENCIES}" if not passed else None
            return passed, failed_df, error_msg
        
        return (
            "valid_currency",
            f"Currency must be one of: {OilGasValidators.VALID_CURRENCIES}",
            validator
        )
    
    @staticmethod
    def valid_transaction_status() -> Tuple[str, str, callable]:
        """
        Validate transaction status is valid
        
        Returns:
            Tuple of (name, description, validator_function)
        """
        def validator(df: DataFrame) -> Tuple[bool, DataFrame, Optional[str]]:
            failed_df = df.filter(~col("transaction_status").isin(OilGasValidators.VALID_STATUSES))
            failed_count = failed_df.count()
            passed = failed_count == 0
            error_msg = f"{failed_count} records have invalid transaction_status. Allowed: {OilGasValidators.VALID_STATUSES}" if not passed else None
            return passed, failed_df, error_msg
        
        return (
            "valid_transaction_status",
            f"Transaction status must be one of: {OilGasValidators.VALID_STATUSES}",
            validator
        )
    
    @staticmethod
    def port_code_format() -> Tuple[str, str, callable]:
        """
        Validate port codes are in correct format (3-5 uppercase letters)
        
        Returns:
            Tuple of (name, description, validator_function)
        """
        def validator(df: DataFrame) -> Tuple[bool, DataFrame, Optional[str]]:
            from pyspark.sql.functions import upper, length, regexp_extract
            
            # Check origin port
            origin_invalid = df.filter(
                (regexp_extract(upper(col("origin_port")), "^[A-Z]{3,5}$", 0) == "") |
                col("origin_port").isNull()
            )
            
            # Check destination port
            dest_invalid = df.filter(
                (regexp_extract(upper(col("destination_port")), "^[A-Z]{3,5}$", 0) == "") |
                col("destination_port").isNull()
            )
            
            # Combine
            failed_df = origin_invalid.unionByName(dest_invalid).distinct()
            failed_count = failed_df.count()
            passed = failed_count == 0
            error_msg = f"{failed_count} records have invalid port code format (must be 3-5 uppercase letters)" if not passed else None
            return passed, failed_df, error_msg
        
        return (
            "port_code_format",
            "Port codes must be 3-5 uppercase letters",
            validator
        )
    
    @staticmethod
    def discharge_after_loading() -> Tuple[str, str, callable]:
        """
        Validate discharge date is after or equal to loading date
        
        Returns:
            Tuple of (name, description, validator_function)
        """
        def validator(df: DataFrame) -> Tuple[bool, DataFrame, Optional[str]]:
            # Only check records where both dates exist
            date_check = df.filter(
                col("loading_date").isNotNull() &
                col("discharge_date").isNotNull() &
                (to_date(col("discharge_date")) < to_date(col("loading_date")))
            )
            
            failed_count = date_check.count()
            passed = failed_count == 0
            error_msg = f"{failed_count} records have discharge_date before loading_date" if not passed else None
            return passed, date_check, error_msg
        
        return (
            "discharge_after_loading",
            "Discharge date must be after or equal to loading date",
            validator
        )
    
    @staticmethod
    def transaction_date_not_future() -> Tuple[str, str, callable]:
        """
        Validate transaction date is not in the future
        
        Returns:
            Tuple of (name, description, validator_function)
        """
        def validator(df: DataFrame) -> Tuple[bool, DataFrame, Optional[str]]:
            failed_df = df.filter(to_date(col("transaction_date")) > current_date())
            failed_count = failed_df.count()
            passed = failed_count == 0
            error_msg = f"{failed_count} records have transaction_date in the future" if not passed else None
            return passed, failed_df, error_msg
        
        return (
            "transaction_date_not_future",
            "Transaction date must not be in the future",
            validator
        )
    
    @staticmethod
    def required_fields_present() -> Tuple[str, str, callable]:
        """
        Validate all required fields are present (not null)
        
        Returns:
            Tuple of (name, description, validator_function)
        """
        def validator(df: DataFrame) -> Tuple[bool, DataFrame, Optional[str]]:
            required_fields = [
                "ticket_id",
                "transaction_id",
                "transaction_date",
                "carrier_id",
                "vessel_id",
                "fuel_type",
                "quantity",
                "quantity_unit",
                "price_per_unit",
                "total_amount",
                "currency",
                "transaction_status"
            ]
            
            # Check each required field
            conditions = []
            for field in required_fields:
                conditions.append(col(field).isNull())
            
            # Combine all conditions
            combined_condition = conditions[0]
            for cond in conditions[1:]:
                combined_condition = combined_condition | cond
            
            failed_df = df.filter(combined_condition)
            failed_count = failed_df.count()
            passed = failed_count == 0
            error_msg = f"{failed_count} records are missing required fields: {required_fields}" if not passed else None
            return passed, failed_df, error_msg
        
        return (
            "required_fields_present",
            "All required fields must be present (not null)",
            validator
        )

