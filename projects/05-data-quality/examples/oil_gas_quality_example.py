"""
Oil & Gas Ticket Transaction Data Quality Example
Practical example of data quality validation for oil & gas transactions
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType, TimestampType
from pyspark.sql.functions import lit, current_timestamp

from src.framework.quality_validator import QualityValidator
from src.validators.basic_validators import BasicValidators
from src.validators.oil_gas_validators import OilGasValidators
from src.framework.data_profiler import DataProfiler
from src.framework.anomaly_detector import AnomalyDetector
from src.framework.quality_reporter import QualityReporter


def create_sample_data(spark: SparkSession):
    """Create sample oil & gas transaction data for testing"""
    
    # Sample data with some quality issues
    data = [
        # Valid records
        ("TKT-001", "TXN-001", "2024-01-15", "CARRIER-001", "VESSEL-001", 
         "Crude Oil", 50000.0, "Barrels", 75.50, 3775000.0, "USD", "Completed",
         "HOUSTON", "ROTTERDAM", "2024-01-15", "2024-01-25", current_timestamp()),
        
        ("TKT-002", "TXN-002", "2024-01-16", "CARRIER-002", "VESSEL-002",
         "Liquefied Natural Gas", 100000.0, "Cubic Meters", 8.25, 825000.0, "USD", "In Transit",
         "DOHA", "TOKYO", "2024-01-16", "2024-01-30", current_timestamp()),
        
        # Records with quality issues
        ("TKT-003", "TXN-003", "2024-01-17", "CARRIER-003", "VESSEL-003",
         "INVALID_FUEL", 5000.0, "Barrels", 80.0, 400000.0, "USD", "Pending",
         "NYC", "LONDON", "2024-01-17", "2024-01-20", current_timestamp()),  # Invalid fuel type
        
        ("TKT-004", "TXN-004", "2024-01-18", "CARRIER-004", "VESSEL-004",
         "Gasoline", None, "Gallons", 2.50, 50000.0, "USD", "Completed",
         "LA", "SF", "2024-01-18", "2024-01-19", current_timestamp()),  # Null quantity
        
        ("TKT-005", "TXN-005", "2024-01-19", "CARRIER-005", "VESSEL-005",
         "Diesel", 10000.0, "Barrels", 85.0, 800000.0, "USD", "Completed",
         "HOUSTON", "MIAMI", "2024-01-19", "2024-01-15", current_timestamp()),  # Discharge before loading
        
        ("TKT-006", "TXN-006", "2024-01-20", "CARRIER-006", "VESSEL-006",
         "Crude Oil", 20000.0, "Barrels", 75.0, 1500000.0, "USD", "Completed",
         "HOUSTON", "ROTTERDAM", "2024-01-20", "2024-01-25", current_timestamp()),  # Wrong total (should be 1.5M)
        
        ("TKT-007", "TXN-007", "2025-12-31", "CARRIER-007", "VESSEL-007",
         "Jet Fuel", 5000.0, "Barrels", 90.0, 450000.0, "USD", "Pending",
         "JFK", "LHR", "2025-12-31", "2026-01-05", current_timestamp()),  # Future date
    ]
    
    schema = StructType([
        StructField("ticket_id", StringType(), True),
        StructField("transaction_id", StringType(), True),
        StructField("transaction_date", StringType(), True),
        StructField("carrier_id", StringType(), True),
        StructField("vessel_id", StringType(), True),
        StructField("fuel_type", StringType(), True),
        StructField("quantity", DecimalType(18, 4), True),
        StructField("quantity_unit", StringType(), True),
        StructField("price_per_unit", DecimalType(10, 4), True),
        StructField("total_amount", DecimalType(18, 2), True),
        StructField("currency", StringType(), True),
        StructField("transaction_status", StringType(), True),
        StructField("origin_port", StringType(), True),
        StructField("destination_port", StringType(), True),
        StructField("loading_date", StringType(), True),
        StructField("discharge_date", StringType(), True),
        StructField("updated_at", TimestampType(), True),
    ])
    
    df = spark.createDataFrame(data, schema)
    
    # Convert date strings to dates
    from pyspark.sql.functions import to_date
    df = df.withColumn("transaction_date", to_date("transaction_date"))
    df = df.withColumn("loading_date", to_date("loading_date"))
    df = df.withColumn("discharge_date", to_date("discharge_date"))
    
    return df


def run_oil_gas_quality_validation():
    """Run comprehensive data quality validation for oil & gas transactions"""
    
    print("="*70)
    print("Oil & Gas Ticket Transaction Data Quality Validation")
    print("="*70)
    print()
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("OilGasDataQuality") \
        .master("local[*]") \
        .getOrCreate()
    
    # Create sample data
    print("Creating sample data...")
    df = create_sample_data(spark)
    print(f"Total records: {df.count()}")
    print()
    
    # Initialize validator
    validator = QualityValidator(spark=spark)
    
    # Add basic validators
    print("Adding basic validation rules...")
    name, desc, func = BasicValidators.not_null("ticket_id")
    validator.add_rule(name, desc, func)
    
    name, desc, func = BasicValidators.not_null("transaction_id")
    validator.add_rule(name, desc, func)
    
    name, desc, func = BasicValidators.not_null("carrier_id")
    validator.add_rule(name, desc, func)
    
    name, desc, func = BasicValidators.not_null("vessel_id")
    validator.add_rule(name, desc, func)
    
    # Add oil & gas specific validators
    print("Adding oil & gas specific validation rules...")
    name, desc, func = OilGasValidators.valid_fuel_type()
    validator.add_rule(name, desc, func)
    
    name, desc, func = OilGasValidators.quantity_range()
    validator.add_rule(name, desc, func)
    
    name, desc, func = OilGasValidators.price_per_unit_range()
    validator.add_rule(name, desc, func)
    
    name, desc, func = OilGasValidators.quantity_price_consistency()
    validator.add_rule(name, desc, func)
    
    name, desc, func = OilGasValidators.valid_quantity_unit()
    validator.add_rule(name, desc, func)
    
    name, desc, func = OilGasValidators.valid_currency()
    validator.add_rule(name, desc, func)
    
    name, desc, func = OilGasValidators.valid_transaction_status()
    validator.add_rule(name, desc, func)
    
    name, desc, func = OilGasValidators.port_code_format()
    validator.add_rule(name, desc, func)
    
    name, desc, func = OilGasValidators.discharge_after_loading()
    validator.add_rule(name, desc, func)
    
    name, desc, func = OilGasValidators.transaction_date_not_future()
    validator.add_rule(name, desc, func)
    
    name, desc, func = OilGasValidators.required_fields_present()
    validator.add_rule(name, desc, func)
    
    print(f"Total rules added: {len(validator.rules)}")
    print()
    
    # Run validation
    print("Running validation...")
    print("-"*70)
    results = validator.validate(df)
    
    # Print results
    print("\n" + "="*70)
    print("Validation Results Summary")
    print("="*70)
    print(f"Overall Status: {'PASSED' if results['overall_passed'] else 'FAILED'}")
    print(f"Total Records: {results['total_records']:,}")
    print(f"Total Failed: {results['total_failed']:,}")
    print(f"Overall Pass Rate: {results['overall_pass_rate']:.2f}%")
    print(f"Rules Executed: {results['rules_executed']}")
    print(f"Rules Passed: {results['rules_passed']}")
    print(f"Rules Failed: {results['rules_failed']}")
    print(f"Execution Time: {results['execution_time']:.2f} seconds")
    print()
    
    print("="*70)
    print("Detailed Rule Results")
    print("="*70)
    for result in results['results']:
        status = "✓ PASSED" if result['passed'] else "✗ FAILED"
        print(f"{status} | {result['rule_name']}")
        print(f"  Failed: {result['failed_count']:,} records")
        print(f"  Pass Rate: {result['pass_rate']:.2f}%")
        if result['error_message']:
            print(f"  Error: {result['error_message']}")
        print()
    
    # Data profiling
    print("="*70)
    print("Data Profiling")
    print("="*70)
    profiler = DataProfiler(spark=spark)
    profile = profiler.profile(df)
    summary = profiler.summary(profile)
    
    print(f"Total Records: {summary['total_records']:,}")
    print(f"Total Columns: {summary['total_columns']}")
    print(f"Columns with Nulls: {summary['columns_with_nulls']}")
    print(f"Columns with High Null Rate (>10%): {summary['columns_with_high_null_rate']}")
    print()
    
    # Anomaly detection
    print("="*70)
    print("Anomaly Detection")
    print("="*70)
    detector = AnomalyDetector(spark=spark)
    anomalies = detector.detect_anomalies(df, columns=["quantity", "total_amount"], method="zscore", threshold=2.0)
    
    for anomaly in anomalies:
        print(f"Column: {anomaly.column_name}")
        print(f"  Anomalies Found: {anomaly.anomaly_count}")
        print(f"  Anomaly Percentage: {anomaly.anomaly_percentage:.2f}%")
        print(f"  Method: {anomaly.method}")
        print()
    
    # Generate report
    print("="*70)
    print("Generating Quality Report")
    print("="*70)
    reporter = QualityReporter(spark=spark)
    html_report = reporter.generate_report(results)
    
    # Save report
    report_path = "oil_gas_quality_report.html"
    reporter.save_report(results, report_path)
    print(f"Quality report saved to: {report_path}")
    print()
    
    # Get failed records
    print("="*70)
    print("Failed Records Summary")
    print("="*70)
    failed_records = validator.get_failed_records(df)
    failed_count = failed_records.count()
    print(f"Total unique failed records: {failed_count}")
    
    if failed_count > 0:
        print("\nFailed Records:")
        failed_records.select("ticket_id", "fuel_type", "quantity", "total_amount", "transaction_status").show(truncate=False)
    
    print("\n" + "="*70)
    print("Validation Complete!")
    print("="*70)
    
    return results


if __name__ == "__main__":
    run_oil_gas_quality_validation()

