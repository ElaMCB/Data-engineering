"""
Basic Data Quality Validation Example
Simple example of using the data quality framework
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from src.framework.quality_validator import QualityValidator
from src.validators.basic_validators import BasicValidators


def create_sample_data(spark: SparkSession):
    """Create sample data with quality issues"""
    data = [
        ("1", "Alice", 25, "active"),
        ("2", "Bob", 30, "active"),
        ("3", None, 35, "inactive"),  # Null name
        ("4", "Charlie", -5, "active"),  # Negative age
        ("5", "Diana", 150, "active"),  # Age too high
        ("6", "Eve", 28, "invalid"),  # Invalid status
    ]
    
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("status", StringType(), True),
    ])
    
    return spark.createDataFrame(data, schema)


def run_basic_validation():
    """Run basic data quality validation"""
    
    print("="*70)
    print("Basic Data Quality Validation Example")
    print("="*70)
    print()
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("BasicDataQuality") \
        .master("local[*]") \
        .getOrCreate()
    
    # Create sample data
    df = create_sample_data(spark)
    print("Sample Data:")
    df.show()
    print()
    
    # Initialize validator
    validator = QualityValidator(spark=spark)
    
    # Add validation rules
    name, desc, func = BasicValidators.not_null("name")
    validator.add_rule(name, desc, func)
    
    name, desc, func = BasicValidators.value_range("age", min_val=0, max_val=120)
    validator.add_rule(name, desc, func)
    
    name, desc, func = BasicValidators.allowed_values("status", ["active", "inactive", "pending"])
    validator.add_rule(name, desc, func)
    
    # Run validation
    print("Running validation...")
    results = validator.validate(df)
    
    # Print results
    print("\n" + "="*70)
    print("Validation Results")
    print("="*70)
    print(f"Overall Status: {'PASSED' if results['overall_passed'] else 'FAILED'}")
    print(f"Total Records: {results['total_records']}")
    print(f"Pass Rate: {results['overall_pass_rate']:.2f}%")
    print()
    
    for result in results['results']:
        status = "✓" if result['passed'] else "✗"
        print(f"{status} {result['rule_name']}: {result['failed_count']} failed")
    
    print("\n" + "="*70)
    print("Failed Records")
    print("="*70)
    failed_records = validator.get_failed_records(df)
    failed_records.show()
    
    return results


if __name__ == "__main__":
    run_basic_validation()

