"""
Unit tests for Data Quality Framework
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from src.framework.quality_validator import QualityValidator
from src.validators.basic_validators import BasicValidators


@pytest.fixture
def spark_session():
    """Create Spark session for testing"""
    spark = SparkSession.builder \
        .appName("test") \
        .master("local[2]") \
        .getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def sample_dataframe(spark_session):
    """Create sample DataFrame for testing"""
    data = [
        ("1", "Alice", 25),
        ("2", "Bob", 30),
        ("3", None, 35),  # Null name
    ]
    
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
    ])
    
    return spark_session.createDataFrame(data, schema)


def test_validator_creation(spark_session):
    """Test validator creation"""
    validator = QualityValidator(spark=spark_session)
    assert validator is not None
    assert len(validator.rules) == 0


def test_add_rule(spark_session):
    """Test adding validation rule"""
    validator = QualityValidator(spark=spark_session)
    name, desc, func = BasicValidators.not_null("name")
    validator.add_rule(name, desc, func)
    
    assert len(validator.rules) == 1
    assert validator.rules[0].name == name


def test_validation_execution(sample_dataframe, spark_session):
    """Test validation execution"""
    validator = QualityValidator(spark=spark_session)
    name, desc, func = BasicValidators.not_null("name")
    validator.add_rule(name, desc, func)
    
    results = validator.validate(sample_dataframe)
    
    assert "overall_passed" in results
    assert "total_records" in results
    assert results["total_records"] == 3


def test_not_null_validation(sample_dataframe, spark_session):
    """Test not null validation"""
    validator = QualityValidator(spark=spark_session)
    name, desc, func = BasicValidators.not_null("name")
    validator.add_rule(name, desc, func)
    
    results = validator.validate(sample_dataframe)
    
    # Should fail because one record has null name
    assert results["overall_passed"] == False
    assert results["total_failed"] > 0


if __name__ == "__main__":
    pytest.main([__file__])

