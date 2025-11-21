"""
Unit tests for ETL Pipeline Framework
"""

import pytest
from pyspark.sql import SparkSession
from src.framework.base_pipeline import ETLPipeline, Extractor, Transformer, Loader
from src.framework.spark_utils import SparkSessionManager


class MockExtractor(Extractor):
    """Mock extractor for testing"""
    def __init__(self, spark):
        self.spark = spark
    
    def extract(self):
        data = [(1, "Alice", 25), (2, "Bob", 30), (3, "Charlie", 35)]
        return self.spark.createDataFrame(data, ["id", "name", "age"])


class MockTransformer(Transformer):
    """Mock transformer for testing"""
    def __init__(self, spark):
        self.spark = spark
    
    def transform(self, data):
        return data.withColumn("processed", data["id"] * 2)


class MockLoader(Loader):
    """Mock loader for testing"""
    def __init__(self):
        self.loaded_data = None
    
    def load(self, data):
        self.loaded_data = data
        return True


@pytest.fixture
def spark_session():
    """Create Spark session for testing"""
    spark_manager = SparkSessionManager(app_name="test", master="local[2]")
    spark = spark_manager.create_session()
    yield spark
    spark_manager.stop_session()


def test_pipeline_creation(spark_session):
    """Test pipeline creation"""
    extractor = MockExtractor(spark_session)
    transformer = MockTransformer(spark_session)
    loader = MockLoader()
    
    pipeline = ETLPipeline(extractor, transformer, loader)
    
    assert pipeline.extractor == extractor
    assert pipeline.transformer == transformer
    assert pipeline.loader == loader


def test_pipeline_validation(spark_session):
    """Test pipeline validation"""
    extractor = MockExtractor(spark_session)
    transformer = MockTransformer(spark_session)
    loader = MockLoader()
    
    pipeline = ETLPipeline(extractor, transformer, loader)
    
    # Should not raise exception
    assert pipeline.validate() is True


def test_pipeline_execution(spark_session):
    """Test pipeline execution"""
    extractor = MockExtractor(spark_session)
    transformer = MockTransformer(spark_session)
    loader = MockLoader()
    
    pipeline = ETLPipeline(extractor, transformer, loader)
    
    results = pipeline.run()
    
    assert results["status"] == "success"
    assert "metrics" in results
    assert results["metrics"]["extract_records"] == 3
    assert loader.loaded_data is not None


if __name__ == "__main__":
    pytest.main([__file__])

