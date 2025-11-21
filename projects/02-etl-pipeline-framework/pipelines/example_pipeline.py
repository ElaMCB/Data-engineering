"""
Example ETL Pipeline
Demonstrates usage of the ETL framework
"""

from src.framework.base_pipeline import ETLPipeline
from src.framework.spark_utils import SparkSessionManager
from src.extractors.s3_extractor import S3Extractor
from src.transformers.data_transformer import DataTransformer
from src.loaders.parquet_loader import ParquetLoader
import yaml
import os


def load_config(config_path: str = "config/pipeline_config.yaml") -> dict:
    """Load pipeline configuration from YAML file"""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def create_example_pipeline(config: dict = None) -> ETLPipeline:
    """
    Create an example ETL pipeline
    
    Args:
        config: Optional configuration dictionary
        
    Returns:
        Configured ETLPipeline instance
    """
    # Initialize Spark
    spark_manager = SparkSessionManager(
        app_name="Example ETL Pipeline",
        config=config.get("spark", {}) if config else {}
    )
    spark = spark_manager.create_session()
    spark_manager.optimize_for_etl(spark)
    
    # Create extractor
    extractor_config = config.get("extract", {}) if config else {}
    extractor = S3Extractor(
        spark=spark,
        source_path=extractor_config.get("source_path", "s3://bucket/input/"),
        file_format=extractor_config.get("file_format", "parquet"),
        partition_filter=extractor_config.get("partition_filter")
    )
    
    # Create transformer
    transformer_config = config.get("transform", {}) if config else {}
    transformer = DataTransformer(
        spark=spark,
        transformations=transformer_config.get("transformations", []),
        column_mappings=transformer_config.get("column_mappings", {}),
        data_quality_rules=transformer_config.get("data_quality_rules", [])
    )
    
    # Create loader
    loader_config = config.get("load", {}) if config else {}
    loader = ParquetLoader(
        spark=spark,
        target_path=loader_config.get("target_path", "s3://bucket/output/"),
        mode=loader_config.get("mode", "overwrite"),
        partition_by=loader_config.get("partition_by", []),
        compression=loader_config.get("compression", "snappy")
    )
    
    # Create and return pipeline
    pipeline = ETLPipeline(
        extractor=extractor,
        transformer=transformer,
        loader=loader,
        config=config
    )
    
    return pipeline


def run_example_pipeline():
    """Run the example pipeline"""
    # Load configuration
    config_path = os.path.join(
        os.path.dirname(__file__),
        "..",
        "config",
        "pipeline_config.yaml"
    )
    
    if os.path.exists(config_path):
        config = load_config(config_path)
    else:
        config = {}
    
    # Create pipeline
    pipeline = create_example_pipeline(config)
    
    # Validate pipeline
    pipeline.validate()
    
    # Run pipeline
    results = pipeline.run()
    
    print("Pipeline execution completed!")
    print(f"Status: {results['status']}")
    print(f"Total Duration: {results['metrics'].get('total_duration', 0):.2f} seconds")
    print(f"Records Processed: {results['metrics'].get('transform_records', 0)}")
    
    return results


if __name__ == "__main__":
    run_example_pipeline()

