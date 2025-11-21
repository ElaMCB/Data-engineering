"""
Incremental ETL Pipeline with CDC
Demonstrates incremental loading with Change Data Capture
"""

from src.framework.spark_utils import SparkSessionManager
from src.framework.cdc_handler import CDCHandler
from src.extractors.s3_extractor import S3Extractor
from src.transformers.data_transformer import DataTransformer
from src.loaders.parquet_loader import ParquetLoader
import os


def run_incremental_pipeline():
    """
    Run incremental ETL pipeline with CDC
    
    This pipeline:
    1. Extracts only new/changed data since last run
    2. Transforms the incremental data
    3. Merges into target using CDC logic
    """
    # Initialize Spark
    spark_manager = SparkSessionManager(
        app_name="Incremental ETL Pipeline with CDC"
    )
    spark = spark_manager.create_session()
    spark_manager.optimize_for_etl(spark)
    
    # Configuration
    source_path = os.getenv("SOURCE_PATH", "s3://bucket/source-data/")
    target_path = os.getenv("TARGET_PATH", "s3://bucket/target-data/")
    key_columns = ["id", "customer_id"]  # Composite key for deduplication
    timestamp_column = "updated_at"
    
    # Initialize CDC Handler
    cdc_handler = CDCHandler(
        spark=spark,
        source_table="source_table",  # Or use source_path
        target_table="target_table",  # Or use target_path
        key_columns=key_columns,
        timestamp_column=timestamp_column,
        source_path=source_path,
        target_path=target_path
    )
    
    # Run incremental load
    print("Starting incremental load with CDC...")
    results = cdc_handler.run_incremental_load(
        lookback_hours=24,  # Look back 24 hours if no previous timestamp
        merge_mode="upsert"
    )
    
    print("\n" + "="*50)
    print("Incremental Load Results")
    print("="*50)
    print(f"Status: {results['status']}")
    print(f"Records Processed: {results['records_processed']}")
    print(f"Duration: {results.get('duration', 0):.2f} seconds")
    
    if results.get("errors"):
        print(f"Errors: {results['errors']}")
    
    return results


def run_full_pipeline_with_cdc():
    """
    Run full ETL pipeline with CDC integration
    
    This demonstrates a complete pipeline that:
    1. Extracts data
    2. Transforms data
    3. Loads with CDC merge logic
    """
    # Initialize Spark
    spark_manager = SparkSessionManager(
        app_name="Full ETL Pipeline with CDC"
    )
    spark = spark_manager.create_session()
    spark_manager.optimize_for_etl(spark)
    
    # Configuration
    source_path = os.getenv("SOURCE_PATH", "s3://bucket/source-data/")
    target_path = os.getenv("TARGET_PATH", "s3://bucket/target-data/")
    
    # Extract
    extractor = S3Extractor(
        spark=spark,
        source_path=source_path,
        file_format="parquet"
    )
    raw_data = extractor.extract()
    
    # Transform
    transformer = DataTransformer(
        spark=spark,
        column_mappings={
            "user_id": "customer_id",
            "created": "created_at"
        }
    )
    transformed_data = transformer.transform(raw_data)
    
    # Load with incremental merge
    loader = ParquetLoader(
        spark=spark,
        target_path=target_path,
        mode="overwrite",  # Will use incremental merge internally
        partition_by=["year", "month"]
    )
    
    # Use incremental load if we have merge keys
    merge_keys = ["id", "customer_id"]
    success = loader.load_incremental(
        data=transformed_data,
        merge_keys=merge_keys
    )
    
    if success:
        print("Pipeline completed successfully!")
    else:
        print("Pipeline failed!")
    
    return success


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "incremental":
        run_incremental_pipeline()
    else:
        run_full_pipeline_with_cdc()

