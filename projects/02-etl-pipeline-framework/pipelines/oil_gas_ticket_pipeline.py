"""
Oil & Gas Carrier Ticket Transaction Pipeline
Production pipeline for processing high-volume ticket transactions
"""

from src.framework.base_pipeline import ETLPipeline
from src.framework.spark_utils import SparkSessionManager
from src.framework.cdc_handler import CDCHandler
from src.extractors.s3_extractor import S3Extractor
from src.transformers.data_transformer import DataTransformer
from src.loaders.parquet_loader import ParquetLoader
import yaml
import os
from datetime import datetime


def load_config(config_path: str = "config/oil_gas_ticket_transactions.yaml") -> dict:
    """Load pipeline configuration from YAML file"""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def create_oil_gas_pipeline(config: dict = None) -> ETLPipeline:
    """
    Create ETL pipeline for oil & gas ticket transactions
    
    Args:
        config: Configuration dictionary
        
    Returns:
        Configured ETLPipeline instance
    """
    # Initialize Spark with optimizations for large transaction volumes
    spark_manager = SparkSessionManager(
        app_name="Oil & Gas Ticket Transaction Pipeline",
        config=config.get("spark", {}) if config else {}
    )
    spark = spark_manager.create_session()
    spark_manager.optimize_for_etl(spark)
    
    # Extract configuration
    extract_config = config.get("extract", {}) if config else {}
    extractor = S3Extractor(
        spark=spark,
        source_path=extract_config.get("source_path", "s3://oil-gas-data-lake/raw-transactions/tickets/"),
        file_format=extract_config.get("file_format", "parquet"),
        partition_filter=extract_config.get("partition_filter"),
        options=extract_config.get("options", {})
    )
    
    # Transform configuration
    transform_config = config.get("transform", {}) if config else {}
    transformer = DataTransformer(
        spark=spark,
        transformations=transform_config.get("transformations", []),
        column_mappings=transform_config.get("column_mappings", {}),
        data_quality_rules=transform_config.get("data_quality_rules", [])
    )
    
    # Load configuration
    load_config = config.get("load", {}) if config else {}
    loader = ParquetLoader(
        spark=spark,
        target_path=load_config.get("target_path", "s3://oil-gas-data-lake/processed-transactions/tickets/"),
        mode=load_config.get("mode", "overwrite"),
        partition_by=load_config.get("partition_by", []),
        compression=load_config.get("compression", "snappy"),
        options=load_config.get("options", {})
    )
    
    # Create pipeline
    pipeline = ETLPipeline(
        extractor=extractor,
        transformer=transformer,
        loader=loader,
        config=config
    )
    
    return pipeline, spark


def run_oil_gas_pipeline(incremental: bool = False):
    """
    Run the oil & gas ticket transaction pipeline
    
    Args:
        incremental: If True, use CDC for incremental processing
    """
    # Load configuration
    config_path = os.path.join(
        os.path.dirname(__file__),
        "..",
        "config",
        "oil_gas_ticket_transactions.yaml"
    )
    
    if os.path.exists(config_path):
        config = load_config(config_path)
    else:
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    if incremental:
        # Run incremental pipeline with CDC
        return run_incremental_pipeline(config)
    else:
        # Run full pipeline
        return run_full_pipeline(config)


def run_full_pipeline(config: dict):
    """Run full ETL pipeline"""
    print("="*70)
    print("Oil & Gas Ticket Transaction Pipeline - Full Load")
    print("="*70)
    print(f"Start Time: {datetime.now()}")
    print()
    
    # Create pipeline
    pipeline, spark = create_oil_gas_pipeline(config)
    
    # Validate
    print("Validating pipeline configuration...")
    pipeline.validate()
    print("✓ Validation passed\n")
    
    # Run pipeline
    print("Starting pipeline execution...")
    results = pipeline.run()
    
    # Print results
    print("\n" + "="*70)
    print("Pipeline Execution Results")
    print("="*70)
    print(f"Status: {results['status']}")
    print(f"Start Time: {results['start_time']}")
    print(f"End Time: {results['end_time']}")
    print(f"\nMetrics:")
    for key, value in results['metrics'].items():
        if isinstance(value, (int, float)):
            print(f"  {key}: {value:,}" if isinstance(value, int) else f"  {key}: {value:,.2f}")
        else:
            print(f"  {key}: {value}")
    
    if results.get("errors"):
        print(f"\nErrors: {results['errors']}")
    
    # Calculate some business metrics
    if results['status'] == 'success' and results['metrics'].get('transform_records', 0) > 0:
        print("\n" + "="*70)
        print("Business Metrics Summary")
        print("="*70)
        print(f"Total Transactions Processed: {results['metrics'].get('transform_records', 0):,}")
        print(f"Processing Rate: {results['metrics'].get('transform_records', 0) / max(results['metrics'].get('total_duration', 1), 1):,.0f} records/second")
    
    return results


def run_incremental_pipeline(config: dict):
    """Run incremental pipeline with CDC"""
    print("="*70)
    print("Oil & Gas Ticket Transaction Pipeline - Incremental Load (CDC)")
    print("="*70)
    print(f"Start Time: {datetime.now()}")
    print()
    
    # Initialize Spark
    spark_manager = SparkSessionManager(
        app_name="Oil & Gas Ticket Transaction Pipeline - CDC"
    )
    spark = spark_manager.create_session()
    spark_manager.optimize_for_etl(spark)
    
    # CDC configuration
    cdc_config = config.get("cdc", {})
    extract_config = config.get("extract", {})
    load_config = config.get("load", {})
    
    # Initialize CDC Handler
    cdc_handler = CDCHandler(
        spark=spark,
        source_table="source_tickets",
        target_table="target_tickets",
        key_columns=cdc_config.get("key_columns", ["ticket_id", "transaction_id"]),
        timestamp_column=cdc_config.get("timestamp_column", "updated_at"),
        source_path=extract_config.get("source_path"),
        target_path=load_config.get("target_path")
    )
    
    # Run incremental load
    print("Starting incremental load with CDC...")
    results = cdc_handler.run_incremental_load(
        lookback_hours=cdc_config.get("lookback_hours", 24),
        merge_mode=cdc_config.get("merge_mode", "upsert")
    )
    
    # Print results
    print("\n" + "="*70)
    print("Incremental Load Results")
    print("="*70)
    print(f"Status: {results['status']}")
    print(f"Records Processed: {results['records_processed']:,}")
    print(f"Duration: {results.get('duration', 0):.2f} seconds")
    
    if results.get("errors"):
        print(f"\nErrors: {results['errors']}")
    
    return results


if __name__ == "__main__":
    import sys
    
    # Check for incremental flag
    incremental = "--incremental" in sys.argv or "-i" in sys.argv
    
    try:
        run_oil_gas_pipeline(incremental=incremental)
    except Exception as e:
        print(f"\n✗ Pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

