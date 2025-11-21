"""
Spark Utilities
Helper functions for Spark session management and optimization
"""

import os
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
import structlog

logger = structlog.get_logger()


class SparkSessionManager:
    """
    Manages Spark session creation and configuration for optimal performance
    """
    
    def __init__(
        self,
        app_name: str = "ETL Pipeline",
        config: Optional[Dict[str, Any]] = None,
        master: Optional[str] = None
    ):
        """
        Initialize Spark Session Manager
        
        Args:
            app_name: Application name
            config: Additional Spark configuration
            master: Spark master URL (defaults to local or EMR cluster)
        """
        self.app_name = app_name
        self.config = config or {}
        self.master = master or self._get_default_master()
        self.spark: Optional[SparkSession] = None
        self.logger = logger.bind(component="SparkSessionManager")
    
    def _get_default_master(self) -> str:
        """Get default Spark master based on environment"""
        # Check if running on EMR
        if os.environ.get("AWS_EMR_CLUSTER_ID"):
            return "yarn"
        # Check if running on local
        return "local[*]"
    
    def create_session(self) -> SparkSession:
        """
        Create and configure Spark session with optimizations
        
        Returns:
            Configured SparkSession
        """
        if self.spark:
            return self.spark
        
        self.logger.info("Creating Spark session", app_name=self.app_name, master=self.master)
        
        builder = SparkSession.builder.appName(self.app_name)
        
        # Default optimizations for ETL workloads
        default_config = {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.sql.execution.arrow.pyspark.enabled": "true",
            "spark.sql.parquet.compression.codec": "snappy",
            "spark.sql.parquet.mergeSchema": "true",
            "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "2",
            "spark.speculation": "true",
            "spark.sql.files.maxPartitionBytes": "134217728",  # 128MB
            "spark.sql.files.openCostInBytes": "4194304",  # 4MB
        }
        
        # Merge with user config
        merged_config = {**default_config, **self.config}
        
        for key, value in merged_config.items():
            builder = builder.config(key, value)
        
        self.spark = builder.getOrCreate()
        self.logger.info("Spark session created successfully")
        
        return self.spark
    
    def get_session(self) -> SparkSession:
        """
        Get existing Spark session or create new one
        
        Returns:
            SparkSession
        """
        if not self.spark:
            return self.create_session()
        return self.spark
    
    def stop_session(self):
        """Stop Spark session"""
        if self.spark:
            self.logger.info("Stopping Spark session")
            self.spark.stop()
            self.spark = None
    
    def optimize_for_etl(self, spark: SparkSession):
        """
        Apply ETL-specific optimizations to Spark session
        
        Args:
            spark: SparkSession instance
        """
        self.logger.info("Applying ETL optimizations")
        
        # Set shuffle partitions based on data size
        spark.conf.set("spark.sql.shuffle.partitions", "200")
        
        # Enable dynamic partition pruning
        spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
        
        # Optimize for large datasets
        spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "134217728")
        
        self.logger.info("ETL optimizations applied")
    
    def read_parquet_optimized(
        self,
        path: str,
        schema: Optional[StructType] = None,
        partition_filter: Optional[Dict[str, Any]] = None
    ):
        """
        Read Parquet files with optimizations
        
        Args:
            path: Path to Parquet files
            schema: Optional schema
            partition_filter: Optional partition filter
            
        Returns:
            DataFrame
        """
        spark = self.get_session()
        
        reader = spark.read
        
        if schema:
            reader = reader.schema(schema)
        
        df = reader.parquet(path)
        
        # Apply partition filter if provided
        if partition_filter:
            for key, value in partition_filter.items():
                df = df.filter(f"{key} = '{value}'")
        
        return df
    
    def write_parquet_optimized(
        self,
        df,
        path: str,
        mode: str = "overwrite",
        partition_by: Optional[list] = None,
        compression: str = "snappy"
    ):
        """
        Write DataFrame to Parquet with optimizations
        
        Args:
            df: DataFrame to write
            path: Output path
            mode: Write mode (overwrite, append, etc.)
            partition_by: Columns to partition by
            compression: Compression codec
        """
        writer = df.write.mode(mode)
        
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        
        writer.option("compression", compression) \
              .option("parquet.block.size", "134217728") \
              .parquet(path)
        
        self.logger.info(
            "Parquet write completed",
            path=path,
            mode=mode,
            partitions=partition_by
        )

