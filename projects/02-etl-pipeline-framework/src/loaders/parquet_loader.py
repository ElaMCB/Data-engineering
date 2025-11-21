"""
Parquet Data Loader
Loads data to Parquet format (typically S3)
"""

from typing import Optional, List, Dict, Any
from pyspark.sql import DataFrame, SparkSession
from src.framework.base_pipeline import Loader
import structlog

logger = structlog.get_logger()


class ParquetLoader(Loader):
    """
    Loads data to Parquet format
    
    Features:
    - Partitioned writes
    - Compression options
    - Write modes (overwrite, append, error)
    - Schema evolution support
    """
    
    def __init__(
        self,
        spark: SparkSession,
        target_path: str,
        mode: str = "overwrite",
        partition_by: Optional[List[str]] = None,
        compression: str = "snappy",
        coalesce_partitions: Optional[int] = None,
        options: Optional[Dict[str, str]] = None
    ):
        """
        Initialize Parquet Loader
        
        Args:
            spark: SparkSession instance
            target_path: Target S3 path (s3://bucket/path/)
            mode: Write mode (overwrite, append, error, ignore)
            partition_by: Columns to partition by
            compression: Compression codec (snappy, gzip, lzo, etc.)
            coalesce_partitions: Number of partitions to coalesce to
            options: Additional write options
        """
        self.spark = spark
        self.target_path = target_path
        self.mode = mode
        self.partition_by = partition_by or []
        self.compression = compression
        self.coalesce_partitions = coalesce_partitions
        self.options = options or {}
        self.logger = logger.bind(component="ParquetLoader", path=target_path)
    
    def load(self, data: DataFrame) -> bool:
        """
        Load data to Parquet format
        
        Args:
            data: DataFrame to write
            
        Returns:
            True if load successful
        """
        self.logger.info(
            "Loading data to Parquet",
            path=self.target_path,
            mode=self.mode,
            partitions=self.partition_by
        )
        
        try:
            # Coalesce if specified (useful for small datasets)
            if self.coalesce_partitions:
                data = data.coalesce(self.coalesce_partitions)
            
            writer = data.write.mode(self.mode)
            
            # Set compression
            writer = writer.option("compression", self.compression)
            
            # Apply additional options
            for key, value in self.options.items():
                writer = writer.option(key, value)
            
            # Apply partitioning
            if self.partition_by:
                writer = writer.partitionBy(*self.partition_by)
            
            # Write data
            writer.parquet(self.target_path)
            
            # Verify write
            record_count = data.count()
            self.logger.info(
                "Data load completed successfully",
                record_count=record_count,
                path=self.target_path
            )
            
            return True
            
        except Exception as e:
            self.logger.error("Data load failed", error=str(e), exc_info=True)
            return False
    
    def load_incremental(
        self,
        data: DataFrame,
        merge_keys: List[str],
        target_path: Optional[str] = None
    ) -> bool:
        """
        Load data with incremental merge logic
        
        Args:
            data: DataFrame to merge
            merge_keys: Columns to use for merge
            target_path: Optional target path override
            
        Returns:
            True if merge successful
        """
        target_path = target_path or self.target_path
        self.logger.info("Performing incremental load with merge", path=target_path)
        
        try:
            # Read existing data if it exists
            try:
                existing_df = self.spark.read.parquet(target_path)
                # Remove records that will be updated
                keys_to_update = data.select(*merge_keys).distinct()
                existing_df = existing_df.join(
                    keys_to_update,
                    on=merge_keys,
                    how="left_anti"
                )
                # Union with new/updated records
                merged_df = existing_df.unionByName(data, allowMissingColumns=True)
            except Exception:
                # Target doesn't exist, use new data
                merged_df = data
            
            # Write merged data
            return self.load(merged_df)
            
        except Exception as e:
            self.logger.error("Incremental load failed", error=str(e), exc_info=True)
            return False

