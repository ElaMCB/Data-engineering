"""
S3 Data Extractor
Extracts data from S3 buckets
"""

from typing import Optional, List, Dict, Any
from pyspark.sql import SparkSession, DataFrame
from src.framework.base_pipeline import Extractor
import structlog

logger = structlog.get_logger()


class S3Extractor(Extractor):
    """
    Extracts data from S3 buckets
    
    Supports:
    - Parquet files
    - CSV files
    - JSON files
    - Partitioned data
    """
    
    def __init__(
        self,
        spark: SparkSession,
        source_path: str,
        file_format: str = "parquet",
        schema: Optional[Any] = None,
        partition_filter: Optional[Dict[str, Any]] = None,
        options: Optional[Dict[str, str]] = None
    ):
        """
        Initialize S3 Extractor
        
        Args:
            spark: SparkSession instance
            source_path: S3 path (s3://bucket/path/)
            file_format: File format (parquet, csv, json)
            schema: Optional schema
            partition_filter: Optional partition filters
            options: Additional read options
        """
        self.spark = spark
        self.source_path = source_path
        self.file_format = file_format.lower()
        self.schema = schema
        self.partition_filter = partition_filter or {}
        self.options = options or {}
        self.logger = logger.bind(component="S3Extractor", path=source_path)
    
    def extract(self) -> DataFrame:
        """
        Extract data from S3
        
        Returns:
            DataFrame with extracted data
        """
        self.logger.info("Extracting data from S3", path=self.source_path, format=self.file_format)
        
        reader = self.spark.read
        
        # Apply schema if provided
        if self.schema:
            reader = reader.schema(self.schema)
        
        # Apply options
        for key, value in self.options.items():
            reader = reader.option(key, value)
        
        # Read based on format
        if self.file_format == "parquet":
            df = reader.parquet(self.source_path)
        elif self.file_format == "csv":
            df = reader.csv(self.source_path, header=True, inferSchema=True)
        elif self.file_format == "json":
            df = reader.json(self.source_path)
        else:
            raise ValueError(f"Unsupported file format: {self.file_format}")
        
        # Apply partition filters
        if self.partition_filter:
            for key, value in self.partition_filter.items():
                df = df.filter(f"{key} = '{value}'")
        
        record_count = df.count()
        self.logger.info("Data extraction completed", record_count=record_count)
        
        return df

