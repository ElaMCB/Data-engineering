"""
RDS Data Extractor
Extracts data from RDS databases
"""

from typing import Optional, Dict, Any
from pyspark.sql import SparkSession, DataFrame
from src.framework.base_pipeline import Extractor
import structlog

logger = structlog.get_logger()


class RDSExtractor(Extractor):
    """
    Extracts data from RDS databases using JDBC
    
    Supports:
    - PostgreSQL
    - MySQL
    - SQL Server
    - Oracle
    """
    
    def __init__(
        self,
        spark: SparkSession,
        jdbc_url: str,
        table: str,
        user: str,
        password: str,
        driver: str = "org.postgresql.Driver",
        partition_column: Optional[str] = None,
        lower_bound: Optional[int] = None,
        upper_bound: Optional[int] = None,
        num_partitions: int = 10,
        fetch_size: int = 10000,
        query: Optional[str] = None
    ):
        """
        Initialize RDS Extractor
        
        Args:
            spark: SparkSession instance
            jdbc_url: JDBC connection URL
            table: Table name
            user: Database username
            password: Database password
            driver: JDBC driver class
            partition_column: Column to partition reads (for parallelization)
            lower_bound: Lower bound for partitioning
            upper_bound: Upper bound for partitioning
            num_partitions: Number of partitions
            fetch_size: Fetch size for JDBC reads
            query: Optional custom SQL query
        """
        self.spark = spark
        self.jdbc_url = jdbc_url
        self.table = table
        self.user = user
        self.password = password
        self.driver = driver
        self.partition_column = partition_column
        self.lower_bound = lower_bound
        self.upper_bound = upper_bound
        self.num_partitions = num_partitions
        self.fetch_size = fetch_size
        self.query = query
        self.logger = logger.bind(component="RDSExtractor", table=table)
    
    def extract(self) -> DataFrame:
        """
        Extract data from RDS
        
        Returns:
            DataFrame with extracted data
        """
        self.logger.info("Extracting data from RDS", table=self.table)
        
        reader = self.spark.read \
            .format("jdbc") \
            .option("url", self.jdbc_url) \
            .option("dbtable", self.query or self.table) \
            .option("user", self.user) \
            .option("password", self.password) \
            .option("driver", self.driver) \
            .option("fetchsize", self.fetch_size)
        
        # Add partitioning options if provided
        if self.partition_column and self.lower_bound is not None and self.upper_bound is not None:
            reader = reader \
                .option("partitionColumn", self.partition_column) \
                .option("lowerBound", self.lower_bound) \
                .option("upperBound", self.upper_bound) \
                .option("numPartitions", self.num_partitions)
        
        df = reader.load()
        
        record_count = df.count()
        self.logger.info("Data extraction completed", record_count=record_count)
        
        return df

