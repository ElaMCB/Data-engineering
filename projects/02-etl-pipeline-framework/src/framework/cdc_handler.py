"""
Change Data Capture (CDC) Handler
Implements incremental loading with change detection
"""

from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, max as spark_max, when, lit
import structlog

logger = structlog.get_logger()


class CDCHandler:
    """
    Handles Change Data Capture for incremental data loading
    
    Supports:
    - Timestamp-based incremental loading
    - Key-based change detection
    - Merge operations (upsert)
    - Full refresh capability
    """
    
    def __init__(
        self,
        spark: SparkSession,
        source_table: str,
        target_table: str,
        key_columns: List[str],
        timestamp_column: str = "updated_at",
        source_path: Optional[str] = None,
        target_path: Optional[str] = None
    ):
        """
        Initialize CDC Handler
        
        Args:
            spark: SparkSession instance
            source_table: Source table name or path
            target_table: Target table name or path
            key_columns: List of columns that uniquely identify records
            timestamp_column: Column name for tracking updates
            source_path: Optional S3 path for source data
            target_path: Optional S3 path for target data
        """
        self.spark = spark
        self.source_table = source_table
        self.target_table = target_table
        self.key_columns = key_columns
        self.timestamp_column = timestamp_column
        self.source_path = source_path
        self.target_path = target_path
        self.logger = logger.bind(component="CDCHandler")
    
    def get_last_processed_timestamp(self) -> Optional[datetime]:
        """
        Get the last processed timestamp from target table
        
        Returns:
            Last processed timestamp or None
        """
        try:
            if self.target_path:
                # Read from Parquet
                target_df = self.spark.read.parquet(self.target_path)
            else:
                # Read from table
                target_df = self.spark.table(self.target_table)
            
            if target_df.count() == 0:
                return None
            
            max_timestamp = target_df.agg(
                spark_max(col(self.timestamp_column))
            ).collect()[0][0]
            
            return max_timestamp
            
        except Exception as e:
            self.logger.warning("Could not get last timestamp, using None", error=str(e))
            return None
    
    def extract_incremental_data(
        self,
        last_timestamp: Optional[datetime] = None,
        lookback_hours: int = 24
    ) -> DataFrame:
        """
        Extract incremental data since last processed timestamp
        
        Args:
            last_timestamp: Last processed timestamp
            lookback_hours: Hours to look back if no timestamp available
            
        Returns:
            DataFrame with incremental data
        """
        self.logger.info("Extracting incremental data", last_timestamp=last_timestamp)
        
        if self.source_path:
            source_df = self.spark.read.parquet(self.source_path)
        else:
            source_df = self.spark.table(self.source_table)
        
        if last_timestamp:
            # Filter by timestamp
            incremental_df = source_df.filter(
                col(self.timestamp_column) > lit(last_timestamp)
            )
        else:
            # Use lookback window
            cutoff_time = datetime.now() - timedelta(hours=lookback_hours)
            incremental_df = source_df.filter(
                col(self.timestamp_column) > lit(cutoff_time)
            )
        
        record_count = incremental_df.count()
        self.logger.info("Incremental data extracted", record_count=record_count)
        
        return incremental_df
    
    def process_incremental_changes(
        self,
        incremental_df: DataFrame,
        merge_mode: str = "upsert"
    ) -> DataFrame:
        """
        Process incremental changes and prepare for merge
        
        Args:
            incremental_df: DataFrame with new/changed records
            merge_mode: Merge mode (upsert, insert_only, update_only)
            
        Returns:
            Processed DataFrame ready for merge
        """
        self.logger.info("Processing incremental changes", merge_mode=merge_mode)
        
        # Add metadata columns
        processed_df = incremental_df.withColumn(
            "cdc_timestamp",
            lit(datetime.now())
        ).withColumn(
            "cdc_operation",
            lit("UPSERT")
        )
        
        return processed_df
    
    def merge_data(
        self,
        incremental_df: DataFrame,
        target_path: Optional[str] = None
    ) -> bool:
        """
        Merge incremental data into target using upsert logic
        
        Args:
            incremental_df: DataFrame with incremental changes
            target_path: Optional target path override
            
        Returns:
            True if merge successful
        """
        target_path = target_path or self.target_path
        self.logger.info("Merging data into target", target_path=target_path)
        
        try:
            # Read existing target data
            if target_path:
                try:
                    existing_df = self.spark.read.parquet(target_path)
                except Exception:
                    # Target doesn't exist, create new
                    existing_df = None
            else:
                try:
                    existing_df = self.spark.table(self.target_table)
                except Exception:
                    existing_df = None
            
            if existing_df is None or existing_df.count() == 0:
                # First load - just write incremental data
                self.logger.info("First load - writing all incremental data")
                if target_path:
                    incremental_df.write.mode("overwrite").parquet(target_path)
                else:
                    incremental_df.write.mode("overwrite").saveAsTable(self.target_table)
            else:
                # Merge logic: remove old records with same keys, then append new
                # Create condition for key matching
                key_conditions = [
                    existing_df[col_name] == incremental_df[col_name]
                    for col_name in self.key_columns
                ]
                condition = key_conditions[0]
                for cond in key_conditions[1:]:
                    condition = condition & cond
                
                # Remove existing records that will be updated
                keys_to_update = incremental_df.select(*self.key_columns).distinct()
                existing_df = existing_df.join(
                    keys_to_update,
                    on=self.key_columns,
                    how="left_anti"
                )
                
                # Union with new/updated records
                merged_df = existing_df.unionByName(incremental_df, allowMissingColumns=True)
                
                # Write merged data
                if target_path:
                    merged_df.write.mode("overwrite").parquet(target_path)
                else:
                    merged_df.write.mode("overwrite").saveAsTable(self.target_table)
            
            self.logger.info("Data merge completed successfully")
            return True
            
        except Exception as e:
            self.logger.error("Data merge failed", error=str(e), exc_info=True)
            return False
    
    def run_incremental_load(
        self,
        lookback_hours: int = 24,
        merge_mode: str = "upsert"
    ) -> Dict[str, Any]:
        """
        Run complete incremental load process
        
        Args:
            lookback_hours: Hours to look back if no previous timestamp
            merge_mode: Merge mode
            
        Returns:
            Dictionary with execution results
        """
        start_time = datetime.now()
        self.logger.info("Starting incremental load process")
        
        results = {
            "status": "success",
            "start_time": start_time.isoformat(),
            "records_processed": 0,
            "errors": []
        }
        
        try:
            # Get last processed timestamp
            last_timestamp = self.get_last_processed_timestamp()
            
            # Extract incremental data
            incremental_df = self.extract_incremental_data(
                last_timestamp=last_timestamp,
                lookback_hours=lookback_hours
            )
            
            record_count = incremental_df.count()
            results["records_processed"] = record_count
            
            if record_count == 0:
                self.logger.info("No new data to process")
                results["status"] = "no_changes"
            else:
                # Process changes
                processed_df = self.process_incremental_changes(
                    incremental_df,
                    merge_mode=merge_mode
                )
                
                # Merge into target
                merge_success = self.merge_data(processed_df)
                
                if not merge_success:
                    results["status"] = "failed"
                    results["errors"].append("Merge operation failed")
            
            results["end_time"] = datetime.now().isoformat()
            results["duration"] = (datetime.now() - start_time).total_seconds()
            
            self.logger.info(
                "Incremental load completed",
                status=results["status"],
                records=record_count,
                duration=results["duration"]
            )
            
        except Exception as e:
            self.logger.error("Incremental load failed", error=str(e), exc_info=True)
            results["status"] = "failed"
            results["errors"].append(str(e))
            results["end_time"] = datetime.now().isoformat()
        
        return results

