"""
Base ETL Pipeline Framework
Provides abstract base class for building ETL pipelines
"""

import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
from datetime import datetime
import structlog

logger = structlog.get_logger()


class Extractor(ABC):
    """Abstract base class for data extractors"""
    
    @abstractmethod
    def extract(self) -> Any:
        """Extract data from source"""
        pass


class Transformer(ABC):
    """Abstract base class for data transformers"""
    
    @abstractmethod
    def transform(self, data: Any) -> Any:
        """Transform extracted data"""
        pass


class Loader(ABC):
    """Abstract base class for data loaders"""
    
    @abstractmethod
    def load(self, data: Any) -> bool:
        """Load transformed data to target"""
        pass


class ETLPipeline:
    """
    Base ETL Pipeline class that orchestrates Extract, Transform, Load operations
    
    Attributes:
        extractor: Extractor instance for data extraction
        transformer: Transformer instance for data transformation
        loader: Loader instance for data loading
        config: Pipeline configuration dictionary
    """
    
    def __init__(
        self,
        extractor: Extractor,
        transformer: Transformer,
        loader: Loader,
        config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize ETL Pipeline
        
        Args:
            extractor: Extractor instance
            transformer: Transformer instance
            loader: Loader instance
            config: Optional configuration dictionary
        """
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader
        self.config = config or {}
        self.logger = logger.bind(component="ETLPipeline")
        
    def run(self) -> Dict[str, Any]:
        """
        Execute the ETL pipeline
        
        Returns:
            Dictionary with execution results and metrics
        """
        start_time = datetime.now()
        self.logger.info("Starting ETL pipeline execution")
        
        results = {
            "status": "success",
            "start_time": start_time.isoformat(),
            "errors": [],
            "metrics": {}
        }
        
        try:
            # Extract phase
            self.logger.info("Starting extraction phase")
            extract_start = datetime.now()
            raw_data = self.extractor.extract()
            extract_duration = (datetime.now() - extract_start).total_seconds()
            results["metrics"]["extract_duration"] = extract_duration
            results["metrics"]["extract_records"] = self._get_record_count(raw_data)
            self.logger.info("Extraction completed", duration=extract_duration)
            
            # Transform phase
            self.logger.info("Starting transformation phase")
            transform_start = datetime.now()
            transformed_data = self.transformer.transform(raw_data)
            transform_duration = (datetime.now() - transform_start).total_seconds()
            results["metrics"]["transform_duration"] = transform_duration
            results["metrics"]["transform_records"] = self._get_record_count(transformed_data)
            self.logger.info("Transformation completed", duration=transform_duration)
            
            # Load phase
            self.logger.info("Starting load phase")
            load_start = datetime.now()
            load_success = self.loader.load(transformed_data)
            load_duration = (datetime.now() - load_start).total_seconds()
            results["metrics"]["load_duration"] = load_duration
            results["status"] = "success" if load_success else "failed"
            self.logger.info("Load completed", duration=load_duration, success=load_success)
            
            # Calculate total duration
            total_duration = (datetime.now() - start_time).total_seconds()
            results["metrics"]["total_duration"] = total_duration
            results["end_time"] = datetime.now().isoformat()
            
            self.logger.info(
                "ETL pipeline execution completed",
                status=results["status"],
                total_duration=total_duration
            )
            
        except Exception as e:
            self.logger.error("ETL pipeline execution failed", error=str(e), exc_info=True)
            results["status"] = "failed"
            results["errors"].append(str(e))
            results["end_time"] = datetime.now().isoformat()
            raise
        
        return results
    
    def _get_record_count(self, data: Any) -> int:
        """
        Get record count from data (handles different data types)
        
        Args:
            data: Data object (DataFrame, list, etc.)
            
        Returns:
            Record count
        """
        try:
            if hasattr(data, "count"):
                return data.count()
            elif hasattr(data, "__len__"):
                return len(data)
            else:
                return 0
        except Exception:
            return 0
    
    def validate(self) -> bool:
        """
        Validate pipeline configuration and components
        
        Returns:
            True if validation passes
        """
        self.logger.info("Validating pipeline configuration")
        
        if not isinstance(self.extractor, Extractor):
            raise ValueError("Extractor must implement Extractor interface")
        
        if not isinstance(self.transformer, Transformer):
            raise ValueError("Transformer must implement Transformer interface")
        
        if not isinstance(self.loader, Loader):
            raise ValueError("Loader must implement Loader interface")
        
        self.logger.info("Pipeline validation passed")
        return True

