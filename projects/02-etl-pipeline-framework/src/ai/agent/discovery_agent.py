"""
Discovery Agent
Discovers and analyzes ETL/ELT pipelines automatically
"""

import os
import ast
import json
from typing import Dict, List, Optional
from pathlib import Path


class DiscoveryAgent:
    """Discovers and analyzes pipelines in codebase"""
    
    def __init__(self, region: str = "us-east-1"):
        self.region = region
        self.codebase_root = self._find_codebase_root()
    
    def discover_pipelines(self) -> Dict:
        """
        Auto-discover all pipelines in codebase
        
        Returns:
            Dictionary with discovered pipelines
        """
        pipelines = []
        pipeline_dir = Path(self.codebase_root) / "pipelines"
        
        if pipeline_dir.exists():
            for file in pipeline_dir.glob("*.py"):
                if file.name != "__init__.py":
                    pipeline_info = self.analyze_pipeline(str(file))
                    pipelines.append(pipeline_info)
        
        return {
            "pipelines": pipelines,
            "total_count": len(pipelines),
            "discovery_time": str(os.path.getmtime(pipeline_dir) if pipeline_dir.exists() else "")
        }
    
    def analyze_pipeline(self, pipeline_path: str) -> Dict:
        """
        Analyze a specific pipeline file
        
        Args:
            pipeline_path: Path to pipeline file
            
        Returns:
            Analysis results
        """
        with open(pipeline_path, 'r') as f:
            code = f.read()
        
        tree = ast.parse(code)
        
        analysis = {
            "path": pipeline_path,
            "name": os.path.basename(pipeline_path),
            "data_sources": [],
            "transformations": [],
            "targets": [],
            "pipeline_type": None,
            "functions": []
        }
        
        # Extract information from AST
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                analysis["functions"].append(node.name)
                
                # Check for ETL/ELT patterns
                if "extract" in node.name.lower():
                    analysis["data_sources"].extend(
                        self._extract_data_sources(node, code)
                    )
                elif "transform" in node.name.lower():
                    analysis["transformations"].extend(
                        self._extract_transformations(node, code)
                    )
                elif "load" in node.name.lower():
                    analysis["targets"].extend(
                        self._extract_targets(node, code)
                    )
        
        # Detect pipeline type
        analysis["pipeline_type"] = self.detect_pipeline_type(analysis)
        
        # Extract imports to understand dependencies
        analysis["dependencies"] = self._extract_imports(tree)
        
        return analysis
    
    def detect_pipeline_type(self, analysis: Dict) -> str:
        """
        Detect if pipeline is ETL or ELT
        
        Args:
            analysis: Pipeline analysis results
            
        Returns:
            'ETL' or 'ELT'
        """
        # Check for SQL-based transformations (ELT)
        transformations = analysis.get("transformations", [])
        code = analysis.get("pipeline_code", "")
        
        # ELT indicators
        elt_indicators = [
            "sql",
            "execute_sql",
            "spark.sql",
            "CREATE TABLE",
            "INSERT INTO",
            "SELECT",
            "TRANSFORM"
        ]
        
        has_sql = any(
            indicator.lower() in str(transformations).lower() or
            indicator.lower() in code.lower()
            for indicator in elt_indicators
        )
        
        # ETL indicators (Python transformations)
        etl_indicators = [
            "transform(",
            "withColumn",
            "DataFrame",
            "pyspark",
            "pandas"
        ]
        
        has_python_transform = any(
            indicator.lower() in code.lower()
            for indicator in etl_indicators
        )
        
        if has_sql and not has_python_transform:
            return "ELT"
        elif has_python_transform:
            return "ETL"
        else:
            # Default to ETL if unclear
            return "ETL"
    
    def _extract_data_sources(self, node: ast.FunctionDef, code: str) -> List[str]:
        """Extract data sources from function"""
        sources = []
        
        # Look for common source patterns
        source_patterns = [
            "s3://",
            "jdbc:",
            "mssql",
            "postgresql",
            "mysql",
            "extract",
            "read",
            "load"
        ]
        
        func_code = ast.get_source_segment(code, node) or ""
        
        for pattern in source_patterns:
            if pattern.lower() in func_code.lower():
                sources.append(pattern)
        
        return list(set(sources))
    
    def _extract_transformations(self, node: ast.FunctionDef, code: str) -> List[str]:
        """Extract transformation logic"""
        transformations = []
        
        func_code = ast.get_source_segment(code, node) or ""
        
        # Look for transformation patterns
        transform_patterns = [
            "withColumn",
            "select",
            "filter",
            "join",
            "groupBy",
            "agg",
            "transform",
            "map",
            "udf"
        ]
        
        for pattern in transform_patterns:
            if pattern.lower() in func_code.lower():
                transformations.append(pattern)
        
        return list(set(transformations))
    
    def _extract_targets(self, node: ast.FunctionDef, code: str) -> List[str]:
        """Extract target systems"""
        targets = []
        
        func_code = ast.get_source_segment(code, node) or ""
        
        target_patterns = [
            "s3://",
            "parquet",
            "delta",
            "write",
            "save",
            "insert",
            "load"
        ]
        
        for pattern in target_patterns:
            if pattern.lower() in func_code.lower():
                targets.append(pattern)
        
        return list(set(targets))
    
    def _extract_imports(self, tree: ast.AST) -> List[str]:
        """Extract import statements"""
        imports = []
        
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    imports.append(alias.name)
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    imports.append(node.module)
        
        return imports
    
    def _find_codebase_root(self) -> str:
        """Find the root of the codebase"""
        current = Path(__file__).resolve()
        
        # Look for common root indicators
        while current != current.parent:
            if (current / "pipelines").exists() or (current / "src").exists():
                return str(current)
            current = current.parent
        
        return str(Path.cwd())

