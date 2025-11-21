#!/usr/bin/env python3
"""
ETL Pipeline Runner Script
Main entry point for running ETL pipelines
"""

import sys
import os
import argparse
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from pipelines.example_pipeline import run_example_pipeline, create_example_pipeline, load_config


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Run ETL Pipeline")
    parser.add_argument(
        "--config",
        type=str,
        default="config/pipeline_config.yaml",
        help="Path to pipeline configuration file"
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Only validate pipeline configuration, don't run"
    )
    
    args = parser.parse_args()
    
    # Load configuration
    config_path = os.path.join(project_root, args.config)
    if os.path.exists(config_path):
        config = load_config(config_path)
    else:
        print(f"Warning: Config file not found at {config_path}, using defaults")
        config = {}
    
    # Create pipeline
    pipeline = create_example_pipeline(config)
    
    # Validate
    try:
        pipeline.validate()
        print("✓ Pipeline validation passed")
    except Exception as e:
        print(f"✗ Pipeline validation failed: {e}")
        sys.exit(1)
    
    if args.validate_only:
        print("Validation complete. Exiting.")
        sys.exit(0)
    
    # Run pipeline
    try:
        results = pipeline.run()
        
        if results["status"] == "success":
            print("\n" + "="*50)
            print("Pipeline Execution Summary")
            print("="*50)
            print(f"Status: {results['status']}")
            print(f"Start Time: {results['start_time']}")
            print(f"End Time: {results['end_time']}")
            print(f"\nMetrics:")
            for key, value in results['metrics'].items():
                print(f"  {key}: {value}")
            
            if results.get("errors"):
                print(f"\nErrors: {results['errors']}")
            
            sys.exit(0)
        else:
            print(f"\nPipeline failed: {results.get('errors', 'Unknown error')}")
            sys.exit(1)
            
    except Exception as e:
        print(f"\nPipeline execution failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

