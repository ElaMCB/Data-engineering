"""
Lambda handler for AI Testing Agent
Main orchestrator function
"""

import json
import os
import sys

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../src'))

from src.ai.agent.testing_agent import TestingAgent


def handler(event, context):
    """
    Lambda handler for autonomous testing agent
    
    Event format:
    {
        "pipeline_path": "optional/path/to/pipeline.py",
        "pipeline_type": "ETL" or "ELT" (optional),
        "test_scope": "quick" | "comprehensive" | "full"
    }
    """
    try:
        # Initialize agent
        agent = TestingAgent(
            region=os.getenv('AWS_REGION', 'us-east-1'),
            memory_table=os.getenv('MEMORY_TABLE')
        )
        
        # Extract parameters from event
        pipeline_path = event.get('pipeline_path')
        pipeline_type = event.get('pipeline_type')
        test_scope = event.get('test_scope', 'comprehensive')
        
        # Run autonomous test session
        results = agent.run_autonomous_test_session(
            pipeline_path=pipeline_path,
            pipeline_type=pipeline_type,
            test_scope=test_scope
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps(results, indent=2)
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'type': type(e).__name__
            })
        }

