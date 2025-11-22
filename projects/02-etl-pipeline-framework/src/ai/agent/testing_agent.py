"""
Autonomous AI Testing Agent
Main orchestrator for autonomous ETL/ELT testing
"""

import boto3
import json
import os
from typing import Dict, List, Optional
from datetime import datetime
from .discovery_agent import DiscoveryAgent
from .execution_agent import ExecutionAgent
from .analysis_agent import AnalysisAgent


class TestingAgent:
    """
    Autonomous AI agent that tests ETL/ELT pipelines
    
    Capabilities:
    - Discovers pipelines automatically
    - Generates and executes tests
    - Analyzes results
    - Suggests fixes
    - Learns from experience
    """
    
    def __init__(
        self,
        region: str = "us-east-1",
        memory_table: Optional[str] = None
    ):
        """
        Initialize Testing Agent
        
        Args:
            region: AWS region
            memory_table: DynamoDB table name for agent memory
        """
        self.region = region
        self.bedrock = boto3.client('bedrock-runtime', region_name=region)
        self.dynamodb = boto3.resource('dynamodb', region_name=region)
        self.s3 = boto3.client('s3', region_name=region)
        
        # Agent modules
        self.discovery = DiscoveryAgent(region=region)
        self.execution = ExecutionAgent(region=region)
        self.analysis = AnalysisAgent(region=region)
        
        # Memory store
        self.memory_table_name = memory_table or os.getenv(
            'AGENT_MEMORY_TABLE',
            'ai-testing-agent-memory'
        )
        self.memory_table = self.dynamodb.Table(self.memory_table_name)
        
        # Agent state
        self.agent_id = f"agent-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        self.session_id = None
    
    def run_autonomous_test_session(
        self,
        pipeline_path: Optional[str] = None,
        pipeline_type: Optional[str] = None,  # 'ETL' or 'ELT'
        test_scope: str = "comprehensive"
    ) -> Dict:
        """
        Run a complete autonomous test session
        
        Args:
            pipeline_path: Optional path to specific pipeline
            pipeline_type: 'ETL' or 'ELT' (auto-detected if None)
            test_scope: 'quick', 'comprehensive', or 'full'
            
        Returns:
            Test session results
        """
        self.session_id = f"session-{datetime.now().isoformat()}"
        
        results = {
            "agent_id": self.agent_id,
            "session_id": self.session_id,
            "start_time": datetime.now().isoformat(),
            "pipeline_path": pipeline_path,
            "pipeline_type": pipeline_type,
            "test_scope": test_scope,
            "phases": {}
        }
        
        try:
            # Phase 1: Discovery
            print(f"[{self.agent_id}] Phase 1: Discovery")
            discovery_results = self._discovery_phase(pipeline_path, pipeline_type)
            results["phases"]["discovery"] = discovery_results
            
            # Determine pipeline type if not specified
            if not pipeline_type:
                pipeline_type = discovery_results.get("pipeline_type", "ETL")
            results["pipeline_type"] = pipeline_type
            
            # Phase 2: Planning
            print(f"[{self.agent_id}] Phase 2: Planning")
            plan = self._planning_phase(discovery_results, test_scope)
            results["phases"]["planning"] = plan
            
            # Phase 3: Test Generation
            print(f"[{self.agent_id}] Phase 3: Test Generation")
            test_suite = self._test_generation_phase(discovery_results, plan)
            results["phases"]["test_generation"] = test_suite
            
            # Phase 4: Execution
            print(f"[{self.agent_id}] Phase 4: Execution")
            execution_results = self._execution_phase(
                discovery_results,
                test_suite,
                pipeline_type
            )
            results["phases"]["execution"] = execution_results
            
            # Phase 5: Analysis
            print(f"[{self.agent_id}] Phase 5: Analysis")
            analysis_results = self._analysis_phase(execution_results, pipeline_type)
            results["phases"]["analysis"] = analysis_results
            
            # Phase 6: Learning
            print(f"[{self.agent_id}] Phase 6: Learning")
            learning_results = self._learning_phase(results)
            results["phases"]["learning"] = learning_results
            
            # Final status
            results["status"] = "success"
            results["end_time"] = datetime.now().isoformat()
            
            # Store in memory
            self._store_session(results)
            
        except Exception as e:
            results["status"] = "failed"
            results["error"] = str(e)
            results["end_time"] = datetime.now().isoformat()
            raise
        
        return results
    
    def _discovery_phase(
        self,
        pipeline_path: Optional[str],
        pipeline_type: Optional[str]
    ) -> Dict:
        """Discover and analyze pipeline"""
        if pipeline_path:
            # Analyze specific pipeline
            discovery = self.discovery.analyze_pipeline(pipeline_path)
        else:
            # Auto-discover pipelines
            discovery = self.discovery.discover_pipelines()
        
        # Auto-detect pipeline type if not specified
        if not pipeline_type:
            pipeline_type = self.discovery.detect_pipeline_type(discovery)
            discovery["pipeline_type"] = pipeline_type
        
        return discovery
    
    def _planning_phase(
        self,
        discovery: Dict,
        test_scope: str
    ) -> Dict:
        """Plan test strategy using AI"""
        context = {
            "pipeline_type": discovery.get("pipeline_type"),
            "data_sources": discovery.get("data_sources", []),
            "transformations": discovery.get("transformations", []),
            "targets": discovery.get("targets", []),
            "test_scope": test_scope,
            "historical_tests": self._get_historical_tests(discovery)
        }
        
        prompt = f"""
        As an expert QA engineer, create a comprehensive test plan for this pipeline:
        
        Pipeline Type: {context['pipeline_type']}
        Data Sources: {json.dumps(context['data_sources'], indent=2)}
        Transformations: {json.dumps(context['transformations'], indent=2)}
        Targets: {json.dumps(context['targets'], indent=2)}
        Test Scope: {test_scope}
        
        Create a test plan that includes:
        1. Test categories (unit, integration, E2E, data quality)
        2. Test data requirements
        3. Validation points
        4. Performance benchmarks
        5. Edge cases to test
        
        Return as JSON with keys: test_categories, test_data_requirements, validation_points, performance_benchmarks, edge_cases
        """
        
        response = self.bedrock.invoke_model(
            modelId="anthropic.claude-v2",
            body=json.dumps({
                "prompt": prompt,
                "max_tokens_to_sample": 3000,
                "temperature": 0.3
            })
        )
        
        result = json.loads(response['body'].read())
        plan_text = result.get('completion', '{}')
        
        # Extract JSON from response
        plan = self._extract_json(plan_text)
        return plan
    
    def _test_generation_phase(
        self,
        discovery: Dict,
        plan: Dict
    ) -> Dict:
        """Generate test suite"""
        from src.ai.test_generator import AITestGenerator
        
        generator = AITestGenerator(region=self.region)
        
        test_suite = {
            "unit_tests": [],
            "integration_tests": [],
            "e2e_tests": [],
            "data_quality_tests": []
        }
        
        # Generate tests for each category
        for category in plan.get("test_categories", []):
            test_type = category.get("type", "unit")
            requirements = category.get("requirements", "")
            
            if requirements:
                generated_tests = generator.generate_tests_from_requirements(
                    requirements=requirements,
                    existing_code=discovery.get("pipeline_code", "")
                )
                
                test_suite[f"{test_type}_tests"].append({
                    "category": category.get("name"),
                    "code": generated_tests,
                    "requirements": requirements
                })
        
        # Generate test data
        test_data_requirements = plan.get("test_data_requirements", [])
        test_suite["test_data"] = []
        
        for data_req in test_data_requirements:
            schema = data_req.get("schema", {})
            count = data_req.get("count", 100)
            
            test_data = generator.generate_test_data(
                schema=schema,
                count=count,
                include_edge_cases=True
            )
            
            test_suite["test_data"].append({
                "name": data_req.get("name"),
                "data": test_data,
                "schema": schema
            })
        
        return test_suite
    
    def _execution_phase(
        self,
        discovery: Dict,
        test_suite: Dict,
        pipeline_type: str
    ) -> Dict:
        """Execute tests"""
        if pipeline_type == "ELT":
            return self.execution.execute_elt_tests(discovery, test_suite)
        else:
            return self.execution.execute_etl_tests(discovery, test_suite)
    
    def _analysis_phase(
        self,
        execution_results: Dict,
        pipeline_type: str
    ) -> Dict:
        """Analyze test results"""
        return self.analysis.analyze_results(execution_results, pipeline_type)
    
    def _learning_phase(
        self,
        session_results: Dict
    ) -> Dict:
        """Learn from test session and update memory"""
        # Extract learnings
        learnings = {
            "patterns": self._extract_patterns(session_results),
            "improvements": session_results.get("phases", {}).get("analysis", {}).get("suggestions", []),
            "test_coverage": self._calculate_coverage(session_results),
            "success_rate": self._calculate_success_rate(session_results)
        }
        
        # Store learnings
        self._store_learnings(learnings)
        
        return learnings
    
    def _get_historical_tests(self, discovery: Dict) -> List[Dict]:
        """Get historical test results for similar pipelines"""
        try:
            response = self.memory_table.query(
                IndexName='pipeline-type-index',  # GSI
                KeyConditionExpression='pipeline_type = :pt',
                ExpressionAttributeValues={
                    ':pt': discovery.get("pipeline_type", "ETL")
                },
                Limit=10
            )
            return response.get('Items', [])
        except Exception:
            return []
    
    def _store_session(self, results: Dict):
        """Store session results in memory"""
        try:
            self.memory_table.put_item(
                Item={
                    'session_id': self.session_id,
                    'agent_id': self.agent_id,
                    'pipeline_type': results.get('pipeline_type', 'ETL'),
                    'timestamp': datetime.now().isoformat(),
                    'results': results,
                    'ttl': int((datetime.now().timestamp() + 90*24*60*60))  # 90 days
                }
            )
        except Exception as e:
            print(f"Warning: Failed to store session: {e}")
    
    def _store_learnings(self, learnings: Dict):
        """Store learnings in memory"""
        try:
            self.memory_table.put_item(
                Item={
                    'session_id': f"learning-{datetime.now().isoformat()}",
                    'agent_id': self.agent_id,
                    'type': 'learning',
                    'timestamp': datetime.now().isoformat(),
                    'learnings': learnings,
                    'ttl': int((datetime.now().timestamp() + 365*24*60*60))  # 1 year
                }
            )
        except Exception as e:
            print(f"Warning: Failed to store learnings: {e}")
    
    def _extract_json(self, text: str) -> Dict:
        """Extract JSON from AI response"""
        import re
        json_match = re.search(r'\{.*\}', text, re.DOTALL)
        if json_match:
            try:
                return json.loads(json_match.group())
            except json.JSONDecodeError:
                pass
        return {}
    
    def _extract_patterns(self, results: Dict) -> List[str]:
        """Extract patterns from test results"""
        patterns = []
        
        # Extract common failure patterns
        analysis = results.get("phases", {}).get("analysis", {})
        failures = analysis.get("failures", [])
        
        for failure in failures:
            pattern = failure.get("pattern", failure.get("type"))
            if pattern:
                patterns.append(pattern)
        
        return list(set(patterns))
    
    def _calculate_coverage(self, results: Dict) -> float:
        """Calculate test coverage"""
        execution = results.get("phases", {}).get("execution", {})
        total_tests = execution.get("total_tests", 0)
        passed_tests = execution.get("passed_tests", 0)
        
        if total_tests == 0:
            return 0.0
        
        return (passed_tests / total_tests) * 100
    
    def _calculate_success_rate(self, results: Dict) -> float:
        """Calculate overall success rate"""
        execution = results.get("phases", {}).get("execution", {})
        total = execution.get("total_tests", 0)
        passed = execution.get("passed_tests", 0)
        
        if total == 0:
            return 0.0
        
        return (passed / total) * 100


# Example usage
if __name__ == "__main__":
    agent = TestingAgent()
    
    # Run autonomous test session
    results = agent.run_autonomous_test_session(
        pipeline_path="pipelines/oil_gas_ticket_pipeline.py",
        test_scope="comprehensive"
    )
    
    print("\n" + "="*50)
    print("Agent Test Session Results")
    print("="*50)
    print(json.dumps(results, indent=2))

