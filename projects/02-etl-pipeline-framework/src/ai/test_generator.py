"""
AI-Powered Test Generator
Uses AWS Bedrock to generate test cases from requirements
"""

import boto3
import json
import os
from typing import Dict, List, Optional
from datetime import datetime


class AITestGenerator:
    """
    Generate test cases using AI (AWS Bedrock)
    Converts natural language requirements into pytest test code
    """
    
    def __init__(self, region: str = "us-east-1"):
        """
        Initialize AI Test Generator
        
        Args:
            region: AWS region for Bedrock
        """
        self.region = region
        self.bedrock = boto3.client(
            'bedrock-runtime',
            region_name=region
        )
        self.model_id = "anthropic.claude-v2"  # or "anthropic.claude-3-sonnet-20240229-v1:0"
    
    def generate_tests_from_requirements(
        self,
        requirements: str,
        existing_code: Optional[str] = None,
        test_framework: str = "pytest"
    ) -> str:
        """
        Generate test code from natural language requirements
        
        Args:
            requirements: Natural language description of what to test
            existing_code: Optional existing code for context
            test_framework: Testing framework to use (default: pytest)
            
        Returns:
            Generated test code as string
        """
        prompt = self._build_test_generation_prompt(
            requirements=requirements,
            existing_code=existing_code,
            test_framework=test_framework
        )
        
        try:
            response = self.bedrock.invoke_model(
                modelId=self.model_id,
                body=json.dumps({
                    "prompt": prompt,
                    "max_tokens_to_sample": 4000,
                    "temperature": 0.3,  # Lower temperature for more deterministic output
                    "stop_sequences": ["\n\nHuman:"]
                })
            )
            
            result = json.loads(response['body'].read())
            generated_code = result.get('completion', '')
            
            return self._clean_generated_code(generated_code)
            
        except Exception as e:
            raise Exception(f"Failed to generate tests: {str(e)}")
    
    def generate_test_data(
        self,
        schema: Dict,
        count: int = 100,
        include_edge_cases: bool = True,
        business_rules: Optional[str] = None
    ) -> List[Dict]:
        """
        Generate realistic test data using AI
        
        Args:
            schema: Data schema dictionary
            count: Number of records to generate
            include_edge_cases: Whether to include edge cases
            business_rules: Optional business rules to follow
            
        Returns:
            List of test data records
        """
        prompt = f"""
        Generate {count} realistic test records for the following schema:
        
        Schema:
        {json.dumps(schema, indent=2)}
        
        Requirements:
        - Generate realistic, business-appropriate data (not random strings)
        - Include edge cases: {include_edge_cases}
        - Follow these business rules: {business_rules or 'None specified'}
        - Include some invalid data for negative testing (10% of records)
        - Ensure data types match schema exactly
        
        Return ONLY a valid JSON array, no additional text.
        Each record should be a JSON object matching the schema.
        """
        
        try:
            response = self.bedrock.invoke_model(
                modelId=self.model_id,
                body=json.dumps({
                    "prompt": prompt,
                    "max_tokens_to_sample": 8000,
                    "temperature": 0.5  # Higher for more variety
                })
            )
            
            result = json.loads(response['body'].read())
            generated_text = result.get('completion', '')
            
            # Extract JSON from response
            json_data = self._extract_json(generated_text)
            return json_data
            
        except Exception as e:
            raise Exception(f"Failed to generate test data: {str(e)}")
    
    def generate_assertions(
        self,
        transformation_logic: str,
        expected_output_schema: Dict,
        data_quality_rules: Optional[List[str]] = None
    ) -> List[str]:
        """
        Generate smart assertions for data transformations
        
        Args:
            transformation_logic: Description or code of transformation
            expected_output_schema: Expected output schema
            data_quality_rules: Optional data quality rules
            
        Returns:
            List of assertion statements
        """
        prompt = f"""
        Generate comprehensive pytest assertions for this data transformation:
        
        Transformation Logic:
        {transformation_logic}
        
        Expected Output Schema:
        {json.dumps(expected_output_schema, indent=2)}
        
        Data Quality Rules:
        {json.dumps(data_quality_rules or [], indent=2)}
        
        Generate assertions that check:
        1. Data type correctness for each column
        2. Value ranges and constraints
        3. Business rule compliance
        4. Data quality metrics (completeness, uniqueness, etc.)
        5. Edge cases and boundary conditions
        6. Referential integrity (if applicable)
        
        Return as a list of Python assertion statements (one per line).
        Use pytest-style assertions (assert statement).
        """
        
        try:
            response = self.bedrock.invoke_model(
                modelId=self.model_id,
                body=json.dumps({
                    "prompt": prompt,
                    "max_tokens_to_sample": 3000,
                    "temperature": 0.2
                })
            )
            
            result = json.loads(response['body'].read())
            assertions_text = result.get('completion', '')
            
            # Parse assertions from text
            assertions = self._parse_assertions(assertions_text)
            return assertions
            
        except Exception as e:
            raise Exception(f"Failed to generate assertions: {str(e)}")
    
    def suggest_test_improvements(
        self,
        existing_tests: str,
        test_results: Optional[Dict] = None
    ) -> Dict:
        """
        Suggest improvements to existing tests
        
        Args:
            existing_tests: Existing test code
            test_results: Optional test execution results
            
        Returns:
            Dictionary with suggestions
        """
        prompt = f"""
        Analyze these test cases and suggest improvements:
        
        Existing Tests:
        {existing_tests}
        
        Test Results:
        {json.dumps(test_results or {}, indent=2)}
        
        Provide suggestions for:
        1. Missing test cases
        2. Test coverage gaps
        3. Flaky tests
        4. Performance improvements
        5. Better assertions
        6. Edge cases to add
        
        Return as JSON with keys: missing_tests, coverage_gaps, improvements
        """
        
        try:
            response = self.bedrock.invoke_model(
                modelId=self.model_id,
                body=json.dumps({
                    "prompt": prompt,
                    "max_tokens_to_sample": 2000,
                    "temperature": 0.3
                })
            )
            
            result = json.loads(response['body'].read())
            suggestions_text = result.get('completion', '')
            
            suggestions = self._extract_json(suggestions_text)
            return suggestions
            
        except Exception as e:
            raise Exception(f"Failed to generate suggestions: {str(e)}")
    
    def _build_test_generation_prompt(
        self,
        requirements: str,
        existing_code: Optional[str],
        test_framework: str
    ) -> str:
        """Build prompt for test generation"""
        return f"""
        Human: As an expert QA engineer, generate comprehensive test cases using {test_framework} for the following requirement:
        
        Requirement:
        {requirements}
        
        {"Existing code context:" + existing_code if existing_code else "No existing code provided."}
        
        Generate:
        1. Unit tests for individual functions/components
        2. Integration tests for data flow between components
        3. Edge case tests (boundary conditions, null values, etc.)
        4. Data quality validation tests
        5. Error handling tests
        
        Requirements:
        - Use {test_framework} framework
        - Follow pytest best practices
        - Include docstrings for each test
        - Use descriptive test names
        - Include setup and teardown if needed
        - Mock external dependencies (databases, APIs)
        - Include assertions for expected outcomes
        
        Return ONLY the test code, no explanations.
        
        Assistant:
        """
    
    def _clean_generated_code(self, code: str) -> str:
        """Clean and format generated code"""
        # Remove markdown code blocks if present
        if "```python" in code:
            code = code.split("```python")[1]
        if "```" in code:
            code = code.split("```")[0]
        
        # Remove leading/trailing whitespace
        code = code.strip()
        
        return code
    
    def _extract_json(self, text: str) -> List[Dict]:
        """Extract JSON from AI response"""
        # Try to find JSON array in the text
        import re
        
        # Look for JSON array pattern
        json_match = re.search(r'\[.*\]', text, re.DOTALL)
        if json_match:
            try:
                return json.loads(json_match.group())
            except json.JSONDecodeError:
                pass
        
        # If no array found, try to parse entire text
        try:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                return parsed
            elif isinstance(parsed, dict):
                return [parsed]
        except json.JSONDecodeError:
            pass
        
        raise ValueError("Could not extract valid JSON from AI response")
    
    def _parse_assertions(self, text: str) -> List[str]:
        """Parse assertions from text"""
        assertions = []
        lines = text.split('\n')
        
        for line in lines:
            line = line.strip()
            if line.startswith('assert ') or 'assert ' in line:
                # Extract assertion
                if 'assert ' in line:
                    assertion = line.split('assert ')[1].strip()
                    if assertion:
                        assertions.append(f"assert {assertion}")
        
        return assertions if assertions else ["assert True  # No assertions generated"]


# Example usage
if __name__ == "__main__":
    generator = AITestGenerator()
    
    # Example: Generate tests from requirements
    requirements = """
    The ETL pipeline should:
    1. Extract data from MSSQL database
    2. Transform fuel type names to standardized format
    3. Validate transaction amounts are positive
    4. Calculate total transaction value
    5. Load transformed data to S3 in Parquet format
    """
    
    generated_tests = generator.generate_tests_from_requirements(requirements)
    print("Generated Tests:")
    print(generated_tests)
    
    # Example: Generate test data
    schema = {
        "ticket_id": "string",
        "carrier_id": "string",
        "fuel_type": "string",
        "quantity": "float",
        "price_per_unit": "float"
    }
    
    test_data = generator.generate_test_data(schema, count=10)
    print("\nGenerated Test Data:")
    print(json.dumps(test_data, indent=2))

