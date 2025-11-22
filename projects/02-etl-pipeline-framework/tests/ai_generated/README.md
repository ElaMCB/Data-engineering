# AI-Generated Tests

This directory contains tests generated using AI (AWS Bedrock).

## Process

1. **Requirements Input**: Provide natural language requirements
2. **AI Generation**: AI generates test code using AWS Bedrock
3. **Human Review**: QA Lead reviews generated tests
4. **Integration**: Approved tests are integrated into test suite

## Usage

```python
from src.ai.test_generator import AITestGenerator

generator = AITestGenerator()

# Generate tests from requirements
requirements = "Test that pipeline extracts data from MSSQL and validates transactions"
tests = generator.generate_tests_from_requirements(requirements)

# Review and save
with open("tests/ai_generated/test_pipeline_ai.py", "w") as f:
    f.write(tests)
```

## Guidelines

- **Always review** AI-generated tests before committing
- **Validate** that tests actually test what's intended
- **Refine** generated tests as needed
- **Document** which tests were AI-generated

## Status

- ✅ Test generation working
- ✅ Test data generation working
- ⏳ Assertion generation (in progress)
- ⏳ Test improvement suggestions (in progress)

