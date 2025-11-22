# AI Testing Agent - Deployment Guide

## Overview

This guide shows how to deploy the autonomous AI testing agent for ETL/ELT pipelines.

---

## Prerequisites

1. **AWS Account** with appropriate permissions
2. **AWS CDK** installed (`npm install -g aws-cdk`)
3. **Python 3.11+** and dependencies
4. **AWS CLI** configured
5. **Bedrock Access** (request access if needed)

---

## Quick Deploy

```bash
# 1. Install dependencies
cd infrastructure/cdk
pip install -r requirements.txt

# 2. Bootstrap CDK (first time only)
cdk bootstrap

# 3. Deploy agent infrastructure
cdk deploy AgentStack

# 4. Deploy test infrastructure (if not already deployed)
cdk deploy ETLTestInfrastructure
```

---

## Manual Agent Execution

### Via Lambda

```bash
# Invoke agent directly
aws lambda invoke \
  --function-name AgentOrchestrator \
  --payload '{"pipeline_path": "pipelines/oil_gas_ticket_pipeline.py", "test_scope": "comprehensive"}' \
  response.json

# Check results
cat response.json | jq
```

### Via Step Functions

```bash
# Start execution
aws stepfunctions start-execution \
  --state-machine-arn <STATE_MACHINE_ARN> \
  --input '{"pipeline_path": "pipelines/oil_gas_ticket_pipeline.py"}'

# Check status
aws stepfunctions describe-execution \
  --execution-arn <EXECUTION_ARN>
```

### Via EventBridge (Scheduled)

The agent runs automatically daily at 2 AM. To change schedule:

```bash
# Update schedule in CDK code
# infrastructure/cdk/stacks/agent_stack.py
# Then redeploy:
cdk deploy AgentStack
```

---

## Testing the Agent

### Test ETL Pipeline

```python
from src.ai.agent.testing_agent import TestingAgent

agent = TestingAgent()

# Test specific ETL pipeline
results = agent.run_autonomous_test_session(
    pipeline_path="pipelines/oil_gas_ticket_pipeline.py",
    pipeline_type="ETL",
    test_scope="comprehensive"
)

print(json.dumps(results, indent=2))
```

### Test ELT Pipeline

```python
# Test ELT pipeline (SQL-based)
results = agent.run_autonomous_test_session(
    pipeline_path="pipelines/sql_transform_pipeline.py",
    pipeline_type="ELT",
    test_scope="comprehensive"
)
```

### Auto-Discover All Pipelines

```python
# Let agent discover all pipelines
results = agent.run_autonomous_test_session(
    pipeline_path=None,  # Auto-discover
    test_scope="quick"
)
```

---

## Monitoring

### CloudWatch Logs

```bash
# View orchestrator logs
aws logs tail /aws/lambda/AgentOrchestrator --follow

# View discovery logs
aws logs tail /aws/lambda/DiscoveryAgent --follow

# View execution logs
aws logs tail /aws/lambda/ExecutionAgent --follow

# View analysis logs
aws logs tail /aws/lambda/AnalysisAgent --follow
```

### DynamoDB Memory

```bash
# Query agent memory
aws dynamodb scan \
  --table-name ai-testing-agent-memory \
  --limit 10
```

### S3 Artifacts

```bash
# List test artifacts
aws s3 ls s3://ai-testing-agent-artifacts-<account>-<region>/
```

---

## Cost Monitoring

### Set Budget Alerts

```bash
# Create budget alert (via AWS Console or CLI)
aws budgets create-budget \
  --account-id <ACCOUNT_ID> \
  --budget file://budget.json
```

**Estimated Monthly Cost**: $80-185/month
- Bedrock: $50-150
- Lambda: $2-5
- Step Functions: $25
- DynamoDB: $1-3
- S3: $0.25
- EventBridge: $1

---

## Troubleshooting

### Agent Not Running

1. **Check EventBridge Rule**:
   ```bash
   aws events list-rules --name-prefix AgentSchedule
   ```

2. **Check Lambda Permissions**:
   ```bash
   aws lambda get-policy --function-name AgentOrchestrator
   ```

3. **Check CloudWatch Logs** for errors

### Bedrock Access Denied

1. Request Bedrock access in AWS Console
2. Wait for approval (usually 24-48 hours)
3. Verify access:
   ```bash
   aws bedrock list-foundation-models
   ```

### Memory Table Not Found

1. Check table exists:
   ```bash
   aws dynamodb describe-table --table-name ai-testing-agent-memory
   ```

2. Redeploy if missing:
   ```bash
   cdk deploy AgentStack
   ```

---

## Updating the Agent

### Update Code

```bash
# 1. Update source code
# Edit files in src/ai/agent/

# 2. Redeploy
cd infrastructure/cdk
cdk deploy AgentStack
```

### Update Configuration

Edit environment variables in `agent_stack.py` and redeploy.

---

## Security

### IAM Permissions

The agent needs:
- `bedrock:InvokeModel` - For AI operations
- `lambda:InvokeFunction` - To call other agent modules
- `dynamodb:*` - For memory storage
- `s3:*` - For artifacts
- `logs:*` - For CloudWatch logging

### Network Security

- Agent runs in default VPC
- No public internet access required (Bedrock is AWS service)
- Consider VPC endpoints for cost optimization

---

## Next Steps

1. **Deploy** agent infrastructure
2. **Run** first test session manually
3. **Review** results and adjust
4. **Enable** scheduled runs
5. **Monitor** costs and performance
6. **Iterate** based on learnings

---

*For questions, see:*
- `AI_TESTING_AGENT.md` - Architecture details
- `LEGACY_MIGRATION_AND_AI_TESTING_PROPOSAL.md` - Full proposal

