# Infrastructure as Code - Quick Start Guide

## Prerequisites

```bash
# Install AWS CDK
npm install -g aws-cdk

# Install Python dependencies
pip install aws-cdk-lib constructs

# Configure AWS credentials
aws configure
```

## Deploy Test Infrastructure

```bash
cd infrastructure/cdk

# Install CDK dependencies
cdk bootstrap

# Deploy test infrastructure
cdk deploy ETLTestInfrastructure

# Get outputs
aws cloudformation describe-stacks \
  --stack-name ETLTestInfrastructure \
  --query 'Stacks[0].Outputs'
```

## Deploy All Infrastructure

```bash
# Deploy all stacks
cdk deploy --all

# Or deploy specific stack
cdk deploy ETLTestInfrastructure
cdk deploy ETLPipelineInfrastructure
cdk deploy ETLDatabaseStack
```

## Destroy Infrastructure

```bash
# Destroy test environment (safe - marked for destruction)
cdk destroy ETLTestInfrastructure

# Destroy all
cdk destroy --all
```

## Cost Estimation

```bash
# Estimate costs before deploying
cdk diff
```

## Common Commands

```bash
# List all stacks
cdk list

# Synthesize CloudFormation templates
cdk synth

# View differences
cdk diff

# Deploy with specific parameters
cdk deploy ETLTestInfrastructure --parameters TestDBInstanceType=t3.small
```

