#!/usr/bin/env python3
"""
AWS CDK App for ETL Pipeline Infrastructure
Replaces manual shell scripts with Infrastructure as Code
"""

import aws_cdk as cdk
from stacks.test_infrastructure_stack import TestInfrastructureStack
from stacks.pipeline_infrastructure_stack import PipelineInfrastructureStack
from stacks.database_stack import DatabaseStack

app = cdk.App()

# Test Environment Stack
test_stack = TestInfrastructureStack(
    app, "ETLTestInfrastructure",
    env=cdk.Environment(
        account=app.node.try_get_context("account"),
        region=app.node.try_get_context("region") or "us-east-1"
    ),
    description="Test infrastructure for ETL pipeline - MSSQL + AWS only"
)

# Pipeline Infrastructure Stack
pipeline_stack = PipelineInfrastructureStack(
    app, "ETLPipelineInfrastructure",
    env=cdk.Environment(
        account=app.node.try_get_context("account"),
        region=app.node.try_get_context("region") or "us-east-1"
    ),
    description="Production ETL pipeline infrastructure"
)

# Database Stack (MSSQL)
database_stack = DatabaseStack(
    app, "ETLDatabaseStack",
    env=cdk.Environment(
        account=app.node.try_get_context("account"),
        region=app.node.try_get_context("region") or "us-east-1"
    ),
    description="RDS MSSQL database for ETL pipeline"
)

app.synth()

