"""
AI Testing Agent Infrastructure Stack
Deploys autonomous testing agent infrastructure
"""

from aws_cdk import (
    Stack,
    aws_lambda as lambda_,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_dynamodb as dynamodb,
    aws_events as events,
    aws_events_targets as targets,
    aws_iam as iam,
    aws_s3 as s3,
    Duration,
    RemovalPolicy,
    CfnOutput
)
from constructs import Construct


class AgentStack(Stack):
    """
    Infrastructure for AI Testing Agent
    - Lambda functions for agent modules
    - Step Functions for orchestration
    - DynamoDB for agent memory
    - EventBridge for scheduling
    """
    
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)
        
        # S3 bucket for test artifacts
        artifacts_bucket = s3.Bucket(
            self, "AgentArtifactsBucket",
            bucket_name=f"ai-testing-agent-artifacts-{self.account}-{self.region}",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            versioned=True
        )
        
        # DynamoDB table for agent memory
        memory_table = dynamodb.Table(
            self, "AgentMemoryTable",
            table_name="ai-testing-agent-memory",
            partition_key=dynamodb.Attribute(
                name="session_id",
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="timestamp",
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
            time_to_live_attribute="ttl"
        )
        
        # Add GSI for pipeline type queries
        memory_table.add_global_secondary_index(
            index_name="pipeline-type-index",
            partition_key=dynamodb.Attribute(
                name="pipeline_type",
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="timestamp",
                type=dynamodb.AttributeType.STRING
            )
        )
        
        # Lambda Layer for dependencies
        agent_layer = lambda_.LayerVersion(
            self, "AgentLayer",
            code=lambda_.Code.from_asset("lambda/layers/agent"),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_11],
            description="AI Testing Agent dependencies"
        )
        
        # Lambda: Discovery Agent
        discovery_function = lambda_.Function(
            self, "DiscoveryAgent",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="discovery_agent.handler",
            code=lambda_.Code.from_asset("lambda/agent"),
            timeout=Duration.minutes(5),
            memory_size=512,
            layers=[agent_layer],
            environment={
                "MEMORY_TABLE": memory_table.table_name,
                "ARTIFACTS_BUCKET": artifacts_bucket.bucket_name
            }
        )
        
        # Lambda: Execution Agent
        execution_function = lambda_.Function(
            self, "ExecutionAgent",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="execution_agent.handler",
            code=lambda_.Code.from_asset("lambda/agent"),
            timeout=Duration.minutes(15),
            memory_size=1024,
            layers=[agent_layer],
            environment={
                "MEMORY_TABLE": memory_table.table_name,
                "ARTIFACTS_BUCKET": artifacts_bucket.bucket_name
            }
        )
        
        # Lambda: Analysis Agent
        analysis_function = lambda_.Function(
            self, "AnalysisAgent",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="analysis_agent.handler",
            code=lambda_.Code.from_asset("lambda/agent"),
            timeout=Duration.minutes(5),
            memory_size=512,
            layers=[agent_layer],
            environment={
                "MEMORY_TABLE": memory_table.table_name,
                "ARTIFACTS_BUCKET": artifacts_bucket.bucket_name,
                "BEDROCK_REGION": self.region
            }
        )
        
        # Lambda: Main Agent Orchestrator
        orchestrator_function = lambda_.Function(
            self, "AgentOrchestrator",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="testing_agent.handler",
            code=lambda_.Code.from_asset("lambda/agent"),
            timeout=Duration.minutes(30),
            memory_size=2048,
            layers=[agent_layer],
            environment={
                "MEMORY_TABLE": memory_table.table_name,
                "ARTIFACTS_BUCKET": artifacts_bucket.bucket_name,
                "BEDROCK_REGION": self.region,
                "DISCOVERY_FUNCTION": discovery_function.function_name,
                "EXECUTION_FUNCTION": execution_function.function_name,
                "ANALYSIS_FUNCTION": analysis_function.function_name
            }
        )
        
        # Grant permissions
        memory_table.grant_read_write_data(orchestrator_function)
        memory_table.grant_read_write_data(discovery_function)
        memory_table.grant_read_write_data(execution_function)
        memory_table.grant_read_write_data(analysis_function)
        
        artifacts_bucket.grant_read_write(orchestrator_function)
        artifacts_bucket.grant_read_write(discovery_function)
        artifacts_bucket.grant_read_write(execution_function)
        artifacts_bucket.grant_read_write(analysis_function)
        
        # Grant Bedrock permissions
        for func in [orchestrator_function, analysis_function]:
            func.add_to_role_policy(
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=["bedrock:InvokeModel"],
                    resources=["*"]
                )
            )
        
        # Grant Lambda invoke permissions
        orchestrator_function.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["lambda:InvokeFunction"],
                resources=[
                    discovery_function.function_arn,
                    execution_function.function_arn,
                    analysis_function.function_arn
                ]
            )
        )
        
        # Step Functions: Agent Workflow
        # Discovery step
        discovery_task = tasks.LambdaInvoke(
            self, "DiscoveryTask",
            lambda_function=discovery_function,
            output_path="$.Payload"
        )
        
        # Execution step
        execution_task = tasks.LambdaInvoke(
            self, "ExecutionTask",
            lambda_function=execution_function,
            output_path="$.Payload"
        )
        
        # Analysis step
        analysis_task = tasks.LambdaInvoke(
            self, "AnalysisTask",
            lambda_function=analysis_function,
            output_path="$.Payload"
        )
        
        # Define workflow
        definition = discovery_task.next(
            execution_task.next(analysis_task)
        )
        
        state_machine = sfn.StateMachine(
            self, "AgentStateMachine",
            definition=definition,
            timeout=Duration.minutes(60),
            state_machine_name="ai-testing-agent-workflow"
        )
        
        # EventBridge: Schedule agent runs
        rule = events.Rule(
            self, "AgentSchedule",
            schedule=events.Schedule.cron(
                hour="2",  # 2 AM daily
                minute="0"
            ),
            description="Daily autonomous test execution"
        )
        
        rule.add_target(
            targets.LambdaFunction(orchestrator_function)
        )
        
        # Outputs
        CfnOutput(
            self, "OrchestratorFunctionArn",
            value=orchestrator_function.function_arn,
            description="ARN of the agent orchestrator Lambda"
        )
        
        CfnOutput(
            self, "StateMachineArn",
            value=state_machine.state_machine_arn,
            description="ARN of the Step Functions state machine"
        )
        
        CfnOutput(
            self, "MemoryTableName",
            value=memory_table.table_name,
            description="DynamoDB table for agent memory"
        )
        
        CfnOutput(
            self, "ArtifactsBucketName",
            value=artifacts_bucket.bucket_name,
            description="S3 bucket for test artifacts"
        )

