"""
Test Infrastructure Stack
Creates test environment with MSSQL and test execution capabilities
"""

from aws_cdk import (
    Stack,
    aws_ec2 as ec2,
    aws_rds as rds,
    aws_lambda as lambda_,
    aws_iam as iam,
    aws_s3 as s3,
    RemovalPolicy,
    Duration,
    CfnOutput
)
from constructs import Construct


class TestInfrastructureStack(Stack):
    """
    Infrastructure for testing ETL pipelines
    - Test MSSQL database (RDS SQL Server)
    - Lambda functions for test execution
    - VPC for isolated test environment
    """
    
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)
        
        # VPC for test environment
        vpc = ec2.Vpc(
            self, "TestVPC",
            max_azs=2,
            nat_gateways=1,  # Single NAT gateway for cost optimization
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="Public",
                    subnet_type=ec2.SubnetType.PUBLIC,
                    cidr_mask=24
                ),
                ec2.SubnetConfiguration(
                    name="Private",
                    subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
                    cidr_mask=24
                )
            ]
        )
        
        # Security Group for RDS
        db_security_group = ec2.SecurityGroup(
            self, "TestDBSecurityGroup",
            vpc=vpc,
            description="Security group for test MSSQL database",
            allow_all_outbound=True
        )
        
        # Security Group for Lambda
        lambda_security_group = ec2.SecurityGroup(
            self, "TestLambdaSecurityGroup",
            vpc=vpc,
            description="Security group for test Lambda functions",
            allow_all_outbound=True
        )
        
        # Allow Lambda to access RDS
        db_security_group.add_ingress_rule(
            lambda_security_group,
            ec2.Port.tcp(1433),  # MSSQL port
            "Allow Lambda to access MSSQL"
        )
        
        # Test MSSQL Database (RDS SQL Server)
        # Using Web Edition for cost optimization in test environment
        test_db = rds.DatabaseInstance(
            self, "TestMSSQL",
            engine=rds.DatabaseInstanceEngine.sql_server_web(
                version=rds.SqlServerEngineVersion.VER_15
            ),
            instance_type=ec2.InstanceType.of(
                ec2.InstanceClass.T3,
                ec2.InstanceSize.MICRO  # Small instance for testing
            ),
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS
            ),
            security_groups=[db_security_group],
            database_name="test_etl_db",
            credentials=rds.Credentials.from_generated_secret(
                "test_db_admin"
            ),
            multi_az=False,  # Single AZ for test (cost savings)
            deletion_protection=False,  # Allow cleanup
            removal_policy=RemovalPolicy.DESTROY,  # For test env
            backup_retention=Duration.days(1),  # Minimal backup for test
            enable_performance_insights=False  # Cost optimization
        )
        
        # S3 Bucket for test data
        test_data_bucket = s3.Bucket(
            self, "TestDataBucket",
            bucket_name=f"etl-test-data-{self.account}-{self.region}",
            removal_policy=RemovalPolicy.DESTROY,  # For test env
            auto_delete_objects=True,
            versioned=False
        )
        
        # Lambda Layer for MSSQL drivers
        mssql_layer = lambda_.LayerVersion(
            self, "MSSQLLayer",
            code=lambda_.Code.from_asset("lambda/layers/mssql"),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_11],
            description="MSSQL drivers and dependencies"
        )
        
        # Lambda for running ETL tests
        test_runner = lambda_.Function(
            self, "TestRunner",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="test_runner.handler",
            code=lambda_.Code.from_asset("lambda/test_runner"),
            timeout=Duration.minutes(15),
            memory_size=512,
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS
            ),
            security_groups=[lambda_security_group],
            layers=[mssql_layer],
            environment={
                "TEST_DB_ENDPOINT": test_db.instance_endpoint.hostname,
                "TEST_DB_NAME": "test_etl_db",
                "TEST_DATA_BUCKET": test_data_bucket.bucket_name,
                "AWS_REGION": self.region
            }
        )
        
        # Grant Lambda permissions
        test_data_bucket.grant_read_write(test_runner)
        test_db.grant_connect(test_runner, "test_db_admin")
        
        # Lambda for AI test generation
        ai_test_generator = lambda_.Function(
            self, "AITestGenerator",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="ai_test_generator.handler",
            code=lambda_.Code.from_asset("lambda/ai_test_generator"),
            timeout=Duration.minutes(5),
            memory_size=1024,
            environment={
                "BEDROCK_REGION": self.region,
                "TEST_DATA_BUCKET": test_data_bucket.bucket_name
            }
        )
        
        # Grant Bedrock permissions
        ai_test_generator.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "bedrock:InvokeModel"
                ],
                resources=["*"]  # Adjust to specific model ARNs
            )
        )
        
        test_data_bucket.grant_read_write(ai_test_generator)
        
        # Outputs
        CfnOutput(
            self, "TestDBEndpoint",
            value=test_db.instance_endpoint.hostname,
            description="Test MSSQL database endpoint"
        )
        
        CfnOutput(
            self, "TestDBSecretArn",
            value=test_db.secret.secret_arn,
            description="ARN of the database secret"
        )
        
        CfnOutput(
            self, "TestRunnerFunctionArn",
            value=test_runner.function_arn,
            description="ARN of the test runner Lambda function"
        )
        
        CfnOutput(
            self, "AITestGeneratorFunctionArn",
            value=ai_test_generator.function_arn,
            description="ARN of the AI test generator Lambda function"
        )
        
        CfnOutput(
            self, "TestDataBucketName",
            value=test_data_bucket.bucket_name,
            description="S3 bucket for test data"
        )

