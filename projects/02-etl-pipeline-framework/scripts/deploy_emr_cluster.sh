#!/bin/bash
# Deploy EMR Cluster for ETL Pipeline
# This script creates an EMR cluster with cost-optimized configuration

set -e

# Configuration
CLUSTER_NAME="${CLUSTER_NAME:-etl-pipeline-cluster}"
RELEASE_LABEL="${RELEASE_LABEL:-emr-6.15.0}"
INSTANCE_TYPE="${INSTANCE_TYPE:-m5.xlarge}"
CORE_INSTANCE_COUNT="${CORE_INSTANCE_COUNT:-2}"
USE_SPOT="${USE_SPOT:-true}"
AUTO_SCALING="${AUTO_SCALING:-true}"

# AWS Configuration
AWS_REGION="${AWS_REGION:-us-east-1}"
SUBNET_ID="${SUBNET_ID:-}"
EC2_KEY_NAME="${EC2_KEY_NAME:-}"
LOG_URI="${LOG_URI:-s3://my-bucket/emr-logs/}"

echo "Creating EMR cluster: $CLUSTER_NAME"

# Build EMR create-cluster command
CMD="aws emr create-cluster \
  --name \"$CLUSTER_NAME\" \
  --release-label \"$RELEASE_LABEL\" \
  --applications Name=Spark Name=Hadoop Name=Livy \
  --instance-type \"$INSTANCE_TYPE\" \
  --instance-count $CORE_INSTANCE_COUNT \
  --use-default-roles"

# Add subnet if provided
if [ -n "$SUBNET_ID" ]; then
  CMD="$CMD --ec2-attributes SubnetId=$SUBNET_ID"
fi

# Add EC2 key if provided
if [ -n "$EC2_KEY_NAME" ]; then
  CMD="$CMD --ec2-attributes KeyName=$EC2_KEY_NAME"
fi

# Add log URI
if [ -n "$LOG_URI" ]; then
  CMD="$CMD --log-uri \"$LOG_URI\""
fi

# Add auto-scaling if enabled
if [ "$AUTO_SCALING" = "true" ]; then
  CMD="$CMD --auto-scaling-role EMR_AutoScaling_DefaultRole"
fi

# Execute command
echo "Executing: $CMD"
CLUSTER_ID=$(eval $CMD | jq -r '.ClusterId')

if [ -z "$CLUSTER_ID" ] || [ "$CLUSTER_ID" = "null" ]; then
  echo "Error: Failed to create cluster"
  exit 1
fi

echo "Cluster created successfully!"
echo "Cluster ID: $CLUSTER_ID"
echo ""
echo "To check cluster status:"
echo "  aws emr describe-cluster --cluster-id $CLUSTER_ID"
echo ""
echo "To terminate cluster:"
echo "  aws emr terminate-clusters --cluster-ids $CLUSTER_ID"

