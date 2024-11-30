#!/bin/bash
set -e

# Get AWS account ID and region using AWS CLI (uses existing credentials)
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
AWS_REGION=$(aws configure get region)

# Repository name
REPOSITORY_NAME=sentiment-inference

# Set up directory paths
REPO_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/../../../" && pwd )"
PIPELINE_DIR="${REPO_DIR}/esgtools/sentiment/inference_pipeline"

# Create ECR repository if it doesn't exist
aws ecr describe-repositories --repository-names ${REPOSITORY_NAME} || \
    aws ecr create-repository --repository-name ${REPOSITORY_NAME}

# Login to ECR
aws ecr get-login-password | docker login --username AWS --password-stdin "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"

# Create a temporary build directory
BUILD_DIR=$(mktemp -d)
echo "Created temporary build directory: ${BUILD_DIR}"

# Copy required files to build directory
echo "Copying files to build directory..."
cp -r "${REPO_DIR}/esgtools" "${BUILD_DIR}/esgtools"
cp "${REPO_DIR}/setup.py" "${BUILD_DIR}/setup.py"
cp "${REPO_DIR}/lambda_requirements.txt" "${BUILD_DIR}/lambda_requirements.txt"
cp "${PIPELINE_DIR}/Dockerfile" "${BUILD_DIR}/Dockerfile"
cp "${PIPELINE_DIR}/requirements.txt" "${BUILD_DIR}/requirements.txt"
cp "${PIPELINE_DIR}/preprocessing.py" "${BUILD_DIR}/preprocessing.py"

# Debug: List contents of build directory
echo "Contents of build directory:"
ls -la "${BUILD_DIR}"

# Build and tag the docker image
echo "Building docker container..."
docker build --platform linux/amd64 -t ${REPOSITORY_NAME} "${BUILD_DIR}"
docker tag "${REPOSITORY_NAME}:latest" "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${REPOSITORY_NAME}:latest"

# Push the image
echo "Pushing docker container to ECR..."
docker push "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${REPOSITORY_NAME}:latest"

# Clean up
echo "Cleaning up temporary files..."
rm -rf "${BUILD_DIR}"