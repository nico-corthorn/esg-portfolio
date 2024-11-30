#!/bin/bash
set -e

# Directory setup
REPO_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/../../../" && pwd )"
PIPELINE_DIR="${REPO_DIR}/esgtools/sentiment/inference_pipeline"

# Create temporary test directories
TEST_DIR="/tmp/sagemaker-test"
mkdir -p "${TEST_DIR}/input/code"
mkdir -p "${TEST_DIR}/output"

# Copy files to test directory
cp "${PIPELINE_DIR}/preprocessing.py" "${TEST_DIR}/input/code/"

echo "Creating test environment..."
echo "Test directory contents:"
ls -la "${TEST_DIR}"

# Run the container locally with the same environment as SageMaker
docker run --rm \
  -v "${TEST_DIR}/input:/opt/ml/processing/input" \
  -v "${TEST_DIR}/output:/opt/ml/processing/output" \
  -v "${PIPELINE_DIR}/esgtools:/opt/ml/code/esgtools" \
  -e AWS_REGION="$(aws configure get region)" \
  -e AWS_ACCESS_KEY_ID="$(aws configure get aws_access_key_id)" \
  -e AWS_SECRET_ACCESS_KEY="$(aws configure get aws_secret_access_key)" \
  -e AWS_SESSION_TOKEN="$(aws configure get aws_session_token)" \
  sentiment-inference \
  python3 /opt/ml/processing/input/code/preprocessing.py --region $(aws configure get region)

# Clean up
rm -rf "${TEST_DIR}"