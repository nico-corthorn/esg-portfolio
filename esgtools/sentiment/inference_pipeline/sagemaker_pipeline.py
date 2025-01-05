import json
import os

import boto3
import sagemaker
from sagemaker.inputs import TransformInput
from sagemaker.model import Model
from sagemaker.processing import ProcessingOutput, ScriptProcessor
from sagemaker.workflow.pipeline import Pipeline
from sagemaker.workflow.steps import ProcessingStep, TransformStep

from esgtools.utils import aws


def create_pipeline(
    role_arn,
    pipeline_name="SentimentAnalysisPipeline",
    processing_instance_type="ml.m5.xlarge",
    transform_instance_type="ml.g5.2xlarge",
    transform_instance_count=1,
):
    """Create a SageMaker pipeline for data processing and batch inference."""

    session = sagemaker.Session()

    # Paths
    current_dir = os.path.dirname(os.path.abspath(__file__))
    bucket = session.default_bucket()
    code_prefix = "sentiment/code"
    processing_output_path = f"s3://{bucket}/sentiment/processing/output"
    transform_output_path = f"s3://{bucket}/sentiment/transform/output"

    # Get the AWS account ID and region
    account_id = boto3.client("sts").get_caller_identity().get("Account")
    region = session.boto_region_name

    # Load config
    config = json.loads(aws.get_s3_file("sentiment/config/model_config.json"))

    # Use the ECR image for both steps
    container_uri = f"{account_id}.dkr.ecr.{region}.amazonaws.com/sentiment-inference:latest"

    # Data Processing Step
    processor = ScriptProcessor(
        command=["python3", "script_runner.py"],
        image_uri=container_uri,
        role=role_arn,
        instance_count=1,
        instance_type=processing_instance_type,
        base_job_name="sentiment-preprocess",
        volume_size_in_gb=30,
        max_runtime_in_seconds=1200,
        sagemaker_session=session,
        env={
            "ESGTOOLS_PACKAGE_URI": f"s3://{bucket}/sentiment/packages/esgtools.tar.gz",
            "PREPROCESSING_SCRIPT_URI": f"s3://{bucket}/{code_prefix}/preprocessing.py",
            "PYTHONPATH": "/opt/ml/code",
            "PYTHONUNBUFFERED": "1",
            "AWS_DEFAULT_REGION": region,
        },
    )

    # Configure processing step
    processing_step = ProcessingStep(
        name="PreprocessData",
        processor=processor,
        outputs=[
            ProcessingOutput(
                output_name="data",
                source="/opt/ml/processing/output",
                destination=processing_output_path,
            )
        ],
        code=os.path.join(current_dir, "script_runner.py"),  # Use local path
        job_arguments=["--region", "us-east-1"],
    )

    # Create model using our container
    model = Model(
        image_uri=container_uri,
        model_data=None,
        role=role_arn,
        sagemaker_session=session,
        env={
            **config["environment"],
            "PYTHONPATH": "/opt/ml/code",
            "PYTHONUNBUFFERED": "1",
            "SAGEMAKER_PROGRAM": "serve",
            "SERVE_SCRIPT_URI": f"s3://{bucket}/{code_prefix}/serve",
            "INFERENCE_SCRIPT_URI": f"s3://{bucket}/{code_prefix}/inference.py",
            "ESGTOOLS_PACKAGE_URI": f"s3://{bucket}/sentiment/packages/esgtools.tar.gz",
            "AWS_DEFAULT_REGION": region,
        },
    )

    # Create transformer from the model
    transformer = model.transformer(
        instance_count=transform_instance_count,
        instance_type=transform_instance_type,
        output_path=transform_output_path,
        assemble_with="Line",
        max_concurrent_transforms=1,
        strategy="SingleRecord",
        accept="application/json",
    )

    # Transform step
    transform_step = TransformStep(
        name="SentimentAnalysis",
        transformer=transformer,
        inputs=TransformInput(
            data=f"{processing_output_path}/data.jsonl",
            content_type="application/jsonlines",
            split_type="Line",
        ),
        depends_on=[processing_step],
    )

    # Create pipeline with both steps
    pipeline = Pipeline(
        name=pipeline_name,
        steps=[processing_step, transform_step],
        sagemaker_session=session,
    )

    return pipeline
