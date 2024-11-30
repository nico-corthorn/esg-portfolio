import os

import boto3
import sagemaker
from sagemaker.inputs import TransformInput
from sagemaker.processing import (ProcessingInput, ProcessingOutput,
                                  ScriptProcessor)
from sagemaker.transformer import Transformer
from sagemaker.workflow.pipeline import Pipeline
from sagemaker.workflow.steps import ProcessingStep, TransformStep


def create_pipeline(
    role_arn,
    pipeline_name="SentimentAnalysisPipeline",
    processing_instance_type="ml.m5.xlarge",
    transform_instance_type="ml.g4dn.xlarge",
    transform_instance_count=1,
):
    """Create a SageMaker pipeline for data processing and batch inference."""

    session = sagemaker.Session()

    # Get the absolute path to preprocessing.py
    current_dir = os.path.dirname(os.path.abspath(__file__))
    preprocessing_script = os.path.join(current_dir, "preprocessing.py")

    # Define S3 paths
    processing_output_path = f"s3://{session.default_bucket()}/sentiment/data"
    transform_output_path = f"s3://{session.default_bucket()}/sentiment/output"

    # Get the AWS account ID and region
    account_id = boto3.client("sts").get_caller_identity().get("Account")
    region = session.boto_region_name

    # Use SageMaker's processing container
    processing_image = (
        f"{account_id}.dkr.ecr.{region}.amazonaws.com/sentiment-inference:latest"
    )

    # Data Processing Step
    processor = ScriptProcessor(
        command=["python3"],
        image_uri=processing_image,
        role=role_arn,
        instance_count=1,
        instance_type=processing_instance_type,
        base_job_name="sentiment-preprocess",
        env={
            "AWS_DEFAULT_REGION": region,
            "PYTHONPATH": "/opt/ml/code",
            "PYTHONUNBUFFERED": "1",
        },
        volume_size_in_gb=30,
        max_runtime_in_seconds=1200,
        sagemaker_session=session,
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
        code=preprocessing_script,
        job_arguments=["--region", region],
    )

    # Batch Transform Step
    # transformer = Transformer(
    #     model_name="sentiment-analysis-model",
    #     instance_count=transform_instance_count,
    #     instance_type=transform_instance_type,
    #     output_path=transform_output_path,
    #     sagemaker_session=session,
    # )

    # transform_step = TransformStep(
    #     name="SentimentAnalysis",
    #     transformer=transformer,
    #     inputs=TransformInput(
    #         data=processing_output_path + "/data.jsonl",
    #         content_type="application/jsonlines",
    #     ),
    # )

    # Create and return the pipeline
    pipeline = Pipeline(
        name=pipeline_name,
        steps=[processing_step],  # transform_step
        sagemaker_session=session,
    )

    return pipeline
