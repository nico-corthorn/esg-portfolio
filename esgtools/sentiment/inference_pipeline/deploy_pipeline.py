import boto3

from esgtools.sentiment.inference_pipeline.sagemaker_pipeline import create_pipeline


def deploy():
    """Deploy the SageMaker pipeline."""

    # Get role ARN using boto3 (uses existing credentials)
    iam = boto3.client("iam")
    role_arn = iam.get_role(RoleName="SageMaker-DataScientist")["Role"]["Arn"]

    # Create and start the pipeline
    pipeline = create_pipeline(role_arn=role_arn)
    pipeline.upsert(role_arn=role_arn)
    execution = pipeline.start()

    print(f"Pipeline started. Execution ID: {execution.arn}")


if __name__ == "__main__":
    deploy()
