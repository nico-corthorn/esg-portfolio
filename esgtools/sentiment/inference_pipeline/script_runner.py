import logging
import os
import subprocess
import sys

import boto3

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def download_script(s3_uri, local_path):
    """Download a script from S3 and make it executable."""
    logger.info("Attempting to download from %s to %s", s3_uri, local_path)
    if not s3_uri.startswith("s3://"):
        return local_path

    bucket = s3_uri.split("/")[2]
    key = "/".join(s3_uri.split("/")[3:])

    logger.info("Downloading from bucket: %s, key: %s", bucket, key)
    s3 = boto3.client("s3")
    s3.download_file(bucket, key, local_path)
    os.chmod(local_path, 0o755)
    logger.info("Downloaded and made executable: %s", local_path)
    return local_path


def install_package(s3_uri, local_path):
    """Download and install a package from S3."""
    logger.info("Starting package installation from %s", s3_uri)
    try:
        download_script(s3_uri, local_path)
        # Use the same Python interpreter that's running this script
        python_exe = sys.executable
        logger.info("Using Python interpreter: %s", python_exe)

        # Install the package with pip
        result = subprocess.run(
            [python_exe, "-m", "pip", "install", "-v", local_path],
            check=True,
            capture_output=True,
            text=True,
            env={**os.environ, "PYTHONPATH": "/opt/ml/code"},
        )
        logger.info("Pip install output: %s", result.stdout)

        # Verify installation by importing in the same Python process
        try:
            logger.info("Package verification successful (direct import)")
        except ImportError as e:
            logger.error("Direct import failed: %s", str(e))
            raise

    except Exception as e:
        logger.error("Error during package installation: %s", str(e))
        raise


def main():
    """Main entry point for the script runner."""
    logger.info("Script runner started")
    logger.info("Python executable: %s", sys.executable)
    logger.info("PYTHONPATH: %s", os.environ.get("PYTHONPATH", "not set"))
    logger.info("Current working directory: %s", os.getcwd())
    logger.info("Directory contents: %s", os.listdir("."))

    # Set region for boto3
    region = "us-east-2"  # Hardcoded for now
    os.environ["AWS_DEFAULT_REGION"] = region

    # Install esgtools package
    bucket = f"sagemaker-{region}-654580413909"
    package_uri = os.environ.get(
        "ESGTOOLS_PACKAGE_URI", f"s3://{bucket}/sentiment/packages/esgtools.tar.gz"
    )
    logger.info("Installing package from: %s", package_uri)

    install_package(package_uri, "/opt/ml/code/esgtools.tar.gz")

    # Check if we're in processing or transform mode
    if os.environ.get("SAGEMAKER_PROGRAM") == "serve":
        logger.info("Running in serve mode")
        serve_script = download_script(
            os.environ.get("SERVE_SCRIPT_URI", "/opt/ml/code/serve"), "/opt/ml/code/serve"
        )
        _ = download_script(
            os.environ.get("INFERENCE_SCRIPT_URI", "/opt/ml/code/inference.py"),
            "/opt/ml/code/inference.py",
        )
        subprocess.run([sys.executable, serve_script], check=True, env=os.environ)
    else:
        logger.info("Running in processing mode")
        preprocess_script = download_script(
            os.environ.get("PREPROCESSING_SCRIPT_URI", "/opt/ml/code/preprocessing.py"),
            "/opt/ml/code/preprocessing.py",
        )
        logger.info("Running preprocessing script: %s", preprocess_script)
        subprocess.run(
            [sys.executable, preprocess_script] + sys.argv[1:], check=True, env=os.environ
        )


if __name__ == "__main__":
    main()
