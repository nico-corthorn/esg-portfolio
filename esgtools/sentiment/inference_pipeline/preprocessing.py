import json
import logging
import os
import sys
import traceback
from ast import literal_eval
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Optional

from esgtools.domain_models.io import convert_dict_to_sql_params
from esgtools.utils import aws, sql_manager

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.handlers = []
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
console_handler.setFormatter(console_formatter)
logger.addHandler(console_handler)


@dataclass
class ModelConfig:
    base_model: str
    base_version: str
    variant_version: str
    hyperparameters: Dict
    prompt_template: Optional[str] = None
    training_status: str = "base"
    description: Optional[str] = None

    def validate_training_status(self):
        valid_statuses = {"base", "fine-tuned", "modified"}
        if self.training_status not in valid_statuses:
            raise ValueError(f"training_status must be one of {valid_statuses}")


def generate_model_id(base_model: str, base_version: str, variant_version: str) -> str:
    """Generate a unique model ID following a consistent format."""
    components = [base_model.upper(), base_version.upper(), variant_version.upper()]
    return "-".join(components)


def register_model(config: ModelConfig, sql: sql_manager.ManagerSQL) -> str:
    """Register a new model in the sentiment_model_registry table."""
    config.validate_training_status()

    model_id = generate_model_id(config.base_model, config.base_version, config.variant_version)

    # Check if the model_id already exists
    existing_model_query = "SELECT * FROM sentiment_model_registry WHERE model_id = '{model_id}'"
    existing_model = sql.select_query(existing_model_query.format(model_id=model_id))

    if not existing_model.empty:
        # Model ID exists, check if the configuration matches (excluding created_at)
        existing_row = existing_model.iloc[0]
        existing_config = {
            "base_model": existing_row["base_model"],
            "base_version": existing_row["base_version"],
            "variant_version": existing_row["variant_version"],
            "hyperparameters": json.dumps(existing_row["hyperparameters"]),
            "prompt_template": existing_row["prompt_template"],
            "training_status": existing_row["training_status"],
            "description": existing_row["description"],
        }
        new_config = {
            "base_model": config.base_model,
            "base_version": config.base_version,
            "variant_version": config.variant_version,
            "hyperparameters": json.dumps(config.hyperparameters),
            "prompt_template": config.prompt_template,
            "training_status": config.training_status,
            "description": config.description,
        }

        if existing_config == new_config:
            # Configurations match, do nothing
            return model_id

        # Log the differences between existing_config and new_config
        for key, value in existing_config.items():
            if value != new_config[key]:
                logger.error(
                    "Configuration mismatch for %s: existing=%s, new=%s",
                    key,
                    value,
                    new_config[key],
                )

        # Configurations do not match, raise an error
        raise ValueError(f"Model ID {model_id} already exists with a different configuration.")

    # Model ID does not exist, insert the new model configuration
    now = datetime.now()
    insert_query = """
        INSERT INTO sentiment_model_registry 
        (model_id, base_model, base_version, variant_version, hyperparameters, prompt_template, 
            training_status, description, created_at)
        VALUES (%(model_id)s, %(base_model)s, %(base_version)s, %(variant_version)s, %(hyperparameters)s, 
                %(prompt_template)s, %(training_status)s, %(description)s, %(created_at)s)
    """
    params = {
        "model_id": model_id,
        "base_model": config.base_model,
        "base_version": config.base_version,
        "variant_version": config.variant_version,
        "hyperparameters": json.dumps(config.hyperparameters),
        "prompt_template": config.prompt_template,
        "training_status": config.training_status,
        "description": config.description,
        "created_at": now,
    }
    sql.query(insert_query, params)
    return model_id


def load_config():
    """Load model configuration from S3."""
    try:
        config_content = aws.get_s3_file("sentiment/config/model_config.json")
        return json.loads(config_content)
    except Exception as e:
        logger.error("Error loading config: %s", str(e))
        raise


def fetch_and_prepare_data():
    try:
        logger.info("Starting fetch_and_prepare_data")

        # Create output directory if running in SageMaker environment
        output_dir = "/opt/ml/processing/output"
        if os.environ.get("SAGEMAKER_PROGRAM"):
            os.makedirs(output_dir, exist_ok=True)

        # Load configuration
        config = load_config()
        logger.info("Configuration loaded successfully")

        # Get database credentials
        sql_params = convert_dict_to_sql_params(
            literal_eval(aws.get_secret("prod/awsportfolio/key"))
        )
        sql = sql_manager.ManagerSQL(sql_params)

        # Load prompt template
        prompt_template = aws.get_s3_file(config["model"]["prompt_template_path"])

        # Register model
        model_config = ModelConfig(
            base_model=config["model"]["base_model"],
            base_version=config["model"]["base_version"],
            variant_version=config["model"]["variant_version"],
            hyperparameters=config["model"]["hyperparameters"],
            prompt_template=prompt_template,
            description=config["model"]["description"],
        )

        model_id = register_model(model_config, sql)
        logger.info("Model registered with ID: %s", model_id)

        # Fetch data
        year_month = "200605"
        query = f"""
        SELECT headline, snippet 
        FROM nyt_archive 
        WHERE year_month = '{year_month}'
        """
        nyt_data = sql.select_query(query)
        nyt_data = nyt_data.head()
        logger.info("Retrieved %d records from database", len(nyt_data))

        inference_data = nyt_data[["headline", "snippet"]]

        # Save to output
        output_path = os.path.join("/opt/ml/processing/output", "data.jsonl")
        records_written = 0

        with open(output_path, "w", encoding="utf-8") as f:
            for _, row in inference_data.iterrows():
                f.write(json.dumps(row.to_dict()) + "\n")
                records_written += 1

        logger.info("Successfully wrote %d records to %s", records_written, output_path)

    except Exception as e:
        logger.error("Error in fetch_and_prepare_data: %s", str(e))
        logger.error("Full traceback: %s", traceback.format_exc())
        raise


if __name__ == "__main__":
    try:
        logger.info("Script started")
        fetch_and_prepare_data()
        logger.info("Preprocessing completed successfully")
    except Exception as e:
        logger.error("Fatal error: %s", str(e))
        sys.exit(1)
