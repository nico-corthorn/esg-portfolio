import json
import logging
import os
import re
from ast import literal_eval

import pandas as pd
import torch
from tqdm import tqdm
from transformers import AutoModelForCausalLM, AutoTokenizer

from esgtools.domain_models.io import convert_dict_to_sql_params
from esgtools.sentiment.inference_pipeline.preprocessing import generate_model_id
from esgtools.utils import aws, sql_manager

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())


class HuggingFaceLoader:
    def __init__(self):
        self.model = None
        self.tokenizer = None
        # Check CUDA and show more info about the GPU
        self.device = self._get_device()

    def _get_device(self):
        """Get the appropriate device with detailed logging."""
        if torch.cuda.is_available():
            device = "cuda"
            logger.info("CUDA is available. Using GPU: %s", torch.cuda.get_device_name(0))
            logger.info("CUDA Device Count: %d", torch.cuda.device_count())
            logger.info("CUDA Version: %s", torch.version.cuda)
            return device
        logger.warning("CUDA is not available. Using CPU. This will be much slower!")
        return "cpu"

    def load_model(self):
        """Load the model and tokenizer if not already loaded."""
        if self.model is None or self.tokenizer is None:
            hf_model_id = os.environ.get("HF_MODEL_ID")
            hf_token_secret_name = os.environ.get("HF_TOKEN_SECRET_NAME")
            hf_token_secret_key = os.environ.get("HF_TOKEN_SECRET_KEY")
            hf_token = literal_eval(aws.get_secret(hf_token_secret_name))[hf_token_secret_key]

            logger.info("Loading tokenizer...")
            self.tokenizer = AutoTokenizer.from_pretrained(hf_model_id, token=hf_token)

            logger.info("Loading model to %s...", self.device)
            hf_config = {
                "token": hf_token,
                "torch_dtype": torch.float16 if self.device == "cuda" else torch.float32,
                "use_safetensors": True,
                "low_cpu_mem_usage": True,
            }

            # Only set device_map if using CUDA
            if self.device == "cuda":
                hf_config["device_map"] = "auto"

            self.model = AutoModelForCausalLM.from_pretrained(hf_model_id, **hf_config)

            # If using CPU, we need to move the model explicitly
            if self.device == "cpu":
                logger.info("Moving model to CPU...")
                self.model = self.model.to(self.device)

            logger.info("Model and tokenizer loaded successfully")
            logger.info("Model is on device: %s", next(self.model.parameters()).device)


# Create a single instance
hf_loader = HuggingFaceLoader()


def save_sentiment_results(
    results_df: pd.DataFrame, original_df: pd.DataFrame, model_id: str, sql: sql_manager.ManagerSQL
):
    """
    Save sentiment analysis results to the nyt_sentiment_inference table.

    Args:
        results_df (pd.DataFrame): DataFrame containing sentiment analysis results
        original_df (pd.DataFrame): Original DataFrame containing web_url and other fields
        model_id (str): ID of the model used for inference
        sql (sql_manager.ManagerSQL): Database connection object
    """
    logger.info("Preparing to save sentiment results for model %s", model_id)

    # Merge results with original data to get web_url
    merged_df = pd.merge(
        results_df,
        original_df[["headline", "snippet", "web_url"]],
        left_on=["headline", "snippet"],
        right_on=["headline", "snippet"],
        how="inner",
    )

    # Prepare data for database insertion
    db_records = []
    for _, row in merged_df.iterrows():
        record = {
            "web_url": row["web_url"],
            "model_id": model_id,
            "text_output": row["output"],
            "sentiment": row["sentiment"],
            "sentiment_score": None,  # Can be updated later when implementing scoring
            "additional_outputs": {
                "retries": row["retries"],
                "headline": row["headline"],
                "snippet": row["snippet"],
            },
        }
        db_records.append(record)

    # Convert to DataFrame for database insertion
    db_df = pd.DataFrame(db_records)

    try:
        # Upload to database in chunks to handle large datasets efficiently
        logger.info("Uploading %d records to nyt_sentiment_inference table", len(db_df))
        sql.upload_df_chunks("nyt_sentiment_inference", db_df, chunk_size=100)
        logger.info("Successfully saved sentiment results to database")
    except Exception as e:
        logger.error("Error saving sentiment results to database: %s", str(e))
        raise


def remove_non_letters_except_spaces(input_string):
    return re.sub(r"[^a-zA-Z\s]", "", input_string)


def create_prompt(headline, snippet):
    """Create a standardized prompt for sentiment analysis."""
    prompt_template = aws.get_s3_file("sentiment/config/prompt_template.txt")
    return prompt_template.format(headline=headline, snippet=snippet)


def run_sentiment_analysis(nyt_df, model, tokenizer):
    """
    Run sentiment analysis on the entire DataFrame with output validation.

    Args:
        nyt_df (pd.DataFrame): Input DataFrame containing 'headline' and 'snippet' columns
        model: The loaded LLaMA model
        tokenizer: The loaded tokenizer

    Returns:
        pd.DataFrame: Results DataFrame with sentiment analysis
    """
    device = hf_loader.device  # Use the same device as the loader
    logger.info("Running inference on device: %s", device)

    if device == "cuda":
        # Log GPU memory usage
        logger.info("GPU Memory before starting: %.2f GB", torch.cuda.memory_allocated() / 1e9)

    # Valid sentiment labels
    VALID_SENTIMENTS = {"positive", "neutral", "negative"}
    MAX_RETRIES = 3

    results = []

    try:
        # Process each article with progress bar
        for idx, row in tqdm(nyt_df.iterrows(), total=len(nyt_df), desc="Processing articles"):
            sentiment = None
            retries = 0

            while sentiment not in VALID_SENTIMENTS and retries < MAX_RETRIES:
                prompt = create_prompt(row["headline"], row["snippet"])

                # Tokenize the input prompt
                inputs = tokenizer(prompt, return_tensors="pt").to(device)

                # Generate output from the model with more tokens to ensure complete response
                with torch.no_grad():
                    output = model.generate(
                        **inputs,
                        max_new_tokens=4,
                        do_sample=True,
                        num_return_sequences=1,
                        pad_token_id=tokenizer.eos_token_id,
                        eos_token_id=tokenizer.eos_token_id,
                    )

                # Decode the generated tokens into text
                generated_tokens = output[0][inputs["input_ids"].shape[1] :]
                original_response = (
                    tokenizer.decode(generated_tokens, skip_special_tokens=True).strip().lower()
                )

                # Clean up the response
                # Replace line break
                response = original_response.replace("\n", " ")
                # Remove non-letter characters
                response = remove_non_letters_except_spaces(response)
                # Remove common prefixes that might appear
                prefixes_to_remove = ["answer:", "answer"]
                for prefix in prefixes_to_remove:
                    if response.startswith(prefix):
                        response = response[len(prefix) :].strip()

                # Extract the first word as sentiment
                sentiment = response.split()[0] if response else None

                # Validate sentiment
                if sentiment not in VALID_SENTIMENTS:
                    retries += 1
                    logger.info(
                        "Invalid response '%s' for article %d. Retry %d/%d",
                        response,
                        idx,
                        retries,
                        MAX_RETRIES,
                    )

            # If still invalid after retries, default to 'neutral'
            if sentiment not in VALID_SENTIMENTS:
                logger.info(
                    "Warning: Could not get valid sentiment for article %d after %d retries. "
                    "Default to 'neutral'",
                    idx,
                    MAX_RETRIES,
                )
                sentiment = "neutral"

            results.append(
                {
                    "id": idx,
                    "headline": row["headline"],
                    "snippet": row["snippet"],
                    "output": original_response,
                    "sentiment": sentiment,
                    "retries": retries,
                }
            )

            # Better CUDA memory management
            if idx % 10 == 0 and device == "cuda":  # Clear cache more frequently
                torch.cuda.empty_cache()
                logger.info(
                    "GPU Memory after %d items: %.2f GB", idx, torch.cuda.memory_allocated() / 1e9
                )

    finally:
        # Clean up CUDA memory at the end
        if device == "cuda":
            torch.cuda.empty_cache()
            logger.info("Final GPU Memory: %.2f GB", torch.cuda.memory_allocated() / 1e9)

    logger.info("results: %s", str(results))

    results_df = pd.DataFrame(results)

    # Get database connection
    try:
        sql_params = convert_dict_to_sql_params(
            literal_eval(aws.get_secret("prod/awsportfolio/key"))
        )
        sql = sql_manager.ManagerSQL(sql_params)

        # Generate model ID using the existing function
        model_id = generate_model_id(
            os.environ.get("BASE_MODEL", "LLAMA2"),
            os.environ.get("BASE_VERSION", "3.1-8B"),
            os.environ.get("VARIANT_VERSION", "INSTRUCT"),
        )

        # Save results to database
        save_sentiment_results(results_df, nyt_df, model_id, sql)

    except Exception as e:
        logger.error("Error in database operations: %s", str(e))
        # Don't raise here to ensure the function still returns results

    return pd.DataFrame(results)


def input_fn(input_data, content_type):
    """Parse input data payload."""
    logger.info("Received content type: %s", content_type)

    if content_type in ["application/json", "application/jsonlines"]:
        # For jsonlines, we need to split the input and parse each line
        if isinstance(input_data, str):
            # Parse each line as a separate JSON object
            records = [json.loads(line) for line in input_data.split("\n") if line.strip()]
            return pd.DataFrame(records)

        # Handle single JSON object/array case
        data = json.loads(input_data)
        return pd.DataFrame(data)
    raise ValueError(f"Unsupported content type: {content_type}")


def model_fn(model_dir):  # pylint: disable=unused-argument
    """Load the model for inference."""
    hf_loader.load_model()
    return hf_loader.model, hf_loader.tokenizer


def predict_fn(input_data, model_and_tokenizer):  # pylint: disable=unused-argument
    """Make prediction using the input data."""
    return run_sentiment_analysis(input_data, hf_loader.model, hf_loader.tokenizer)


def output_fn(prediction, accept):
    """Format prediction output."""
    if accept == "application/json":
        return json.dumps(prediction.to_dict(orient="records"))
    raise ValueError(f"Unsupported accept type: {accept}")
