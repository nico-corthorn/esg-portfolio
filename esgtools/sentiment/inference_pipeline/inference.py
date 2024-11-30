import json
import os

import pandas as pd
import torch
from tqdm import tqdm
from transformers import AutoModelForCausalLM, AutoTokenizer

from esgtools.domain_models.io import convert_dict_to_sql_params
from esgtools.utils import aws, sql_manager, utils


# Your existing functions here
def remove_non_letters_except_spaces(input_string):
    return re.sub(r"[^a-zA-Z\s]", "", input_string)


def create_prompt(headline, snippet):
    """Create a standardized prompt for sentiment analysis."""
    return f"""
You are a financial analyst tasked with analyzing news about a specific company. For each news headline and snippet, your job is to determine whether the news is positive, neutral, negative, or unknown for the company's future and its stock price in particular. Respond only with one of these three words: "positive", "neutral", or "negative".

Here is the criteria for each label:
- positive: the news is likely to have a positive impact on the stock price
- neutral: the news is likely to have little to no impact on the stock price
- negative: the news is likely to have a negative impact on the stock price

Provide no explanations, code, or additional information—just the single word answer.

Here are some examples:

News Input:

Credit Suisse Profit Rose 36% in Quarter
The figures beat estimates because costs were lower than expected at the investment bank and revenue was higher.

Answer:
positive

News Input:

Apple Confirms November Event
Apple has confirmed it will hold a product launch event on November 1st, but provided no details about what will be announced.

Answer:
neutral

News Input:

Ford May Produce Its Own Reality TV Show
Ford is pitching a reality show where aspiring car designers would compete to design the next hot Ford vehicle.

Answer:
neutral

News Input:

Merck Admits a Data Error on Vioxx
Merck said that it erred when it reported in early 2005 that a crucial statistical test showed that Vioxx caused heart problems only after 18 months of continuous use.

Answer:
negative

News Input:

Profit Falls as Sales Rise at Verizon
Verizon said its profit dipped as it absorbed the costs of integrating MCI and building a fiber optic network designed to deliver television to homes.

Answer:
negative


Now, analyze this new input:

News Input:
{headline}
{snippet}

Answer:
"""


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
    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"Using device: {device}")

    # Valid sentiment labels
    VALID_SENTIMENTS = {"positive", "neutral", "negative"}
    MAX_RETRIES = 3

    results = []

    # Process each article with progress bar
    for idx, row in tqdm(
        nyt_df.iterrows(), total=len(nyt_df), desc="Processing articles"
    ):
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
                    # temperature=0.3,
                    num_return_sequences=1,
                    pad_token_id=tokenizer.eos_token_id,
                    eos_token_id=tokenizer.eos_token_id,
                )

            # Decode the generated tokens into text
            generated_tokens = output[0][inputs["input_ids"].shape[1] :]
            original_response = (
                tokenizer.decode(generated_tokens, skip_special_tokens=True)
                .strip()
                .lower()
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
                print(
                    f"\nInvalid response '{response}' for article {idx}. Retry {retries}/{MAX_RETRIES}"
                )

        # If still invalid after retries, default to 'neutral'
        if sentiment not in VALID_SENTIMENTS:
            print(
                f"\nWarning: Could not get valid sentiment for article {idx} after {MAX_RETRIES} retries. Default to 'neutral'"
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

        # Clear CUDA cache periodically
        if idx % 100 == 0 and device == "cuda":
            torch.cuda.empty_cache()

    return pd.DataFrame(results)


def model_fn(model_dir):
    """Load the model for inference."""
    repo_id = "meta-llama/Llama-3.1-8B-Instruct"
    hf_token = os.environ.get("HF_TOKEN")  # Will be passed as env variable

    tokenizer = AutoTokenizer.from_pretrained(repo_id, use_auth_token=hf_token)
    model = AutoModelForCausalLM.from_pretrained(
        repo_id,
        torch_dtype=torch.float16 if torch.cuda.is_available() else torch.float32,
        device_map="auto",
        use_auth_token=hf_token,
    )
    return model, tokenizer


def input_fn(input_data, content_type):
    """Parse input data payload."""
    if content_type == "application/json":
        data = json.loads(input_data)
        return pd.DataFrame(data)
    else:
        raise ValueError(f"Unsupported content type: {content_type}")


def predict_fn(input_data, model_and_tokenizer):
    """Make prediction using the input data."""
    model, tokenizer = model_and_tokenizer
    return run_sentiment_analysis(input_data, model, tokenizer)


def output_fn(prediction, accept):
    """Format prediction output."""
    if accept == "application/json":
        return json.dumps(prediction.to_dict(orient="records"))
    raise ValueError(f"Unsupported accept type: {accept}")
