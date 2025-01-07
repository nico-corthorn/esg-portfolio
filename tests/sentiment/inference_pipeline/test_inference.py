import json
from unittest.mock import patch

import pandas as pd
import pytest

from esgtools.sentiment.inference_pipeline.inference import (
    ModelHyperparameters,
    create_prompt,
    input_fn,
    output_fn,
    remove_non_letters_except_spaces,
    save_sentiment_results,
)
from esgtools.sentiment.inference_pipeline.preprocessing import ModelConfig, register_model


@pytest.mark.unit
def test_remove_non_letters_except_spaces():
    """Test the remove_non_letters_except_spaces function"""
    input_string = "Hello, World! 123"
    expected_output = "Hello World"
    assert remove_non_letters_except_spaces(input_string) == expected_output


@pytest.mark.unit
def test_create_prompt():
    """Test the create_prompt function with mocked S3 response"""
    mock_prompt_template = "Headline: {headline}\nSnippet: {snippet}"
    with patch("esgtools.utils.aws.get_s3_file") as mock_get_s3_file:
        mock_get_s3_file.return_value = mock_prompt_template
        prompt = create_prompt("Test Headline", "Test Snippet")
        assert prompt == "Headline: Test Headline\nSnippet: Test Snippet"
        mock_get_s3_file.assert_called_once_with("sentiment/config/prompt_template.txt")


@pytest.mark.unit
def test_input_fn():
    """Test the input_fn function"""
    input_data = '{"headline": "Test Headline", "snippet": "Test Snippet"}'
    content_type = "application/json"
    df = input_fn(input_data, content_type)
    assert isinstance(df, pd.DataFrame)
    assert df.iloc[0]["headline"] == "Test Headline"
    assert df.iloc[0]["snippet"] == "Test Snippet"


@pytest.mark.unit
def test_output_fn():
    """Test the output_fn function"""
    prediction = pd.DataFrame([{"headline": "Test Headline", "sentiment": "positive"}])
    accept = "application/json"
    output = output_fn(prediction, accept)
    assert isinstance(output, str)
    assert json.loads(output)[0]["headline"] == "Test Headline"
    assert json.loads(output)[0]["sentiment"] == "positive"


@pytest.mark.integration
def test_save_sentiment_results_integration(sql):
    """Test saving sentiment results to the database"""
    # Register a model in sentiment_model_registry using register_model function
    model_config = ModelConfig(
        base_model="TESTMODEL",
        base_version="0B",
        variant_version="TEST",
        hyperparameters={"temperature": 0.7},
        prompt_template="Test prompt template {headline} {snippet}",
        training_status="base",
        description="Test model",
    )
    model_id = register_model(model_config, sql)

    # Save sentiment results
    input_df = pd.DataFrame(
        [
            {
                "web_url": "https://www.nytimes.com/2012/01/02/technology/google-hones-its-advertising-message-playing-to-emotions.html",
                "output": "neutral",
                "sentiment": "neutral",
                "retries": 0,
            }
        ]
    )
    save_sentiment_results(input_df, model_id, sql)

    #  Load the saved results from the database
    actual_df = sql.select_query(
        f"SELECT * FROM nyt_sentiment_inference WHERE model_id = '{model_id}'"
    )

    # Cleanup
    sql.query(
        "DELETE FROM nyt_sentiment_inference WHERE model_id = %(model_id)s", {"model_id": model_id}
    )
    sql.query(
        "DELETE FROM sentiment_model_registry WHERE model_id = %(model_id)s", {"model_id": model_id}
    )

    assert actual_df is not None
    actual_row = actual_df.loc[0]
    expected_row = input_df.iloc[0]
    assert actual_row["web_url"] == expected_row["web_url"]
    assert actual_row["model_id"] == model_id
    assert actual_row["text_output"] == expected_row["output"]
    assert actual_row["sentiment"] == expected_row["sentiment"]
    assert actual_row["additional_outputs"] == {"retries": expected_row["retries"]}


@pytest.mark.unit
def test_load_hyperparameters():
    """Test the load_hyperparameters method"""
    mock_config = {
        "model": {
            "base_model": "llama2",
            "base_version": "7B",
            "variant_version": "001",
            "hyperparameters": {"temperature": 0.7, "max_tokens": 2},
        }
    }

    with patch("esgtools.sentiment.inference_pipeline.inference.load_config") as mock_load_config:
        mock_load_config.return_value = mock_config

        model_hyper = ModelHyperparameters()
        model_hyper.load_hyperparameters()

        assert model_hyper.config == mock_config
        assert model_hyper.model_id == "LLAMA2-7B-001"
        assert model_hyper.hyperparameters == {"temperature": 0.7, "max_tokens": 2}
