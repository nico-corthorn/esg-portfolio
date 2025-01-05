import json
from unittest.mock import MagicMock, patch

import pytest

from esgtools.sentiment.inference_pipeline.preprocessing import (
    ModelConfig,
    generate_model_id,
    load_config,
    register_model,
)
from esgtools.utils.sql_manager import ManagerSQL


@pytest.mark.unit
def test_model_config_validation_unit_test():
    """Test ModelConfig validation"""
    # Valid config
    config = ModelConfig(
        base_model="llama2",
        base_version="7B",
        variant_version="001",
        hyperparameters={"temperature": 0.7},
        training_status="base",
    )
    config.validate_training_status()  # Should not raise

    # Invalid training status
    with pytest.raises(ValueError):
        invalid_config = ModelConfig(
            base_model="llama2",
            base_version="7B",
            variant_version="001",
            hyperparameters={"temperature": 0.7},
            training_status="invalid",
        )
        invalid_config.validate_training_status()


@pytest.mark.unit
def test_generate_model_id_unit_test():
    """Test model ID generation"""
    model_id = generate_model_id("llama2", "7B", "001")
    assert model_id == "LLAMA2-7B-001"
    assert isinstance(model_id, str)
    assert len(model_id.split("-")) == 3


@pytest.mark.unit
def test_load_config_unit_test():
    """Test config loading with mocked S3 responses"""
    mock_config = {
        "model": {
            "base_model": "llama2",
            "base_version": "7B",
            "variant_version": "001",
            "hyperparameters": {"temperature": 0.7},
            "prompt_template_path": "sentiment/config/prompt_template.txt",
            "description": "Test model",
        }
    }

    with patch("esgtools.utils.aws.get_s3_file") as mock_get_s3_file:
        mock_get_s3_file.return_value = json.dumps(mock_config)
        config = load_config()
        assert config["model"]["base_model"] == "llama2"
        assert config["model"]["prompt_template_path"] == "sentiment/config/prompt_template.txt"
        mock_get_s3_file.assert_called_once_with("sentiment/config/model_config.json")


@pytest.mark.unit
def test_register_model_unit_test():
    """Test register_model function with mocked SQL connection"""
    # Mock SQL manager
    mock_sql = MagicMock(spec=ManagerSQL)

    config = ModelConfig(
        base_model="llama2",
        base_version="7B",
        variant_version="001",
        hyperparameters={"temperature": 0.7},
        training_status="base",
    )

    expected_model_id = "LLAMA2-7B-001"

    actual_model_id = register_model(config, mock_sql)

    # Assert the model_id is correct
    assert actual_model_id == expected_model_id
    mock_sql.query.assert_called_once()


@pytest.mark.integration
def test_register_model_integration(sql):
    """Test actual model registration in database"""
    # Mock S3 responses for config loading
    mock_config = {
        "model": {
            "base_model": "TESTMODEL",
            "base_version": "0B",
            "variant_version": "TEST",
            "hyperparameters": {"temperature": 0.7},
            "prompt_template_file": "sentiment/config/prompt_template.txt",
            "description": "Test model",
        }
    }

    mock_prompt = "Test prompt template {headline} {snippet}"

    with patch("esgtools.utils.aws.get_s3_file") as mock_get_s3_file:
        mock_get_s3_file.side_effect = [json.dumps(mock_config), mock_prompt]

        config = ModelConfig(
            base_model=mock_config["model"]["base_model"],
            base_version=mock_config["model"]["base_version"],
            variant_version=mock_config["model"]["variant_version"],
            hyperparameters=mock_config["model"]["hyperparameters"],
            prompt_template=mock_prompt,
            training_status="base",
            description=mock_config["model"]["description"],
        )

        # Register model
        model_id = register_model(config, sql)

        # Verify model was registered
        result = sql.select_query(
            f"SELECT * FROM sentiment_model_registry WHERE model_id = '{model_id}'"
        )

        # Cleanup
        sql.query(
            "DELETE FROM sentiment_model_registry WHERE model_id = %(model_id)s",
            {"model_id": model_id},
        )

        assert result is not None
        row = result.loc[0]
        assert row["model_id"] == model_id
        assert row["base_model"] == config.base_model
        assert row["base_version"] == config.base_version
        assert row["variant_version"] == config.variant_version
        assert json.dumps(row["hyperparameters"]) == json.dumps(config.hyperparameters)
        assert row["prompt_template"] == config.prompt_template
        assert row["training_status"] == config.training_status
        assert row["description"] == config.description
