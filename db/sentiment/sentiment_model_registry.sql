
-- DROP TABLE sentiment_model_registry

CREATE TABLE sentiment_model_registry (
    model_id VARCHAR(50) PRIMARY KEY,
    base_model VARCHAR(100) NOT NULL,
    base_version VARCHAR(20) NOT NULL,
    variant_version VARCHAR(20) NOT NULL,
    hyperparameters JSONB,
    prompt_template TEXT,
    training_status VARCHAR(20) CHECK (training_status IN ('base', 'fine-tuned', 'modified')),
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
