
-- DROP TABLE nyt_sentiment_inference

CREATE TABLE nyt_sentiment_inference (
    web_url text NOT NULL,
    model_id VARCHAR(50) REFERENCES sentiment_model_registry(model_id) NOT NULL,
    sentiment VARCHAR(10),
    sentiment_score DECIMAL(4,3),
	additional_outputs JSONB,
    lud TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY (web_url, model_id)
);

