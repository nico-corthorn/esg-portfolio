

CREATE TABLE nyt_sentiment_inference (
    web_url text REFERENCES nyt_archive(web_url),
    model_id VARCHAR(50) REFERENCES sentiment_model_registry(model_id),
    text_output text NOT NULL,
    sentiment VARCHAR(10) NOT NULL CHECK (sentiment IN ('positive', 'neutral', 'negative')),
	sentiment_score DECIMAL(4,3),
	additional_outputs JSONB,
	lud TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (web_url, model_id)
);