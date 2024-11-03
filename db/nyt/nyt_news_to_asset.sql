
-- DROP TABLE nyt_news_to_asset

CREATE TABLE nyt_news_to_asset
(
    web_url text NOT NULL,
    year_month varchar(6) NOT NULL,
	organization text NOT NULL,
	asset_ids text NOT NULL,
	PRIMARY KEY (web_url, organization)
)