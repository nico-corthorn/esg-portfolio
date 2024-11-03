
-- DROP TABLE nyt_org_to_asset

CREATE TABLE nyt_org_to_asset
(
	organization text NOT NULL,
	count int NOT NULL,
	asset_ids text NOT NULL,
	PRIMARY KEY (organization)
)