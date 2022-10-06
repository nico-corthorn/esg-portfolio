
-- DROP TABLE assets_tiingo

CREATE TABLE assets_tiingo
(
	ticker varchar(20) NOT NULL,
	exchange varchar(20) NOT NULL,
	asset_type varchar(20) NOT NULL,
	start_date date NULL,
	end_date date NULL,
	lud timestamp without time zone NOT NULL,
	PRIMARY KEY (ticker)
)
