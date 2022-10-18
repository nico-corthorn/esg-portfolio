
-- DROP TABLE assets_alpha

CREATE TABLE assets_alpha
(
	symbol varchar(20) NOT NULL,
	name text NOT NULL,
	exchange varchar(20) NOT NULL,
	asset_type varchar(20) NOT NULL,
	ipo_date date NOT NULL,
	delisting_date date NULL,
	status varchar(20) NOT NULL
)
