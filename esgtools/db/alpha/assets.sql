

-- DROP TABLE assets

CREATE TABLE assets
(
	pid int NOT NULL,
	symbol varchar(20) NOT NULL,
	name text NOT NULL,
	asset_type varchar(20) NOT NULL,
	ipo_date date NOT NULL,
	delisting_date date NULL,
	PRIMARY KEY (pid)
)