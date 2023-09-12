--drop table returns_wrds;

CREATE TABLE IF NOT EXISTS returns_wrds
(
    permno int NOT NULL,
    date timestamp without time zone NOT NULL,
	primary_exch varchar(1) NOT NULL,
	share_class varchar(1),
    ticker varchar(20),
	price float NOT NULL,
	bid float,
	ask float,
	shrout int NOT NULL,
	std_month float,
	volume_month float,
	ret_month float,
    PRIMARY KEY (permno, date)
);

CREATE INDEX IF NOT EXISTS returns_wrds_idx_date ON returns_wrds(date);
--CREATE INDEX IF NOT EXISTS returns_wrds_idx_permno ON returns_wrds(permno);

