
--drop table factors_wrds;

CREATE TABLE IF NOT EXISTS factors_wrds
(
    permno int NOT NULL,
    date timestamp without time zone NOT NULL,
	share_class varchar(1),
    ticker varchar(20),
    beta float,
    size int,
    bm float,
    mom float,
    vol float,
    rf float NOT NULL,
    rm float NOT NULL,
    ret float NOT NULL,
	PRIMARY KEY (permno, date)
);

CREATE INDEX IF NOT EXISTS factors_wrds_idx_date ON factors_wrds(date);