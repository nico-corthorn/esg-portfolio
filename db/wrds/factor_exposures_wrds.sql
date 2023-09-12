
--drop table factor_exp_wrds;

CREATE TABLE IF NOT EXISTS factor_exp_wrds
(
    permno int NOT NULL,
    date timestamp without time zone NOT NULL,
	share_class varchar(1),
    ticker varchar(20),
    beta float,
    size numeric(12, 1),
    bm float,
    mom float,
    vol float,
    rf float NOT NULL,
    rm float NOT NULL,
    ret float NOT NULL,
	next_ret float NULL,
	PRIMARY KEY (permno, date)
);

CREATE INDEX IF NOT EXISTS factor_exp_wrds_idx_date ON factor_exp_wrds(date);