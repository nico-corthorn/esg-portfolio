
--drop table factor_exposures_scaled_wrds;

CREATE TABLE IF NOT EXISTS factor_exposures_scaled_wrds
(
    permno int NOT NULL,
    date timestamp without time zone NOT NULL,
	share_class varchar(1),
    ticker varchar(20),
    beta float,
    size float,
    bm float,
    mom float,
    vol float,
    rf float NOT NULL,
    rm float NOT NULL,
    ret float NOT NULL,
	next_ret float NULL,
	weight float,
	PRIMARY KEY (permno, date)
);

CREATE INDEX IF NOT EXISTS factor_exposures_scaled_wrds_idx_date ON factor_exposures_scaled_wrds(date);