
CREATE TABLE IF NOT EXISTS reg_coefs_wrds
(
    date timestamp without time zone NOT NULL,
	factor varchar(50) NOT NULL,	
    coef float NOT NULL,
    tstat float NOT NULL,
    pval float NOT NULL,
    PRIMARY KEY (date, factor)
);