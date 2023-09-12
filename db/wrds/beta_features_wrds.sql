
CREATE TABLE IF NOT EXISTS beta_features_wrds
(
    permno int NOT NULL,
    date timestamp without time zone NOT NULL,
    ticker varchar(20),
	log_size float,
	log_bm float,
	log_pcf float,
	mom float,
	strev float,
	vol float,
	roa float,
	roe float,
	log_age_lb float,
	price float,
	bid float,
	ask float,
	log_to float,
	rf float,
	rm float,
	ols_3m_d float,
	ols_1y_d float,
	ols_5y_m float,
	f_ols_1y_d float,
	PRIMARY KEY (permno, date)
);