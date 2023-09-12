
CREATE TABLE IF NOT EXISTS betas_wrds
(
    permno int NOT NULL,
    date timestamp without time zone NOT NULL,
    ols_3m_d float,
    ols_1y_d float,
    ols_5y_m float,
    f_ols_1y_d float,
	PRIMARY KEY (permno, date)
);