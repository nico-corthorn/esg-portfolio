

CREATE TABLE prices_alpha
(
	symbol varchar(20) NOT NULL,
    date date NOT NULL,
    open numeric(14,2) NOT NULL,
    high numeric(14,2) NOT NULL,
    low numeric(14,2) NOT NULL,
    close numeric(14,2) NOT NULL,
    adjusted_close numeric(14,2) NULL,
	volume integer NOT NULL,
    dividend_amount numeric(14,1) NULL,
    split_coefficient numeric(14,1) NULL,
	lud timestamp NOT NULL,
	PRIMARY KEY (symbol, date, lud)
)



