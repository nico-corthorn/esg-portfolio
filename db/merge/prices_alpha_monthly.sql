--DROP TABLE prices_alpha_monthly;
CREATE TABLE prices_alpha_monthly
(
	symbol varchar(20) NOT NULL,
    date date NOT NULL,
    open numeric(14,2),
    high numeric(14,2),
    low numeric(14,2),
    close numeric(14,2),
    adjusted_close numeric(14,2),
	monthly_volume numeric(14,0),
    monthly_return_std float,
    monthly_return float,
	day_count int,
	source_lud timestamp,
	lud timestamp NOT NULL,
	PRIMARY KEY (symbol, date)
);

CREATE INDEX prices_alpha_monthly_symbol_idx ON prices_alpha_monthly (symbol);
CREATE INDEX prices_alpha_monthly_date_idx ON prices_alpha_monthly (date);
