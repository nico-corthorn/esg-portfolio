

CREATE TABLE prices_alpha
(
	symbol varchar(20) NOT NULL,
    date date NOT NULL,
    open numeric(14,2) NOT NULL,
    high numeric(14,2) NOT NULL,
    low numeric(14,2) NOT NULL,
    close numeric(14,2) NOT NULL,
    adjusted_close numeric(18,2) NULL,
	volume bigint NOT NULL,
    dividend_amount numeric(14,1) NULL,
    split_coefficient numeric(14,1) NULL,
	lud timestamp NOT NULL,
	PRIMARY KEY (symbol, date, lud)
)

--DROP INDEX symbol_idx;
CREATE INDEX prices_alpha_symbol_idx ON prices_alpha (symbol);
CREATE INDEX prices_alpha_date_idx ON prices_alpha (date);




