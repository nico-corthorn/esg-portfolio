
--DROP TABLE prices_tiingo

CREATE TABLE prices_tiingo
(
	ticker varchar(20) NOT NULL,
    trade_date date NOT NULL,
    price_open numeric(14,2) NOT NULL,
    price_close numeric(14,2) NOT NULL,
	volume integer NOT NULL,
    adj_close numeric(14,2) NULL,
    div_cash numeric(14,1) NULL,
    split numeric(14,1) NULL,
	PRIMARY KEY (ticker, trade_date),
    FOREIGN KEY (ticker) REFERENCES assets_tiingo (ticker) ON DELETE CASCADE
)
