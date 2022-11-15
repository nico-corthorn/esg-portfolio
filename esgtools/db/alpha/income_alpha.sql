-- DROP TABLE income_alpha

CREATE TABLE income_alpha
(
	symbol varchar(20) NOT NULL,
	report_type varchar(1) NOT NULL, 
	report_date date NOT NULL,
	currency varchar(3) NOT NULL,
	account_name text NOT NULL,
	account_value bigint, 
	lud timestamp NOT NULL,
	PRIMARY KEY (symbol, report_type, report_date, currency, account_name)
)

