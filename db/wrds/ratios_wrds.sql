-- DROP TABLE ratios_wrds;


CREATE TABLE ratios_wrds
(
	permno int NOT NULL,
	adate timestamp without time zone,
	qdate timestamp without time zone NOT NULL,
	public_date timestamp without time zone NOT NULL,
	bm float,
	divyield float,
	pcf float,
	roa float,
	roe float
)

