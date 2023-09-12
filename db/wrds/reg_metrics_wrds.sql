

CREATE TABLE IF NOT EXISTS reg_metrics_wrds
(
    date timestamp without time zone NOT NULL,
	metric varchar(50) NOT NULL,	
    val float NOT NULL,
    PRIMARY KEY (date, metric)
);