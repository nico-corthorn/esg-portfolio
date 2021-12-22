
-- DROP TABLE nyt_archive

CREATE TABLE nyt_archive
(
	web_url text NOT NULL,
	year_month varchar(6) NOT NULL,
	pub_date timestamp without time zone NOT NULL,
	organizations text NOT NULL,
	subjects text NOT NULL,
	headline text NOT NULL,
	snippet text NOT NULL,
	lud timestamp without time zone NOT NULL,
	PRIMARY KEY (web_url)
)