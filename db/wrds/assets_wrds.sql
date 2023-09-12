
-- drop table assets_wrds

create table assets_wrds as
select
ticker, permno
, case 
	when primary_exch = 'Q' then 'NASDAQ'
	when primary_exch = 'N' then 'NYSE'
	when primary_exch = 'R' then 'NYSE ARCA'
	when primary_exch = 'B' then 'BATS'
	when primary_exch = 'A' then 'NYSE MKT'
	else primary_exch
	end primary_exch
, share_class
, row_number() over(partition by ticker order by share_class, max_date desc) rnk
, min_date ipo_date_proxy
, case when max_date >= '2022-03-31' then NULL else max_date end delisting_date_proxy
from (
	select 
	ticker, permno, primary_exch
	, case when share_class is NULL then 'A' else share_class end share_class
	, min(date) min_date, max(date) max_date 
	from returns_wrds
	group by ticker, permno, primary_exch, share_class
) a
order by ticker, max_date, permno