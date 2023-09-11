

from utils import sql_manager, utils, date_utils

def merge_alpha_and_wrds_assets(sql_params=None):

    sql = sql_manager.ManagerSQL(sql_params)

    # Delete table
    sql.query("drop table if exists assets")

    # Read query for creating assets table
    query = """
create table assets as
select 
concat(case when w.ticker is null then coalesce(a.symbol, '0') else w.ticker end
	   , '-', coalesce(cast(permno as TEXT), '0')) as id
, coalesce(w.ticker, a.symbol) symbol
, w.permno
, a.name
, coalesce(a.exchange, w.primary_exch) exchange
, coalesce(a.asset_type, 'Stock') asset_type
, case 
	when coalesce(a.asset_type, 'Stock') = 'Stock' then coalesce(w.share_class, 'A')
	else w.share_class
	end share_class
, case 
	when ipo_date_proxy is not null and ipo_date is not null then 
		case when ipo_date_proxy < ipo_date then cast(ipo_date_proxy as date)
		else ipo_date end
	else cast(coalesce(ipo_date, ipo_date_proxy) as date) 
	end ipo_date
, cast(coalesce(delisting_date, delisting_date_proxy) as date) delisting_date
, case when a.symbol is not null then 1 else 0 end in_alpha
, case 
	when a.status is null and delisting_date_proxy is not null then 'Delisted' 
	else a.status
  end status
, lud alpha_lud
, Now() lud
from assets_wrds w
full outer join (
	select * 
	from (
		select *
		, row_number() over(partition by symbol order by coalesce(delisting_date, CURRENT_DATE) desc) rnk
		from assets_alpha
		where symbol not like '%-%'
		order by symbol, delisting_date
	) a
	where rnk = 1
) a
on 
w.delisting_date_proxy is NULL
and w.rnk=1
and a.symbol = w.ticker    
    """


    # Create assets table
    sql.query(query)



    





