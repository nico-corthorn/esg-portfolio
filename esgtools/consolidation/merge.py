
import numpy as np
import pandas as pd
from datetime import datetime
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


def update_prices_alpha_monthly(sql_params=None):
	sql = sql_manager.ManagerSQL(sql_params)
	alpha_prices = sql.select_query("select * from prices_alpha;")

	# Compute monthly 
	alpha_prices["date"] = pd.to_datetime(alpha_prices.date)
	alpha_prices = alpha_prices.sort_values(by=["symbol", "date"])
	alpha_prices["previous_adjusted_close"] = alpha_prices.groupby("symbol")["adjusted_close"].shift(1)
	alpha_prices["daily_return"] = alpha_prices.adjusted_close / alpha_prices.previous_adjusted_close - 1
	alpha_prices['daily_cont_return'] = np.log(1 + alpha_prices.daily_return)
	agg_map = {
		"volume": np.sum,
		"daily_cont_return": np.sum,
		"daily_return": np.std,
		"symbol": len
	}
	agg_rename = {
		"volume": "monthly_volume",
		"daily_cont_return": "monthly_cont_return",
		"daily_return": "monthly_return_std",
		"symbol": "day_count"
	}

	# Last monthly values
	alpha_prices_month_last = alpha_prices.set_index('date').groupby('symbol').resample('BM').last()

	# Aggregate monthly values
	alpha_prices_month_agg = (
		alpha_prices
			.set_index("date")
			.groupby("symbol")
			.resample("BM")
			.agg(agg_map)
			.rename(columns=agg_rename)
	)
	alpha_prices_month_agg['monthly_return'] = np.exp(alpha_prices_month_agg['monthly_cont_return']) - 1

	# Join monthly values
	alpha_prices_month = (
		alpha_prices_month_last
			.join(alpha_prices_month_agg)
	)

	# Format and filter columns
	alpha_prices_month.rename(columns={"lud": "source_lud"}, inplace=True)
	alpha_prices_month["lud"] = datetime.now()
	alpha_prices_month = alpha_prices_month.drop(columns="symbol").reset_index()
	cols = ["symbol", "date", "open", "high", "low", "close", "adjusted_close", "monthly_volume", "monthly_return_std", "monthly_return", "day_count", "source_lud", "lud"]
	missing_data_cond = alpha_prices_month.adjusted_close.isnull()
	one_record_cond = (alpha_prices_month.day_count == 1) & (alpha_prices_month.monthly_return == 0)
	alpha_prices_month = alpha_prices_month.loc[~missing_data_cond & ~one_record_cond, cols].copy()

	# Clean table
	table_name = "prices_alpha_monthly"
	query = f"delete from {table_name}"
	sql.query(query)

	# Upload to database
	sql.upload_df_chunks(table_name, alpha_prices_month)

