/*
select ticker, count(*) from assets_wrds
where delisting_date_proxy is NULL and rnk=1
group by ticker
having count(*)>1
order by ticker
*/

/*
select symbol, count(*) from assets_alpha
where delisting_date >= '2022-03-01' or delisting_date is NULL
group by symbol
having count(*)>1
order by symbol
*/


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
		--and symbol in ('AGTI','AIP','ALPS','BAM','BCTX','BHVN','BTDR','CGAU','COHR','CVU','DC','DCBO','EGLX','EP','ERO','FLJ','FLOW','GETR-WS','GHI','GRFX','GRNT-WS','GSD','GSDWW','HITI','HOLO','HR','HZON-WS','IAUX','ICCM','INM','IPA','KNTK','KNW','LAB','LGV-WS','MCLD','MCVT','MDVL','MLTX','MNMD','MXCT','NCRA','NEE-P-Q','NMG','NOTE-WS','NVRI','NYAX','NYMTZ','NYXH','OAIM','ODV','ORLA','OWL-WS','OXLCN','PACWP','PEAK','PERF-WS','POET','PRTG','QTNT','RBCP','RBT-WS','RCM','REAX','RIV-P-A','RUMBW','RVYL','SAFE','SGML','SLND','STEL','SYTA','UHAL-B','UROY','VICE','VOXR')
		--and not symbol like '%-WS' and not symbol like '%-B' and not symbol like '%-P%'
		order by symbol, delisting_date
	) a
	where rnk = 1
) a
on 
w.delisting_date_proxy is NULL
and w.rnk=1
and a.symbol = w.ticker
--where a.delisting_date < w.delisting_date_proxy
-- where w.ticker is null

/*
select * from (
	select *
	, row_number() over(partition by symbol order by coalesce(delisting_date, CURRENT_DATE) desc) rnk
	from assets_alpha
	where symbol not like '%-%'
	--and symbol in ('AGTI','AIP','ALPS','BAM','BCTX','BHVN','BTDR','CGAU','COHR','CVU','DC','DCBO','EGLX','EP','ERO','FLJ','FLOW','GETR-WS','GHI','GRFX','GRNT-WS','GSD','GSDWW','HITI','HOLO','HR','HZON-WS','IAUX','ICCM','INM','IPA','KNTK','KNW','LAB','LGV-WS','MCLD','MCVT','MDVL','MLTX','MNMD','MXCT','NCRA','NEE-P-Q','NMG','NOTE-WS','NVRI','NYAX','NYMTZ','NYXH','OAIM','ODV','ORLA','OWL-WS','OXLCN','PACWP','PEAK','PERF-WS','POET','PRTG','QTNT','RBCP','RBT-WS','RCM','REAX','RIV-P-A','RUMBW','RVYL','SAFE','SGML','SLND','STEL','SYTA','UHAL-B','UROY','VICE','VOXR')
	--and not symbol like '%-WS' and not symbol like '%-B' and not symbol like '%-P%'
	order by symbol, delisting_date
) a
where rnk = 1
*/

/*
select w.*, 1 in_wrds, a.symbol, a.name, a.exchange, a.asset_type, a.ipo_date, a.delisting_date, a.status, a.lud
from assets_wrds w
left join assets_alpha a 
on a.symbol = w.ticker and w.delisting_date_proxy is NULL and w.rnk=1
*/	


