
-- drop view nyt_org_asset_view

create view nyt_org_asset_view as
select n.*, a.name, a.exchange, a.ipo_date, a.delisting_date
from nyt_org_to_asset n
left join assets a
on n.asset_ids LIKE CONCAT('%', a.id, '%')
--order by n.count desc