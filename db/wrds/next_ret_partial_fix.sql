
insert into factor_exp_wrds
select *
	, LEAD(ret) OVER (PARTITION BY permno ORDER BY date) next_ret
from factor_exposures_wrds

