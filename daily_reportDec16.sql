select * from eventstats.diego_metric 
where dt>='20161222'


with trades as
(
select date_trunc('day', create_time)::date dt, price, quantity, underlying, root_id
from events.new nw
where nw.create_time >= '20161222' 
and nw.session_id like '%pslchi6pstkexec01%'
and nw.actor_name='PortfolioSSendActor'
and nw.account='F99'
)
select  dt, sum(ntnl) ntnl, sum(transcost)*0.0001 transcost, sum(transcost)/sum(ntnl) bps
from
(
select a.dt, a.price*a.quantity ntnl, a.price*a.quantity*((quantity-sh1)*(bps2-bps1)/(sh2-sh1) + bps1) transcost 
from
-- gs low
(
select distinct on (trd.dt, root_id, underlying) trd.dt, root_id, underlying, price, quantity, shares sh1, bps bps1
from trades trd
join eventstats.ticker_shares_bps bps
on trd.dt=bps.dt and trd.underlying=bps.ticker and quantity>shares
order by trd.dt, root_id, underlying, shares desc
) a
join
-- gs high
(
select distinct on (trd.dt, root_id, underlying) trd.dt, root_id, underlying, shares sh2, bps bps2
from trades trd
join eventstats.ticker_shares_bps bps
on trd.dt=bps.dt and trd.underlying=bps.ticker and quantity<=shares
order by trd.dt, root_id, underlying, shares 
) b
on a.dt=b.dt and a.root_id=b.root_id and a.underlying=b.underlying
) c
group by dt

