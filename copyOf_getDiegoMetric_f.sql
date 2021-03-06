﻿-- -- Function: get_diego_metrics(date, date, numeric)
-- 
-- -- DROP FUNCTION get_diego_metrics(date, date, numeric);
-- 
-- CREATE OR REPLACE FUNCTION get_diego_metrics(
--     startdt date,
--     enddt date,
--     bsktratio numeric DEFAULT 0.65)
--   RETURNS SETOF _diego_metrics_holder AS
-- $BODY$
-- declare
--   r _diego_metrics_holder%rowtype;
-- begin
-- -- get root_id, tactic
-- drop table if exists diego_metrics_t;
-- create temporary table diego_metrics_t
-- (
--   session_id text,
--   root_id text,
--   stock_tactic text,
--   trader_id bigint,
--   ah text,
--   hedge_for_order text
-- );
-- insert into diego_metrics_t
-- select a.*, case when b.root_id is null then '_STOCK_ONLY' else '_OPT_LEGGED' end isah, hedge_for_order
-- from
-- (
-- select session_id, root_id, max(stock_tactic) stock_tactic, max(trader_id) trader_id
-- from events.new
-- where create_time between startdt and enddt + interval '1 day'
-- --where create_time between current_date and current_date + interval '1 day'
-- and actor_name='StockAlgoActor'
-- group by session_id, root_id
-- ) a
-- left join
-- (
-- select distinct session_id, root_id, hedge_for_order
-- from events.new
-- where create_time between startdt and enddt + interval '1 day'
-- --where create_time between current_date and current_date + interval '1 day'
-- and actor_name='AutoHedgeActor'
-- ) b
-- on a.session_id=b.session_id and a.root_id=b.root_id;
-- 
-- -- sh, ntnl, commission
-- drop table if exists diego_metrics_t00;
-- create temporary table diego_metrics_t00
-- (
--   dt date,
--   crossing_type text,
--   actor_name text,
--   stock_tactic text,
--   trader_id bigint,
--   metrics_key text,
--   metrics_val numeric
-- );
-- insert into diego_metrics_t00
-- select date_trunc('day', f.create_time), case when crossing_type='QUOTER' then crossing_type||ah else crossing_type end crossing_type, actor_name, stock_tactic, trader_id, 'sh', sum(case when crossing_type='TRADER_CROSS' then 0.5 else 1 end * fill_quantity)
-- from events.fills f
-- join diego_metrics_t t
-- on f.session_id=t.session_id and f.root_id=t.root_id
-- where create_time between startdt and enddt + interval '1 day'
-- --where create_time between current_date and current_date + interval '1 day'
-- and crossing_type in ('QUOTER','IGOOGI','INTERNALIZED','TRADER_CROSS')
-- and f.root_id=f.order_id
-- and f.security like 'S:%'
-- group by date_trunc('day', f.create_time), case when crossing_type='QUOTER' then crossing_type||ah else crossing_type end, actor_name, stock_tactic, trader_id;
-- 
-- insert into diego_metrics_t00
-- select date_trunc('day', f.create_time), case when crossing_type='QUOTER' then crossing_type||ah else crossing_type end, actor_name, stock_tactic, trader_id, 'dlr_ntnl', sum(case when crossing_type='TRADER_CROSS' then 0.5 else 1 end * fill_price * fill_quantity)
-- from events.fills f
-- join diego_metrics_t t
-- on f.session_id=t.session_id and f.root_id=t.root_id
-- where create_time between startdt and enddt + interval '1 day'--where create_time between current_date and current_date + interval '1 day'
-- and crossing_type in ('QUOTER','IGOOGI','INTERNALIZED','TRADER_CROSS')
-- and f.root_id=f.order_id
-- and f.security like 'S:%'
-- group by date_trunc('day', f.create_time), case when crossing_type='QUOTER' then crossing_type||ah else crossing_type end, actor_name, stock_tactic, trader_id;
-- 
-- insert into diego_metrics_t00
-- select date_trunc('day', f.create_time), case when crossing_type='QUOTER' then crossing_type||ah else crossing_type end, actor_name, stock_tactic, trader_id, 'dlr_commission', sum(commission * fill_quantity)
-- from events.fills f
-- join diego_metrics_t t
-- on f.session_id=t.session_id and f.root_id=t.root_id
-- where create_time between startdt and enddt + interval '1 day'--where create_time between current_date and current_date + interval '1 day'
-- and crossing_type in ('QUOTER','IGOOGI','INTERNALIZED','TRADER_CROSS')
-- and f.root_id=f.order_id
-- and f.security like 'S:%'
-- group by date_trunc('day', f.create_time), case when crossing_type='QUOTER' then crossing_type||ah else crossing_type end, actor_name, stock_tactic, trader_id;
-- 
-- -- market, basket
-- insert into diego_metrics_t00
-- select date_trunc('day', f.create_time), 'MARKET_DIRECT', actor_name, stock_tactic, trader_id, 'sh', sum(fill_quantity)
-- from events.fills f
-- join diego_metrics_t t
-- on f.session_id=t.session_id and f.root_id=t.root_id
-- where create_time between startdt and enddt + interval '1 day'--where create_time between current_date and current_date + interval '1 day'
-- and actor_name='StockXI'
-- and f.session_id not like '%stkexec01%'
-- group by date_trunc('day', f.create_time), actor_name, stock_tactic, trader_id;
-- 
-- insert into diego_metrics_t00
-- select date_trunc('day', f.create_time), 'MARKET_DIRECT', actor_name, stock_tactic, trader_id, 'dlr_ntnl', sum(fill_quantity*fill_price)
-- from events.fills f
-- join diego_metrics_t t
-- on f.session_id=t.session_id and f.root_id=t.root_id
-- where f.create_time between startdt and enddt + interval '1 day'--where f.create_time between current_date and current_date + interval '1 day'
-- and actor_name='StockXI'
-- and f.session_id not like '%stkexec01%'
-- group by date_trunc('day', f.create_time), actor_name, stock_tactic, trader_id;
-- 
-- insert into diego_metrics_t00
-- select date_trunc('day', f.create_time), 'MARKET_BASKET', f.actor_name, nw.stock_tactic, nw.trader_id, 'sh', sum(fill_quantity)
-- from events.fills f
-- join events.new nw
-- on f.session_id=nw.session_id and f.root_id=nw.root_id and f.pid=nw.pid
-- where f.create_time between startdt and enddt + interval '1 day'
-- and nw.create_time between startdt and enddt + interval '1 day'
-- and f.session_id like '%stkexec01%'
-- and f.actor_name='PortfolioSSendActor'
-- group by date_trunc('day', f.create_time), f.actor_name,nw.stock_tactic, nw.trader_id;
-- 
-- insert into diego_metrics_t00
-- select date_trunc('day', f.create_time), 'MARKET_BASKET', f.actor_name, nw.stock_tactic, nw.trader_id, 'dlr_ntnl', sum(fill_quantity*fill_price)
-- from events.fills f
-- join events.new nw
-- on f.session_id=nw.session_id and f.root_id=nw.root_id and f.pid=nw.pid
-- where f.create_time between startdt and enddt + interval '1 day'
-- and nw.create_time between startdt and enddt + interval '1 day'
-- and f.session_id like '%stkexec01%'
-- and f.actor_name='PortfolioSSendActor'
-- group by date_trunc('day', f.create_time), f.actor_name,nw.stock_tactic, nw.trader_id;
-- 
-- drop table if exists diego_metrics_fills;create temporary table diego_metrics_fills
-- (
--   dt date,
--   session_id text,
--   root_id text,
--   fill_id text,
--   fill_price numeric,
--   fill_quantity bigint,
--   security text,
--   side text
-- );
-- insert into diego_metrics_fills
-- select date_trunc('day', f.create_time), f.session_id, f.root_id, f.fill_id, f.fill_price, f.fill_quantity, right(security,-2), side
-- from events.fills f
-- where create_time between startdt and enddt + interval '1 day'and crossing_type in ('QUOTER','IGOOGI','INTERNALIZED','TRADER_CROSS') and f.security like 'S:%'and f.root_id=f.order_id;
-- -- impact for root_id, fill_id
-- drop table if exists diego_metrics_impact;
-- create temporary table diego_metrics_impact
-- (
--   session_id text,
--   root_id text,
--   fill_id text,
--   impact numeric
-- );
-- with high as
-- (
-- select distinct on (f.session_id, f.root_id, fill_id) f.session_id, f.root_id, f.fill_id, side, fill_price, fill_quantity, bps
-- from diego_metrics_fills f
-- join eventstats.ticker_shares_bps tb
-- on security=ticker
-- and f.fill_quantity<=shares
-- and f.dt=tb.dt
-- where tb.dt between startdt and enddt
-- order by f.session_id, f.root_id, fill_id, shares
-- )
-- ,low as
-- (
-- select distinct on (f.session_id, f.root_id, fill_id) f.session_id, f.root_id, f.fill_id, side, fill_price, fill_quantity, bps
-- from diego_metrics_fills f
-- join eventstats.ticker_shares_bps tb
-- on security=ticker
-- and f.fill_quantity>=shares
-- and f.dt=tb.dt
-- where tb.dt between startdt and enddt
-- order by f.session_id, f.root_id, fill_id, shares desc
-- )
-- insert into diego_metrics_impact
-- select a.session_id, a.root_id, a.fill_id, (a.hi+b.lo)/2
-- from
-- (select session_id, root_id, fill_id, (fill_price*fill_quantity*bps*0.0001) hi
-- from high
-- ) a
-- join
-- (
-- select session_id, root_id, fill_id, (fill_price*fill_quantity*bps*0.0001) lo
-- from low
-- ) b
-- on a.session_id=b.session_id and a.root_id=b.root_id and a.fill_id=b.fill_id;
-- 
-- -- additional option ctx
-- insert into diego_metrics_t00
-- select date_trunc('day', nw2.create_time), crossing_type, a.actor_name, a.stock_tactic, a.trader_id, 'ctx',
--   sum(case when nw2.quantity <= abs(a.fill_quantity / (gk.implied_vol_delta * 100))::integer then nw2.quantity else abs(a.fill_quantity / (gk.implied_vol_delta * 100))::integer end) hedged_optqty
-- from events.new nw2
-- join
-- (
-- select f.root_id, case when crossing_type='QUOTER' then crossing_type||ah else crossing_type end crossing_type, f.actor_name, t.stock_tactic, t.trader_id, hedge_for_order, sum(fill_quantity) fill_quantity
-- from events.fills f
-- join diego_metrics_t t
-- on f.session_id=t.session_id and f.root_id=t.root_id
-- where f.create_time between startdt and enddt + interval '1 day'--where f.create_time between current_date and current_date + interval '1 day'
-- and crossing_type in ('QUOTER')
-- and f.security like 'S:%'
-- and f.actor_name='AutoHedgeActor'
-- group by f.root_id, case when crossing_type='QUOTER' then crossing_type||ah else crossing_type end, f.actor_name, t.stock_tactic, hedge_for_order, t.trader_id
-- ) a
-- on nw2.root_id=a.hedge_for_order
-- left join events.greeks gk
-- on nw2.session_id=gk.session_id and nw2.event_id=gk.event_id
-- where gk.create_time between startdt and enddt + interval '1 day'--where gk.create_time between current_date and current_date + interval '1 day'
-- and nw2.root_id=nw2.order_id
-- and implied_vol_delta is not null
-- group by date_trunc('day', nw2.create_time), crossing_type, a.actor_name, a.stock_tactic, a.trader_id;
-- 
-- drop table if exists diego_metrics_t2;
-- create temporary table diego_metrics_t2
-- (
--   root_id_stk text,
--   crossing_type text,
--   actor_name text,
--   pid bigint,
--   stock_tactic text,
--   trader_id bigint,
--   hedge_for_order text,
--   fill_quantity bigint,
--   fill_price numeric,
--   ordqty bigint,
--   dt date,
--   root_id_opt text,
--   optordqty bigint,
--   vol numeric,
--   side text,
--   implied_vol_delta numeric,
--   ord_hedged_stksz numeric,
--   stk_side text,
--   street_bid_size bigint,
--   street_ask_size bigint
-- );
-- 
-- with stkahfill as
-- -- get each stockalgoactor's crossing fills and it's hedged option orders when diego is joining the market at the time of the fill
-- (
-- select f.root_id, case when crossing_type='QUOTER' then crossing_type||ah else crossing_type end crossing_type, f.actor_name, f.pid, t.stock_tactic, t.trader_id, t.hedge_for_order, sum(fill_quantity) fill_quantity,
--   sum(fill_price * fill_quantity) / sum(fill_quantity) fill_price, max(nw.quantity) ordqty
-- from events.fills f
-- join diego_metrics_t t
-- on f.session_id=t.session_id and f.root_id=t.root_id
-- join events.new nw
-- on f.session_id=nw.session_id and f.root_id=nw.root_id and f.pid=nw.pid
-- where f.create_time between startdt and enddt + interval '1 day'
-- --where f.create_time between current_date and current_date + interval '1 day'
-- and crossing_type in ('QUOTER')
-- and f.security like 'S:%'
-- and f.actor_name='StockAlgoActor'
-- and ((f.side='Buy' and fill_price=street_stk_ask) or (f.side='Sell' and fill_price=street_stk_bid))
-- group by f.root_id, case when crossing_type='QUOTER' then crossing_type||ah else crossing_type end, f.actor_name, f.pid, t.stock_tactic, t.hedge_for_order, t.trader_id
-- ), optorder as
-- -- get delta at option submission time and hedged stock shares based on stockalgoactor's order size
-- (
-- select date_trunc('day', nw.create_time) dt, nw.root_id, ah.pid, nw.quantity, nw.vol, nw.side, gk.implied_vol_delta, round(abs(nw.quantity * gk.implied_vol_delta)) * 100 - 1 ord_hedged_stksz,
--   case when (nw.side='Buy' and implied_vol_delta>0) or (nw.side='Sell' and implied_vol_delta<0) then 'stkbid' else 'stkask' end stk_side
-- from events.new nw
-- join stkahfill ah
-- on nw.root_id=ah.hedge_for_order
-- left join events.greeks gk
-- on nw.session_id=gk.session_id and nw.event_id=gk.event_id
-- where gk.create_time between startdt and enddt + interval '1 day'
-- --where gk.create_time between current_date and current_date + interval '1 day'
-- and nw.root_id=nw.order_id
-- and delta is not null
-- )
-- , gks as
-- -- get street bid, ask size for the the stockalgo actor
-- (
-- select gk.root_id, gk.pid, max(street_bid_size)*100 street_bid_size, max(street_ask_size)*100 street_ask_size
-- from events.greeks gk
-- join stkahfill f
-- on gk.root_id=f.root_id and gk.pid=f.pid
-- where gk.create_time between startdt and enddt + interval '1 day'
-- --where gk.create_time between current_date and current_date + interval '1 day'
-- group by gk.root_id, gk.pid
-- )
-- insert into diego_metrics_t2
-- select f.*, ord.dt, ord.root_id, optordqty, ord.vol, ord.side, ord.implied_vol_delta, ord.ord_hedged_stksz, ord.stk_side, gk.street_bid_size, gk.street_ask_size
-- from stkahfill f
-- join optorder ord
-- on f.hedge_for_order=ord.root_id and f.pid=ord.pid
-- join (select root_id, max(quantity) optordqty from optorder group by root_id) ord2
-- on f.hedge_for_order=ord2.root_id
-- join gks gk
-- on f.root_id=gk.root_id and f.pid=gk.pid;
-- 
-- insert into diego_metrics_t00
-- select dt, crossing_type, actor_name, stock_tactic, trader_id,
--   case when stk_side='stkbid' and street_bid_size>=ord_hedged_stksz or stk_side='stkask' and street_ask_size>=ord_hedged_stksz then 'sh_diego_joins_not_outsizing'
--      when stk_side='stkbid' and street_bid_size<ord_hedged_stksz or stk_side='stkask' and street_ask_size<ord_hedged_stksz then 'sh_diego_joins_outsizing'
--   end,
--   sum(fill_quantity)
-- from diego_metrics_t2
-- group by dt, crossing_type, actor_name, stock_tactic, trader_id,
-- case when stk_side='stkbid' and street_bid_size>=ord_hedged_stksz or stk_side='stkask' and street_ask_size>=ord_hedged_stksz then 'sh_diego_joins_not_outsizing'
--      when stk_side='stkbid' and street_bid_size<ord_hedged_stksz or stk_side='stkask' and street_ask_size<ord_hedged_stksz then 'sh_diego_joins_outsizing'
-- end;
-- 
-- insert into diego_metrics_t00
-- select dt, crossing_type, actor_name, stock_tactic, trader_id,
--   case when stk_side='stkbid' and street_bid_size>=ord_hedged_stksz or stk_side='stkask' and street_ask_size>=ord_hedged_stksz then 'dlr_ntnl_diego_joins_not_outsizing'
--      when stk_side='stkbid' and street_bid_size<ord_hedged_stksz or stk_side='stkask' and street_ask_size<ord_hedged_stksz then 'dlr_ntnl_diego_joins_outsizing'
--   end,
--   sum(fill_quantity*fill_price)
-- from diego_metrics_t2
-- group by dt, crossing_type, actor_name, stock_tactic, trader_id,
-- case when stk_side='stkbid' and street_bid_size>=ord_hedged_stksz or stk_side='stkask' and street_ask_size>=ord_hedged_stksz then 'dlr_ntnl_diego_joins_not_outsizing'
--      when stk_side='stkbid' and street_bid_size<ord_hedged_stksz or stk_side='stkask' and street_ask_size<ord_hedged_stksz then 'dlr_ntnl_diego_joins_outsizing'
-- end;
-- 
-- insert into diego_metrics_t00
-- select date_trunc('day', create_time) dt, case when crossing_type='QUOTER' then crossing_type||ah else crossing_type end, actor_name, null::text stock_tactic, null::bigint trader_id, 'sh_diego_improves', sum(fill_quantity)
-- from events.fills f
-- join diego_metrics_t t
-- on f.session_id=t.session_id and f.root_id=t.root_id
-- where create_time between startdt and enddt + interval '1 day'
-- --where create_time between current_date and current_date + interval '1 day'
-- and crossing_type in ('QUOTER')
-- and actor_name='StockAlgoActor'
-- and ((side='Buy' and fill_price<street_stk_ask) or (side='Sell' and fill_price>street_stk_bid))
-- and hedge_for_order is not null
-- group by date_trunc('day', create_time), case when crossing_type='QUOTER' then crossing_type||ah else crossing_type end, actor_name;
-- 
-- insert into diego_metrics_t00
-- select date_trunc('day', create_time) dt, case when crossing_type='QUOTER' then crossing_type||ah else crossing_type end, actor_name, null::text stock_tactic, null::bigint trader_id, 'dlr_ntnl_diego_improves', sum(fill_quantity*fill_price)
-- from events.fills f
-- join diego_metrics_t t
-- on f.session_id=t.session_id and f.root_id=t.root_id
-- where create_time between startdt and enddt + interval '1 day'
-- --where create_time between current_date and current_date + interval '1 day'
-- and crossing_type in ('QUOTER')
-- and actor_name='StockAlgoActor'
-- and ((side='Buy' and fill_price<street_stk_ask) or (side='Sell' and fill_price>street_stk_bid))
-- and hedge_for_order is not null
-- group by date_trunc('day', create_time), case when crossing_type='QUOTER' then crossing_type||ah else crossing_type end, actor_name;
-- 
-- insert into diego_metrics_t00
-- select dt, crossing_type, actor_name, stock_tactic, trader_id,
--   case when stk_side='stkbid' and street_bid_size>=ord_hedged_stksz or stk_side='stkask' and street_ask_size>=ord_hedged_stksz then 'ctx_diego_joins_not_outsizing'
--      when stk_side='stkbid' and street_bid_size<ord_hedged_stksz or stk_side='stkask' and street_ask_size<ord_hedged_stksz then 'ctx_diego_joins_outsizing'
--   end,
--   sum(abs(fill_quantity / (implied_vol_delta * 100))::integer)  
-- from diego_metrics_t2
-- group by dt, crossing_type, actor_name, stock_tactic, trader_id,
-- case when stk_side='stkbid' and street_bid_size>=ord_hedged_stksz or stk_side='stkask' and street_ask_size>=ord_hedged_stksz then 'ctx_diego_joins_not_outsizing'
--      when stk_side='stkbid' and street_bid_size<ord_hedged_stksz or stk_side='stkask' and street_ask_size<ord_hedged_stksz then 'ctx_diego_joins_outsizing'
-- end;
-- 
-- with optfill as
-- -- option market fillvol
-- (
-- select f1.root_id,
--   sum(
--       case when implied_vol_bid is null and nbbo_ask-nbbo_bid!=0 then implied_vol_ask/(nbbo_ask-nbbo_bid)*(case when fill_price<nbbo_bid then nbbo_bid
--                                                                                                           when fill_price>nbbo_ask then nbbo_ask
--                                                                                                       else fill_price end - nbbo_bid)
--          when implied_vol_bid is null and nbbo_ask-nbbo_bid=0 then implied_vol_ask
--          when nbbo_ask-nbbo_bid!=0 then implied_vol_bid+(implied_vol_ask-implied_vol_bid)/(nbbo_ask-nbbo_bid)*(case when fill_price<nbbo_bid then nbbo_bid
--                                                             when fill_price>nbbo_ask then nbbo_ask
--                                                         else fill_price end - nbbo_bid)
--          else (implied_vol_bid+implied_vol_ask)/2
--   end * fill_quantity) / sum(fill_quantity) fillvol, sum(fill_quantity) fill_quantity,
--   sum(street_stk_bid * fill_quantity) / sum(fill_quantity) street_stk_bid,  sum(street_stk_ask * fill_quantity) / sum(fill_quantity) street_stk_ask
-- from events.fills f1
-- join diego_metrics_t t1
-- on f1.root_id=t1.hedge_for_order
-- where f1.create_time between startdt and enddt + interval '1 day'
-- --where f1.create_time between current_date and current_date + interval '1 day'
-- and actor_name='XIProxy'
-- group by f1.root_id
-- )
-- , stkahfill as
-- -- crossing fills from autohedged stock orders when quoter is better than market
-- (
-- select f.root_id, case when crossing_type='QUOTER' then crossing_type||ah else crossing_type end crossing_type, f.actor_name, t.stock_tactic, t.trader_id, hedge_for_order, sum(fill_quantity) fill_quantity,
--   sum(fill_price * fill_quantity) / sum(fill_quantity) fill_price, sum(street_stk_bid * fill_quantity) / sum(fill_quantity) street_stk_bid, sum(street_stk_ask * fill_quantity) / sum(fill_quantity) streek_stk_ask, max(side) side
-- from events.fills f
-- join diego_metrics_t t
-- on f.session_id=t.session_id and f.root_id=t.root_id
-- where f.create_time between startdt and enddt + interval '1 day'
-- --where f.create_time between current_date and current_date + interval '1 day'
-- and crossing_type in ('QUOTER')
-- and f.security like 'S:%'
-- and f.actor_name='AutoHedgeActor'
-- and ((side='Buy' and fill_price<street_stk_ask) or (side='Sell' and fill_price>street_stk_bid))
-- group by f.root_id, case when crossing_type='QUOTER' then crossing_type||ah else crossing_type end, f.actor_name, t.stock_tactic, hedge_for_order, t.trader_id
-- )
-- , optorder as
-- -- option order quantity and delta, vega
-- (
-- select date_trunc('day', nw.create_time) dt, nw.root_id, nw.quantity, nw.vol, nw.side, gk.implied_vol_vega, gk.implied_vol_delta
-- from events.new nw
-- join stkahfill ah
-- on nw.root_id=ah.hedge_for_order
-- left join events.greeks gk
-- on nw.session_id=gk.session_id and nw.event_id=gk.event_id
-- where gk.create_time between startdt and enddt + interval '1 day'
-- --where gk.create_time between current_date and current_date + interval '1 day'
-- and nw.root_id=nw.order_id
-- and delta is not null
-- )
-- , t as
-- -- option internal fillvol from stock prices difference, vega and market fillvol
-- (
-- select dt, ord.root_id, ah.root_id, vol vol_lmt, fillvol fillvol_mkt, ah.crossing_type, ah.actor_name, ah.stock_tactic, ah.trader_id, ah.fill_quantity stk_fillqty, ord.quantity, opt.fill_quantity opt_fillqty,
--   fillvol + case when ord.side='Buy' then -1 else 1 end * case when ah.side='Buy' then opt.street_stk_ask - fill_price else fill_price - opt.street_stk_bid end / implied_vol_vega fillvol_diego, ord.side,
--   (case when ord.quantity <= abs(ah.fill_quantity / (implied_vol_delta * 100))::integer then ord.quantity else abs(ah.fill_quantity / (implied_vol_delta * 100))::integer end) hedged_optqty
-- from optorder ord
-- join optfill opt
-- on ord.root_id=opt.root_id
-- join stkahfill ah
-- on ord.root_id=ah.hedge_for_order
-- )
-- insert into diego_metrics_t00
-- select dt, crossing_type, actor_name, stock_tactic, trader_id,
--   case when (side='Buy' and fillvol_diego<vol_lmt and vol_lmt<fillvol_mkt) or (side='Sell' and fillvol_diego>vol_lmt and vol_lmt>fillvol_mkt) then 'ctx_diego_bt_limit_bt_mkt'
--        when (side='Buy' and fillvol_diego<fillvol_mkt and fillvol_mkt<vol_lmt) or (side='Sell' and fillvol_diego>fillvol_mkt and fillvol_mkt>vol_lmt) then 'ctx_diego_bt_mkt_bt_limit'
--   else 'ctx_other' end,  
--   sum(hedged_optqty)
-- from t  
-- group by dt, crossing_type, actor_name, stock_tactic, trader_id,
--   case when (side='Buy' and fillvol_diego<vol_lmt and vol_lmt<fillvol_mkt) or (side='Sell' and fillvol_diego>vol_lmt and vol_lmt>fillvol_mkt) then 'ctx_diego_bt_limit_bt_mkt'
--        when (side='Buy' and fillvol_diego<fillvol_mkt and fillvol_mkt<vol_lmt) or (side='Sell' and fillvol_diego>fillvol_mkt and fillvol_mkt>vol_lmt) then 'ctx_diego_bt_mkt_bt_limit'
--   else 'ctx_other' end;
-- 
-- -- savings from trader trades no stock in market
-- insert into diego_metrics_t00
-- select date_trunc('day', create_time) dt, case when crossing_type='QUOTER' then crossing_type||ah else crossing_type end, f.actor_name, t.stock_tactic, t.trader_id, 'dlr_Savings_VolArb_avoiding_MKT', sum(impact) impact
-- from events.fills f
-- join diego_metrics_t t
-- on f.session_id=t.session_id and f.root_id=t.root_id
-- join diego_metrics_impact di
-- on f.session_id=di.session_id and f.root_id=di.root_id and f.fill_id=di.fill_id
-- where create_time between startdt and enddt + interval '1 day'--where create_time between current_date and current_date + interval '1 day'
-- and f.root_id=f.order_id
-- and f.security like 'S:%'
-- and f.crossing_type in ('IGOOGI','INTERNALIZED','TRADER_CROSS') or (f.crossing_type='QUOTER' and ah='(stock_only)')
-- group by date_trunc('day', create_time), case when crossing_type='QUOTER' then crossing_type||ah else crossing_type end, f.actor_name, t.stock_tactic, t.trader_id;
-- 
-- -- savings from STG trades no stock in market
-- insert into diego_metrics_t00
-- select date_trunc('day', create_time) dt, case when crossing_type='QUOTER' then crossing_type||ah else crossing_type end, f.actor_name, t.stock_tactic, t.trader_id, 'dlr_Savings_STG_avoiding_MKT', sum(impact) impact
-- from events.fills f
-- join diego_metrics_t t
-- on f.session_id=t.session_id and f.root_id=t.root_id
-- join diego_metrics_impact di
-- on f.session_id=di.session_id and f.root_id=di.root_id and f.fill_id=di.fill_id
-- where create_time between startdt and enddt + interval '1 day'--where create_time between current_date and current_date + interval '1 day'
-- and f.root_id=f.order_id
-- and f.security like 'S:%'
-- and f.crossing_type='QUOTER'
-- group by date_trunc('day', create_time), case when crossing_type='QUOTER' then crossing_type||ah else crossing_type end, f.actor_name, t.stock_tactic, t.trader_id;
-- 
-- -- cost impact for IGOOGI internalization
-- insert into diego_metrics_t00
-- select dt, crossing_type, actor_name, stock_tactic, trader_id, 'dlr_Cost_to_trade_out_in_basket', -bsktratio*metrics_val
-- from diego_metrics_t00
-- where crossing_type in ('IGOOGI', 'INTERNALIZED')
-- and metrics_key = 'dlr_Savings_VolArb_avoiding_MKT';
-- 
-- -- daymm
-- insert into diego_metrics_t00
-- select date_trunc('day', create_time), case when crossing_type='QUOTER' then crossing_type||ah else crossing_type end, actor_name, stock_tactic, trader_id, 'dlr_Rsk_STG_dayMM', sum(case when side='Buy' then 1 else -1 end * ((street_stk_bid + street_stk_ask)/2 - cls.close) * fill_quantity) daymm
-- from events.fills f
-- join diego_metrics_t t
-- on f.session_id=t.session_id and f.root_id=t.root_id
-- join eventstats.security_closing_prices cls
-- on date_trunc('day', create_time)=date_trunc('day', date_created) and f.security=cls.security_key
-- where create_time between startdt and enddt + interval '1 day'--where create_time between current_date and current_date + interval '1 day'
-- and crossing_type in ('QUOTER','IGOOGI','INTERNALIZED')
-- and f.security like 'S:%' and cls.security_key like 'S:%'
-- and f.root_id=f.order_id
-- group by date_trunc('day', create_time), case when crossing_type='QUOTER' then crossing_type||ah else crossing_type end, actor_name, stock_tactic, trader_id;
-- 
-- -- pip from crossing
-- -- price term
-- insert into diego_metrics_t00
-- select date_trunc('day', create_time), case when crossing_type='QUOTER' then crossing_type||ah else crossing_type end, actor_name, stock_tactic, trader_id, 'dlr_PIP_crossing',
--   sum(case when side='Buy' then street_stk_ask-fill_price else fill_price-street_stk_bid end * fill_quantity) savings_not_crossing
-- from events.fills f
-- join diego_metrics_t t
-- on f.session_id=t.session_id and f.root_id=t.root_id
-- where create_time between startdt and enddt + interval '1 day'--where create_time between current_date and current_date + interval '1 day'
-- and crossing_type in ('QUOTER','IGOOGI','INTERNALIZED')
-- and f.root_id=f.order_id
-- and f.security like 'S:%'
-- group by date_trunc('day', create_time), case when crossing_type='QUOTER' then crossing_type||ah else crossing_type end, actor_name, stock_tactic, trader_id;
-- 
-- insert into diego_metrics_t00
-- select date_trunc('day', create_time), case when crossing_type='QUOTER' then crossing_type||ah else crossing_type end, actor_name, stock_tactic, trader_id, 'dlr_PIP_GS_exec',
--   sum(case when side='Buy' then (street_stk_bid+street_stk_ask)/2-fill_price else fill_price-(street_stk_bid+street_stk_ask)/2 end * fill_quantity + impact) savings_not_crossing
-- from events.fills f
-- join diego_metrics_t t
-- on f.session_id=t.session_id and f.root_id=t.root_id
-- join diego_metrics_impact di
-- on f.session_id=di.session_id and f.root_id=di.root_id and f.fill_id=di.fill_id
-- where create_time between startdt and enddt + interval '1 day'--where create_time between current_date and current_date + interval '1 day'
-- and crossing_type in ('QUOTER','IGOOGI','INTERNALIZED')
-- and f.root_id=f.order_id
-- and f.security like 'S:%'
-- group by date_trunc('day', create_time), case when crossing_type='QUOTER' then crossing_type||ah else crossing_type end, actor_name, stock_tactic, trader_id;
-- 
-- -- bps term
-- insert into diego_metrics_t00
-- select date_trunc('day', create_time), case when crossing_type='QUOTER' then crossing_type||ah else crossing_type end, actor_name, stock_tactic, trader_id, 'bps_PIP_crossing',
--   10000 * sum(case when side='Buy' then street_stk_ask-fill_price else fill_price-street_stk_bid end / fill_price * fill_quantity) / sum(fill_quantity) savings_not_crossing
-- from events.fills f
-- join diego_metrics_t t
-- on f.session_id=t.session_id and f.root_id=t.root_id
-- where create_time between startdt and enddt + interval '1 day'--where create_time between current_date and current_date + interval '1 day'
-- and crossing_type in ('QUOTER','IGOOGI','INTERNALIZED')
-- and f.root_id=f.order_id
-- and f.security like 'S:%'
-- group by date_trunc('day', create_time), case when crossing_type='QUOTER' then crossing_type||ah else crossing_type end, actor_name, stock_tactic, trader_id;
-- 
-- insert into diego_metrics_t00
-- select date_trunc('day', create_time), case when crossing_type='QUOTER' then crossing_type||ah else crossing_type end, actor_name, stock_tactic, trader_id, 'bps_PIP_GS_exec',
--   10000 * sum((case when side='Buy' then (street_stk_bid+street_stk_ask)/2-fill_price else fill_price-(street_stk_bid+street_stk_ask)/2 end * fill_quantity + impact) / fill_price) / sum(fill_quantity) savings_not_crossing
-- from events.fills f
-- join diego_metrics_t t
-- on f.session_id=t.session_id and f.root_id=t.root_id
-- join diego_metrics_impact di
-- on f.session_id=di.session_id and f.root_id=di.root_id and f.fill_id=di.fill_id
-- where create_time between startdt and enddt + interval '1 day'--where create_time between current_date and current_date + interval '1 day'
-- and crossing_type in ('QUOTER','IGOOGI','INTERNALIZED')
-- and f.root_id=f.order_id
-- and f.security like 'S:%'
-- group by date_trunc('day', create_time), case when crossing_type='QUOTER' then crossing_type||ah else crossing_type end, actor_name, stock_tactic, trader_id;
-- 
-- -- spread term
-- insert into diego_metrics_t00
-- select date_trunc('day', create_time), case when crossing_type='QUOTER' then crossing_type||ah else crossing_type end, actor_name, stock_tactic, trader_id, 'sprdcut_crossing',
--   sum(case when side='Buy' then street_stk_ask-fill_price else fill_price-street_stk_bid end / (street_stk_ask - street_stk_bid) * fill_quantity) / sum(fill_quantity)
-- from events.fills f
-- join diego_metrics_t t
-- on f.session_id=t.session_id and f.root_id=t.root_id
-- where create_time between startdt and enddt + interval '1 day'--where create_time between current_date and current_date + interval '1 day'
-- and crossing_type in ('QUOTER','IGOOGI','INTERNALIZED')
-- and f.root_id=f.order_id
-- and f.security like 'S:%'
-- and street_stk_ask <> street_stk_bid
-- group by date_trunc('day', create_time), case when crossing_type='QUOTER' then crossing_type||ah else crossing_type end, actor_name, stock_tactic, trader_id;
-- 
-- insert into diego_metrics_t00
-- select date_trunc('day', create_time), case when crossing_type='QUOTER' then crossing_type||ah else crossing_type end, actor_name, stock_tactic, trader_id, 'sprdcut_GS_exec',
--   sum((case when side='Buy' then 1 else -1 end * ((street_stk_bid+street_stk_ask)/2-fill_price) + impact/fill_quantity) / ((street_stk_ask - street_stk_bid)/2 + impact/fill_quantity) * fill_quantity) / sum(fill_quantity) savings_not_crossing
-- from events.fills f
-- join diego_metrics_t t
-- on f.session_id=t.session_id and f.root_id=t.root_id
-- join diego_metrics_impact di
-- on f.session_id=di.session_id and f.root_id=di.root_id and f.fill_id=di.fill_id
-- where create_time between startdt and enddt + interval '1 day'--where create_time between current_date and current_date + interval '1 day'
-- and crossing_type in ('QUOTER','IGOOGI','INTERNALIZED')
-- and f.root_id=f.order_id
-- and f.security like 'S:%'
-- group by date_trunc('day', create_time), case when crossing_type='QUOTER' then crossing_type||ah else crossing_type end, actor_name, stock_tactic, trader_id;
-- 
-- for r in
-- select *
-- from diego_metrics_t00
-- loop
--   return next r;
-- end loop;
-- return;
-- end;
-- $BODY$
--   LANGUAGE plpgsql VOLATILE
--   COST 100
--   ROWS 1000;
-- ALTER FUNCTION get_diego_metrics(date, date, numeric)
--   OWNER TO execution;
