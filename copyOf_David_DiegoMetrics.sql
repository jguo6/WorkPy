SET enable_nestloop TO FALSE;
with xi as (
select
distinct nw3.pid, mw.root_id, sum(mw.quantity) as quantity
from events.new mw
left join events.new nw1 on nw1.pid=mw.from_pid and nw1.root_id=mw.root_id and nw1.actor_name='SSendActor' and nw1.create_time > current_date
left join events.new nw2 on nw2.pid=nw1.from_pid and nw2.root_id=nw1.root_id and nw2.create_time > current_date
left join events.new nw3 on nw3.pid=nw2.from_pid and nw3.root_id=nw2.root_id and nw3.actor_name='StockAlgoActor' and nw3.create_time > current_date
where mw.create_time > current_date
and mw.actor_name='XIProxy'
group by nw3.pid, mw.root_id
),

main as (
select
date(nw.create_time)
,nw.root_id
,nw.order_id
,nw.actor_name
,nw.from_pid
,nw.pid
,nw.account
,nw.aggression_level
,nw.auto_hedge_id
,nw.holdback_percent
,nw.price
,nw.quantity
,nw.side
,nw.stock_tactic
,nw.strategy
,nw.trader_id
,nw.underlying
,nw.create_time 
,nw.order_source
,gks.nbbo_bid
,gks.nbbo_ask
,gks.nbbo_bid_size
,gks.nbbo_ask_size
,gks.street_bid
,gks.street_ask
,(gks.street_bid+gks.street_ask)/2 as street_mid
,gks.street_bid_size
,gks.street_ask_size
,ngksd.firm_greek as FirmDelta
,ngksd.account_greek as TraderDelta
,ngksg.firm_greek as FirmGamma
,ngksg.account_greek as TraderGamma
,ngksv.firm_greek as FirmVega
,ngksv.account_greek as TraderVega
,gks.on_new_impact_bps as RawBPS
,gks.on_new_impact_bps as AdjBPS
,coalesce(sum(fls.fill_quantity),0) as TotalFillQty
,sum(fls.fill_quantity * fls.fill_price)/sum(fls.fill_quantity) as TotalAvgPx
,sum(case when fls.broker_id!=0 then fls.fill_quantity else 0 end) as MarketQty
,sum(case when fls.broker_id!=0 then fls.fill_quantity else NULL end * case when fls.broker_id!=0 then fls.fill_price else NULL end)/sum(case when fls.broker_id!=0 then fls.fill_quantity else NULL end) as MarketAvgPx
,sum(case when fls.broker_id=0 then fls.fill_quantity else 0 end) as InternalQty
,sum(case when fls.broker_id=0 then fls.fill_quantity else NULL end * case when fls.broker_id=0 then fls.fill_price else NULL end)/sum(case when fls.broker_id=0 then fls.fill_quantity else NULL end) as InternalAvgPx
,sum(case when fls.crossing_type='QUOTER' then fls.fill_quantity else 0 end) as DiegoQty
,sum(case when fls.crossing_type='QUOTER' then fls.fill_quantity else NULL end * case when fls.crossing_type='QUOTER' then fls.fill_price else NULL end)/sum(case when fls.crossing_type='QUOTER' then fls.fill_quantity else NULL end) as DiegoAvgPx
,sum(case when fls.crossing_type='INTERNALIZED' then fls.fill_quantity else 0 end) as InternalizedQty
,sum(case when fls.crossing_type='INTERNALIZED' then fls.fill_quantity else NULL end * case when fls.crossing_type='INTERNALIZED' then fls.fill_price else NULL end)/sum(case when fls.crossing_type='INTERNALIZED' then fls.fill_quantity else NULL end) as InternalizedAvgPx
,sum(case when fls.crossing_type='IGOOGI' then fls.fill_quantity else 0 end) as IgoogiQty
,sum(case when fls.crossing_type='IGOOGI' then fls.fill_quantity else NULL end * case when fls.crossing_type='IGOOGI' then fls.fill_price else NULL end)/sum(case when fls.crossing_type='IGOOGI' then fls.fill_quantity else NULL end) as IgoogiAvgPx
,sum(case when fls.crossing_type='TRADER_CROSS' then fls.fill_quantity else 0 end) as TraderCrossQty
,sum(case when fls.crossing_type='TRADER_CROSS' then fls.fill_quantity else NULL end * case when fls.crossing_type='TRADER_CROSS' then fls.fill_price else NULL end)/sum(case when fls.crossing_type='TRADER_CROSS' then fls.fill_quantity else NULL end) as TraderCrossAvgPx
,case when nw.from_pid=0 then 'NAH' else 'AH' end as Autohedge
,case when ngksg.account_greek > 0 then 'LongGamma' when ngksg.account_greek < 0 then 'ShortGamma' when ngksg.account_greek=0 then '0' else 'n/a' end as TraderGammaSide
,case when ngksg.firm_greek > 0 then 'LongGamma' when ngksg.firm_greek < 0 then 'ShortGamma' when ngksg.firm_greek=0 then '0' else 'n/a' end as FirmGammaSide
,case when ngksd.firm_greek > 0 then 'LongDelta' when ngksd.firm_greek < 0 then 'ShortDelta' when ngksd.firm_greek=0 then '0' else 'n/a' end as FirmDeltaSide
,case when ngksd.account_greek > 0 then 'LongDelta' when ngksd.account_greek < 0 then 'ShortDelta' when ngksd.account_greek =0 then '0' else 'n/a' end as TraderDeltaSide
,CASE WHEN nw.create_time::time between '08:30:00' and '15:00:00' THEN 'DAY' ELSE 'EXT' END AS Session
,CASE WHEN nw.side='Buy' THEN (CASE WHEN nw.price<gks.street_bid THEN 'AWAY' WHEN nw.price=gks.street_bid THEN 'PASSIVE' WHEN nw.price>gks.street_ask THEN 'THROUGH' WHEN nw.price=gks.street_ask THEN 'CROSS' ELSE 'MID' END) ELSE 
(CASE WHEN nw.price>gks.street_ask THEN 'AWAY' WHEN nw.price=gks.street_ask THEN 'PASSIVE' WHEN nw.price<gks.street_bid THEN 'THROUGH' WHEN nw.price=gks.street_bid THEN 'CROSS' ELSE 'MID' END) END AS Aggressiveness
,case when stock_tactic='SFIRE' then 'SFIRE' else 'ALGO' end as OrderType
,cast('Phoenix' as text) as OrderSource
,cls.close
, 1 as HRBase
,fls.commission as commission_rate
,case   WHEN nw.create_time::time between '08:30' and '09:00' THEN 1 
        WHEN nw.create_time::time between '09:00' and '09:30' THEN 2
        WHEN nw.create_time::time between '09:30' and '10:00' THEN 3
        WHEN nw.create_time::time between '10:00' and '10:30' THEN 4
        WHEN nw.create_time::time between '10:30' and '11:00' THEN 5
        WHEN nw.create_time::time between '11:00' and '11:30' THEN 6
        WHEN nw.create_time::time between '11:30' and '12:00' THEN 7
        WHEN nw.create_time::time between '12:00' and '12:30' THEN 8
        WHEN nw.create_time::time between '12:30' and '13:00' THEN 9
        WHEN nw.create_time::time between '13:00' and '13:30' THEN 10
        WHEN nw.create_time::time between '13:30' and '14:00' THEN 11
        WHEN nw.create_time::time between '14:00' and '14:30' THEN 12
        WHEN nw.create_time::time between '14:30' and '15:00' THEN 13
        ELSE 0 END AS TimeBucket
,x.quantity as MarketOrderedQty
from events.new nw
left join events.stockalgoactor gks on nw.root_id=gks.root_id and nw.order_id=gks.order_id and gks.begin_state='NEW' and gks.create_time > current_date
left join events.new_greek_risk_events ngksd on nw.root_id=ngksd.root_id and nw.order_id=ngksd.order_id and ngksd.greek_type='DELTA' and ngksd.create_time > current_date
left join events.new_greek_risk_events ngksg on nw.root_id=ngksg.root_id and nw.order_id=ngksg.order_id and ngksg.greek_type='GAMMA' and ngksg.create_time > current_date
left join events.new_greek_risk_events ngksv on nw.root_id=ngksv.root_id and nw.order_id=ngksv.order_id and ngksv.greek_type='VEGA'  and ngksv.create_time > current_date
left join events.fills fls on nw.root_id=fls.root_id and nw.order_id=fls.order_id and fls.actor_name='StockAlgoActor' and fls.create_time >= current_date
left join eventstats.security_closing_prices cls on nw.underlying=right(cls.security_key,-2) and date(nw.create_time)=date(cls.date_created) and cls.date_created >= current_date
--left join eventstats.stk_clspx cls on nw.underlying=cls.underlying and date(nw.create_time)=cls.tradedate
left join xi x on nw.root_id=x.root_id and nw.pid=x.pid
where nw.create_time > current_date and nw.actor_name='StockAlgoActor'
group by
nw.root_id
,nw.order_id
,nw.actor_name
,nw.from_pid
,nw.pid
,nw.account
,nw.aggression_level
,nw.auto_hedge_id
,nw.holdback_percent
,nw.price
,nw.quantity
,nw.side
,nw.stock_tactic
,nw.strategy
,nw.trader_id
,nw.underlying
,nw.create_time
,nw.order_source
,gks.nbbo_bid
,gks.nbbo_ask
,gks.nbbo_bid_size
,gks.nbbo_ask_size
,gks.street_bid
,gks.street_ask
,gks.street_bid_size
,gks.street_ask_size
,ngksd.firm_greek
,ngksd.account_greek
,ngksg.firm_greek
,ngksg.account_greek
,cls.close
,ngksv.firm_greek
,ngksv.account_greek
,gks.on_new_impact_bps
,gks.on_new_impact_bps
,x.quantity
,fls.commission
),

cte_greeks as (
                                    SELECT c.*, row_number() over (partition by c.event_id, c.security_id order by c.create_time desc) as rowNum
                                    FROM events.trader_basket a
                                    inner join events.trader_basket b ON b.order_id=a.order_id and b.create_time >= current_date
                                    inner join events.greeks c ON a.session_id=c.session_id and a.event_id=c.event_id and a.security_id=c.security_id and c.create_time >= current_date
                                    where a.category='BasketTrade' and a.account NOT IN ('F91','F97','F99') and b.category='fillPriceCalc' and a.create_time >= current_date
                                    ),

baskets as (

SELECT  distinct a.create_time::date as Date
            ,a.root_id
            ,a.order_id
            ,a.category
            ,a.event_id
            ,a.event_id
            ,a.account
            ,'_1' as aggression_level
            ,cast(null as int8) as auto_hedge_id
            ,cast(null as int8) as holdback_percent
            ,b.price
            ,a.quantity - coalesce(r.rejected,0) as Qty
            ,a.side
            ,'BASKET'
            ,'stock'
            ,a.account_owner
            ,split_part(a.security_id,':',2)
            ,a.create_time
            ,'basket' as order_source
            ,c.bid as nbbo_bid
            ,c.ask as nbbo_ask
            ,c.bid_size as nbbo_bid_size
            ,c.ask_size as nbbo_ask_size
            ,c.street_bid
            ,c.street_ask
            ,(c.street_bid+c.street_ask)/2 as street_mid
            ,c.street_bid_size
            ,c.street_ask_size
            ,evtd.firm_greek as FirmDelta
            ,evtd.account_greek as TraderDelta
            ,evtg.firm_greek as FirmGamma
            ,evtg.account_greek as TraderGamma
            ,evtv.firm_greek as FirmVega
            ,evtv.account_greek as TraderVega
            ,b.raw_bps as RawBPS
            ,b.raw_bps * b.multiplier as AdjBPS
            ,a.quantity as totalfillqty
            ,b.price as totalavgpx
            ,cast(0 as numeric) as marketqty
            ,cast(null as float8) as marketavgpx
            ,a.quantity as internalqty
            ,b.price as internalavgpx
            ,cast(null as float8) as diegoqty
            ,cast(null as float8) as diegoavgpx
            ,cast(null as float8) as internalizedqty
            ,cast(null as float8) as internalizedavgpx
            ,cast(null as float8) as igoogiqty
            ,cast(null as float8) as igoogiavgpx
            ,cast(null as float8) as tradercrossqty
            ,cast(null as float8) as tradercrossavgpx
            ,'NAH' as autohedge
,case when evtg.account_greek > 0 then 'LongGamma' when evtg.account_greek < 0 then 'ShortGamma' when evtg.account_greek=0 then '0' else 'n/a' end as TraderGammaSide
,case when evtg.firm_greek > 0 then 'LongGamma' when evtg.firm_greek < 0 then 'ShortGamma' when evtg.firm_greek=0 then '0' else 'n/a' end as FirmGammaSide
,case when evtd.firm_greek > 0 then 'LongDelta' when evtd.firm_greek < 0 then 'ShortDelta' when evtd.firm_greek=0 then '0' else 'n/a' end as FirmDeltaSide
,case when evtd.account_greek > 0 then 'LongDelta' when evtd.account_greek < 0 then 'ShortDelta' when evtd.account_greek =0 then '0' else 'n/a' end as TraderDeltaSide
            ,'DAY' as session
            ,'THROUGH' as aggressiveness
            ,'BASKET' as ordertype
            ,'BASKET' as ordersource
    ,cls.close as close
            ,1 as HRBase
            ,abs(b.commission) as commission_rate
            ,case   WHEN b.create_time::time between '08:30' and '09:00' THEN 1 
        WHEN b.create_time::time between '09:00' and '09:30' THEN 2
        WHEN b.create_time::time between '09:30' and '10:00' THEN 3
        WHEN b.create_time::time between '10:00' and '10:30' THEN 4
        WHEN b.create_time::time between '10:30' and '11:00' THEN 5
        WHEN b.create_time::time between '11:00' and '11:30' THEN 6
        WHEN b.create_time::time between '11:30' and '12:00' THEN 7
        WHEN b.create_time::time between '12:00' and '12:30' THEN 8
        WHEN b.create_time::time between '12:30' and '13:00' THEN 9
        WHEN b.create_time::time between '13:00' and '13:30' THEN 10
        WHEN b.create_time::time between '13:30' and '14:00' THEN 11
        WHEN b.create_time::time between '14:00' and '14:30' THEN 12
        WHEN b.create_time::time between '14:30' and '15:00' THEN 13
        ELSE 0 END AS TimeBucket
            ,cast(null as numeric) as MarketOrderedQty

              FROM events.trader_basket a
              inner join events.trader_basket b ON b.order_id=a.order_id and b.create_time >= current_date
              inner join cte_greeks c ON a.session_id=c.session_id and a.event_id=c.event_id and a.security_id=c.security_id and c.create_time >= current_date
              --inner join (SELECT distinct name, traderid from eventstats.trader_account_subentity) as nm on nm.traderid=a.account_owner
              left join eventstats.security_closing_prices cls on split_part(a.security_id,':',2)=right(cls.security_key,-2) and date(a.create_time)=date(cls.date_created) and cls.date_created >= current_date
              left join events.new_greek_risk_events evtd on a.order_id=evtd.order_id and evtd.event_type='BASKET_TRADE' and evtd.greek_type='DELTA'  and evtd.create_time >= current_date
              left join events.new_greek_risk_events evtg on a.order_id=evtg.order_id and evtg.event_type='BASKET_TRADE' and evtg.greek_type='GAMMA' and evtg.create_time >= current_date
              left join events.new_greek_risk_events evtv on a.order_id=evtv.order_id and evtv.event_type='BASKET_TRADE' and evtv.greek_type='VEGA'  and evtv.create_time >= current_date
      left join (select rejected, order_id from events.trader_basket where category = 'LocateRejects' and create_time >= current_date) as r on r.order_id = a.order_id        
      where a.category='BasketTrade' and a.account NOT IN ('F91','F97','F99') and b.category='fillPriceCalc' and 
              c.rowNum = 1 and a.create_time >= current_date
),

pet as (
select
date(nw.create_time)
,nw.root_id
,nw.order_id
,nw.actor_name
,nw.from_pid
,nw.pid
,nw.account
,nw.aggression_level
,nw.auto_hedge_id
,nw.holdback_percent
,nw.price
,nw.quantity
,nw.side
,nw.stock_tactic
,nw.strategy
,nw.trader_id
,nw.underlying
,nw.create_time 
,nw.order_source
,gks.bid
,gks.ask
,gks.bid_size
,gks.ask_size
,gks.street_bid
,gks.street_ask
,(gks.street_bid+gks.street_ask)/2 as street_mid
,gks.street_bid_size
,gks.street_ask_size
,ngksd.firm_greek as FirmDelta
,ngksd.account_greek as TraderDelta
,ngksg.firm_greek as FirmGamma
,ngksg.account_greek as TraderGamma
,ngksv.firm_greek as FirmVega
,ngksv.account_greek as TraderVega
,cast(null as float8) as  RawBPS
,cast(null as float8) as  AdjBPS
,coalesce(sum(fls.fill_quantity),0) as TotalFillQty
,sum(fls.fill_quantity * fls.fill_price)/sum(fls.fill_quantity) as TotalAvgPx
,sum(case when fls.broker_id!=0 then fls.fill_quantity else 0 end) as MarketQty
,sum(case when fls.broker_id!=0 then fls.fill_quantity else NULL end * case when fls.broker_id!=0 then fls.fill_price else NULL end)/sum(case when fls.broker_id!=0 then fls.fill_quantity else NULL end) as MarketAvgPx
,sum(case when fls.broker_id=0 then fls.fill_quantity else 0 end) as InternalQty
,sum(case when fls.broker_id=0 then fls.fill_quantity else NULL end * case when fls.broker_id=0 then fls.fill_price else NULL end)/sum(case when fls.broker_id=0 then fls.fill_quantity else NULL end) as InternalAvgPx
,sum(case when fls.crossing_type='QUOTER' then fls.fill_quantity else 0 end) as DiegoQty
,sum(case when fls.crossing_type='QUOTER' then fls.fill_quantity else NULL end * case when fls.crossing_type='QUOTER' then fls.fill_price else NULL end)/sum(case when fls.crossing_type='QUOTER' then fls.fill_quantity else NULL end) as DiegoAvgPx
,sum(case when fls.crossing_type='INTERNALIZED' then fls.fill_quantity else 0 end) as InternalizedQty
,sum(case when fls.crossing_type='INTERNALIZED' then fls.fill_quantity else NULL end * case when fls.crossing_type='INTERNALIZED' then fls.fill_price else NULL end)/sum(case when fls.crossing_type='INTERNALIZED' then fls.fill_quantity else NULL end) as InternalizedAvgPx
,sum(case when fls.crossing_type='IGOOGI' then fls.fill_quantity else 0 end) as IgoogiQty
,sum(case when fls.crossing_type='IGOOGI' then fls.fill_quantity else NULL end * case when fls.crossing_type='IGOOGI' then fls.fill_price else NULL end)/sum(case when fls.crossing_type='IGOOGI' then fls.fill_quantity else NULL end) as IgoogiAvgPx
,sum(case when fls.crossing_type='TRADER_CROSS' then fls.fill_quantity else 0 end) as TraderCrossQty
,sum(case when fls.crossing_type='TRADER_CROSS' then fls.fill_quantity else NULL end * case when fls.crossing_type='TRADER_CROSS' then fls.fill_price else NULL end)/sum(case when fls.crossing_type='TRADER_CROSS' then fls.fill_quantity else NULL end) as TraderCrossAvgPx
,case when nw.from_pid=0 then 'NAH' else 'AH' end as Autohedge
,case when ngksg.account_greek > 0 then 'LongGamma' when ngksg.account_greek < 0 then 'ShortGamma' when ngksg.account_greek=0 then '0' else 'n/a' end as TraderGammaSide
,case when ngksg.firm_greek > 0 then 'LongGamma' when ngksg.firm_greek < 0 then 'ShortGamma' when ngksg.firm_greek=0 then '0' else 'n/a' end as FirmGammaSide
,case when ngksd.firm_greek > 0 then 'LongDelta' when ngksd.firm_greek < 0 then 'ShortDelta' when ngksd.firm_greek=0 then '0' else 'n/a' end as FirmDeltaSide
,case when ngksd.account_greek > 0 then 'LongDelta' when ngksd.account_greek < 0 then 'ShortDelta' when ngksd.account_greek =0 then '0' else 'n/a' end as TraderDeltaSide
,CASE WHEN nw.create_time::time between '08:30:00' and '15:00:00' THEN 'DAY' ELSE 'EXT' END AS Session
,CASE WHEN nw.side='Buy' THEN (CASE WHEN nw.price<gks.street_bid THEN 'AWAY' WHEN nw.price=gks.street_bid THEN 'PASSIVE' WHEN nw.price>gks.street_ask THEN 'THROUGH' WHEN nw.price=gks.street_ask THEN 'CROSS' ELSE 'MID' END) ELSE 
(CASE WHEN nw.price>gks.street_ask THEN 'AWAY' WHEN nw.price=gks.street_ask THEN 'PASSIVE' WHEN nw.price<gks.street_bid THEN 'THROUGH' WHEN nw.price=gks.street_bid THEN 'CROSS' ELSE 'MID' END) END AS Aggressiveness
,cast('PET' as text) as OrderType
,cast('PET' as text) as OrderSource
,cls.close
, 1 as HRBase
,0 as commission_rate
,case   WHEN nw.create_time::time between '08:30' and '09:00' THEN 1 
        WHEN nw.create_time::time between '09:00' and '09:30' THEN 2
       WHEN nw.create_time::time between '09:30' and '10:00' THEN 3
        WHEN nw.create_time::time between '10:00' and '10:30' THEN 4
        WHEN nw.create_time::time between '10:30' and '11:00' THEN 5
        WHEN nw.create_time::time between '11:00' and '11:30' THEN 6
        WHEN nw.create_time::time between '11:30' and '12:00' THEN 7
        WHEN nw.create_time::time between '12:00' and '12:30' THEN 8
        WHEN nw.create_time::time between '12:30' and '13:00' THEN 9
        WHEN nw.create_time::time between '13:00' and '13:30' THEN 10
        WHEN nw.create_time::time between '13:30' and '14:00' THEN 11
        WHEN nw.create_time::time between '14:00' and '14:30' THEN 12
        WHEN nw.create_time::time between '14:30' and '15:00' THEN 13
        ELSE 0 END AS TimeBucket
       ,nw.quantity as MarketOrderedQty      

from events.new nw

left join events.greeks gks on nw.root_id=gks.root_id  and gks.actor_name=nw.actor_name and nw.event_id=gks.event_id and gks.create_time >= current_date
left join events.new_greek_risk_events ngksd on nw.root_id=ngksd.root_id and nw.order_id=ngksd.order_id and ngksd.greek_type='DELTA' and ngksd.create_time >= current_date
left join events.new_greek_risk_events ngksg on nw.root_id=ngksg.root_id and nw.order_id=ngksg.order_id and ngksg.greek_type='GAMMA' and ngksg.create_time >= current_date
left join events.new_greek_risk_events ngksv on nw.root_id=ngksv.root_id and nw.order_id=ngksv.order_id and ngksv.greek_type='VEGA' and ngksv.create_time >= current_date
left join events.fills fls on nw.root_id=fls.root_id and nw.order_id=fls.order_id and fls.actor_name='PortfolioSSendActor' and fls.create_time >= current_date
left join eventstats.security_closing_prices cls on nw.underlying=right(cls.security_key,-2) and date(nw.create_time)=date(cls.date_created) and cls.date_created >= current_date
--left join eventstats.stk_clspx cls on nw.underlying=cls.underlying and date(nw.create_time)=cls.tradedate

where nw.create_time > current_date and nw.actor_name='PortfolioSSendActor'

group by
nw.root_id
,nw.order_id
,nw.actor_name
,nw.from_pid
,nw.pid
,nw.account
,nw.aggression_level
,nw.auto_hedge_id
,nw.holdback_percent
,nw.price
,nw.quantity
,nw.side
,nw.stock_tactic
,nw.strategy
,nw.trader_id
,nw.underlying
,nw.create_time
,nw.order_source
,gks.bid
,gks.ask
,gks.bid_size
,gks.ask_size
,gks.street_bid
,gks.street_ask
,gks.street_bid_size
,gks.street_ask_size
,ngksd.firm_greek
,ngksd.account_greek
,ngksg.firm_greek
,ngksg.account_greek
,cls.close
,ngksv.firm_greek
,ngksv.account_greek
--,gks.on_new_impact_bps
--,gks.on_new_impact_bps

),

last as (

select mn1.*
from main mn1
union
select bsk.*
from baskets bsk
union
select pt.*
from pet pt
)

select 
mn.date
,mn.root_id
,mn.order_id
,mn.actor_name
,mn.from_pid
,mn.pid
,mn.account
,mn.aggression_level
,mn.auto_hedge_id
,mn.holdback_percent
,mn.price
,mn.quantity
,mn.side
,mn.stock_tactic
,mn.strategy
,mn.trader_id
,mn.underlying
,mn.create_time
,mn.order_source
,mn.nbbo_bid
,mn.nbbo_ask
,mn.nbbo_bid_size
,mn.nbbo_ask_size
,mn.street_bid
,mn.street_ask
,mn.street_mid
,mn.street_bid_size
,mn.street_ask_size
,mn.firmdelta
,mn.traderdelta
,mn.firmgamma
,mn.tradergamma
,mn.firmvega
,mn.tradervega
,mn.rawbps
,mn.adjbps
,mn.totalfillqty
,mn.totalavgpx
,mn.marketqty
,mn.marketavgpx
,mn.internalqty
,mn.internalavgpx
,mn.diegoqty
,mn.diegoavgpx
,mn.internalizedqty
,mn.internalizedavgpx
,mn.igoogiqty
,mn.igoogiavgpx
,mn.tradercrossqty
,mn.tradercrossavgpx
,mn.autohedge
,mn.tradergammaside
,mn.firmgammaside
,mn.firmdeltaside
,mn.traderdeltaside
,mn.session
,mn.aggressiveness
,mn.ordertype
,mn.ordersource
,mn.close
,mn.hrbase
,mn.timebucket
,mn.marketorderedqty
,CASE WHEN mn.timebucket=1 THEN '08:30-09:00' WHEN mn.timebucket=2 THEN '09:00-09:30' WHEN mn.timebucket=3 THEN '09:30-10:00' WHEN mn.timebucket=4 THEN '10:00-10:30' WHEN mn.timebucket=5 THEN '10:30-11:00' WHEN mn.timebucket=6 THEN '11:00-11:30' WHEN mn.timebucket=7 THEN '11:30-12:00'
WHEN mn.timebucket=8 THEN '12:00-12:30' WHEN mn.timebucket=9 THEN '12:30-13:00' WHEN mn.timebucket=10 THEN '13:00-13:30' WHEN mn.timebucket=11 THEN '13:30-14:00' when mn.timebucket=12 THEN '14:00-14:30' WHEN mn.timebucket=13 THEN '14:30-15:00' ELSE 'EXT' END As TimeBuckets

---PNL CLose Calcs----
,case when mn.side='Buy' then (mn.close - mn.totalavgpx) * mn.totalfillqty else (mn.totalavgpx - mn.close)* mn.totalfillqty end as TotalFillPnlClose
,case when mn.side='Buy' then (mn.close - mn.marketavgpx) * mn.marketqty else (mn.marketavgpx - mn.close)* mn.marketqty end as MktPnlClose
,case when mn.side='Buy' then (mn.close - mn.internalavgpx) * mn.internalqty else (mn.internalavgpx - mn.close)* mn.internalqty end as InternalPnlClose
,case when mn.side='Buy' then (mn.close - mn.internalizedavgpx) * mn.internalizedqty else (mn.internalizedavgpx- mn.close)* mn.internalizedqty end as InternalizedPnlClose
,case when mn.side='Buy' then (mn.close - mn.diegoavgpx) * mn.diegoqty else (mn.diegoavgpx- mn.close)* mn.diegoqty end as DiegoPnlClose
,case when mn.side='Buy' then (mn.close - mn.igoogiavgpx) * mn.igoogiqty else (mn.igoogiavgpx- mn.close)* mn.igoogiqty end as IgoogIPnlClose
,case when mn.side='Buy' then (mn.close - mn.tradercrossavgpx) * mn.tradercrossqty else (mn.tradercrossavgpx- mn.close)* mn.tradercrossqty end as TraderCrossPnlClose
,case when mn.side='Buy' then (mn.close - mn.price) * mn.quantity else (mn.price - mn.close)* mn.quantity end as OrderLimitPnLClose
,case when mn.side='Buy' then (mn.close - mn.price) * (mn.quantity - mn.totalfillqty) else (mn.price - mn.close)* (mn.quantity -  mn.totalfillqty) end as UnexLimitPnLClose

---Slippage Calcs---
, case when mn.side='Buy' then (mn.street_mid - mn.totalavgpx) * mn.totalfillqty else (mn.totalavgpx - mn.street_mid) * mn.totalfillqty end as TotalFillSlippage
, case when mn.side='Buy' then (mn.street_mid - mn.marketavgpx) * mn.marketqty else (mn.marketavgpx - mn.street_mid) * mn.marketqty end as MarketSlippage
, case when mn.side='Buy' then (mn.street_mid - mn.internalavgpx) * mn.internalqty else (mn.internalavgpx - mn.street_mid) * mn.internalqty end as InternalSlippage
, case when mn.side='Buy' then (mn.street_mid - mn.internalizedavgpx) * mn.internalizedqty else (mn.internalizedavgpx - mn.street_mid) * mn.internalizedqty end as InternalizedSlippage
, case when mn.side='Buy' then (mn.street_mid - mn.diegoavgpx) * mn.diegoqty else (mn.diegoavgpx - mn.street_mid) * mn.diegoqty end as DiegoSlippage
, case when mn.side='Buy' then (mn.street_mid - mn.igoogiavgpx) * mn.igoogiqty else (mn.igoogiavgpx - mn.street_mid) * mn.igoogiqty end as IgoogISlippage
, case when mn.side='Buy' then (mn.street_mid - mn.tradercrossavgpx) * mn.tradercrossqty else (mn.tradercrossavgpx - mn.street_mid) * mn.tradercrossqty end as TraderCrossSlippage

---Price Improvement Calcs---
,case when mn.side='Buy' then (mn.street_ask - mn.totalavgpx) * mn.totalfillqty else (mn.totalavgpx - mn.street_bid)* mn.totalfillqty end as TotalFillPriceImp
,case when mn.side='Buy' then (mn.street_ask - mn.marketavgpx) * mn.marketqty else (mn.marketavgpx - mn.street_bid)* mn.marketqty end as MktPriceImp
,case when mn.side='Buy' then (mn.street_ask - mn.internalavgpx) * mn.internalqty else (mn.internalavgpx - mn.street_bid)* mn.internalqty end as InternalPriceImp
,case when mn.side='Buy' then (mn.street_ask - mn.internalizedavgpx) * mn.internalizedqty else (mn.internalizedavgpx- mn.street_bid)* mn.internalizedqty end as InternalizedPriceImp
,case when mn.side='Buy' then (mn.street_ask - mn.diegoavgpx) * mn.diegoqty else (mn.diegoavgpx- mn.street_bid)* mn.diegoqty end as DiegoPriceImp
,case when mn.side='Buy' then (mn.street_ask - mn.igoogiavgpx) * mn.igoogiqty else (mn.igoogiavgpx- mn.street_bid)* mn.igoogiqty end as IgoogIPriceImp
,case when mn.side='Buy' then (mn.street_ask - mn.tradercrossavgpx) * mn.tradercrossqty else (mn.tradercrossavgpx- mn.street_bid)* mn.tradercrossqty end as TraderCrossPriceImp

---Hit Rate Calcs----
,case when (case when mn.side='Buy' then (mn.close - mn.totalavgpx) * mn.totalfillqty else (mn.totalavgpx - mn.close)* mn.totalfillqty end) > 0 then 1 else 0 end as TotalFillPnlCloseHR
,case when (case when mn.side='Buy' then (mn.close - mn.marketavgpx) * mn.marketqty else (mn.marketavgpx - mn.close)* mn.marketqty end) > 0 then 1 else 0 end as MktPnlCloseHR
,case when (case when mn.side='Buy' then (mn.close - mn.internalavgpx) * mn.internalqty else (mn.internalavgpx - mn.close)* mn.internalqty end) > 0 then 1 else 0 end as InternalPnlCloseHR
,case when (case when mn.side='Buy' then (mn.close - mn.internalizedavgpx) * mn.internalizedqty else (mn.internalizedavgpx- mn.close)* mn.internalizedqty end) > 0 then 1 else 0 end as InternalizedPnlCloseHR
,case when (case when mn.side='Buy' then (mn.close - mn.diegoavgpx) * mn.diegoqty else (mn.diegoavgpx- mn.close)* mn.diegoqty end) > 0 then 1 else 0 end as DiegoPnlCloseHR
,case when (case when mn.side='Buy' then (mn.close - mn.igoogiavgpx) * mn.igoogiqty else (mn.igoogiavgpx- mn.close)* mn.igoogiqty end) > 0 then 1 else 0 end as IgoogIPnlCloseHR
,case when (case when mn.side='Buy' then (mn.close - mn.tradercrossavgpx) * mn.tradercrossqty else (mn.tradercrossavgpx- mn.close)* mn.tradercrossqty end) > 0 then 1 else 0 end as TraderCrossPnlCloseHR
,case when (case when mn.side='Buy' then (mn.close - mn.price) * mn.quantity else (mn.price - mn.close)* mn.quantity end) > 0 then 1 else 0 end as OrderLimitPnLCloseHR
,case when (case when mn.side='Buy' then (mn.close - mn.price) * (mn.quantity - mn.totalfillqty) else (mn.price - mn.close)* (mn.quantity -  mn.totalfillqty) end) > 0 then 1 else 0 end as UnexLimitPnLCloseHR
,mn.commission_rate
from last mn
