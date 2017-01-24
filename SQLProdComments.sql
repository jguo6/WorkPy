-- get close prices
if OBJECT_ID('tempdb..#stkpx', 'U') IS NOT NULL drop table #stkpx
select pr.securityid, date tradedate, ticker symbol, OpenPrice, ClosePrice, pr.AdjustmentFactor, Volume
into #stkpx
from IvyDB..SECURITY_PRICE pr
join
(
select SECURITYID, case when Class='' then LTRIM(RTRIM(Ticker)) else LTRIM(RTRIM(Ticker))+'.'+Class end ticker
from IvyDB..SECURITY with (nolock)
--where case when Class='' then LTRIM(RTRIM(Ticker)) else LTRIM(RTRIM(Ticker))+'.'+Class end = 'AAPL'
)  ivyid
on pr.SecurityID=ivyid.SecurityID
where pr.Date>='20141201'
and AdjustmentFactor>0

select top 10 * from #stkpx


-- get overnight close to open and interploate them 
if OBJECT_ID('tempdb..#ngtpx', 'U') IS NOT NULL drop table #ngtpx
select ngtchg.tradedate, symbol, (px2-px1)/39*n + px1 px, px1 stkcls_prv
into #ngtpx
from
(
select th_opn.tradedate, th_opn.symbol, th_cls.ClosePrice*th_cls.AdjustmentFactor / th_opn.AdjustmentFactor px1, th_opn.OpenPrice px2
from #stkpx th_cls
join PEAK6Research..tblcalendar tc
on th_cls.tradedate=tc.CalendarDate
join #stkpx th_opn
on tc.NextTradeDate=th_opn.tradedate and th_cls.SecurityID=th_opn.SecurityID
where th_cls.tradedate>='20141201' and th_cls.symbol='WMT'
) ngtchg
cross join
(
SELECT DISTINCT n = number 
FROM master..[spt_values] 
WHERE number between 1 and 39
) bar

select top 10 * from #ngtpx

CREATE NONCLUSTERED INDEX idx_dt_symbol
	ON [dbo].#ngtpx ([tradedate], [symbol])	

	

-- get the daily px standard deviation from the combined overnight and intraday prices
if OBJECT_ID('tempdb..#devpx', 'U') IS NOT NULL drop table #devpx
select a.tradedate, a.symbol, STDEVP(px) stdevpx, count(*) cnt
into #devpx
from
(
select *
from #ngtpx
union all
select tvwap.tradedate, tvwap.symbol, close_px  px, 0
from CMSandbox..tblVwapStockPrice tvwap with (nolock)
where tradetime between '08:30'  and '15:00'
and tvwap.tradedate>='20141201' and tvwap.symbol='WMT'
) a
group by a.tradedate, a.symbol

select top 10 * from #devpx   --cnt, be wary of small numbers 

CREATE NONCLUSTERED INDEX idx_symbol_dt
	ON [dbo].[#devpx] ([symbol], [tradedate])


-- get the predicted daily px standard deviation from weighted average
-- the weighted average weight = 0.7
if OBJECT_ID('tempdb..#devpxidx', 'U') IS NOT NULL drop table #devpxidx
select *, ROW_NUMBER() over (partition by symbol order by tradedate) idx  
into #devpxidx
from #devpx

select top 10 * from #devpxidx --weighted average purposes 

CREATE NONCLUSTERED INDEX idx_symbol_idx
	ON [dbo].[#devpxidx] ([symbol], [idx])	

if OBJECT_ID('tempdb..#devpxbase', 'U') IS NOT NULL drop table #devpxbase
select d1.symbol, d1.tradedate, max(d1.stdevpx) stdevpx,
  sum(d2.stdevpx *   
	  case when d1.idx-d2.idx = 1 then 0.3606 --days apart cond 
		   when d1.idx-d2.idx = 2 then 0.25
		   when d1.idx-d2.idx = 3 then 0.177
		   when d1.idx-d2.idx = 4 then 0.1234
		   else 0.087
	  end) stdevpx_base
into #devpxbase
from #devpxidx d1
join #devpxidx d2
on d1.symbol=d2.symbol and d2.idx between d1.idx-5 and d1.idx-1
where d1.symbol='WMT'
group by d1.symbol, d1.tradedate

select top 10 * from #devpxbase --weighted average 

CREATE NONCLUSTERED INDEX idx_symbol_dt
	ON [dbo].[#devpxbase] ([symbol], [tradedate])


 --looking at crossed trades YTD and see if the trades correponds to a big market move on the day, such trades' pnl till end of the day
if OBJECT_ID('tempdb..#trds', 'U') IS NOT NULL drop table #trds

select ta.TransactionDate,  cast(dateadd(hour, -1, ta.ExecutionTime) as time) tm, ta.StockSymbol, ta.price, ta.Quantity, ta.Side, ta.TicketSourceID, ta.TraderID stgID, tb.TraderID , ta.ActivityID,
  sum(case when tc.side='S' then -1 else 1 end * tc.Quantity) stgpos
into #trds
from RTPosition..tblactivities ta with (nolock)
join RTPosition..tblactivities tb with (nolock)
on ta.TransactionDate=tb.TransactionDate and ta.TransactionID=tb.TransactionID
left join RTPosition..tblactivities tc with (nolock)
on ta.TransactionDate=tc.TransactionDate and ta.StockSymbol=tc.StockSymbol and ta.ActivityID>=tc.ActivityID
and tc.IsIncluded=1 and tc.type in ('A', 'O') and tc.cps in ('S') and tc.account like 'F9%'
where ta.TransactionDate>='20160101' and tb.TransactionDate>='20160101' 
and ta.IsIncluded=1 and ta.type='A' and ta.cps in ('S')
and tb.IsIncluded=1 and tb.type='A' and tb.cps in ('S')
and ta.account='F99' and tb.account not like 'F9%' 
and ta.TicketSourceID in (303,304,391)
group by ta.TransactionDate,  cast(dateadd(hour, -1, ta.ExecutionTime) as time), ta.StockSymbol, ta.price, ta.Quantity, ta.Side, ta.TicketSourceID, ta.TraderID, tb.TraderID , ta.ActivityID

select top 10 * from #trds
-- adding standard deviation from today's actual movement
--CREATE NONCLUSTERED INDEX idx_dt_sym_tm
--	ON [dbo].[#trds] ([transactiondate], [stocksymbol], [tm])

if OBJECT_ID('tempdb..#devpxactual', 'U') IS NOT NULL drop table #devpxactual

select a.ActivityID, STDEVP(px) stdevpx
into #devpxactual
from
(
select trds.ActivityID, px1.px
from cmsandbox.[dbo].[YQ_tblactinternal] trds
join #ngtpx px1
on trds.dt=px1.tradedate and trds.StockSymbol=px1.symbol
union all
select trds.ActivityID, close_px  px
from #trds trds
join (select top 10 * from CMSandbox..tblVwapStockPrice) tvwap with (nolock)
on tvwap.tradedate=trds.TransactionDate and tvwap.tradetime<=trds.tm and tvwap.symbol=trds.StockSymbol
where tradetime between '08:30'  and '15:00'
and tvwap.tradedate>='20141201' and tvwap.symbol='WMT'
) a
group by a.ActivityID

select top 10 * from #devpxactual --intraday stdev from open until STG basket trade 

CREATE NONCLUSTERED INDEX idx_actvyid
	ON [dbo].[#devpxactual] ([activityid])

--CREATE NONCLUSTERED INDEX idx_activid
--	ON [dbo].[#trds] ([activityid])


if OBJECT_ID('tempdb..#trdsactual', 'U') IS NOT NULL drop table #trdsactual

select a.*, b.stdevpx stdevpxtilnow, c.stdevpx_base, c.stdevpx stdevpxday, tp.close_px, prev_close_px,
  case when b.stdevpx > c.stdevpx_base then b.stdevpx else c.stdevpx_base end stdevpx,
  case when b.stdevpx > c.stdevpx_base then 1 else 0 end isoverride
into #trdsactual
from cmsandbox.[dbo].[YQ_tblactinternal] a
join #devpxactual b
on a.ActivityID=b.ActivityID
join #devpxbase c
on a.dt=c.tradedate and a.StockSymbol=c.symbol
join CMSandbox..tblVwapStockPrice tp with (nolock)
on a.dt=tp.tradedate and a.StockSymbol=tp.symbol and tp.tradetime='15:00:00'
join 
(select tradedate, symbol, max(stkcls_prv) prev_close_px from #ngtpx group by tradedate, symbol) d 
on a.dt=d.tradedate and a.StockSymbol=d.symbol

select top 10 * from #trdsactual 

-- based on $2K, 5K, 10K when VaR (=1.96 * stdevpx * qty)
select dt, isoverride, ticketsourceid, side, traderid,
  case when stdevpx*quantity*1.96 > 2000 then 'block' else 'noblock' end actvy,
  LEFT(CONVERT(varchar, dt,112),6) dtmm,
  count(*) cnt, sum(quantity) qty, sum(price*quantity) ntnl, --cnt is based on buckets in group bay 
  sum(case when side='S' then -1 else 1 end * (close_px - price) * quantity) daymm
from #trdsactual
group by dt, isoverride, ticketsourceid, side, traderid, LEFT(CONVERT(varchar, dt,112),6),
case when stdevpx*quantity*1.96 > 2000 then 'block' else 'noblock' end 