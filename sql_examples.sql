
--indexing trade days 
SELECT [TradeDate], RANK() over (order by tradedate desc) as num 
  FROM[igtdev].[dbo].[tblStockHistory] 
  where TradeDate > '2015-01-01' and Symbol = 'AAPL' 
  group by TradeDate
  order by TradeDate desc


---------looks for unique TransactionIDs which only appear once 
WITH x as (SELECT *
  FROM [RTPosition].[dbo].[tblactivities] a
  Where CPS = 'S' and TradeDate > '2016-12-01'  and (OperatorID = '218')
	and TradeSource != 'BASKET-PRD' 
  )
  
  select * from x a
  where TransactionID not in (select TransactionID from x b where b.TradeDate = a.TradeDate group by TransactionID having COUNT(*) > 1) 
  order by ExecutionTime asc 
  
 ------------grouping broker stock cost information based on liquidity and time 
 with bc as (select left(ticker, CHARINDEX('.',ticker)-1) ticker, executed_val, date, mid from [CMSandbox].[dbo].[JG_Bclays16] 
			union select left(ticker, CHARINDEX('.',ticker)-1) ticker, executed_val,  '2016-01-01' as date, mid  from [CMSandbox].[dbo].[JG_BclaysJan16] 
			union select left(ticker, CHARINDEX('.',ticker)-1) ticker, executed_val, '2016-03-01' as date, mid  from [CMSandbox].[dbo].[JG_BclaysMar16]
			union select left(ticker, CHARINDEX('.',ticker)-1) ticker, executed_val, '2016-05-01' date, mid  from [CMSandbox].[dbo].[JG_BclaysMay16]
			union select left(ticker, CHARINDEX('.',ticker)-1) ticker, executed_val, '2016-04-01' date, mid  from [CMSandbox].[dbo].[JG_BclaysApril16]
			union select ticker, [principal_val] as executed_val, date, -[arvl_shortfall_bps] as mid  from [CMSandbox].[dbo].[JG_cowen16])
select  case when Liq_Rank<=125 then '01.[1-125]'
            when Liq_Rank<=250 then '02.[126-250]'
              when Liq_Rank<=500 then '03.[251-500]'
              when Liq_Rank<=750 then '04.[501-750]'
              when Liq_Rank<=1000 then '05.[751-1000]'
              when Liq_Rank<=1500 then '06.[1001-1500]'
              when Liq_Rank<=2000 then '07.[1501-2000]'
              else '08.[2000+]'
       end liq_rank,
       sum(executed_val) ntnl, -sum(mid*executed_val)*0.0001 impact, -sum(mid*executed_val)/sum(executed_val) bps, date
from bc jg with (nolock)
join CMSandbox..tblLiquidityRankings tl with (nolock)
on jg.date between tl.RowIn and tl.RowOut and jg.ticker =tl.Symbol
group by case when Liq_Rank<=125 then '01.[1-125]'
            when Liq_Rank<=250 then '02.[126-250]'
              when Liq_Rank<=500 then '03.[251-500]'
              when Liq_Rank<=750 then '04.[501-750]'
              when Liq_Rank<=1000 then '05.[751-1000]'
              when Liq_Rank<=1500 then '06.[1001-1500]'
              when Liq_Rank<=2000 then '07.[1501-2000]'
              else '08.[2000+]'
       end ,date
order by DATE, liq_rank 
select jg.date, 
       sum(executed_val) ntnl, -sum(mid*executed_val)*0.0001 impact, -sum(mid*executed_val)/sum(executed_val) bps
from bc jg with (nolock)
join CMSandbox..tblLiquidityRankings tl with (nolock)
on jg.date between tl.RowIn and tl.RowOut and jg.ticker=tl.Symbol
group by jg.date
order by jg.DATE
-----------------censoring to get past month or so's worth of stock change data and computing rolling sum on them 
 WITH rawE as (SELECT distinct stkh.[Symbol]
      ,stkh.[TradeDate]
      ,stkh.[close]
	  ,((stkh.PercentChange * .01) + 1) as trueChange
	  ,stkh.[Close] as closePX
      ,erngs.Confirmed
      ,erngs.datetype
  FROM [igtdev].[dbo].[tblStockHistory] stkh
  left Join (SELECT s.StockSymbol
		  ,e.[CompanyID]
		  ,[DataSourceID]
		  ,[Confirmed]
		  ,[Date]
		  ,e.DateType
	  FROM [companies].[dbo].[tblEarnings] e
	  Left Join [companies].[dbo].[tblstocks] s on s.CompanyID = e.CompanyID
	  Where DataSourceID = '139' and Confirmed = '1') erngs on erngs.StockSymbol = stkh.Symbol and stkh.TradeDate = erngs.Date
  where TradeDate > '2015-01-01' and stkh.PercentChange is not null),
  
  --table for 20 day vols 
select x.Symbol,(CASE WHEN (x.TradeDate = '2016-11-25') then DATEADD(day, -2, x.TradeDate) 
	WHEN (DATEADD(day, -1, x.TradeDate) NOT IN (select TradeDate from rawE) and x.TradeDate != '2016-11-25') THEN DATEADD(day, -3, x.TradeDate) 
	else DATEADD(day, -1, x.TradeDate) END) as tradeDate, 
	100 * sqrt((252/19) * SUM(power(log(a.trueChange), 2))) as RV
  from rawE x
  left join rawE a on a.symbol = x.Symbol and a.TradeDate between DATEADD(day, -30, x.TradeDate) and DATEADD(day, 1, x.TradeDate)
  where x.symbol = a. symbol 
   and a.TradeDate not in (select t.tradeDate from rawE t where t.Symbol = a.Symbol and t.Confirmed = '1') --day of 
   and a.TradeDate not in (select DATEADD(day, 1, g.tradedate) from rawE g where g.Confirmed = '1' and g.Symbol = a.Symbol)         --day after 
	and ((a.TradeDate NOT IN (select DATEADD(day, 2, s3.tradedate) from rawE s3 where s3.Symbol = a.Symbol and s3.Confirmed = '1' and s3.DateType = 'AMC')) and
		(a.TradeDate NOT IN (select DATEADD(day, -1, s4.tradedate) from rawE s4 where s4.Symbol = a.Symbol and s4.Confirmed = '1' and s4.DateType = 'BMO'))) and 
		a.trueChange > 0
 group by x.Symbol, x.TradeDate