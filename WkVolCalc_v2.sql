/****** Script for SelectTopNRows command from SSMS  ******/

--CREATE TABLE #dy (
--	tradeDate datetime, 
--	wknum integer 
--)

Declare @W as Date 
Declare @W1 as Date 
Declare @W2 as Date 
Declare @W3 as Date 
Declare @W4 as Date 
Declare @W5 as Date 
Declare @W6 as Date 
Declare @W7 as Date 
Declare @W8 as Date 
Declare @W9 as Date 
Declare @W10 as Date 
Declare @W11 as Date 
Declare @W12 as Date 
Declare @W13 as Date 
Declare @W14 as Date 
Declare @W15 as Date 
Declare @W16 as Date 
Declare @W17 as Date 
Declare @W18 as Date 
Declare @W19 as Date 
Declare @W20 as Date 
Declare @W21 as Date 
Declare @W22 as Date 
Declare @W23 as Date 
Declare @W24 as Date 
Declare @W25 as Date 
Declare @W26 as Date 
Declare @W27 as Date 
Declare @W28 as Date 
Declare @W29 as Date 
Declare @W30 as Date 
Declare @W31 as Date 
Declare @W32 as Date 
Declare @W33 as Date 
Declare @W34 as Date 
Declare @W35 as Date 
Declare @W36 as Date 
Declare @W37 as Date 
Declare @W38 as Date 
Declare @W39 as Date 
Declare @W40 as Date 
Declare @W41 as Date 
Declare @W42 as Date 
Declare @W43 as Date 
Declare @W44 as Date 
Declare @W45 as Date 
Declare @W46 as Date 
Declare @W47 as Date 
Declare @W48 as Date 
Declare @W49 as Date 
Declare @W50 as Date 
Declare @W51 as Date 

SET @W = GETDATE() 
Set @W1 = DATEADD(DAY, -7, GETDATE()) 
set @W2 = DATEADD(DAY, -14, GETDATE()) 
set @W3 = DATEADD(DAY, -21, GETDATE()) 
set @W4 = DATEADD(DAY, -28, GETDATE()) 
set @W5 = DATEADD(DAY, -35, GETDATE())
set @W6 = DATEADD(DAY, -42, GETDATE()) 
set @W7 = DATEADD(DAY, -49, GETDATE()) 
set @W8 = DATEADD(DAY, -56, GETDATE()) 
set @W9 = DATEADD(DAY, -63, GETDATE()) 
set @W10 = DATEADD(DAY, -70, GETDATE()) 
SET @W11 = DATEADD(DAY, -77, GETDATE()) 
SET @W12 = DATEADD(DAY, -84, GETDATE()) 
SET @W13 = DATEADD(DAY, -91, GETDATE()) 
SET @W14 = DATEADD(DAY, -98, GETDATE()) 
SET @W15 = DATEADD(DAY, -105, GETDATE()) 
SET @W16 = DATEADD(DAY, -112, GETDATE()) 
SET @W17 = DATEADD(DAY, -119, GETDATE()) 
SET @W18 = DATEADD(DAY, -126, GETDATE()) 
SET @W19 = DATEADD(DAY, -133, GETDATE()) 
SET @W20 = DATEADD(DAY, -140, GETDATE()) 
SET @W21 = DATEADD(DAY, -147, GETDATE()) 
SET @W22 = DATEADD(DAY, -154, GETDATE()) 
SET @W23 = DATEADD(DAY, -161, GETDATE()) 
SET @W24 = DATEADD(DAY, -168, GETDATE()) 
SET @W25 = DATEADD(DAY, -175, GETDATE()) 
SET @W26 = DATEADD(DAY, -182, GETDATE()) 
SET @W27 = DATEADD(DAY, -189, GETDATE()) 
SET @W28 = DATEADD(DAY, -196, GETDATE()) 
SET @W29 = DATEADD(DAY, -203, GETDATE()) 
SET @W30 = DATEADD(DAY, -210, GETDATE()) 
SET @W31 = DATEADD(DAY, -217, GETDATE()) 
SET @W32 = DATEADD(DAY, -224, GETDATE()) 
SET @W33 = DATEADD(DAY, -231, GETDATE()) 
SET @W34 = DATEADD(DAY, -238, GETDATE()) 
SET @W35 = DATEADD(DAY, -245, GETDATE()) 
SET @W36 = DATEADD(DAY, -252, GETDATE()) 
SET @W37 = DATEADD(DAY, -259, GETDATE()) 
SET @W38 = DATEADD(DAY, -266, GETDATE()) 
SET @W39 = DATEADD(DAY, -273, GETDATE()) 
SET @W40 = DATEADD(DAY, -280, GETDATE()) 
SET @W41 = DATEADD(DAY, -287, GETDATE()) 
SET @W42 = DATEADD(DAY, -294, GETDATE()) 
SET @W43 = DATEADD(DAY, -301, GETDATE()) 
SET @W44 = DATEADD(DAY, -308, GETDATE()) 
SET @W45 = DATEADD(DAY, -315, GETDATE()) 
SET @W46 = DATEADD(DAY, -322, GETDATE()) 
SET @W47 = DATEADD(DAY, -329, GETDATE()) 
SET @W48 = DATEADD(DAY, -336, GETDATE()) 
SET @W49 = DATEADD(DAY, -343, GETDATE()) 
SET @W50 = DATEADD(DAY, -350, GETDATE()) 
SET @W51 = DATEADD(DAY, -357, GETDATE()) 



--INSERT INTO #dy 
--VALUES (@W, 1), (@W1, 2), (@W2, 3), (@W3, 4),(@W4, 5), (@W5, 6), (@W6, 7), (@W7, 8), (@W8, 9), (@W9, 10), (@W10, 11), 
--(@W11, 12), (@W12, 13),(@W13, 14),(@W14, 15), (@W15, 16), (@W16, 17), (@W17, 18), (@W18, 19), (@W19, 20), (@W20, 21), 
--(@W21, 22), (@W22, 23),(@W23, 24),(@W24, 25), (@W25, 26), (@W26, 27), (@W27, 28), (@W28, 29), (@W29, 30), (@W30, 31), 
--(@W31, 32), (@W32, 33),(@W33, 34),(@W34, 35), (@W35, 36), (@W36, 37), (@W37, 38), (@W38, 39), (@W39, 40), (@W40, 41), 
--(@W41, 42), (@W42, 43),(@W43, 44),(@W44, 45), (@W45, 46), (@W46, 47), (@W47, 48), (@W48, 49), (@W49, 50), (@W50, 51), (@W51, 52)

--select *
--from #dy

--censored returns on a stock 
 ;WITH rawE as (SELECT distinct stkh.[Symbol]
      ,stkh.TradeDate 
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
T1 as (select d.tradedate, x.Symbol, log(ROUND(EXP(SUM(LOG(a.trueChange))),10)) as WeeklyReturns 
from #dy d 
left join rawE x on x.TradeDate = d.tradedate 
left join rawE a on a.Symbol = x.Symbol and a.TradeDate between DATEADD(day, -6, x.TradeDate) and x.TradeDate 
  where x.symbol = a. symbol 
   and a.TradeDate not in (select t.tradeDate from rawE t where t.Symbol = a.Symbol and t.Confirmed = '1') --day of 
   and a.TradeDate not in (select DATEADD(day, 1, g.tradedate) from rawE g where g.Confirmed = '1' and g.Symbol = a.Symbol)         --day after 
	and ((a.TradeDate NOT IN (select DATEADD(day, 2, s3.tradedate) from rawE s3 where s3.Symbol = a.Symbol and s3.Confirmed = '1' and s3.DateType = 'AMC')) and
		(a.TradeDate NOT IN (select DATEADD(day, -1, s4.tradedate) from rawE s4 where s4.Symbol = a.Symbol and s4.Confirmed = '1' and s4.DateType = 'BMO'))) and 
		a.trueChange > 0 and d.wknum <= 5 --gets past week's returns as Week 1, then two weeks ago as Week 3, etc. Naming convention error  
group by d.tradedate, x.Symbol),
T4 as (select d.tradedate, x.Symbol, log(ROUND(EXP(SUM(LOG(a.trueChange))),10)) as WeeklyReturns
from #dy d 
left join rawE x on x.TradeDate = d.tradedate 
left join rawE a on a.Symbol = x.Symbol and a.TradeDate between DATEADD(day, -6, x.TradeDate) and x.TradeDate 
  where x.symbol = a. symbol 
   and a.TradeDate not in (select t.tradeDate from rawE t where t.Symbol = a.Symbol and t.Confirmed = '1') --day of 
   and a.TradeDate not in (select DATEADD(day, 1, g.tradedate) from rawE g where g.Confirmed = '1' and g.Symbol = a.Symbol)         --day after 
	and ((a.TradeDate NOT IN (select DATEADD(day, 2, s3.tradedate) from rawE s3 where s3.Symbol = a.Symbol and s3.Confirmed = '1' and s3.DateType = 'AMC')) and
		(a.TradeDate NOT IN (select DATEADD(day, -1, s4.tradedate) from rawE s4 where s4.Symbol = a.Symbol and s4.Confirmed = '1' and s4.DateType = 'BMO'))) and 
		a.trueChange > 0 and d.wknum < 10 --gets past week's returns as Week 1, then two weeks ago as Week 3, etc. Naming convention error  
group by d.tradedate, x.Symbol),
T6 as (select d.tradedate, x.Symbol, log(ROUND(EXP(SUM(LOG(a.trueChange))),10)) as WeeklyReturns
from #dy d 
left join rawE x on x.TradeDate = d.tradedate 
left join rawE a on a.Symbol = x.Symbol and a.TradeDate between DATEADD(day, -6, x.TradeDate) and x.TradeDate 
  where x.symbol = a. symbol 
   and a.TradeDate not in (select t.tradeDate from rawE t where t.Symbol = a.Symbol and t.Confirmed = '1') --day of 
   and a.TradeDate not in (select DATEADD(day, 1, g.tradedate) from rawE g where g.Confirmed = '1' and g.Symbol = a.Symbol)         --day after 
	and ((a.TradeDate NOT IN (select DATEADD(day, 2, s3.tradedate) from rawE s3 where s3.Symbol = a.Symbol and s3.Confirmed = '1' and s3.DateType = 'AMC')) and
		(a.TradeDate NOT IN (select DATEADD(day, -1, s4.tradedate) from rawE s4 where s4.Symbol = a.Symbol and s4.Confirmed = '1' and s4.DateType = 'BMO'))) and 
		a.trueChange > 0 and d.wknum < 14 --gets past week's returns as Week 1, then two weeks ago as Week 3, etc. Naming convention error  
group by d.tradedate, x.Symbol),
T125 as (select d.tradedate, x.Symbol, log(ROUND(EXP(SUM(LOG(a.trueChange))),10)) as WeeklyReturns
from #dy d 
left join rawE x on x.TradeDate = d.tradedate 
left join rawE a on a.Symbol = x.Symbol and a.TradeDate between DATEADD(day, -6, x.TradeDate) and x.TradeDate 
  where x.symbol = a. symbol 
   and a.TradeDate not in (select t.tradeDate from rawE t where t.Symbol = a.Symbol and t.Confirmed = '1') --day of 
   and a.TradeDate not in (select DATEADD(day, 1, g.tradedate) from rawE g where g.Confirmed = '1' and g.Symbol = a.Symbol)         --day after 
	and ((a.TradeDate NOT IN (select DATEADD(day, 2, s3.tradedate) from rawE s3 where s3.Symbol = a.Symbol and s3.Confirmed = '1' and s3.DateType = 'AMC')) and
		(a.TradeDate NOT IN (select DATEADD(day, -1, s4.tradedate) from rawE s4 where s4.Symbol = a.Symbol and s4.Confirmed = '1' and s4.DateType = 'BMO'))) and 
		a.trueChange > 0 and d.wknum < 27 --gets past week's returns as Week 1, then two weeks ago as Week 3, etc. Naming convention error  
group by d.tradedate, x.Symbol),
T250 as (select d.tradedate, x.Symbol, log(ROUND(EXP(SUM(LOG(a.trueChange))),10)) as WeeklyReturns
from #dy d 
left join rawE x on x.TradeDate = d.tradedate 
left join rawE a on a.Symbol = x.Symbol and a.TradeDate between DATEADD(day, -6, x.TradeDate) and x.TradeDate 
  where x.symbol = a. symbol 
   and a.TradeDate not in (select t.tradeDate from rawE t where t.Symbol = a.Symbol and t.Confirmed = '1') --day of 
   and a.TradeDate not in (select DATEADD(day, 1, g.tradedate) from rawE g where g.Confirmed = '1' and g.Symbol = a.Symbol)         --day after 
	and ((a.TradeDate NOT IN (select DATEADD(day, 2, s3.tradedate) from rawE s3 where s3.Symbol = a.Symbol and s3.Confirmed = '1' and s3.DateType = 'AMC')) and
		(a.TradeDate NOT IN (select DATEADD(day, -1, s4.tradedate) from rawE s4 where s4.Symbol = a.Symbol and s4.Confirmed = '1' and s4.DateType = 'BMO'))) and 
		a.trueChange > 0 and d.wknum <= 52 --gets past week's returns as Week 1, then two weeks ago as Week 3, etc. Naming convention error  
group by d.tradedate, x.Symbol),

TX as (select symbol, 100 * sqrt(52) * STDEV(WeeklyReturns) as RV
from T1
group by symbol),
TF as (select symbol, 100 * sqrt(52) * STDEV(WeeklyReturns) as RV
from T4
group by symbol),
TS as (select symbol, 100 * sqrt(52) * STDEV(WeeklyReturns) as RV
from T6
group by symbol),
TOH as (select symbol, 100 * sqrt(52) * STDEV(WeeklyReturns) as RV
from T125
group by symbol),
TY as (select symbol, 100 * sqrt(52) * STDEV(WeeklyReturns) as RV
from T250
group by symbol)
select a.symbol, round(a.RV,2) as WK20, round(b.RV,2) as WK40, round(c.RV,2) as WK60, round(d.RV,2) as WK125, round(e.RV, 2) as WK250
from TX a
left join TF b on a.symbol = b.symbol 
left join TS c on a.symbol = c.symbol 
left join TOH d on a.symbol = d.symbol 
left join TY e on a.symbol = e.symbol 
order by symbol
