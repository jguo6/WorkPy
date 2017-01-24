import pandas as pd
from sqlalchemy import create_engine
from pycake import DataMap, SQLManager
 
def connect_to_sql(host, usr, pwd, db):
    conn_str = 'mssql+pymssql://%s:%s@%s/%s' % (usr, pwd, host, db)
    engine = create_engine(conn_str, pool_size=5, pool_recycle=10)
    return engine

## Login Credentials
host = 'PVWCHI6PSQL1'
db = 'igtdev' 

# Service Account Auth
#user = 'sparky1'
#pwd = 'Sp@rk_users'

# Windows Auth
user = ''
pwd  = ''
## connect to sql
engine = connect_to_sql(host, user, pwd, db) 
 
## get data
query = """
 WITH rawE as (SELECT distinct stkh.[Symbol]
      ,stkh.[TradeDate]
      ,stkh.[close]
	  ,(100 - stkh.PercentChange) as PercentChange 
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
  where TradeDate > '2015-01-01'),

One as (select x.Symbol,(CASE WHEN (x.TradeDate = '2016-11-25') then DATEADD(day, -2, x.TradeDate) 
	WHEN (DATEADD(day, -1, x.TradeDate) NOT IN (select TradeDate from rawE) and x.TradeDate != '2016-11-25') THEN DATEADD(day, -3, x.TradeDate) 
	else DATEADD(day, -1, x.TradeDate) END) as tradeDate, 
	100 * sqrt((252/19) * SUM(power(log(a.trueChange), 2))) as RV
  from rawE x
  left join rawE a on a.symbol = x.Symbol and a.TradeDate between DATEADD(day, -30, x.TradeDate) and DATEADD(day, 1, x.TradeDate)
  where x.symbol = a. symbol 
   and a.TradeDate not in (select t.tradeDate from rawE t where t.Symbol = a.Symbol and t.Confirmed = '1') --day of 
   and a.TradeDate not in (select DATEADD(day, 1, g.tradedate) from rawE g where g.Confirmed = '1' and g.Symbol = a.Symbol)         --day after 
	and ((a.TradeDate NOT IN (select DATEADD(day, 2, s3.tradedate) from rawE s3 where s3.Symbol = a.Symbol and s3.Confirmed = '1' and s3.DateType = 'AMC')) and
		(a.TradeDate NOT IN (select DATEADD(day, -1, s4.tradedate) from rawE s4 where s4.Symbol = a.Symbol and s4.Confirmed = '1' and s4.DateType = 'BMO'))) 
 group by x.Symbol, x.TradeDate),
 
Four as (select x1.Symbol,(CASE WHEN (x1.TradeDate = '2016-11-25') then DATEADD(day, -2, x1.TradeDate) 
	WHEN (DATEADD(day, -1, x1.TradeDate) NOT IN (select TradeDate from rawE) and x1.TradeDate != '2016-11-25') 
	THEN DATEADD(day, -3, x1.TradeDate) else DATEADD(day, -1, x1.TradeDate) END) as tradeDate, 
	100 * sqrt((252/39) * SUM(power(log(a1.trueChange), 2))) as RV
  from rawE x1
  left join rawE a1 on a1.symbol = x1.Symbol and a1.TradeDate between DATEADD(day, -60, x1.TradeDate) and DATEADD(day, 1, x1.TradeDate)
  where x1.symbol = a1. symbol 
   and a1.TradeDate not in (select t.tradeDate from rawE t where t.Symbol = a1.Symbol and t.Confirmed = '1') --day of 
   and a1.TradeDate not in (select DATEADD(day, 1, g.tradedate) from rawE g where g.Confirmed = '1' and g.Symbol = a1.Symbol)         --day after 
	and ((a1.TradeDate NOT IN (select DATEADD(day, 2, s3.tradedate) from rawE s3 where s3.Symbol = a1.Symbol and s3.Confirmed = '1' and s3.DateType = 'AMC')) and
		(a1.TradeDate NOT IN (select DATEADD(day, -1, s4.tradedate) from rawE s4 where s4.Symbol = a1.Symbol and s4.Confirmed = '1' and s4.DateType = 'BMO'))) 
 group by x1.Symbol, x1.TradeDate),
 
Six as (select x2.Symbol,(CASE WHEN (x2.TradeDate = '2016-11-25') then DATEADD(day, -2, x2.TradeDate) 
	WHEN (DATEADD(day, -1, x2.TradeDate) NOT IN (select TradeDate from rawE) and x2.TradeDate != '2016-11-25') 
	THEN DATEADD(day, -3, x2.TradeDate) else DATEADD(day, -1, x2.TradeDate) END) as tradeDate, 
	100 * sqrt((252/59) * SUM(power(log(a2.trueChange), 2))) as RV
  from rawE x2
  left join rawE a2 on a2.symbol = x2.Symbol and a2.TradeDate between DATEADD(day, -90, x2.TradeDate) and DATEADD(day, 1, x2.TradeDate)
  where x2.symbol = a2. symbol 
   and a2.TradeDate not in (select t.tradeDate from rawE t where t.Symbol = a2.Symbol and t.Confirmed = '1') --day of 
   and a2.TradeDate not in (select DATEADD(day, 1, g.tradedate) from rawE g where g.Confirmed = '1' and g.Symbol = a2.Symbol)         --day after 
	and ((a2.TradeDate NOT IN (select DATEADD(day, 2, s3.tradedate) from rawE s3 where s3.Symbol = a2.Symbol and s3.Confirmed = '1' and s3.DateType = 'AMC')) and
		(a2.TradeDate NOT IN (select DATEADD(day, -1, s4.tradedate) from rawE s4 where s4.Symbol = a2.Symbol and s4.Confirmed = '1' and s4.DateType = 'BMO'))) 
 group by x2.Symbol, x2.TradeDate),
 
Oneh as (select x.Symbol,(CASE WHEN (x.TradeDate = '2016-11-25') then DATEADD(day, -2, x.TradeDate) 
	WHEN (DATEADD(day, -1, x.TradeDate) NOT IN (select TradeDate from rawE) and x.TradeDate != '2016-11-25') 
	THEN DATEADD(day, -3, x.TradeDate) else DATEADD(day, -1, x.TradeDate) END) as tradeDate, 
	100 * sqrt((252/124) * SUM(power(log(a.trueChange), 2))) as RV
  from rawE x
  left join rawE a on a.symbol = x.Symbol and a.TradeDate between DATEADD(day, -180, x.TradeDate) and DATEADD(day, 1, x.TradeDate)
  where x.symbol = a. symbol 
   and a.TradeDate not in (select t.tradeDate from rawE t where t.Symbol = a.Symbol and t.Confirmed = '1') --day of 
   and a.TradeDate not in (select DATEADD(day, 1, g.tradedate) from rawE g where g.Confirmed = '1' and g.Symbol = a.Symbol)         --day after 
	and ((a.TradeDate NOT IN (select DATEADD(day, 2, s3.tradedate) from rawE s3 where s3.Symbol = a.Symbol and s3.Confirmed = '1' and s3.DateType = 'AMC')) and
		(a.TradeDate NOT IN (select DATEADD(day, -1, s4.tradedate) from rawE s4 where s4.Symbol = a.Symbol and s4.Confirmed = '1' and s4.DateType = 'BMO'))) 
 group by x.Symbol, x.TradeDate),
 
YearV as (select x.Symbol,(CASE WHEN (x.TradeDate = '2016-11-25') then DATEADD(day, -2, x.TradeDate) 
	WHEN (DATEADD(day, -1, x.TradeDate) NOT IN (select TradeDate from rawE) and x.TradeDate != '2016-11-25') 
	THEN DATEADD(day, -3, x.TradeDate) else DATEADD(day, -1, x.TradeDate) END) as tradeDate, 
	100 * sqrt((252/250) * SUM(power(log(a.trueChange), 2))) as RV
  from rawE x
  left join rawE a on a.symbol = x.Symbol and a.TradeDate between DATEADD(day, -360, x.TradeDate) and DATEADD(day, 1, x.TradeDate)
  where x.symbol = a. symbol 
   and a.TradeDate not in (select t.tradeDate from rawE t where t.Symbol = a.Symbol and t.Confirmed = '1') --day of 
   and a.TradeDate not in (select DATEADD(day, 1, g.tradedate) from rawE g where g.Confirmed = '1' and g.Symbol = a.Symbol)         --day after 
	and ((a.TradeDate NOT IN (select DATEADD(day, 2, s3.tradedate) from rawE s3 where s3.Symbol = a.Symbol and s3.Confirmed = '1' and s3.DateType = 'AMC')) and
		(a.TradeDate NOT IN (select DATEADD(day, -1, s4.tradedate) from rawE s4 where s4.Symbol = a.Symbol and s4.Confirmed = '1' and s4.DateType = 'BMO'))) 
 group by x.Symbol, x.TradeDate
)


select  one.tradeDate, one.Symbol, one.RV as RV20, four.RV as RV40, six.RV as RV60, oneh.RV as RV125, yearv.RV as RV250
from One
left join Four on four.Symbol = one.Symbol and four.tradeDate = one.tradeDate
left join Six on six.Symbol = one.Symbol and six.tradeDate = one.tradeDate
left join Oneh on oneh.Symbol = one.Symbol and oneh.tradedate = one.tradedate
left join YearV on yearv.Symbol = one.Symbol and yearv.tradeDate = one.tradeDate
 """
 
df = pd.read_sql(query, engine)
# Increment index by 1
df.index += 1
# Need this?
#df.set_index('Account', inplace=True)
#df.dtypes 

## write data
map_name = 'DM_JG_WeeklyRV'
data_map = DataMap(map_name, True, True)
data_map.notify(data=df)
data_map.close()