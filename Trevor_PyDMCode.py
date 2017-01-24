import pandas as pd
from sqlalchemy import create_engine
from pycake import DataMap, SQLManager
 
def connect_to_sql(host, usr, pwd, db):
    conn_str = 'mssql+pymssql://%s:%s@%s/%s' % (usr, pwd, host, db)
    engine = create_engine(conn_str, pool_size=5, pool_recycle=10)
    return engine

## Login Credentials
host = 'PVWCHI6PSQL1'
db = 'StockGroup' 

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
Declare @Today as Date 
Declare @ENDDATE As Date

Set @Today = '2016-05-01'
Set @ENDDATE = '2016-06-01'

SELECT CE.[Symbol] as StockSymbol
	  --,T2.ClosePrice as Price
      --,[SectorName]
      ,CE.category 
      ,CE.fair_move
      ,CE.event_date
      ,Round([RawBeta], 2) As Beta
      ,ReturnTable3.StockReturns as Return_Prev
      ,ReturnTable.StockReturns as Return_0
      ,ReturnTable2.StockReturns as Return_1
FROM [companies].[dbo].[tblCatalystEvents] CE
  LEFT JOIN [companies].[dbo].[tblBetaCurrent_staging] BC ON CE.symbol = BC.Symbol 
  left Join (SELECT S.Ticker as Company
				,DATE as dt
				,[ClosePrice]
			 FROM [IvyDB].[dbo].[SECURITY_PRICE] SP
				LEFT Join [IvyDB].[dbo].[SECURITY] S on 
					S.SecurityID = SP.SecurityID) 
			T2 on (T2.Company = BC.Symbol and T2.dt = CE.event_date)
  LEFT JOIN (Select S2.Ticker
				--T2 is the first occuring date 
				,T2.Date as EventsDay 
				,T1.Date as PriorDay 
				,t2.ClosePrice as SecondPrice 
				,t1.ClosePrice as FirstPrice 
				,round((100 * ((t2.ClosePrice/t1.ClosePrice) - 1)), 2) as [StockReturns]
			From [IvyDb].[dbo].SECURITY_PRICE t1 
				LEFT JOIN [ivydb].[dbo].SECURITY_PRICE t2 on t2.SecurityID = t1.SecurityID and (t1.Date = DATEADD(day,-1,t2.date))
				LEFT JOIN [IvyDB].[dbo].[SECURITY] S2 on S2.SecurityID = T2.SecurityID
			Where T2.ClosePrice IS NOT NULL) 
			ReturnTable on (ReturnTable.Ticker = CE.Symbol and ReturnTable.EventsDay = CE.event_date)
  LEFT JOIN (Select S3.Ticker,
				--T2 is the first occuring date 
				T4.Date as NextDay
				,T3.Date as EventsD 
				,round((100 * ((t4.ClosePrice/t3.ClosePrice) - 1)), 2) as [StockReturns]
			From [IvyDb].[dbo].SECURITY_PRICE t3
				LEFT JOIN [ivydb].[dbo].SECURITY_PRICE t4 on t4.SecurityID = t3.SecurityID and (t4.Date = DATEADD(day,1,t3.date))
				LEFT JOIN [IvyDB].[dbo].[SECURITY] S3 on S3.SecurityID = T4.SecurityID
			Where T4.ClosePrice IS NOT NULL) 
			ReturnTable2 on (ReturnTable2.Ticker = CE.Symbol and ReturnTable2.EventsD = CE.event_date)
	--Get previous day's return 
  LEFT JOIN (Select Stock.Ticker
				--T2 is the first occuring date 
				,T6.Date as WhichDay -- 2 days after backdate, the more current one 
				,T5.Date as WhichDayIsThis --backDate 
				,prevD.Date as RightMiddleDate 
				,round((100 * ((prevD.ClosePrice/t5.ClosePrice) - 1)), 2) as [StockReturns]
			From [IvyDb].[dbo].SECURITY_PRICE T6
				LEFT JOIN [ivydb].[dbo].SECURITY_PRICE t5 on t6.SecurityID = t5.SecurityID and (t5.Date = DATEADD(day,-2,t6.date))
				LEFT Join [ivydb].[dbo].SECURITY_PRICE PrevD on t6.SecurityID = PrevD.SecurityID and (prevD.Date = DATEADD(day,-1,t6.date))
				LEFT JOIN [IvyDB].[dbo].[SECURITY] Stock on Stock.SecurityID = T6.SecurityID
			Where prevD.ClosePrice IS NOT NULL) 
			ReturnTable3 on (ReturnTable3.Ticker = CE.Symbol and ReturnTable3.WhichDay = CE.event_date)		
Where (event_date BETWEEN @Today AND @ENDDATE) 
   and (ReturnTable2.StockReturns IS not null) 
Order By CE.symbol
 """
 
df = pd.read_sql(query, engine)
# Increment index by 1
df.index += 1
# Need this?
#df.set_index('Account', inplace=True)
#df.dtypes 

## write data
map_name = 'DM_GJI_FairMoveReturns_May16'
data_map = DataMap(map_name, True, True)
data_map.notify(data=df)
data_map.close()