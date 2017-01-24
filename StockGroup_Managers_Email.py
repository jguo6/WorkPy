from sqlalchemy import create_engine, types
import datetime as dt
import pandas as pd
from mailer import Message, Mailer
import smtplib
from email.MIMEBase import MIMEBase
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from email.MIMEImage import MIMEImage
from email import Encoders
import numpy as np
import matplotlib as mpl
#matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.dates import MonthLocator, WeekdayLocator, DateFormatter
import seaborn as sns

def connect_to_sql(host, usr, pwd, db):
    conn_str = 'mssql+pymssql://%s:%s@%s/%s' % (usr, pwd, host, db)
    engine = create_engine(conn_str, pool_size=5, pool_recycle=10)
    return engine

## Login Credentials
sql1_host = 'PVWCHI6PSQL1'

# SQL1
#sql1_user = 'TSPython'
#sql1_pwd = 'W!nt3r2015'


sql1_user = 'sparky1'
sql1_pwd = 'Sp@rk_users'

db = 'StockGroup' # Using Risk_TS so that we can simplify the to_sql stage

## connect to sql
engine = connect_to_sql(sql1_host, sql1_user, sql1_pwd, db) 

#==============================================================================
# Define Querries
#==============================================================================

DeltaC_q="""
with pnl as (
Select 'Crossing Account' as Account, dc.CrossingActDeltaC as DeltaC, hc.CrossingActDayMM as DayMM, hc.CrossingActHaircut as Haircut
  FROM [StockGroup].[dbo].[tblSG_DeltaCMetrics] dc
  left join [StockGroup].[dbo].[tblSG_HaircutMetrics] hc on dc.RecordDate=hc.RecordDate
  where dc.RecordDate=convert(date, getdate()) 
  UNION
  Select 'Gamma Account' as Account, dc.GammaActDeltaC as DeltaC, hc.GammaActDayMM as DayMM, hc.GammaActHaircut as Haircut
  FROM [StockGroup].[dbo].[tblSG_DeltaCMetrics] dc
  left join [StockGroup].[dbo].[tblSG_HaircutMetrics] hc on dc.RecordDate=hc.RecordDate
  where dc.RecordDate=convert(date, getdate())
  UNION
  Select 'Vol Arb Hedgers' as Account, dc.VAHDeltaC as DeltaC, null as DayMM, null as Haircut
  FROM [StockGroup].[dbo].[tblSG_DeltaCMetrics] dc
  left join [StockGroup].[dbo].[tblSG_HaircutMetrics] hc on dc.RecordDate=hc.RecordDate
  where dc.RecordDate=convert(date,getdate()) 
  )
  Select 
  Account
  ,FORMAT(DeltaC, '#,###') as DeltaC
  ,FORMAT(DayMM, '#,###') as DayMM
  ,FORMAT(Haircut,'#,###') as Haircut
  from pnl
"""
 
#DeltaCHist
DeltaCHist_q = """
SELECT convert(date,RecordDate) as 'Date'
,'VolArbHedgers' as Account
,VAHDeltaC as 'DeltaC'
FROM [StockGroup].[dbo].[tblSG_DeltaCMetrics]
where RecordDate between CONVERT(date,getdate()-20) and CONVERT(date,getdate())
UNION
SELECT convert(date,RecordDate) as 'Date'
,'CrossingAccount' as Account
,CrossingActDeltaC as 'DeltaC'
FROM [StockGroup].[dbo].[tblSG_DeltaCMetrics]
where RecordDate between CONVERT(date,getdate()-20) and CONVERT(date,getdate())
"""

# DeltaCSectors
DeltaCSectors_q = """
SELECT Entity, FORMAT(DeltaC, '#,###') 'DeltaC', SectorName, RecordDate 'Date'
FROM [StockGroup].[dbo].[tblSG_DeltaCSectorMetrics]
WHERE RecordDate = CONVERT(date,getdate())
"""

# EfficiencyMetrics
#EfficiencyMetrics_q = """
#SELECT 
#CONVERT(VARCHAR(4),ROUND(SharesTradedOut*100,0)) + '%' AS 'Shares Traded Out',
#CONVERT(VARCHAR(4),ROUND(NotionalTradedOut*100,0)) + '%' AS 'Notional Traded Out',
#FORMAT(TotalInternalShares, '#,###') 'Total Internal Shares'
#FROM [StockGroup].[dbo].[tblSG_EfficiencyMetrics]
#WHERE RecordDate = CONVERT(date,getdate())
#"""



# PET Metrics
PETMetrics_q = """
SELECT 
	--FORMAT(orderedqty, '#,###') 'Order Qty'
	 FORMAT(totalfillqty, '#,###') 'Fill Qty'
  	, FORMAT(notional, '#,###') 'Fill Notional'
   	, CONVERT(VARCHAR(6),ROUND(bps_slippage, 2)) + ' bps' AS 'Arrival Slip BPS'
	, FORMAT(dollarslippage, '#,###') 'Arrival Slip $'
 	--, CONVERT(VARCHAR(6),ROUND(nbbo_bps_slippage, 2)) + ' bps' AS 'NBBO Slip BPS'
	--, FORMAT(nbbodollarslippage, '#,###') 'NBBO Slip $'
 	--, CONVERT(VARCHAR(6),ROUND(close_bps_slippage, 2)) + ' bps' AS 'Close Slip BPS'
	--, FORMAT(closedollarslippage, '#,###') 'Close Slip $'
	--,FORMAT(ROUND(transcost,0), '#,###') as 'Estimated GS Cost'
	--, RecordDate 'Date'
FROM [StockGroup].[dbo].[tblSG_PETMetrics]
WHERE RecordDate = CONVERT(date,getdate())
"""

# PET Hist Metrics
PET_HistMetrics_q = """
SELECT 
 	 ROUND(bps_slippage, 2) AS 'PET Realized BPS'
    ,ROUND(bm.slippagebps,2) as 'Trader Basket Impact BPS'
    ,ROUND(pm.goldmanbps * -1,2) as 'PET Estimated BPS'
	,convert(date,pm.RecordDate) as  'Date'
FROM [StockGroup].[dbo].[tblSG_PETMetrics] pm
LEFT JOIN [StockGroup].[dbo].[tblSG_BasketMetrics] bm on pm.RecordDate=bm.RecordDate
WHERE pm.RecordDate between CONVERT(date,getdate()-20) and CONVERT(date,getdate())
"""

#PET_HistMetrics_q = """
#SELECT 
#     convert(date,pm.RecordDate) as  'Date'
#     ,'PET Arrival Slip BPS' as 'Type'
# 	 ,ROUND(bps_slippage, 2) AS 'Slippage'
#FROM [StockGroup].[dbo].[tblSG_PETMetrics] pm
#WHERE pm.RecordDate between CONVERT(date,getdate()-20) and CONVERT(date,getdate())
#UNION
#SELECT 
#     convert(date,pm.RecordDate) as  'Date'
#     ,'Goldman Estimated BPS' as 'Type'
# 	 ,ROUND(goldmanbps * -1, 2) AS 'Slippage'
#FROM [StockGroup].[dbo].[tblSG_PETMetrics] pm
#WHERE pm.RecordDate between CONVERT(date,getdate()-20) and CONVERT(date,getdate())
#UNION
#SELECT 
#     convert(date,RecordDate) as  'Date'
#     ,'Trader Impact BPS' as 'Type'
# 	 ,ROUND(slippagebps, 2) AS 'Slippage'
#FROM [StockGroup].[dbo].[tblSG_BasketMetrics]
#WHERE RecordDate between CONVERT(date,getdate()-20) and CONVERT(date,getdate())
#"""
 
 # TraderBasketMetrics
#TraderBasketMetrics_q = """
#SELECT 
#      FORMAT(qty, '#,###') 'Quantity'
#	,FORMAT(notional, '#,###') 'Notional' 
#	, FORMAT(slippage, '#,###') 'Impact $'
#	, CONVERT(VARCHAR(5),ROUND(slippagebps,2)) + ' bps' AS 'Impact BPS'
#	--, RecordDate 'Date'
#FROM [StockGroup].[dbo].[tblSG_BasketMetrics]
#WHERE RecordDate = CONVERT(date,getdate()) """
 

# Strategy Metrics
StrategyMetrics_q = """
DECLARE @tot_fq FLOAT
SET @tot_fq = (SELECT SUM(totalfillqty)  FROM [StockGroup].[dbo].[tblSG_TradeVisionMetrics] where RecordDate = CONVERT(Date, getdate()) and account NOT IN ('F99','F97','F91','F96') and session='DAY');

WITH
tvm as (
 SELECT *
  FROM [StockGroup].[dbo].[tblSG_TradeVisionMetrics]
 where RecordDate = CONVERT(Date, getdate())
 --and aggressiveness IN ('THROUGH','CROSS','MID')
 and session='DAY'
and account NOT IN ('F99','F97','F91','F96')
)
SELECT (CASE WHEN ordersource='Phoenix' then 'PHOENIX' else ordersource end) as 'Order Source'
, convert(VARCHAR(6),round(SUM(totalfillqty)/@tot_fq * 100,0)) + '%' AS '% Total Fill'
, FORMAT(SUM(totalfillqty),'#,###') AS 'Total Fill Quantity'
--, convert(VARCHAR(6),ROUND((SUM(totalfillslippage)/SUM(totalfillqty * totalavgpx))*10000,2)) + ' bps' AS 'Slippage BPS'
FROM tvm
GROUP BY ordersource
 """

# Destination Metrics
#DestinationMetrics_q = """
#SELECT 
#	stock_tactic 'Tactic'
#	, FORMAT(SUM(totalfillqty), '#,###') 'Fill Quantity'
#	, CONVERT(VARCHAR(10),ROUND(SUM(CONVERT(FLOAT,totalfillqty))/SUM(CONVERT(FLOAT,quantity)) *100,0)) + '%' AS 'Fill Rate %'
#	, CONVERT(VARCHAR(10),ROUND((SUM([totalfillslippage])/SUM(totalfillqty * [totalavgpx]))*10000,2)) + ' bps' AS 'Slippage BPS'
#	, FORMAT(SUM([marketqty]), '#,###') 'Market Fills'
#	, FORMAT(SUM(diegoqty), '#,###') 'Diego Fills'
#	, FORMAT(SUM([igoogiqty]), '#,###') 'IgoogI Fills'
#	, FORMAT(SUM(internalizedqty), '#,###') 'Internalized Fills'
#	, FORMAT(SUM(totalfillpnlclose), '#,###') 'PnL to Close $'
#FROM [StockGroup].[dbo].[tblSG_TradeVisionMetrics]
#WHERE RecordDate = CONVERT(Date,getdate())
#	AND account NOT IN ('F99','F97','F91') AND stock_tactic != 'BASKET'
#  and aggressiveness IN ('THROUGH','CROSS','MID')
#  and session='DAY'
#GROUP BY stock_tactic
#ORDER BY sum(totalfillqty) DESC
#"""

# Execution Metrics
ExecutionMetrics_q = """
DECLARE @tot_fq FLOAT
SET @tot_fq = (SELECT SUM(totalfillqty)  FROM [StockGroup].[dbo].[tblSG_TradeVisionMetrics] where RecordDate = CONVERT(Date, getdate()) and account NOT IN ('F99','F97','F91','F96') and stock_tactic!='BASKET');

WITH execution AS (
SELECT 'QUOTER' AS stock_tactic, SUM(diegoqty) 'Quantity', SUM(diegopnlclose) 'PnLClose' --, SUM(diegoslippage)/SUM(diegoqty * diegoavgpx) * 10000 'SlippageBPS', SUM(diegopriceimp)/SUM(diegoqty * diegoavgpx) * 10000 'PriceImp'
FROM [StockGroup].[dbo].[tblSG_TradeVisionMetrics]
WHERE RecordDate =CONVERT(Date, getdate())
AND account NOT IN ('F99','F97','F91','F96') AND diegoqty IS NOT NULL
and session='DAY'
--and aggressiveness IN ('THROUGH','CROSS','MID')

UNION

SELECT 'IGOOGI' AS stock_tactic, SUM(igoogiqty), SUM(igoogipnlclose) 
FROM [StockGroup].[dbo].[tblSG_TradeVisionMetrics]
WHERE RecordDate =CONVERT(Date, getdate())
AND account NOT IN ('F99','F97','F91','F96') AND igoogiqty IS NOT NULL
and session='DAY'
--and aggressiveness IN ('THROUGH','CROSS','MID')

UNION

SELECT 'INTERNALIZATION' AS stock_tactic, SUM(internalizedqty), SUM(internalizedpnlclose)
FROM [StockGroup].[dbo].[tblSG_TradeVisionMetrics]
WHERE RecordDate =CONVERT(Date, getdate())
AND account NOT IN ('F99','F97','F91','F96') AND internalizedqty IS NOT NULL
and session='DAY'
--and aggressiveness IN ('THROUGH','CROSS','MID')

UNION

SELECT 'MARKET' AS stock_tactic, SUM(marketqty), SUM(mktpnlclose)
FROM [StockGroup].[dbo].[tblSG_TradeVisionMetrics]
WHERE RecordDate =CONVERT(Date, getdate())
AND account NOT IN ('F99','F97','F91','F96') AND marketqty IS NOT NULL
and session='DAY'
--and aggressiveness IN ('THROUGH','CROSS','MID')
and session='DAY'

UNION

SELECT 'TRADER TO TRADER' AS stock_tactic, SUM(tradercrossqty), SUM(tradercrosspnlclose)
FROM [StockGroup].[dbo].[tblSG_TradeVisionMetrics]
WHERE RecordDate =CONVERT(Date, getdate())
AND account NOT IN ('F99','F97','F91','F96')


)
SELECT stock_tactic 'Tactic'
, FORMAT(Quantity, '#,###') AS 'Fill Quantity'
, convert(VARCHAR(6),round(SUM(Quantity)/@tot_fq * 100,0)) + '%' AS '% of Total Fill'
, FORMAT([PnLClose], '#,###') 'PnL to Close $'
FROM execution
group by stock_tactic, Quantity, PnLClose
order by Quantity DESC
"""


# VAR Metrics
VARMetrics_q = """
WITH var as (SELECT Metric 
,CASE WHEN Metric = 'Return@Risk' THEN CAST(ROUND(Value, 2) AS VARCHAR(4)) + '%' ELSE FORMAT(Value, '#,###') END AS 'Value'
, Date
FROM [StockGroup].[dbo].[tblSG_VAR]
WHERE Date=CONVERT(date,getdate())
UNION
SELECT 'Prev Day $VAR',
CASE WHEN Metric = 'Return@Risk' THEN CAST(ROUND(Value, 2) AS VARCHAR(4)) + '%' ELSE FORMAT(Value, '#,###') END AS 'Value'
,Date
FROM [StockGroup].[dbo].[tblSG_VAR]
WHERE Date=(SELECT MAX(Date) FROM [StockGroup].[dbo].[tblSG_VAR] WHERE Date < (SELECT MAX(Date) FROM [StockGroup].[dbo].[tblSG_VAR]))
AND Metric = '$VAR')

SELECT Metric
, Value
FROM var
ORDER BY Date DESC
"""

# Hist VAR
VARHist_q="""
SELECT 
 Value AS '$VAR'
, Date
FROM [StockGroup].[dbo].[tblSG_VAR]
WHERE Date between CONVERT(date,GETDATE()-20) and CONVERT(date,getdate()) and Metric='$VAR'
"""

# AutoHedge Destinations
#AutoHedge_q = """
#with pivot1 as (
#SELECT 
#	stock_tactic 'Tactic'
#	,autohedge as 'Type'
#	, SUM(totalfillqty) as 'Fill Quantity'
#	, cast(SUM(totalfillpnlclose) as int) as 'PnL to Close $'
#FROM [StockGroup].[dbo].[tblSG_TradeVisionMetrics]
#WHERE RecordDate = CONVERT(Date,getdate())
#	AND account NOT IN ('F99','F97','F91') AND stock_tactic != 'BASKET'
#GROUP BY stock_tactic, autohedge
#--ORDER BY sum(totalfillqty) DESC
#)
#
#select Tactic,Type,Metric, Value 
#from pivot1
#unpivot
#( 
#Value
#for Metric in ([Fill Quantity],[PnL to Close $])
#)u
#"""

# Autohedge Execution Types
AutoHedge_Executions_q = """
WITH execution AS (
SELECT 'QUOTER' AS stock_tactic,autohedge as 'Type',SUM(diegoqty) 'Quantity', SUM(diegopnlclose) 'PnLClose', SUM(diegoslippage)/SUM(diegoqty * diegoavgpx) * 10000 'SlippageBPS', SUM(diegopriceimp)/SUM(diegoqty * diegoavgpx) * 10000 'Price Imp'
FROM [StockGroup].[dbo].[tblSG_TradeVisionMetrics]
WHERE RecordDate =CONVERT(Date, getdate())
AND account NOT IN ('F99','F97','F91') AND diegoqty IS NOT NULL
group by autohedge

UNION

SELECT 'IGOOGI' AS stock_tactic, autohedge as 'Type',SUM(igoogiqty), SUM(igoogipnlclose), SUM(igoogislippage)/SUM(igoogiqty * igoogiavgpx) * 10000 , SUM(igoogipriceimp)/SUM(igoogiqty * igoogiavgpx) * 10000
FROM [StockGroup].[dbo].[tblSG_TradeVisionMetrics]
WHERE RecordDate =CONVERT(Date, getdate())
AND account NOT IN ('F99','F97','F91') AND igoogiqty IS NOT NULL
group by autohedge

UNION

SELECT 'INTERNALIZATION' AS stock_tactic,autohedge as 'Type', SUM(internalizedqty), SUM(internalizedpnlclose), SUM(internalizedslippage)/SUM(internalizedqty * internalizedavgpx) * 10000, SUM(internalizedpriceimp)/SUM(internalizedqty * internalizedavgpx) * 10000
FROM [StockGroup].[dbo].[tblSG_TradeVisionMetrics]
WHERE RecordDate =CONVERT(Date, getdate())
AND account NOT IN ('F99','F97','F91') AND internalizedqty IS NOT NULL
group by autohedge

UNION

SELECT 'MARKET' AS stock_tactic,autohedge as 'Type', SUM(marketqty), SUM(mktpnlclose), SUM(marketslippage)/SUM(marketqty * marketavgpx) * 10000, SUM(mktpriceimp)/SUM(marketqty * marketavgpx) * 10000
FROM [StockGroup].[dbo].[tblSG_TradeVisionMetrics]
WHERE RecordDate =CONVERT(Date, getdate())
AND account NOT IN ('F99','F97','F91') AND marketqty IS NOT NULL
group by autohedge

UNION

SELECT 'TRADER TO TRADER' AS stock_tactic,autohedge as 'Type', SUM(tradercrossqty), SUM(tradercrosspnlclose), SUM(tradercrossslippage)/SUM(tradercrossqty * tradercrossavgpx) * 10000, SUM(tradercrosspriceimp)/SUM(tradercrossqty * tradercrossavgpx) * 10000
FROM [StockGroup].[dbo].[tblSG_TradeVisionMetrics]
WHERE RecordDate =CONVERT(Date, getdate())
AND account NOT IN ('F99','F97','F91') AND tradercrossqty IS NOT NULL
group by autohedge

--UNION

--SELECT stock_tactic, SUM(quantity), SUM(totalfillpnlclose), SUM(totalfillslippage)/SUM(totalfillqty * totalavgpx) * 10000, SUM(totalfillpriceimp)/SUM(totalfillqty * totalavgpx) * 10000
--FROM [StockGroup].[dbo].[tblSG_TradeVisionMetrics]
--WHERE RecordDate =CONVERT(Date, getdate()) AND stock_tactic='BASKET'
--AND account NOT IN ('F99','F97','F91')
--GROUP BY stock_tactic
),

main as (
SELECT stock_tactic 'Tactic'
,Type
, Quantity AS 'Fill Quantity'
, CONVERT(VARCHAR(6),ROUND([SlippageBPS],2)) + ' bps' 'SlippageBPS'
--, CONVERT(VARCHAR(6),ROUND([Price Imp],2)) + ' bps' 'Price Imp BPS'
, cast([PnLClose] as int) 'PnL to Close $'
FROM execution
)


select Tactic,Type,Metric, Value 
from main
unpivot
( 
Value
for Metric in ([Fill Quantity],[PnL to Close $])
)u
"""

#==============================================================================
# Data From SQL
#==============================================================================
 
DeltaCHist_df = pd.read_sql(DeltaCHist_q, engine)
DeltaCSectors_df = pd.read_sql(DeltaCSectors_q, engine)
DeltaC_df = pd.read_sql(DeltaC_q, engine)
#EfficiencyMetrics_df = pd.read_sql(EfficiencyMetrics_q, engine)
PETMetrics_df = pd.read_sql(PETMetrics_q, engine)
PET_HistMetrics_df = pd.read_sql(PET_HistMetrics_q, engine)
StrategyMetrics_df = pd.read_sql(StrategyMetrics_q, engine)
#TraderBasketMetrics_df = pd.read_sql(TraderBasketMetrics_q, engine)
#DestinationMetrics_df = pd.read_sql(DestinationMetrics_q, engine)
ExecutionMetrics_df = pd.read_sql(ExecutionMetrics_q, engine)
VARMetrics_df = pd.read_sql(VARMetrics_q , engine)
VARHist_df = pd.read_sql(VARHist_q , engine)

#NextGenMetrics_df = pd.read_sql(NextGenMetrics_q, engine) 
#NextGenMetrics_df=NextGenMetrics_df[NextGenMetrics_df['Account'].isin(['Optimizer','CrossingAccount','GammaAccount','VolArbHedgers'])]
#NextGenMetrics_df=pd.pivot_table(NextGenMetrics_df,values='Value',index=['Metric'],columns=['Account','Detail'])
#NextGenMetrics_df=NextGenMetrics_df.sort(ascending=False) 

#AutoHedge_df = pd.read_sql(AutoHedge_q, engine)
#AutoHedge_df=pd.pivot_table(AutoHedge_df,values='Value',index=['Tactic'],columns=['Metric','Type'], fill_value=0)
#AutoHedge_df=AutoHedge_df.sort(ascending=False) 

AutoHedge_Executions_df = pd.read_sql(AutoHedge_Executions_q, engine)
AutoHedge_Executions_df=pd.pivot_table(AutoHedge_Executions_df,values='Value',index=['Tactic'],columns=['Metric','Type'], fill_value=0)
#AutoHedge_Executions_df.sort_values([('Fill Quantity', 'NAH')], ascending=False, inplace=True)

DeltaC_df= DeltaC_df.fillna('')



#==============================================================================
# Report Building
#==============================================================================

## Python Numeric Formatting
# Define Formatting Functions for NextGenMetrics
num_format = lambda x: '{:,}'.format(x)

def build_formatters(df, format):
    return {column:format 
    for (column, dtype) in df.dtypes.iteritems()
    if dtype in [np.dtype('int64'), np.dtype('float64')]}

#NextGenMetrics_formatters = build_formatters(NextGenMetrics_df, num_format)
#AutoHedge_formatters = build_formatters(AutoHedge_df, num_format)
AutoHedge_Executions_formatters = build_formatters(AutoHedge_Executions_df, num_format)

#Hist DeltaC bar chart
# DeltaCHist_df_formatters = build_formatters(DeltaCHist_df, num_format)
sns.set_style("whitegrid")
x, y, hue = 'Date', 'DeltaC', 'Account'  # just using this line for flexibility
sns.barplot(x=x, y=y, hue=hue, data=DeltaCHist_df,palette="bright")
locs, labels = plt.xticks()
plt.setp(labels, rotation=270)

def formatter(x, pos):
    return ',.0f'.format(x)

ax = plt.gca()
ax.yaxis.set_major_formatter(mpl.ticker.FuncFormatter(lambda x, p: format(int(x), ',')))

plt.xlabel(x)
plt.ylabel(y)
plt.tight_layout()
#plt.show()

#DeltaC_loc = r"U:\Python"
#DeltaC_png = 'DeltaC.png'
#DeltaC_png_path = DeltaC_loc + '\\' + DeltaC_png
#plt.savefig(DeltaC_png_path)
plt.savefig('DeltaC.png')
plt.close()


#Hist PET line chart
PET_HistMetrics_df.set_index('Date', inplace=True)
PET_HistMetrics_df.plot().invert_yaxis()

locs, labels = plt.xticks()
plt.setp(labels, rotation=270)  # rotation can be any angle -- I usually use 270 but it looks weird for a short date range
plt.xlabel(x)
plt.ylabel('BPS')
plt.ylim([0, np.floor(PET_HistMetrics_df.min().min())])
#plt.title('Slippage from Arrival')
plt.tight_layout()
#plt.show()

#plt.show()

#PET_loc = r"U:\Python"
#PET_png = 'PET.png'
#PET_png_path = PET_loc + '\\' + PET_png
#plt.savefig(PET_png_path)
plt.savefig('PET.png')
plt.close()


#Hist VAR bar chart
x, y= 'Date', '$VAR'  # just using this line for flexibility
palette = sns.color_palette("Blues", len(VARHist_df))  # this section sets up the palette
rank = VARHist_df['$VAR'].argsort().argsort()
sns.barplot(x=x, y=y, data=VARHist_df,palette=np.array(palette[::1])[rank])
locs, labels = plt.xticks()
plt.setp(labels, rotation=270)

ax = plt.gca()
ax.yaxis.set_major_formatter(mpl.ticker.FuncFormatter(lambda x, p: format(int(x), ',')))

plt.xlabel(x)
plt.ylabel(y)
plt.tight_layout()
#plt.show()

#VAR_loc = r"U:\Python"
#VAR_png = 'VAR.png'
#VAR_png_path = VAR_loc + '\\' + VAR_png
#plt.savefig(VAR_png_path)
plt.savefig('VAR.png')
plt.close()


#==============================================================================
# Send Email
#==============================================================================

body = str('''<html><head>
<style>
    table.dataframe{
      font-family: verdana,arial,sans-serif;
      font-size:11px;
      color:#333333;
      border-width: 1px;
      border-color: #666666;
      border-collapse: collapse;
    }
    table.dataframe thead th {
      border-width: 1px;
      padding: 14px;
      border-style: solid;
      border-color: #666666;
      background-color: #dedede;
      white-space: nowrap;
    }
    table.dataframe td {
      border-width: 1px;
      padding: 12px;
      border-style: solid;
      border-color: #666666;
      text-align: right;
    }
    table.dataframe td:first-child {
      text-align: center;
      font-weight: bold;
    }
    table.dataframe th:first-child {
      text-align: center;
      font-weight: bold;
    }
    td{
        text-align:left;
        vertical-align: top;
    }
    #last-table table {
        width: 700px;  
    }
    table.dataframe tr:nth-child(odd)   { background-color:#eee; }
    table.dataframe tr:nth-child(even)    { background-color:#fff; }
</style>
</head><body>''' + \
      
                '<table border="0" cellpadding="0" cellspacing="20" width="75%">'
                        '<tr>'
                            '<td align="center" valign="top" style="font-size: 26px;font-weight: bold; padding: 20px; width: 75%">'
                               'Firm Stock Report     ' + dt.date.today().strftime("%m/%d/%Y") + 
                            '</td>'
                       ' </tr>'
               

              ' </table> '                    
              
              
              
              
               '<table border="0" cellpadding="0" cellspacing="20" style="width:75%;float:left">' 
               '<tbody>'                                                   
               
               '<tr>' 
                   '<td align="center">' +  "<b>Vol Arb Strategy Breakdown</b>"  +'</td>'
                   '<td align="center">'+ "<b>Vol Arb Phoenix Execution Types</b>" + '</td>' 
               '</tr>'
               '<tr>' 
                   '<td>' + StrategyMetrics_df.to_html(index=False) +  '</td>'
                   '<td>' + ExecutionMetrics_df.to_html(index=False) +'</td>' 
               '</tr>'                
               
               
               '</tbody>'
               '</table>' 
               
                '<table border="0" cellpadding="0" cellspacing="20" style="width:75%;float:left">' 
               '<tbody>'                                                   
             
               
                   #'<td>'+ "<b>PET Metrics</b>" + '</td>' 
                '<td align="center">'+ "<b>Vol Arb Basket and Stock Group PET Trading</b>" + '</td>'                                
               '</tr>'
               '<tr>'                  
                   #'<td>' + PETMetrics_df.T.to_html(header=False) +   '</td>'
                   '<td>' + '<img width="600" src="cid:image2">' + '</td>'
               '</tr>'              
              
              
               '</tbody>'
               '</table>'                
               
               
               
               '<table border="0" cellpadding="0" cellspacing="20" style="width:75%;float:left">' 
               '<tbody>'   
               
               '<tr>' 
                   '<td align="center">'+ "<b>PNL Metrics</b>" + '</td>'   
                   '<td align="center">'+ "<b>Historical DeltaC</b>" + '</td>'                                    
               '</tr>'
                '<tr>'                  
                   '<td>' + DeltaC_df.to_html(index=False)  +'</td>' 
                      '<td>' + '<img width="500" src="cid:image1">' + '</td>'
               '</tr>'               
               
               '<tr>' 
                   '<td align="center">'+ "<b>Portfolio Risk Metrics</b>" + '</td>'   
                   '<td align="center">'+ "<b>Historical $VAR</b>" + '</td>'                         
               '</tr>'       
               '<tr>'                  
                   '<td>' + VARMetrics_df.to_html(index=False,header=False) +'</td>'
                   '<td>' + '<img width="500" src="cid:image3">' + '</td>'
               '</tr>' 

                              
               '<tr>' 
                   '<td align="center",colspan="2">'+ "<b>AutoHedge Execution Metrics</b>" + '</td>'                                      
               '</tr>'
               '<tr>'                  
                   '<td colspan="2" id="last-table">' + AutoHedge_Executions_df.to_html(formatters=AutoHedge_Executions_formatters) +'</td>'  
               '</tr>'  
                                        
               '</tbody>'
               '</table>' 
          
              '</body></html>')
              
   


sender = 'dsamhat@peak6.com'
recipients = ['swebb@peak6.com', 'jcowden@peak6.com', 'kgilboy@peak6.com','dsamhat@peak6.com']
#recipients = ['dsamhat@peak6.com']
msg = MIMEMultipart('related')
msg['From'] = sender
msg['To'] = ", ".join(recipients)
msg['Subject'] = 'Firm Stock Report'
msgAlternative = MIMEMultipart('alternative')
msg.attach(msgAlternative)

#fp = open(DeltaC_png_path, 'rb')
fp = open('DeltaC.png', 'rb')
msgImage = MIMEImage(fp.read())
fp.close()
msgImage.add_header('Content-ID', '<image1>')
msg.attach(msgImage)

#fp1 = open(PET_png_path, 'rb')
fp1 = open('PET.png', 'rb')
msgImage1 = MIMEImage(fp1.read())
fp1.close()
msgImage1.add_header('Content-ID', '<image2>')
msg.attach(msgImage1)

#fp2 = open(VAR_png_path, 'rb')
fp2 = open('VAR.png', 'rb')
msgImage2 = MIMEImage(fp2.read())
fp2.close()
msgImage2.add_header('Content-ID', '<image3>')
msg.attach(msgImage2)
  
#full_body = body1 + body2 + '<br><img src="cid:image1"><br>'
full_body= body
msgAlternative.attach(MIMEText(full_body, 'html'))
#
#part = MIMEBase('application', "octet-stream")
#part.set_payload(full_body)
#Encoders.encode_base64(part)
#part.add_header('Content-Disposition', 'attachment; filename="report.html"')
#msg.attach(part)

s = smtplib.SMTP('smtp.peak6.net')
msgstr = msg.as_string()
s.sendmail(sender, recipients, msgstr)






#            frame.sort(['ts'], ascending = False, inplace=True)
#            checkStock = frame['StockSymbol'][0]
#            checkPrintTime = frame['ts'][0]
#            checkStrike = frame['Strike'][0]

#            if (checkStock == lastStock) and (checkPrintTime == lastPrintTime) and (checkStrike == lastStrike): 
#                print 'Not updating the stock'
#            else:
#                lastStock = checkStock
#                lastPrintTime = checkPrintTime
#                lastStrike = lastStrike
