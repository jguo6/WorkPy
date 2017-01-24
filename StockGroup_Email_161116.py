from sqlalchemy import create_engine, types
import datetime as dt
import pandas as pd
from mailer import Message, Mailer
import smtplib
from email.MIMEBase import MIMEBase
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from email import Encoders
import numpy as np

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

 # DeltaC
DeltaC_q = """
SELECT
'' as 'DeltaC'
,FORMAT(dc.[CrossingActDeltaC], '#,###') as 'Crossing Act DeltaC'
,FORMAT(dc.[VAHDeltaC], '#,###') as 'Vol Arb Hedge DeltaC'
,FORMAT(dc.[GammaActDeltaC], '#,###') as 'Gamma Act DeltaC'
--,FORMAT(dc.[ExcludedDeltaC], '#,###') as 'Excluded Acts DeltaC'
,'' as 'DayMM'
,FORMAT(dm.CrossingActDayMM, '#,###') as 'Crossing Act DayMM'
,FORMAT(dm.GammaActDayMM, '#,###') as 'Gamma Act DayMM'
,'' as 'Haircut'
,FORMAT(dm.CrossingActHaircut, '#,###') as 'Crossing Act Haircut'
,FORMAT(dm.GammaActHaircut, '#,###') as 'Gamma Act Haircut'
 FROM [StockGroup].[dbo].[tblSG_DeltaCMetrics] dc
 LEFT JOIN  [StockGroup].[dbo].[tblSG_HaircutMetrics] dm on dc.RecordDate=dm.RecordDate
 --WHERE CONVERT(date, RecordDate) = CONVERT(date,getdate())
 WHERE CONVERT(date, dc.RecordDate) = CONVERT(date,getdate()) and CrossingActDeltaC IS NOT NULL
"""
 
 # DeltaC5Day
#DeltaC5Day_q = """
#SELECT
#  FORMAT(FirmDeltaC, '#,###') 'FirmDeltaC'
#, FORMAT(VAHDeltaC, '#,###') 'VAHDeltaC'
#, FORMAT(CrossingActDeltaC, '#,###') 'CrossingActDeltaC'
#, FORMAT(GammaActDeltaC, '#,###') 'GammaActDeltaC'
#, FORMAT(ExcludedDeltaC, '#,###') 'ExcludedDeltaC'
#, FORMAT(VAH_CrossingActDeltaC, '#,###') 'CrossingActDeltaC'
#, RecordDate AS 'Date'
#FROM [StockGroup].[dbo].[tblSG_DeltaCMetrics]
#where RecordDate between CONVERT(date,getdate()-20) and CONVERT(date,getdate())
#"""

# DeltaCSectors
DeltaCSectors_q = """
SELECT Entity, FORMAT(DeltaC, '#,###') 'DeltaC', SectorName, RecordDate 'Date'
FROM [StockGroup].[dbo].[tblSG_DeltaCSectorMetrics]
WHERE RecordDate = CONVERT(date,getdate())
"""

# EfficiencyMetrics
EfficiencyMetrics_q = """
SELECT 
CONVERT(VARCHAR(4),ROUND(SharesTradedOut*100,0)) + '%' AS 'Shares Traded Out',
CONVERT(VARCHAR(4),ROUND(NotionalTradedOut*100,0)) + '%' AS 'Notional Traded Out',
FORMAT(TotalInternalShares, '#,###') 'Total Internal Shares'
FROM [StockGroup].[dbo].[tblSG_EfficiencyMetrics]
WHERE RecordDate = CONVERT(date,getdate())
"""

# NextGenMetrics
NextGenMetrics_q = """
WITH all_metrics AS (
-------Count of Symbols-----------------
SELECT RecordDate, 'Optimizer' as Account, 'Symbols' as Metric,'Short' as Detail,COUNT(*) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where OptimizerDelta < 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate,  'Optimizer' as Account, 'Symbols' as Metric,'Long' as Detail,COUNT(*) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where OptimizerDelta > 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate,  'Optimizer' as Account,'Symbols' as Metric, 'Total' as Detail,COUNT(*) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where OptimizerDelta != 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate, 'CrossingAccount' as Account,'Symbols' as Metric, 'Short' as Detail,COUNT(*) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where SGDELTA < 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate, 'CrossingAccount' as Account,'Symbols' as Metric,'Long' as Detail,COUNT(*) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where SGDELTA > 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate, 'CrossingAccount' as Account,'Symbols' as Metric,'Total' as Detail,COUNT(*) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where SGDELTA != 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate, 'VolArbHedgers' as Account, 'Symbols' as Metric,'Short' as Detail,COUNT(*) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where VAHedgingDeltas < 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate,  'VolArbHedgers' as Account, 'Symbols' as Metric,'Long' as Detail,COUNT(*) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where VAHedgingDeltas > 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate,  'VolArbHedgers' as Account,'Symbols' as Metric, 'Total' as Detail,COUNT(*) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where VAHedgingDeltas != 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate, 'VolArbExcluded' as Account, 'Symbols' as Metric,'Short' as Detail,COUNT(*) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where VAExcludedDeltas < 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate,  'VolArbExcluded' as Account, 'Symbols' as Metric,'Long' as Detail,COUNT(*) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where VAExcludedDeltas > 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate,  'VolArbExcluded' as Account,'Symbols' as Metric, 'Total' as Detail,COUNT(*) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where VAExcludedDeltas != 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate, 'Firm' as Account, 'Symbols' as Metric,'Short' as Detail,COUNT(*) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where FirmDelta < 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate,  'Firm' as Account, 'Symbols' as Metric,'Long' as Detail,COUNT(*) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where FirmDelta > 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate,  'Firm' as Account,'Symbols' as Metric, 'Total' as Detail,COUNT(*) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where FirmDelta != 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate, 'GammaAccount' as Account,'Symbols' as Metric, 'Short' as Detail,COUNT(*) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where Delta97 < 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate, 'GammaAccount' as Account,'Symbols' as Metric,'Long' as Detail,COUNT(*) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where Delta97 > 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate, 'GammaAccount' as Account,'Symbols' as Metric,'Total' as Detail,COUNT(*) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where Delta97 != 0 and RecordDate=convert(date,getdate())
group by RecordDate

-----Sum of Notional------------
union

SELECT RecordDate,  'Optimizer' as Account, 'Notional' as Metric,'Short' as Detail,SUM(OptimizerNotional) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where OptimizerDelta < 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate,  'Optimizer' as Account, 'Notional' as Metric,'Long' as Detail,SUM(OptimizerNotional) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where OptimizerDelta > 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate,  'Optimizer' as Account, 'Notional' as Metric,'Total' as Detail,SUM(OptimizerNotional) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where OptimizerDelta != 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate, 'CrossingAccount' as Account,'Notional' as Metric, 'Short' as Detail,SUM(SGNotional) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where SGNotional < 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate, 'CrossingAccount' as Account,'Notional' as Metric,'Long' as Detail,SUM(SGNotional) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where SGNotional > 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate, 'CrossingAccount' as Account,'Notional' as Metric,'Total' as Detail,SUM(SGNotional) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where SGNotional != 0 and RecordDate=convert(date,getdate())
group by RecordDate

union
--

SELECT RecordDate, 'VolArbHedgers' as Account, 'Notional' as Metric,'Short' as Detail,SUM(VAHedgingNot) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where VAHedgingDeltas < 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate,  'VolArbHedgers' as Account, 'Notional' as Metric,'Long' as Detail,SUM(VAHedgingNot)  as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where VAHedgingDeltas > 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate,  'VolArbHedgers' as Account,'Notional' as Metric, 'Total' as Detail,SUM(VAHedgingNot)  as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where VAHedgingDeltas != 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate, 'VolArbExcluded' as Account, 'Notional' as Metric,'Short' as Detail,SUM(VAExcludedNot) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where VAExcludedDeltas < 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate,  'VolArbExcluded' as Account, 'Notional' as Metric,'Long' as Detail,SUM(VAExcludedNot) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where VAExcludedDeltas > 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate,  'VolArbExcluded' as Account,'Notional' as Metric, 'Total' as Detail,SUM(VAExcludedNot) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where VAExcludedDeltas != 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate, 'Firm' as Account, 'Notional' as Metric,'Short' as Detail,SUM(FirmNotional) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where FirmDelta < 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate,  'Firm' as Account, 'Notional' as Metric,'Long' as Detail,SUM(FirmNotional) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where FirmDelta > 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate,  'Firm' as Account,'Notional' as Metric, 'Total' as Detail,SUM(FirmNotional)as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where FirmDelta != 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate, 'GammaAccount' as Account, 'Notional' as Metric,'Short' as Detail,SUM(Notional97) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where Delta97 < 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate,  'GammaAccount' as Account, 'Notional' as Metric,'Long' as Detail,SUM(Notional97) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where Delta97 > 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate,  'GammaAccount' as Account,'Notional' as Metric, 'Total' as Detail,SUM(Notional97) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where Delta97 != 0 and RecordDate=convert(date,getdate())
group by RecordDate


-----------Sum of Shares-----------
union

SELECT RecordDate, 'Optimizer' as Account, 'Shares' as Metric,'Short' as Detail,SUM(OptimizerDelta) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where OptimizerDelta < 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate,  'Optimizer' as Account, 'Shares' as Metric,'Long' as Detail,SUM(OptimizerDelta) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where OptimizerDelta > 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate,  'Optimizer' as Account,'Shares' as Metric, 'Total' as Detail,SUM(OptimizerDelta) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where OptimizerDelta != 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate, 'CrossingAccount' as Account,'Shares' as Metric, 'Short' as Detail,SUM(SGDELTA) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where SGDELTA < 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate, 'CrossingAccount' as Account,'Shares' as Metric,'Long' as Detail,SUM(SGDELTA)  as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where SGDELTA > 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate, 'CrossingAccount' as Account,'Shares' as Metric,'Total' as Detail,SUM(SGDELTA)  as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where SGDELTA != 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate, 'VolArbHedgers' as Account, 'Shares' as Metric,'Short' as Detail,SUM(VAHedgingDeltas) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where VAHedgingDeltas < 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate,  'VolArbHedgers' as Account, 'Shares' as Metric,'Long' as Detail,SUM(VAHedgingDeltas)  as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where VAHedgingDeltas > 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate,  'VolArbHedgers' as Account,'Shares' as Metric, 'Total' as Detail,SUM(VAHedgingDeltas)  as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where VAHedgingDeltas != 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate, 'VolArbExcluded' as Account, 'Shares' as Metric,'Short' as Detail,SUM(VAExcludedDeltas) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where VAExcludedDeltas < 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate,  'VolArbExcluded' as Account, 'Shares' as Metric,'Long' as Detail,SUM(VAExcludedDeltas) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where VAExcludedDeltas > 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate,  'VolArbExcluded' as Account,'Shares' as Metric, 'Total' as Detail,SUM(VAExcludedDeltas) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where VAExcludedDeltas != 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate, 'Firm' as Account, 'Shares' as Metric,'Short' as Detail,SUM(FirmDelta) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where FirmDelta < 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate,  'Firm' as Account, 'Shares' as Metric,'Long' as Detail,SUM(FirmDelta) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where FirmDelta > 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate,  'Firm' as Account,'Shares' as Metric, 'Total' as Detail,SUM(FirmDelta) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where FirmDelta != 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate, 'GammaAccount' as Account,'Shares' as Metric, 'Short' as Detail,SUM(Delta97) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where Delta97 < 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate, 'GammaAccount' as Account,'Shares' as Metric,'Long' as Detail,SUM(Delta97) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where Delta97 > 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate, 'GammaAccount' as Account,'Shares' as Metric,'Total' as Detail,SUM(Delta97)as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where Delta97 != 0 and RecordDate=convert(date,getdate())
group by RecordDate

---------SUM of Beta Notional------------------

union

SELECT RecordDate,  'Optimizer' as Account, 'Beta Notional' as Metric,'Short' as Detail,SUM(OptBetaNotional) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where OptBetaNotional < 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate,  'Optimizer' as Account, 'Beta Notional' as Metric,'Long' as Detail,SUM(OptBetaNotional) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where OptBetaNotional > 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate,  'Optimizer' as Account, 'Beta Notional' as Metric,'Total' as Detail,SUM(OptBetaNotional) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where OptBetaNotional != 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate, 'CrossingAccount' as Account,'Beta Notional' as Metric, 'Short' as Detail,SUM(SGBeta_Notional) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where SGBeta_Notional < 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate, 'CrossingAccount' as Account,'Beta Notional' as Metric,'Long' as Detail,SUM(SGBeta_Notional) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where SGBeta_Notional > 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate, 'CrossingAccount' as Account,'Beta Notional' as Metric,'Total' as Detail,SUM(SGBeta_Notional) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where SGBeta_Notional != 0 and RecordDate=convert(date,getdate())
group by RecordDate

union
--

SELECT RecordDate, 'VolArbHedgers' as Account, 'Beta Notional' as Metric,'Short' as Detail,SUM(VolArbBeta_Notional) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where VolArbBeta_Notional < 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate,  'VolArbHedgers' as Account, 'Beta Notional' as Metric,'Long' as Detail,SUM(VolArbBeta_Notional)  as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where VolArbBeta_Notional > 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate,  'VolArbHedgers' as Account,'Beta Notional' as Metric, 'Total' as Detail,SUM(VolArbBeta_Notional)  as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where VolArbBeta_Notional != 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate, 'VolArbExcluded' as Account, 'Beta Notional' as Metric,'Short' as Detail,SUM(VAExcludedBetaNot) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where VAExcludedBetaNot < 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate,  'VolArbExcluded' as Account, 'Beta Notional' as Metric,'Long' as Detail,SUM(VAExcludedBetaNot) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where VAExcludedBetaNot > 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate,  'VolArbExcluded' as Account,'Beta Notional' as Metric, 'Total' as Detail,SUM(VAExcludedBetaNot) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where VAExcludedBetaNot != 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate, 'Firm' as Account, 'Beta Notional' as Metric,'Short' as Detail,SUM(FirmNotional) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where FirmBetaNotional < 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate,  'Firm' as Account, 'Beta Notional' as Metric,'Long' as Detail,SUM(FirmNotional) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where FirmBetaNotional > 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate,  'Firm' as Account,'Beta Notional' as Metric, 'Total' as Detail,SUM(FirmNotional)as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where FirmBetaNotional != 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate, 'GammaAccount' as Account, 'Beta Notional' as Metric,'Short' as Detail,SUM(BetaNotional97) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where BetaNotional97 < 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate,  'GammaAccount' as Account, 'Beta Notional' as Metric,'Long' as Detail,SUM(BetaNotional97) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where BetaNotional97 > 0 and RecordDate=convert(date,getdate())
group by RecordDate

union

SELECT RecordDate,  'GammaAccount' as Account,'Beta Notional' as Metric, 'Total' as Detail,SUM(BetaNotional97) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where BetaNotional97 != 0 and RecordDate=convert(date,getdate())
group by RecordDate

-------------- SPY BETA NOTIONAL -----------
union


SELECT RecordDate,  'Optimizer' as Account, 'SPYBetaNotional' as Metric,'Short' as Detail,SUM(OptBetaNotional) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where OptBetaNotional < 0 and RecordDate=convert(date,getdate()) and ETF='SPY'
group by RecordDate

union

SELECT RecordDate,  'Optimizer' as Account, 'SPYBetaNotional' as Metric,'Long' as Detail,SUM(OptBetaNotional) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where OptBetaNotional > 0 and RecordDate=convert(date,getdate()) and ETF='SPY'
group by RecordDate

union

SELECT RecordDate,  'Optimizer' as Account, 'SPYBetaNotional' as Metric,'Total' as Detail,SUM(OptBetaNotional) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where OptBetaNotional != 0 and RecordDate=convert(date,getdate()) and ETF='SPY'
group by RecordDate

union

SELECT RecordDate, 'CrossingAccount' as Account,'SPYBetaNotional' as Metric, 'Short' as Detail,SUM(SGBeta_Notional) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where SGBeta_Notional < 0 and RecordDate=convert(date,getdate()) and ETF='SPY'
group by RecordDate

union

SELECT RecordDate, 'CrossingAccount' as Account,'SPYBetaNotional' as Metric,'Long' as Detail,SUM(SGBeta_Notional) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where SGBeta_Notional > 0 and RecordDate=convert(date,getdate()) and ETF='SPY'
group by RecordDate

union

SELECT RecordDate, 'CrossingAccount' as Account,'SPYBetaNotional' as Metric,'Total' as Detail,SUM(SGBeta_Notional) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where SGBeta_Notional != 0 and RecordDate=convert(date,getdate()) and ETF='SPY'
group by RecordDate

union
--

SELECT RecordDate, 'VolArbHedgers' as Account, 'SPYBetaNotional' as Metric,'Short' as Detail,SUM(VolArbBeta_Notional) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where VolArbBeta_Notional < 0 and RecordDate=convert(date,getdate()) and ETF='SPY'
group by RecordDate

union

SELECT RecordDate,  'VolArbHedgers' as Account, 'SPYBetaNotional' as Metric,'Long' as Detail,SUM(VolArbBeta_Notional)  as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where VolArbBeta_Notional > 0 and RecordDate=convert(date,getdate()) and ETF='SPY'
group by RecordDate

union

SELECT RecordDate,  'VolArbHedgers' as Account,'SPYBetaNotional' as Metric, 'Total' as Detail,SUM(VolArbBeta_Notional)  as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where VolArbBeta_Notional != 0 and RecordDate=convert(date,getdate()) and ETF='SPY'
group by RecordDate

union

SELECT RecordDate, 'VolArbExcluded' as Account, 'SPYBetaNotional' as Metric,'Short' as Detail,SUM(VAExcludedBetaNot) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where VAExcludedBetaNot < 0 and RecordDate=convert(date,getdate()) and ETF='SPY'
group by RecordDate

union

SELECT RecordDate,  'VolArbExcluded' as Account, 'SPYBetaNotional' as Metric,'Long' as Detail,SUM(VAExcludedBetaNot) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where VAExcludedBetaNot > 0 and RecordDate=convert(date,getdate()) and ETF='SPY'
group by RecordDate

union

SELECT RecordDate,  'VolArbExcluded' as Account,'SPYBetaNotional' as Metric, 'Total' as Detail,SUM(VAExcludedBetaNot) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where VAExcludedBetaNot != 0 and RecordDate=convert(date,getdate()) and ETF='SPY'
group by RecordDate

union

SELECT RecordDate, 'Firm' as Account, 'SPYBetaNotional' as Metric,'Short' as Detail,SUM(FirmNotional) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where FirmBetaNotional < 0 and RecordDate=convert(date,getdate()) and ETF='SPY'
group by RecordDate

union

SELECT RecordDate,  'Firm' as Account, 'SPYBetaNotional' as Metric,'Long' as Detail,SUM(FirmNotional) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where FirmBetaNotional > 0 and RecordDate=convert(date,getdate()) and ETF='SPY'
group by RecordDate

union

SELECT RecordDate,  'Firm' as Account,'SPYBetaNotional' as Metric, 'Total' as Detail,SUM(FirmNotional)as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where FirmBetaNotional != 0 and RecordDate=convert(date,getdate()) and ETF='SPY'
group by RecordDate

union

SELECT RecordDate, 'GammaAccount' as Account, 'SPYBetaNotional' as Metric,'Short' as Detail,SUM(BetaNotional97) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where BetaNotional97 < 0 and RecordDate=convert(date,getdate()) and ETF='SPY'
group by RecordDate

union

SELECT RecordDate,  'GammaAccount' as Account, 'SPYBetaNotional' as Metric,'Long' as Detail,SUM(BetaNotional97) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where BetaNotional97 > 0 and RecordDate=convert(date,getdate()) and ETF='SPY'
group by RecordDate

union

SELECT RecordDate,  'GammaAccount' as Account,'SPYBetaNotional' as Metric, 'Total' as Detail,SUM(BetaNotional97) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where BetaNotional97 != 0 and RecordDate=convert(date,getdate()) and ETF='SPY'
group by RecordDate

------------------IWM BETA NOTIONAL ----------------
union

SELECT RecordDate,  'Optimizer' as Account, 'IWMBetaNotional' as Metric,'Short' as Detail,SUM(OptBetaNotional) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where OptBetaNotional < 0 and RecordDate=convert(date,getdate()) and ETF='IWM'
group by RecordDate

union

SELECT RecordDate,  'Optimizer' as Account, 'IWMBetaNotional' as Metric,'Long' as Detail,SUM(OptBetaNotional) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where OptBetaNotional > 0 and RecordDate=convert(date,getdate()) and ETF='IWM'
group by RecordDate

union

SELECT RecordDate,  'Optimizer' as Account, 'IWMBetaNotional' as Metric,'Total' as Detail,SUM(OptBetaNotional) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where OptBetaNotional != 0 and RecordDate=convert(date,getdate()) and ETF='IWM'
group by RecordDate

union

SELECT RecordDate, 'CrossingAccount' as Account,'IWMBetaNotional' as Metric, 'Short' as Detail,SUM(SGBeta_Notional) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where SGBeta_Notional < 0 and RecordDate=convert(date,getdate()) and ETF='IWM'
group by RecordDate

union

SELECT RecordDate, 'CrossingAccount' as Account,'IWMBetaNotional' as Metric,'Long' as Detail,SUM(SGBeta_Notional) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where SGBeta_Notional > 0 and RecordDate=convert(date,getdate()) and ETF='IWM'
group by RecordDate

union

SELECT RecordDate, 'CrossingAccount' as Account,'IWMBetaNotional' as Metric,'Total' as Detail,SUM(SGBeta_Notional) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where SGBeta_Notional != 0 and RecordDate=convert(date,getdate()) and ETF='IWM'
group by RecordDate

union
--

SELECT RecordDate, 'VolArbHedgers' as Account, 'IWMBetaNotional' as Metric,'Short' as Detail,SUM(VolArbBeta_Notional) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where VolArbBeta_Notional < 0 and RecordDate=convert(date,getdate()) and ETF='IWM'
group by RecordDate

union

SELECT RecordDate,  'VolArbHedgers' as Account, 'IWMBetaNotional' as Metric,'Long' as Detail,SUM(VolArbBeta_Notional)  as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where VolArbBeta_Notional > 0 and RecordDate=convert(date,getdate()) and ETF='IWM'
group by RecordDate

union

SELECT RecordDate,  'VolArbHedgers' as Account,'IWMBetaNotional' as Metric, 'Total' as Detail,SUM(VolArbBeta_Notional)  as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where VolArbBeta_Notional != 0 and RecordDate=convert(date,getdate()) and ETF='IWM'
group by RecordDate

union

SELECT RecordDate, 'VolArbExcluded' as Account, 'IWMBetaNotional' as Metric,'Short' as Detail,SUM(VAExcludedBetaNot) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where VAExcludedBetaNot < 0 and RecordDate=convert(date,getdate()) and ETF='IWM'
group by RecordDate

union

SELECT RecordDate,  'VolArbExcluded' as Account, 'IWMBetaNotional' as Metric,'Long' as Detail,SUM(VAExcludedBetaNot) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where VAExcludedBetaNot > 0 and RecordDate=convert(date,getdate()) and ETF='IWM'
group by RecordDate

union

SELECT RecordDate,  'VolArbExcluded' as Account,'IWMBetaNotional' as Metric, 'Total' as Detail,SUM(VAExcludedBetaNot) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where VAExcludedBetaNot != 0 and RecordDate=convert(date,getdate()) and ETF='IWM'
group by RecordDate

union

SELECT RecordDate, 'Firm' as Account, 'IWMBetaNotional' as Metric,'Short' as Detail,SUM(FirmBetaNotional) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where FirmBetaNotional < 0 and RecordDate=convert(date,getdate()) and ETF='IWM'
group by RecordDate

union

SELECT RecordDate,  'Firm' as Account, 'IWMBetaNotional' as Metric,'Long' as Detail,SUM(FirmBetaNotional) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where FirmBetaNotional > 0 and RecordDate=convert(date,getdate()) and ETF='IWM'
group by RecordDate

union

SELECT RecordDate,  'Firm' as Account,'IWMBetaNotional' as Metric, 'Total' as Detail,SUM(FirmBetaNotional)as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where FirmBetaNotional != 0 and RecordDate=convert(date,getdate()) and ETF='IWM'
group by RecordDate

union

SELECT RecordDate, 'GammaAccount' as Account, 'IWMBetaNotional' as Metric,'Short' as Detail,SUM(BetaNotional97) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where BetaNotional97 < 0 and RecordDate=convert(date,getdate()) and ETF='IWM'
group by RecordDate

union

SELECT RecordDate,  'GammaAccount' as Account, 'IWMBetaNotional' as Metric,'Long' as Detail,SUM(BetaNotional97) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where BetaNotional97 > 0 and RecordDate=convert(date,getdate()) and ETF='IWM'
group by RecordDate

union

SELECT RecordDate,  'GammaAccount' as Account,'IWMBetaNotional' as Metric, 'Total' as Detail,SUM(BetaNotional97) as Value
FROM [StockGroup].[dbo].[tblSG_NextGenMetrics]
where BetaNotional97 != 0 and RecordDate=convert(date,getdate()) and ETF='IWM'
group by RecordDate
)

SELECT Account, Metric, Detail, Value AS Value
FROM all_metrics
WHERE Metric NOT IN ('IWMBetaNotional', 'SPYBetaNotional') AND Detail != 'Total'
"""

# PET Metrics
PETMetrics_q = """
SELECT 
	--FORMAT(orderedqty, '#,###') 'Order Qty'
	 FORMAT(totalfillqty, '#,###') 'Fill Qty'
  	, FORMAT(notional, '#,###') 'Fill Notional'
   	, CONVERT(VARCHAR(6),ROUND(bps_slippage, 2)) + ' bps' AS 'Arrival Slip BPS'
	, FORMAT(dollarslippage, '#,###') 'Arrival Slip $'
 	, CONVERT(VARCHAR(6),ROUND(nbbo_bps_slippage, 2)) + ' bps' AS 'NBBO Slip BPS'
	, FORMAT(nbbodollarslippage, '#,###') 'NBBO Slip $'
 	, CONVERT(VARCHAR(6),ROUND(close_bps_slippage, 2)) + ' bps' AS 'Close Slip BPS'
	, FORMAT(closedollarslippage, '#,###') 'Close Slip $'
	,FORMAT(ROUND(transcost,0), '#,###') as 'Estimated GS Cost'
	--, RecordDate 'Date'
FROM [StockGroup].[dbo].[tblSG_PETMetrics]
WHERE RecordDate = CONVERT(date,getdate())
"""
 
 # TraderBasketMetrics
TraderBasketMetrics_q = """
SELECT 
      FORMAT(qty, '#,###') 'Quantity'
	,FORMAT(notional, '#,###') 'Notional' 
	, FORMAT(slippage, '#,###') 'Impact $'
	, CONVERT(VARCHAR(5),ROUND(slippagebps,2)) + ' bps' AS 'Impact BPS'
	--, RecordDate 'Date'
FROM [StockGroup].[dbo].[tblSG_BasketMetrics]
WHERE RecordDate = CONVERT(date,getdate()) """
 

# Strategy Metrics
StrategyMetrics_q = """
DECLARE @tot_fq FLOAT
SET @tot_fq = (SELECT SUM(totalfillqty)  FROM [StockGroup].[dbo].[tblSG_TradeVisionMetrics] where RecordDate = CONVERT(Date, getdate()) and account NOT IN ('F99','F97','F91'));

WITH
tvm as (
 SELECT *
  FROM [StockGroup].[dbo].[tblSG_TradeVisionMetrics]
 where RecordDate = CONVERT(Date, getdate())
 and aggressiveness IN ('THROUGH','CROSS','MID')
 and session='DAY'
and account NOT IN ('F99','F97','F91')
)
SELECT (CASE WHEN ordersource='Phoenix' then 'PHOENIX' else ordersource end) as 'Order Source'
, convert(VARCHAR(6),round(SUM(totalfillqty)/@tot_fq * 100,0)) + '%' AS '% Total Fill'
, FORMAT(SUM(totalfillqty),'#,###') AS 'Total Fill Quantity'
--, convert(VARCHAR(6),ROUND((SUM(totalfillslippage)/SUM(totalfillqty * totalavgpx))*10000,2)) + ' bps' AS 'Slippage BPS'
FROM tvm
GROUP BY ordersource
 """

# Destination Metrics
DestinationMetrics_q = """
SELECT 
	stock_tactic 'Tactic'
	, FORMAT(SUM(totalfillqty), '#,###') 'Total Fill Qty'
	, CONVERT(VARCHAR(10),ROUND(SUM(CONVERT(FLOAT,totalfillqty))/SUM(CONVERT(FLOAT,quantity)) *100,0)) + '%' AS 'Fill Rate'
	, CONVERT(VARCHAR(10),ROUND((SUM([totalfillslippage])/SUM(totalfillqty * [totalavgpx]))*10000,2)) + ' bps' AS 'Slippage BPS'
	, FORMAT(SUM([marketqty]), '#,###') 'Market Fills'
	, FORMAT(SUM(diegoqty), '#,###') 'Diego Fills'
	, FORMAT(SUM([igoogiqty]), '#,###') 'IgoogI Fills'
	, FORMAT(SUM(internalizedqty), '#,###') 'Internalized Fills'
	, FORMAT(SUM(totalfillpnlclose), '#,###') 'PnL to Close'
FROM [StockGroup].[dbo].[tblSG_TradeVisionMetrics]
WHERE RecordDate = CONVERT(Date,getdate())
	AND account NOT IN ('F99','F97','F91') AND stock_tactic != 'BASKET'
  and aggressiveness IN ('THROUGH','CROSS','MID')
  and session='DAY'
GROUP BY stock_tactic
ORDER BY sum(totalfillqty) DESC
"""

# Execution Metrics
ExecutionMetrics_q = """
WITH execution AS (
SELECT 'DIEGO' AS stock_tactic, SUM(diegoqty) 'Quantity', SUM(diegopnlclose) 'PnL Close', SUM(diegoslippage)/SUM(diegoqty * diegoavgpx) * 10000 'SlippageBPS', SUM(diegopriceimp)/SUM(diegoqty * diegoavgpx) * 10000 'Price Imp'
FROM [StockGroup].[dbo].[tblSG_TradeVisionMetrics]
WHERE RecordDate =CONVERT(Date, getdate())
AND account NOT IN ('F99','F97','F91') AND diegoqty IS NOT NULL

UNION

SELECT 'IGOOGI' AS stock_tactic, SUM(igoogiqty), SUM(igoogipnlclose), SUM(igoogislippage)/SUM(igoogiqty * igoogiavgpx) * 10000 , SUM(igoogipriceimp)/SUM(igoogiqty * igoogiavgpx) * 10000
FROM [StockGroup].[dbo].[tblSG_TradeVisionMetrics]
WHERE RecordDate =CONVERT(Date, getdate())
AND account NOT IN ('F99','F97','F91') AND igoogiqty IS NOT NULL

UNION

SELECT 'INTERNALIZATION' AS stock_tactic, SUM(internalizedqty), SUM(internalizedpnlclose), SUM(internalizedslippage)/SUM(internalizedqty * internalizedavgpx) * 10000, SUM(internalizedpriceimp)/SUM(internalizedqty * internalizedavgpx) * 10000
FROM [StockGroup].[dbo].[tblSG_TradeVisionMetrics]
WHERE RecordDate =CONVERT(Date, getdate())
AND account NOT IN ('F99','F97','F91') AND internalizedqty IS NOT NULL

UNION

SELECT 'MARKET' AS stock_tactic, SUM(marketqty), SUM(mktpnlclose), SUM(marketslippage)/SUM(marketqty * marketavgpx) * 10000, SUM(mktpriceimp)/SUM(marketqty * marketavgpx) * 10000
FROM [StockGroup].[dbo].[tblSG_TradeVisionMetrics]
WHERE RecordDate =CONVERT(Date, getdate())
AND account NOT IN ('F99','F97','F91') AND marketqty IS NOT NULL
and aggressiveness IN ('THROUGH','CROSS','MID')
and session='DAY'

UNION

SELECT 'TRADER2TRADER' AS stock_tactic, SUM(tradercrossqty), SUM(tradercrosspnlclose), SUM(tradercrossslippage)/SUM(tradercrossqty * tradercrossavgpx) * 10000, SUM(tradercrosspriceimp)/SUM(tradercrossqty * tradercrossavgpx) * 10000
FROM [StockGroup].[dbo].[tblSG_TradeVisionMetrics]
WHERE RecordDate =CONVERT(Date, getdate())

UNION

SELECT stock_tactic, SUM(quantity), SUM(totalfillpnlclose), SUM(totalfillslippage)/SUM(totalfillqty * totalavgpx) * 10000, SUM(totalfillpriceimp)/SUM(totalfillqty * totalavgpx) * 10000
FROM [StockGroup].[dbo].[tblSG_TradeVisionMetrics]
WHERE RecordDate =CONVERT(Date, getdate()) AND stock_tactic='BASKET'
AND account NOT IN ('F99','F97','F91')
GROUP BY stock_tactic
)
SELECT stock_tactic 'Tactic'
, FORMAT(Quantity, '#,###') AS 'Fill Quantity'
, CONVERT(VARCHAR(6),ROUND([SlippageBPS],2)) + ' bps' 'Slippage BPS'
, CONVERT(VARCHAR(6),ROUND([Price Imp],2)) + ' bps' 'Price Imp BPS'
, FORMAT([PnL Close], '#,###') 'PnL to Close'
FROM execution
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

# AutoHedge Destinations
AutoHedge_q = """
with pivot1 as (
SELECT 
	stock_tactic 'Tactic'
	,autohedge as 'Type'
	, SUM(totalfillqty) as 'TotalFillQty'
	, cast(SUM(totalfillpnlclose) as int) as 'PnLtoClose'
FROM [StockGroup].[dbo].[tblSG_TradeVisionMetrics]
WHERE RecordDate = CONVERT(Date,getdate())
	AND account NOT IN ('F99','F97','F91') AND stock_tactic != 'BASKET'
GROUP BY stock_tactic, autohedge
--ORDER BY sum(totalfillqty) DESC
)

select Tactic,Type,Metric, Value 
from pivot1
unpivot
( 
Value
for Metric in (TotalFillQty,PnLtoClose)
)u
"""

# Autohedge Execution Types
AutoHedge_Executions_q = """
WITH execution AS (
SELECT 'DIEGO' AS stock_tactic,autohedge as 'Type',SUM(diegoqty) 'Quantity', SUM(diegopnlclose) 'PnLClose', SUM(diegoslippage)/SUM(diegoqty * diegoavgpx) * 10000 'SlippageBPS', SUM(diegopriceimp)/SUM(diegoqty * diegoavgpx) * 10000 'Price Imp'
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

SELECT 'TRADER2TRADER' AS stock_tactic,autohedge as 'Type', SUM(tradercrossqty), SUM(tradercrosspnlclose), SUM(tradercrossslippage)/SUM(tradercrossqty * tradercrossavgpx) * 10000, SUM(tradercrosspriceimp)/SUM(tradercrossqty * tradercrossavgpx) * 10000
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
, Quantity AS 'FillQuantity'
, CONVERT(VARCHAR(6),ROUND([SlippageBPS],2)) + ' bps' 'SlippageBPS'
--, CONVERT(VARCHAR(6),ROUND([Price Imp],2)) + ' bps' 'Price Imp BPS'
, cast([PnLClose] as int) 'PnLtoClose'
FROM execution
)


select Tactic,Type,Metric, Value 
from main
unpivot
( 
Value
for Metric in (FillQuantity,PnLtoClose)
)u
"""

#==============================================================================
# Data From SQL
#==============================================================================
 
#DeltaC5Day_df = pd.read_sql(DeltaC5Day_q, engine)
DeltaCSectors_df = pd.read_sql(DeltaCSectors_q, engine)
DeltaC_df = pd.read_sql(DeltaC_q, engine)
EfficiencyMetrics_df = pd.read_sql(EfficiencyMetrics_q, engine)
PETMetrics_df = pd.read_sql(PETMetrics_q, engine)
StrategyMetrics_df = pd.read_sql(StrategyMetrics_q, engine)
TraderBasketMetrics_df = pd.read_sql(TraderBasketMetrics_q, engine)
DestinationMetrics_df = pd.read_sql(DestinationMetrics_q, engine)
ExecutionMetrics_df = pd.read_sql(ExecutionMetrics_q, engine)
VARMetrics_df = pd.read_sql(VARMetrics_q , engine)

NextGenMetrics_df = pd.read_sql(NextGenMetrics_q, engine) 
NextGenMetrics_df=NextGenMetrics_df[NextGenMetrics_df['Account'].isin(['Optimizer','CrossingAccount','GammaAccount','VolArbHedgers'])]
NextGenMetrics_df=pd.pivot_table(NextGenMetrics_df,values='Value',index=['Metric'],columns=['Account','Detail'])
NextGenMetrics_df=NextGenMetrics_df.sort(ascending=False) 

AutoHedge_df = pd.read_sql(AutoHedge_q, engine)
AutoHedge_df=pd.pivot_table(AutoHedge_df,values='Value',index=['Tactic'],columns=['Metric','Type'])
AutoHedge_df=AutoHedge_df.sort(ascending=False) 

AutoHedge_Executions_df = pd.read_sql(AutoHedge_Executions_q, engine)
AutoHedge_Executions_df=pd.pivot_table(AutoHedge_Executions_df,values='Value',index=['Tactic'],columns=['Metric','Type'])
AutoHedge_Executions_df=AutoHedge_Executions_df.sort(ascending=False) 
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

NextGenMetrics_formatters = build_formatters(NextGenMetrics_df, num_format)
AutoHedge_formatters = build_formatters(AutoHedge_df, num_format)
AutoHedge_Executions_formatters = build_formatters(AutoHedge_Executions_df, num_format)

                 
#message.Html = '<b><font size=\"14\"> Firm Stock Report ' + datetime.date.today().strftime(\"%m/%d/%Y\") + '</font> </b>'                 

# Strategy Breakdown
#message.Html = '<b>Strategy Breakdown</b>'
#message.Html += StrategyMetrics_df.to_html(index=False)
#message.Html +=  '<br></br>'
#
## Trader Basket Metrics
#message.Html += '<b>Trader Basket Metrics</b>'
#message.Html += TraderBasketMetrics_df.T.to_html(header=False)
#message.Html +=  '<br></br>'
#
## PET Meterics
#message.Html += '<b>PET Metrics</b>'
#message.Html += PETMetrics_df.T.to_html(header=False)
#message.Html +=  '<br></br>'
#
## Risk Metrics
#message.Html += '<b>VAR Metrics</b>'
#message.Html += VARMetrics_df.to_html(index=False)
#message.Html +=  '<br></br>'
#
## Efficiency Metrics
#message.Html += '<b>Efficiency Metrics</b>'
#message.Html += EfficiencyMetrics_df.T.to_html(header=False)
#message.Html +=  '<br></br>'
#
## PnL Metrics
#message.Html += '<b>PnL Metrics</b>'
#message.Html += DeltaC_df.T.to_html(header=False)
#message.Html +=  '<br></br>'
#
## Portfolio Metrics
#message.Html += '<b>Portfolio Metrics</b>'
##NextGenMetrics_out = NextGenMetrics_df.set_index(['Account','Detail'])
#message.Html += NextGenMetrics_df.to_html(formatters=NextGenMetrics_formatters)
#message.Html +=  '<br></br>'
#
## Destination Metrics
#message.Html += '<b>Destination Metrics</b>'
#message.Html += DestinationMetrics_df.to_html(index=False)
#message.Html +=  '<br></br>'
#
## Execution Metrics
#message.Html += '<b>Execution Metrics</b>'
#message.Html += ExecutionMetrics_df.to_html(index=False)
#message.Html +=  '<br></br>'
#
## Autohedge Destination Metrics
#message.Html += '<b>Autohedge Destination Metrics</b>'
#message.Html += AutoHedge_df.to_html(formatters=AutoHedge_formatters)
#message.Html +=  '<br></br>'
#
## Autohedge Destination Metrics
#message.Html += '<b>Autohedge Execution Metrics</b>'
#message.Html += AutoHedge_Executions_df.to_html(formatters=AutoHedge_Executions_formatters)
#message.Html +=  '<br></br>'

#==============================================================================
# Send Email
#==============================================================================



def send_email(StrategyMetrics_df,TraderBasketMetrics_df,PETMetrics_df,VARMetrics_df,EfficiencyMetrics_df,DeltaC_df,NextGenMetrics_df,DestinationMetrics_df,ExecutionMetrics_df,AutoHedge_df):    
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
                        padding: 8px;
                        border-style: solid;
                        border-color: #666666;
                        background-color: #dedede;
        white-space: nowrap;
        }

        table.dataframe td {
                        border-width: 1px;
                        padding: 8px;
                        border-style: solid;
                        border-color: #666666;
        text-align: right;
        }

        table.dataframe tr:nth-child(odd)   { background-color:#eee; }
        table.dataframe tr:nth-child(even)    { background-color:#fff; }
    </style>
    </head><body>''' + \
               '<p>' + "<b>Strategy Breakdown</b>" + "</p>"
               '<p>' + StrategyMetrics_df.to_html(index=False) + '</p>'                        
               '<p>' + "<b>Trader Basket Metrics</b>" + "</p>"
               '<p>' + TraderBasketMetrics_df.T.to_html(header=False) + '</p>' +\
               '<p>' + "<b>PET Metrics</b>" + "</p>"+\
               '<p>' + PETMetrics_df.T.to_html(header=False) + '</p>' +\
               '<p>' + "<b>Portfolio Risk Metrics</b>" + "</p>"+\
               '<p>' + VARMetrics_df.to_html(index=False,header=False) + '</p>' +\
               '<p>' + "<b>Efficiency Metrics</b>" + "</p>"+\
               '<p>' + EfficiencyMetrics_df.T.to_html(header=False) + '</p>' +\
               '<p>' + "<b>PNL Metrics</b>" + "</p>"+\
               '<p>' + DeltaC_df.T.to_html(header=False) + '</p>' +\
               '<p>' + "<b>Portfolio Metrics</b>" + "</p>"+\
               '<p>' + NextGenMetrics_df.to_html(formatters=NextGenMetrics_formatters) + '</p>' +\
               '<p>' + "<b>Trader Destination Metrics</b>" + "</p>"+\
               '<p>' + DestinationMetrics_df.to_html(index=False) + '</p>' +\
               '<p>' + "<b>Trader Execution Metrics</b>" + "</p>"+\
               '<p>' + ExecutionMetrics_df.to_html(index=False) + '</p>' +\
               '<p>' + "<b>AutoHedge Destination Metrics</b>" + "</p>"+\
               '<p>' + AutoHedge_df.to_html(formatters=AutoHedge_formatters) + '</p>' +\
               '<p>' + "<b>AutoHedge Execution Metrics</b>" + "</p>"+\
               '<p>' + AutoHedge_Executions_df.to_html(formatters=AutoHedge_Executions_formatters) + '</p>' +\
              # '<p>' + "<b>Today's Buy/Sell Ratio by Symbol (only shown if >=200 ctx traded on day) \n(Total includes all volume) </b>" + "</p>"+\
              
                '</body></html>')


    sender = 'dsamhat@peak6.com'
   # recipients =['7@peak6.com', 'gruhana@peak6.com','dperper@peak6.com','kgilboy@peak6.com','TS@peak6.com']
    recipients = ['stockgroup@peak6.com', 'ts@peak6.com', 'jcowden@peak6.com']
    msg = MIMEMultipart()
    msg['From'] = sender
    msg['To'] = ", ".join(recipients)
    msg['Subject'] = 'Firm Stock Report'
    msg.attach(MIMEText(body, 'html'))
    part = MIMEBase('application', "octet-stream")
    part.set_payload(body)
    Encoders.encode_base64(part)
    part.add_header('Content-Disposition', 'attachment; filename="report.html"')
    msg.attach(part)

    s = smtplib.SMTP('smtp.peak6.net')
    msgstr = msg.as_string()
    s.sendmail(sender, recipients, msgstr)
    return True
    
send_email(StrategyMetrics_df,TraderBasketMetrics_df,PETMetrics_df,VARMetrics_df,EfficiencyMetrics_df,DeltaC_df,NextGenMetrics_df,DestinationMetrics_df,ExecutionMetrics_df,AutoHedge_df)
