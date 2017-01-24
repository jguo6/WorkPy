# -*- coding: utf-8 -*-
"""
Created on Wed Dec 18 07:44:53 2013

@author: dsamhat
"""
from beef import sql_script_runner 

import pyodbc
import datetime
import time
import pymssql
import pymmd
from pyspark import pyspark as ps
import numpy as np
from collections import defaultdict
import datetime
import pandas as pd
import time
import os
print os.getpid()


s=r"""

SELECT  rtrim(StockSymbol) as Symbol,BPS, round((MedianDailyVlm*(Percentage/100)),0) as Shares, NTILE(5) over(order by BPS) as Bucket
FROM [StockTrading].[dbo].[tblEstimatedTradingCost]
where convert(date, LastUpdated)=(SELECT MAX(CONVERT(date,LastUpdated)) FROM [StockTrading].[dbo].[tblEstimatedTradingCost]) and Percentage=0.1 and EndDate IS NOT NULL
order by StockSymbol

"""

data=sql_script_runner(s)

df=data.get_df()
d = defaultdict(list)

now = str(datetime.datetime.now())


for i in range (0,len(df)):   
    sym = df.ix[i][0] 
    BPS = float(df.ix[i][1])
    Shares = int(df.ix[i][2])
    d[sym]={'BPS':BPS, 'Shares':Shares, 'Date':now}

print datetime.datetime.now()

try: c2 = pymmd.MMDConnection('delphi', 9999)
except: pass
try: c = pymmd.MMDConnection('asgard', 9999)
except: pymmd.MMDConnection('olympus', 9999)

req = {'action':"import", 'name':"DM_SAMHAT_BPS", 'data':d, 'expiration':'11/05/2055'}
try: response = c.call('data.map.manager', req)
except: pass
try: response = c2.call('data.map.manager', req)
except: pass



print response

  






