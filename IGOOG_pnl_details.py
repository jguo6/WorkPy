# -*- coding: utf-8 -*-
"""
Created on Fri Jan 20 09:38:19 2017

@author: jguo
"""

from __future__ import division 
import pandas as pd, pandas.io.sql as psql
import pyodbc
from datetime import datetime, timedelta 
from scipy import stats 
from pyspark import pyspark as ps
import datetime
from sqlalchemy import create_engine
import pymmd 
import pdb 
import collections 
from scipy import stats 
import statsmodels.api as sm
import matplotlib.pyplot as pltI
import smtplib
import math
import numpy as np

import psycopg2
import pandas.io.sql as psql

db = {
    'LatencyStats': ('pslchi6ppgsql10', 5800),
    'PEZ dev': ('pvlchi6dpgsql1', 5500),
    'PEZ prod': ('pslchi5pepgsql10', 5500),
    'PEZ prod archive': ('pslchi6ppgsql10', 5500),
    'PEZ staging': ('pvlchi6spgsql1', 5500),
    'PEZ Uat': ('pvlchi6upgsql1', 5500),
    'PEZ Uat archive': ('pslchi6ppgsql10', 5550),
    'SQL3': ('SQL3',),
    'SQL2': ('pswchi6psql2', 'LabsProjects')
    }

server = 'PEZ prod archive'
host = db[server][0]
port = db[server][1]
engine = create_engine('postgresql://execution:execution@' + host + ':' + str(port) + '/execution')

c = pymmd.MMDConnection('pvlchi6ppymmd1', 9999) 
#CHANGE USER TO YOUR OWN USERNAME 
r = c.call('auth.auto', {'user':'jguo'}) 
            
sql1_host = 'PVWCHI6PSQL1'
sql1_user = 'TSPython'
sql1_pwd = 'W!nt3r2015'
db = 'CMSandbox'
    
def orders(typec):
    qry = '''
    with x as (select symbol, liq_rank, rowin, ROW_NUMBER() OVER (PARTITION BY symbol order by rowin desc) as rnk 
    from eventstats.liq_rank_inout),
    y as (
    select *
    from x 
    where rnk = 1)
    select root_id, order_id, fill_id, fill_price, fill_quantity, cls_px, nbbo_bid, nbbo_ask, substring(security from 3) as security , side, 
    stock_last, order_state, order_state_filled_quantity, order_state_order_price, order_state_order_quantity,
    commission, cast(execution_time as text) as execution_time, stk_bid, stk_ask, street_stk_bid, street_stk_ask, (CASE WHEN side = 'Buy' then (cls_px - fill_price) else (fill_price - cls_px) END) * fill_quantity as pnl, c.liq_rank
    from events.fills a  
    left join eventstats.stk_clspx b on b.tradedate = cast(left(CAST(execution_time as text), 10) as date) and b.underlying = substring(security from 3)
    left join y c on c.symbol = substring(security from 3)
    where create_time between '20170101' and '20170124'
    and crossing_type= %s
    and actor_name='StockAlgoActor' 
    ''' % typec

    frame = psql.read_sql(qry, engine)[['execution_time', 'fill_id', 'fill_price', 'fill_quantity', 'cls_px', 'security', 'side', 'pnl', 'liq_rank']]
    frame.rename(columns = { 
                            'execution_time': 'time', 
                            'fill_id' : 'fill_id',
                            'fill_price' : 'price', 
                            'fill_quantity': 'qty', 
                            'cls_px': 'close',
                            'security': 'stock', 
                            'side': 'side',
                            'pnl': 'pnl',
                            'liq_rank': 'liq'}, inplace=True)
    frame['stock'] = frame['stock'].str.strip() 
    frame['rTime'] = frame['time'].apply(lambda x: '2016-11-01 ' + x[11:19])
    frame['rTime'] = pd.to_datetime(frame['rTime'])
    frame['rTime'] = frame['rTime'].apply(lambda dt: datetime.datetime(dt.year, dt.month, dt.day, dt.hour, 30 * (dt.minute // 30)))
    frame['rTime'] = frame['rTime'].apply(lambda dt: '%s:%s' % (dt.hour, dt.minute))
    frame['day'] = frame['time'].apply(lambda x: x[:10])
    frame['AbsPnl'] = abs(frame['close'] - frame['price']) * frame['qty']

    frame.reset_index(drop=True, inplace=True)
    return frame 

    
def connect_to_sql(host, usr, pwd, db):
    conn_str = 'mssql+pymssql://%s:%s@%s/%s' % (usr, pwd, host, db)
    engine = create_engine(conn_str, pool_size=5, pool_recycle=10)
    return engine
    
    
def time_report_stock(ts):
    print ts
    quantities = ts['qty']
    times = ts['rTime']

    total =  quantities.values.sum()
    print 'Total number of IGOOGI shares this month is %s' % total

    halfH = {}
    for x in range(len(times)):
        if times[x] in halfH:
            halfH[times[x]] += quantities[x] / total
        else:
            halfH[times[x]] =  quantities[x] / total
    print 'Time Breakdown - IGOOGI Stock volume as Percentage'
    print '8:30 - %s' % (halfH['8:30'] * 100)
    print '9:00 - %s' % (halfH['9:0'] * 100)
    print '9:30 - %s' % (halfH['9:30'] * 100)
    print '10:00 - %s' % (halfH['10:0'] * 100)
    print '10:30 - %s' % (halfH['10:30'] * 100)
    print '11:00 - %s' % (halfH['11:0'] * 100)
    print '11:30 - %s' % (halfH['11:30'] * 100)
    print '12:00 - %s' % (halfH['12:0'] * 100)
    print '12:30 - %s' % (halfH['12:30'] * 100)
    print '1:00 - %s' % (halfH['13:0'] * 100)
    print '1:30 - %s' % (halfH['13:30'] * 100)
    print '2:00 - %s' % (halfH['14:0'] * 100)
    print '2:30 - %s' % (halfH['14:30'] * 100)
    return 
    
def time_report_pnl(ts):
    pnl = ts['AbsPnl']
    times = ts['rTime']

    total =  np.nansum(pnl)

    halfH = {}
    for x in range(len(times)):
        if times[x] in halfH and math.isnan(pnl[x]) == False:
            halfH[times[x]] += pnl[x] / total
        elif math.isnan(pnl[x]) == False:
            halfH[times[x]] =  pnl[x] / total
    print 'Time Breakdown - IGOOGI Pnl Impact as Percentage, Taken from absolute Pnl'
    print '8:30 - %s' % (halfH['8:30'] * 100)
    print '9:00 - %s' % (halfH['9:0'] * 100)
    print '9:30 - %s' % (halfH['9:30'] * 100)
    print '10:00 - %s' % (halfH['10:0'] * 100)
    print '10:30 - %s' % (halfH['10:30'] * 100)
    print '11:00 - %s' % (halfH['11:0'] * 100)
    print '11:30 - %s' % (halfH['11:30'] * 100)
    print '12:00 - %s' % (halfH['12:0'] * 100)
    print '12:30 - %s' % (halfH['12:30'] * 100)
    print '1:00 - %s' % (halfH['13:0'] * 100)
    print '1:30 - %s' % (halfH['13:30'] * 100)
    print '2:00 - %s' % (halfH['14:0'] * 100)
    print '2:30 - %s' % (halfH['14:30'] * 100)
    return
    
    
def liquidity(typec):
    qry = '''
    with x as (select symbol, liq_rank, rowin, ROW_NUMBER() OVER (PARTITION BY symbol order by rowin desc) as rnk 
    from eventstats.liq_rank_inout),
    y as (
    select *
    from x 
    where rnk = 1),
    large as (select root_id, order_id, fill_id, fill_price, fill_quantity, cls_px, nbbo_bid, nbbo_ask, substring(security from 3) as security , side, 
    stock_last, order_state, order_state_filled_quantity, order_state_order_price, order_state_order_quantity,
    commission, execution_time, stk_bid, stk_ask, street_stk_bid, street_stk_ask, (CASE WHEN side = 'Buy' then (cls_px - fill_price) else (fill_price - cls_px) END) * fill_quantity as pnl, c.liq_rank
    from events.fills a  
    left join eventstats.stk_clspx b on b.tradedate = cast(left(CAST(execution_time as text), 10) as date) and b.underlying = substring(security from 3)
    left join y c on c.symbol = substring(security from 3)
    where create_time between '20170101' and '20170124'
    and crossing_type= %s
    and actor_name='StockAlgoActor')
    select CASE WHEN liq_rank between 1 and 250 then '1-250' 
    	WHEN liq_rank between 251 and 500 then '251-500'
    	WHEN liq_rank between 501 and 750 then '501-750'
    	WHEN liq_rank between 751 and 1000 then '751-1000'
    	WHEN liq_rank between 1001 and 1500 then '1001-1500'
    	WHEN liq_rank between 1501 and 2000 then '1501-2000'
    	else '2001+' END as liquidity, sum(pnl) as liq_pnl, sum(fill_quantity) as shares 
    from large 
    group by CASE WHEN liq_rank between 1 and 250 then '1-250' 
    	WHEN liq_rank between 251 and 500 then '251-500'
    	WHEN liq_rank between 501 and 750 then '501-750'
    	WHEN liq_rank between 751 and 1000 then '751-1000'
    	WHEN liq_rank between 1001 and 1500 then '1001-1500'
    	WHEN liq_rank between 1501 and 2000 then '1501-2000'
    	else '2001+' END
    order by CASE WHEN liq_rank between 1 and 250 then '1-250' 
    	WHEN liq_rank between 251 and 500 then '251-500'
    	WHEN liq_rank between 501 and 750 then '501-750'
    	WHEN liq_rank between 751 and 1000 then '751-1000'
    	WHEN liq_rank between 1001 and 1500 then '1001-1500'
    	WHEN liq_rank between 1501 and 2000 then '1501-2000'
    	else '2001+' END
    ''' % typec
    frame = psql.read_sql(qry, engine)[['liquidity', 'liq_pnl', 'shares']]
    frame.rename(columns = {'liquidity': 'liq',
                            'liq_pnl': 'pnl',
                            'shares': 'shares'}, inplace=True)
    frame.reset_index(drop=True, inplace=True)
    frame['liq'] = frame['liq'].str.strip() 
    print 'By liquidity'
    frame.set_index('liq', inplace=True)
    #fix the index afterwards
    frame.reindex(["'1-250'", "'251-500'", "501-750'", "'751-1000'", "'1001-1500'", "'1501-2000'", "'2001+'"])
    return frame 

    
def market_width(typec):
    #define market width buckets...could be penny wide, nickel wide, dime wide, dollar wide, over 3 dollars wide? 
    qry = '''
    with large as (select root_id, order_id, fill_id, fill_price, fill_quantity, cls_px, nbbo_bid, nbbo_ask, substring(security from 3) as security , side, 
    stock_last, order_state, order_state_filled_quantity, order_state_order_price, order_state_order_quantity,
    commission, execution_time, stk_bid, stk_ask, street_stk_bid, street_stk_ask, (CASE WHEN side = 'Buy' then (cls_px - fill_price) else (fill_price - cls_px) END) * fill_quantity as pnl
    from events.fills a  
    left join eventstats.stk_clspx b on b.tradedate = cast(left(CAST(execution_time as text), 10) as date) and b.underlying = substring(security from 3)
    where create_time between '20170101' and '20170124'
    and crossing_type= %s
    and actor_name='StockAlgoActor')
    select CASE WHEN (nbbo_ask - nbbo_bid <= .5) then '.01 - .5' 
    	WHEN (nbbo_ask - nbbo_bid > .5 and nbbo_ask - nbbo_bid <= 1) then '.51 - 1.00'
    	WHEN (nbbo_ask - nbbo_bid > 1 and nbbo_ask - nbbo_bid <= 3) then '1.01 - 3.00'
    	WHEN (nbbo_ask - nbbo_bid > 3 and nbbo_ask - nbbo_bid <= 5) then '3.01 - 5.00'
    	else '5.01+' END as final, sum(pnl) as pnl, sum(abs(pnl)) as AbsPnl, sum(fill_quantity) as shares 
    from large 
    group by CASE WHEN (nbbo_ask - nbbo_bid <= .5) then '.01 - .5' 
    	WHEN (nbbo_ask - nbbo_bid > .5 and nbbo_ask - nbbo_bid <= 1) then '.51 - 1.00'
    	WHEN (nbbo_ask - nbbo_bid > 1 and nbbo_ask - nbbo_bid <= 3) then '1.01 - 3.00'
    	WHEN (nbbo_ask - nbbo_bid > 3 and nbbo_ask - nbbo_bid <= 5) then '3.01 - 5.00'
    	else '5.01+' END
    order by CASE WHEN (nbbo_ask - nbbo_bid <= .5) then '.01 - .5' 
    	WHEN (nbbo_ask - nbbo_bid > .5 and nbbo_ask - nbbo_bid <= 1) then '.51 - 1.00'
    	WHEN (nbbo_ask - nbbo_bid > 1 and nbbo_ask - nbbo_bid <= 3) then '1.01 - 3.00'
    	WHEN (nbbo_ask - nbbo_bid > 3 and nbbo_ask - nbbo_bid <= 5) then '3.01 - 5.00'
    	else '5.01+' END
    ''' % typec
    frame = psql.read_sql(qry, engine)[['final', 'pnl', 'abspnl', 'shares']]
    frame.rename(columns = {'final': 'market',
                            'pnl': 'pnl',
                            'abspnl': 'absPnl',
                            'shares': 'shares'}, inplace=True)
    frame.reset_index(drop=True, inplace=True)
    print 'By Stock Market Width'
    frame['market'] = frame['market'].str.strip() 
    frame.set_index('market', inplace=True)
    #fix the index afterwards
    return frame 
    
    
def connect_to_sql(host, usr, pwd, db):
    conn_str = 'mssql+pymssql://%s:%s@%s/%s' % (usr, pwd, host, db)
    engine = create_engine(conn_str, pool_size=5, pool_recycle=10)
    return engine
    
    
def stock_std_dev(st, pnl):
    engine = connect_to_sql(sql1_host, sql1_user, sql1_pwd, db)
    query = """
    select Symbol, HistVol, VolAvg
    from igtdev..histVoltbl
    where PeriodType = 'CC' and histPeriod = '21'
    """
    frame = psql.read_sql(query, engine) [['Symbol', 'HistVol', 'VolAvg']]
    frame.rename(columns = {'Symbol' : 'Symbol',
                            'HistVolt': 'HistVol',
                            'VolAvg' : 'VolAvg'}, inplace=True)
    frame['Symbol'] = frame['Symbol'].str.strip() 
    frame.drop_duplicates(['Symbol', 'HistVol', 'VolAvg'], inplace=True)
    frame.reset_index(drop=True, inplace=True)
     
    vols = {}
    pointer = frame['Symbol']
    volPoint = frame['HistVol']
    avgPoint = frame['VolAvg']
    for x in range(len(pointer)):
        vols[pointer[x]] = (volPoint[x], avgPoint[x])
    
    vols_buckets = {}
    vols_buckets['1 - 20'] = (0, 0)
    vols_buckets['21 - 40'] = (0, 0)
    vols_buckets['41 - 60'] = (0, 0)
    vols_buckets['61 - 80'] = (0, 0)
    vols_buckets['81+'] = (0, 0)
    pnls =  pnl
    abspnl = abs(pnl)

    for y in range(len(st)):
        if st[y] in vols and math.isnan(pnls[y]) == False:
            (v1, v2) = vols[st[y]]
            if v1 <= 20:
                (t1 ,t2) = vols_buckets['1 - 20']
                t1 += pnls[y]
                t2 += abspnl[y]
                vols_buckets['1 - 20'] = (t1, t2)
            elif v1 > 20 and v1 <= 40: #when count get to here
                (t1, t2) = vols_buckets['21 - 40']
                t1 += pnls[y]
                t2 += abspnl[y]
                vols_buckets['21 - 40'] = (t1, t2)
            elif v1 > 40 and v1 <= 60:
                (t1, t2) = vols_buckets['41 - 60']
                t1 += pnls[y]
                t2 += abspnl[y]
                vols_buckets['41 - 60'] = (t1, t2)
            elif v1 > 60 and v1 < 80:
                (t1, t2) = vols_buckets['61 - 80']
                t1 += pnls[y]
                t2 += abspnl[y]
                vols_buckets['61 - 80'] = (t1, t2)
            else:
                (t1, t2) = vols_buckets['81+']
                t1 += pnls[y]
                t2 += abspnl[y]
                vols_buckets['81+'] = (t1, t2)

    print '21 day CC Stock Volatility Breakdown - Pnl including absolute'
    print '1 - 20: %f, %f' % vols_buckets['1 - 20']
    print '21 - 40: %f, %f' % vols_buckets['21 - 40']
    print '41 - 60: %f, %f' % vols_buckets['41 - 60']
    print '61 - 80: %f, %f' % vols_buckets['61 - 80']
    print '81+: %f, %f' % vols_buckets['81+']

    return frame
    

def day_report_Pnl(ts):
    pnl = ts['pnl']

    npd = ts.groupby('day').sum()
    days = npd.index 
    dayPnl = npd['AbsPnl']
    total = npd['AbsPnl'].values.sum() 
    
    print 'Total Absolute Pnl (excluding saved fees, etc) for the month'
    print total
    
    dayMM = {}
    for x in range(len(dayPnl)):
        dayMM[str(days[x])] =  dayPnl[x] / total
        print '%s: ' % str(days[x]) + '{:.2%}'.format(dayMM[str(days[x])]) + ', %s' % dayPnl[x]
    return 
    
    
if __name__ == '__main__':
    #name for the Diego section that you want ONLY CHANGE WHERE IGOOGI IS
    quoter = "'%s'" % 'IGOOGI'
    
    #raw order information 
    allOrders = orders(quoter)
    
#    #gets the liquidity breakdown 
#    print liquidity(quoter)
#    print '\n'
#    #stock market width breakdown 
#    print market_width(quoter)
#    print '\n'
#    #Stock 21 day CC volatility breakdown 
#    stock_std_dev(allOrders['stock'], allOrders['pnl'])
#    print '\n'
#    #day breakdown of pnl impact
    time_report_pnl(allOrders)
    print '\n'
    #time breakdown of stock orders
   # time_report_stock(allOrders)
    
    
    