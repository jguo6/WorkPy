# -*- coding: utf-8 -*-
"""
Created on Wed Jan 18 14:26:31 2017

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


def fills():
    qry = '''
    with fills as (select root_id, LEFT(CAST(execution_time as text), 19) as execution_time, ROW_NUMBER() OVER (PARTITION BY root_id order by create_time asc) as rnk
    from events.fills_201611 
    where order_state = 'FILLED' and security like 'O:%%')
    select execution_time, root_id
    from fills f
    where f.rnk = 1
    '''
    frame = psql.read_sql(qry, engine)[['root_id', 'execution_time']]
    frame.rename(columns = {'execution_time': 'time',
                            'root_id': 'root'}, inplace=True)
    frame.reset_index(drop=True, inplace=True)
    frame['time'] = frame['time'].apply(lambda x: '2016-11-01 ' + x[11:19])
    frame['time'] = pd.to_datetime(frame['time'])
    frame['newTime'] = frame['time'].apply(lambda dt: datetime.datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute, 30 * (dt.second // 30)))
    day_times = {}
    frame['newTime'] = frame['newTime'].apply(lambda dt: '%s:%s:%s' % (dt.hour, dt.minute, dt.second))
    for stamp in frame['newTime']:
        if stamp in day_times:
            day_times[stamp] += 1
        else:
            day_times[stamp] = 1
    return day_times

def openOrders():
    qry = '''
    with something as (select root_id, LEFT(CAST(create_time as text), 19) as create_time, ROW_NUMBER() OVER(PARTITION BY root_id order by execution_time asc) as rk  
    from events.fills_201611 
    where security like 'O:%%' and order_state = 'OPEN')
    select s.*
    from something s
    where s.rk = 1
    limit 100
    '''
    frame = psql.read_sql(qry, engine)[['root_id', 'create_time']]
    frame.rename(columns = {'create_time': 'time',
                            'root_id': 'root'}, inplace=True)
    frame.reset_index(drop=True, inplace=True)
    frame['time'] = frame['time'].apply(lambda x: '2016-11-01 ' + x[11:19])
    frame['time'] = pd.to_datetime(frame['time'])
    frame['newTime'] = frame['time'].apply(lambda dt: datetime.datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute, 30 * (dt.second // 30)))
    day_times = {}
    frame['newTime'] = frame['newTime'].apply(lambda dt: '%s:%s:%s' % (dt.hour, dt.minute, dt.second))
    for stamp in frame['newTime']:
        if stamp in day_times:
            day_times[stamp] += 1
        else:
            day_times[stamp] = 1
    return day_times


def cancels():
    qry = '''
    with cancels as (select root_id, LEFT(CAST(create_time as text), 19) as create_time, ROW_NUMBER() OVER (PARTITION BY root_id order by create_time asc) as rnk
    from events.xi_201611
    where actor_name = 'OptionXI' and end_state = 'CANCELED')
    select create_time, root_id 
    from cancels c
    where c.rnk = 1
    '''
    frame = psql.read_sql(qry, engine)[['root_id', 'create_time']]
    frame.rename(columns = {'create_time': 'time',
                            'root_id': 'root'}, inplace=True)
    frame.reset_index(drop=True, inplace=True)
    frame['time'] = frame['time'].apply(lambda x: '2016-11-01 ' + x[11:19])
    frame['time'] = pd.to_datetime(frame['time'])
    frame['newTime'] = frame['time'].apply(lambda dt: datetime.datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute, 30 * (dt.second // 30)))

    day_times = {}
    frame['newTime'] = frame['newTime'].apply(lambda dt: '%s:%s:%s' % (dt.hour, dt.minute, dt.second))
    for stamp in frame['newTime']:
        if stamp in day_times:
            day_times[stamp] += 1
        else:
            day_times[stamp] = 1
    return day_times



    

def calculations(fill, cancel):
    fill_cancel = {}
    fill_total = {}
    for x in fill:
        if x in cancel:
            fill_cancel[x] = fill[x] / cancel[x]
            fill_total[x] = fill[x] / (cancel[x] + fill[x])
        else:
            fill_total[x] = 1

    print fill_cancel
    return fill_cancel 
    
    
def dollar_delta():
    #
    return 
    
    
if __name__ == "__main__" :
#    f = fills()
#    c = cancels()
#    y = calculations(f, c)
    print liquidity()