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
    #filled orders, simple count given each 30 second rolling window 
    qry = '''
    with fills as (select root_id, LEFT(CAST(execution_time as text), 19) as execution_time, ROW_NUMBER() OVER (PARTITION BY root_id order by create_time asc) as rnk
    from events.fills_201611 
    where security like 'O:%%')
    select execution_time, root_id
    from fills f
    where f.rnk = 1
    ''' 
    #
    frame = psql.read_sql(qry, engine)[['root_id', 'execution_time']]
    frame.rename(columns = {'execution_time': 'time',
                            'root_id': 'root'}, inplace=True)
    frame.reset_index(drop=True, inplace=True) #
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
    print frame
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
    #cancels based on contract per count for each 30 second window 
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

    
def dollar_delta_fills():
    qry = '''
    with orders as (select root_id, LEFT(CAST(execution_time as text), 19) as execution_time, order_id, fill_price, fill_quantity, security, side, stock_last, order_state, order_state_filled_quantity, order_state_order_price
    from events.fills_201611
    where security like 'O:%%'), 
    deltas as (select root_id, delta, greek_spot_price
    from events.greeks_201611
    where security_id like 'O:%%' and from_pid = 0)
    select x.root_id, x.execution_time, (fill_quantity * delta * stock_last) as DollarDelta, x.order_id, x.fill_price, x.fill_quantity, x.security, x.side, x.stock_last, x.order_state_filled_quantity, x.order_state_order_price, y.delta, y.greek_spot_price
    from orders x
    left join deltas y on x.root_id = y.root_id
    '''
    frame = psql.read_sql(qry, engine)[['root_id', 'execution_time', 'dollardelta', 'order_id', 'fill_price', 'fill_quantity', \
         'security', 'side', 'stock_last', 'order_state_filled_quantity', 'order_state_order_price']]
    frame.rename(columns = {'root_id': 'root_id',
                            'execution_time': 'time',
                            'dollardelta': 'dollardelta',
                            'order_id': 'order_id',
                            'fill_price': 'fill_price',
                            'fill_quantity': 'fill_quantity',
                            'security': 'security',
                            'side': 'side',
                            'stock_last': 'stock_last',
                            'order_state_filled_quantity': 'order_state_filled_quantity',
                            'order_state_order_price': 'order_state_order_price'}, inplace=True)
    frame.reset_index(drop=True, inplace=True)
    frame['time'] = frame['time'].apply(lambda x: '2016-11-01 ' + x[11:19])
    frame['time'] = pd.to_datetime(frame['time'])
    frame['newTime'] = frame['time'].apply(lambda dt: datetime.datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute, 30 * (dt.second // 30)))
    frame['absddelta'] = abs(frame['dollardelta'])

    day_times = {}
    frame['newTime'] = frame['newTime'].apply(lambda dt: '%s:%s:%s' % (dt.hour, dt.minute, dt.second))
    totaldeltas = frame['dollardelta'].values.sum() 
    for x in range(len(frame['newTime'])):
        if frame['newTime'][x] in day_times:
            day_times[frame['newTime'][x]] += frame['dollardelta'][x] / totaldeltas 
        else:
            day_times[frame['newTime'][x]] = frame['dollardelta'][x] / totaldeltas 
    print day_times

    return frame


def dollar_delta_breakdown(frame):
    totalDeltas = frame['dollardelta'].values
    totalDeltas = totalDeltas.sum()
    
    #dayView = frame.groupby(['rDay']).sum()
    #gets dollar deltas as a breakdown of the total, maybe should also group it by day and get that percentage as well? 
    window = {} #How to do it 
    for x in range(len(frame['dollardelta'])):
        if frame['newTime'][x] in window:
            print 'this is the dollar delta'
            print frame['dollardelta'][x]
            window[frame['newTime'][x]] += frame['dollardelta'][x] / totalDeltas
        else:
            print 'we are inserting something new into the dictionary'
            print frame['dollardelta'][x]
            window[frame['newTime'][x]] = frame['dollardelta'][x] / totalDeltas
    print window 
    return window
    #
    
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
    
    #stop running around 15 minutes, 
    
if __name__ == "__main__" :
#    f = fills()
#    c = cancels()
#    y = calculations(f, c)
    dollar_delta_frame = dollar_delta_fills()
    dollar_delta_breakdown(dollar_delta_frame)
    #frame breakdown and then convert it to dataframe format