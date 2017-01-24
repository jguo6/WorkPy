# -*- coding: utf-8 -*-
"""
Created on Tue Jan 03 13:34:33 2017

@author: jguo
"""
import pandas as pd, pandas.io.sql as psql
import pyodbc
import csv 

def pull_prices():
    connstr = 'DRIVER={SQL Server Native Client 11.0}; SERVER=pvwchi6psql1; Database=master; Trusted_Connection=yes'
    connection = pyodbc.connect(connstr)
    
    query = """
    with tradeDays as (select tradedate, rank() OVER (Order By TradeDate desc) as rank
    from [igtdev].[dbo].[tblStockHistory]
    group by tradedate)
    select Symbol, x.Tradedate, LEFT(AdjHigh, charindex('.', AdjHigh) + 2) as AdjHigh, LEFT(AdjLow, charindex('.', AdjLow) + 2) as AdjLow 
    from [igtdev].[dbo].[tblStockHistory] x
    join tradeDays y on x.TradeDate = y.TradeDate
    where y.rank <= 500 and charindex('_', symbol) = 0 and AdjHigh is not null and AdjLow is not null 
    order by symbol asc, tradedate asc
    """
    frame = psql.read_sql(query, connection)[['Symbol', 'Tradedate', 'AdjHigh', 'AdjLow']]
    frame.rename(columns = {'Symbol' : 'Symbol',
                            'Tradedate': 'TradeDate',
                            'AdjHigh' : 'High',
                            'AdjLow' : 'Low'}, inplace=True)
    frame['Symbol'] = frame['Symbol'].str.strip() 
    frame.drop_duplicates(['Symbol', 'TradeDate', 'High', 'Low'], inplace=True)
    frame.reset_index(drop=True, inplace=True)
    return frame 
    

def stock_dict():
    prices = pull_prices() 
    highs = {}
    lows = {}

    for x in prices.iterrows():
        if x[1]['Symbol'] in highs:
            highs[x[1]['Symbol']].append(float(x[1]['High']))
            
        if x[1]['Symbol'] in lows:
            lows[x[1]['Symbol']].append(float(x[1]['Low']))
            
        if x[1]['Symbol'] not in highs and x[1]['Symbol'] not in lows:
            temp = []
            temp2 = []
            temp.append(float(x[1]['High']))
            highs[x[1]['Symbol']] = temp
            
            temp2.append(float(x[1]['Low']))
            lows[x[1]['Symbol']] = temp2

    return highs, lows


if __name__ == '__main__':
    high, low = stock_dict()
    
    #writing to the highs.csv and lows.csv files which 
    #should be all empty before writing 
    with open('highs.csv', 'wb') as csv_file:
        writer = csv.writer(csv_file)
        for key, value in high.items():
            writer.writerow([key, value])
            
    with open('lows.csv', 'wb') as csv_file:
        writer = csv.writer(csv_file)
        for key, value in low.items():
            writer.writerow([key, value])

    