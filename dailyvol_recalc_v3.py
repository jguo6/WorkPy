import pandas as pd, pandas.io.sql as psql
import numpy as np
import math 
import pyodbc
from datetime import date
from pycake import DataMap, SQLManager

def pull_raw_earns(startdt, enddt):

    connstr = 'DRIVER={SQL Server Native Client 11.0}; SERVER=pvwchi6psql1; Database=master; Trusted_Connection=yes'
    connection = pyodbc.connect(connstr)

    query = """
    WITH
    syms As (
    SELECT StockSymbol,
        CompanyID
    FROM [companies].[dbo].[tblStocks])

    SELECT syms.StockSymbol,
        cast(earns.[Date]  as DATE) AS dt, DateType 
    FROM [companies].[dbo].[tblEarnings] earns (NOLOCK)
    LEFT JOIN syms
    ON earns.CompanyID = syms.CompanyID
    WHERE (syms.StockSymbol != '')
        AND earns.[Date] BETWEEN '{0}' AND '{1}'
        AND earns.IsDeleted != 1
        AND earns.Confirmed = 1  
    ORDER BY StockSymbol, dt
    """.format(startdt, enddt)

    frame = psql.read_sql(query, connection)[['dt', 'StockSymbol', 'DateType']]
    
    frame.rename(columns={'dt': 'TradeDate',
                          'StockSymbol': 'Symbol',
                          'DateType' : 'BMO/AMC'},
                 inplace=True)   
    frame['Symbol'] = frame['Symbol'].str.strip() 
    frame = frame[(frame['TradeDate'] >= startdt) & (frame['TradeDate'] <= enddt)] #just gets dates between start and end as requested 
    frame.drop_duplicates(['TradeDate', 'Symbol'], inplace=True) 
    frame.reset_index(drop=True, inplace=True)
    frame['Earn'] = 1
    
    return frame

def get_stock(tick, start, end):

    connstr = 'DRIVER={SQL Server Native Client 11.0}; SERVER=pvwchi6psql1; Database=master; Trusted_Connection=yes'
    connection = pyodbc.connect(connstr)

    query = """
    SELECT TradeDate,
        [AdjClose] AS "Close"
    FROM [igtdev].[dbo].[tblStockHistory]
    WHERE Symbol = '{0}'
        AND TradeDate BETWEEN '{1}' AND '{2}' 
    ORDER BY TradeDate ASC
    """.format(tick, start, end)

    return psql.read_sql(query, connection)

    
def get_stocks(startdt, enddt):

    connstr = 'DRIVER={SQL Server Native Client 11.0}; SERVER=pvwchi6psql1; Database=master; Trusted_Connection=yes'
    connection = pyodbc.connect(connstr)

    query = """
    SELECT CAST(TradeDate AS DATE) TradeDate,
        Symbol,
        [AdjClose] AS "Close"
    FROM [igtdev].[dbo].[tblStockHistory]
    WHERE TradeDate BETWEEN '{0}' AND '{1}'
    ORDER BY TradeDate ASC
    """.format(startdt, enddt)

    frame = psql.read_sql(query, connection)
    frame['Symbol'] = frame['Symbol'].str.strip()
    return frame

def fix_earns(gp):

    inds = np.array(np.where(gp['Earn'].values==1)) #adding in the conditional into here filters out the values and gives you back the keys in list format 
    amc = np.array(np.where(gp['BMO/AMC'].values == 'AMC'))
    bmo = np.array(np.where(gp['BMO/AMC'].values == 'BMO'))
    

    if len(bmo) > 0:
        #if  len(bmo[0]) > 0:
        preearn = bmo - 1   
        preearn = preearn[preearn >= 0] 
        gp['Earn'].iloc[preearn] = 1 

    if len(amc) > 0:
        #(len(amc[0])) > 0:
        if len(amc[0]) > 0:
            postpostearn = amc + 2
            postpostearn = postpostearn[postpostearn >= 0]
            if postpostearn[-1] < len(gp.index):
                gp['Earn'].iloc[postpostearn] = 1
       
    postearn = inds + 1
    postearn = postearn[postearn < len(gp)]
    gp['Earn'].iloc[postearn] = 1

    gp['LogRets'] = np.log(gp['Close'] / gp['Close'].shift(1))

    return gp


def DM_vol(series):
    #x number of days back 
    sum = []

    for item in series:
        x = np.nansum(np.power(item, 2))
        sum.append(round(math.sqrt(x / len(item)) * math.sqrt(252) * 100, 2)) 
    return np.asarray(sum)
    

def DM_vol_w(series):
    sum = []

    for item in series:
        x = np.nansum(np.power(item, 2))
        sum.append(round(math.sqrt(x / len(item)) * math.sqrt(52) * 100, 2)) 
    return np.asarray(sum)
    
    
def calc_log_vols(ser):
    stk = ser.values
    #
    period = [20, 40, 60, 125, 250] 
    daily = []
    weekly = []
    for i in range(len(period)): 
        daily.append(stk[-period[i]:])
        temp_r_sum = pd.rolling_sum(stk[-(period[i]+5):], 5) 
        temp_wback = temp_r_sum[-period[i]:]
        
        k = period[i] / 5 
        weekly.append(temp_wback[k::5])

    vols_d = DM_vol(daily)
    vols_w = DM_vol_w(weekly)

    vols_all = np.concatenate([vols_d, vols_w])
    return pd.Series(vols_all, index=['RV20', 'RV40', 'RV60', 'RV125', 'RV250', 'WK20', 'WK40', 'WK60', 'WK125', 'WK250']) #prints out two values, daily and weekly 


if __name__ == '__main__':
    #what 
    startdt = date(2015, 11, 25)
    enddt = date.today() 
    earns = pull_raw_earns(startdt, enddt); 
    stocks = get_stocks(startdt, enddt); 

    merged = stocks.merge(earns, on=['Symbol', 'TradeDate'], how='left')    
    merged.sort_values(by = ['Symbol', 'TradeDate'], ascending=True, inplace=True)

    grouped = merged.groupby(['Symbol']).apply(fix_earns)                #applies the earnings to days around the earnings date with helper function 
    grouped['Earn'].fillna(0, inplace=True) 

    censored = grouped[grouped['Earn']==0]  #returns back to you the days with no earnings based on grouping 

    volDM = censored.groupby(['Symbol'])['LogRets'].apply(calc_log_vols).reset_index()
    volDM = volDM.pivot('Symbol', 'level_1', 'LogRets')  #changes table so column 2's values are indexes and 3's are values 
    volDM = volDM[['RV20', 'RV40', 'RV60', 'RV125', 'RV250', 'WK20', 'WK40', 'WK60', 'WK125', 'WK250']]

    map_name = 'DM_JGWeekly'
    data_map = DataMap(map_name, True, True)
    data_map.notify(data=volDM)
    data_map.close()
    print volDM
