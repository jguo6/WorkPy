import pandas as pd, pandas.io.sql as psql
import numpy as np
import pyodbc
from datetime import date


def pull_trade_pnl():
    connstr = 'DRIVER={SQL Server Native Client 10.0}; SERVER=pvwchi6psql1; Database=master; Trusted_Connection=yes'
    connection = pyodbc.connect(connstr)

    query = """
    SELECT LastUpdated as dt
    	,UnderlyingSymbol as Symbol
    	,Liq_Rank 
    	,td.initials as Account
      ,td.name
    	,SUM(Gamma_C) as Gamma_C
    	,SUM(ThetaCNew) as ThetaCNew
      ,(1 - PercentChange) * 100 as Change
      ,bt.sectorName as Sector
      FROM [CMSandbox].[dbo].[tblPnLPositionHistory] pnl 
      left join CMSandbox.dbo.tblLiquidityRankings t on (pnl.LastUpdated between t.rowin and t.rowout) and pnl.UnderlyingSymbol = t.symbol
      left join [trades].[dbo].[tblTraders] td on td.initials  = (CASE WHEN CHARINDEX('.', Account) = 0 then Account else 
                                                                 LEFT(Account, CHARINDEX('.', Account) - 1) END)
      left join [igtdev].[dbo].[tblStockHistory] x on CAST(convert(date, x.tradedate, 103) as DATETIME) = CAST(convert(date, pnl.lastUpdated, 103) as DATETIME)
      left join from companies.dbo.tblStockBeta bt on bt.stocksymbol = pnl.underlyingsymbol
      Where SecurityType != 'S' and ThetaCNew is not null and x.symbol = 'SPY' 
          and (Trader = 1 and StopDate is null and td.Name not like '%Strategies%' and td.Name not like '%Seven%' 
            and td.name not like '%Desk%' and td.Name not like '%Firm%' and td.Name like '% %'
            and td.Name not like '%early%' and td.Name not like '%maker%' and td.Name not like '%PXN%' 
            and td.Name not like '%execution%' and td.Name not like '%Basket%' and td.Name not like '%group%'
            and td.Name not like '%Account%' and td.Name not like '%holding%' and td.Name not like '%lots%' 
            and td.Name not like '%Autobuilder%' and td.Name not like '%brokerage%') or td.Name = 'Steve Lockwood'
      Group by LastUpdated, td.initials, td.name, UnderlyingSymbol, Liq_Rank, PercentChange, bt.sectorName
    """
    
    frame = psql.read_sql(query, connection)[['dt', 'Symbol', 'Liq_Rank', 'Account', 'name', 'Gamma_C', 'ThetaCNew', 'Change', 'Sector']]
    frame.rename(columns = {'dt': 'TradeDate',
                            'Symbol' : 'Symbol',
                            'Liq_Rank': 'Liq',
                            'Account': 'Account',
                            'name': 'Name',
                            'Gamma_C' : 'Gamma_C',
                            'ThetaCNew' : 'Theta_C_New',
                            'Change': 'Change',
                            'Sector': 'Sector'}, inplace=True)
    frame['Symbol'] = frame['Symbol'].str.strip() 
    frame.drop_duplicates(['TradeDate', 'Symbol', 'Liq', 'Account', 'Name', 'Gamma_C', 'Theta_C_New', 'Change', 'Sector'], inplace=True)
    frame.reset_index(drop=True, inplace=True)
    return frame 

    
def pull_traders():
    
    leaps = ['Chad Evans', 'Brian Muller', 'Marc Rothman', 'Ryo Saotome', 'Andy Grigus', 'Nic ' + "O'" + 'Connor', 'Stephen Landefeld', 'Ali Amjad']
    convexity = ['Tom Simpson', 'Neel Shah', 'Steve Lockwood', 'Roxy Rong', 'Matt MacFarlane']
    
    connstr = 'DRIVER={SQL Server Native Client 10.0}; SERVER=pvwchi6psql1; Database=master; Trusted_Connection=yes'
    connection = pyodbc.connect(connstr)

    query = """
    SELECT Name, Initials as Account
    FROM [trades].[dbo].[tblTraders]
    where (Trader = 1 and StopDate is null and Name not like '%Strategies%' and Name not like '%Seven%' 
        and name not like '%Desk%' and Name not like '%Firm%' and Name like '% %'
        and Name not like '%early%' and Name not like '%maker%' and Name not like '%PXN%' 
        and Name not like '%execution%' and Name not like '%Basket%' and Name not like '%group%'
        and Name not like '%Account%' and Name not like '%holding%' and Name not like '%lots%' 
        and Name not like '%Autobuilder%' and Name not like '%brokerage%') or Name = 'Steve Lockwood'
    order by Initials asc
    """
    
    frame = psql.read_sql(query, connection)[['Name', 'Account']]
    frame['Name'] = frame['Name'].str.strip() 
    lps = frame.loc[frame['Name'].isin(leaps)]
    cxty = frame.loc[frame['Name'].isin(convexity)]
    
    return lps, cxty


def pull_cat():
    connstr = 'DRIVER={SQL Server Native Client 10.0}; SERVER=pvwchi6psql1; Database=master; Trusted_Connection=yes'
    connection = pyodbc.connect(connstr)

    query = """
    SELECT LastUpdated as dt 
        ,Account
        ,sum(Gamma_C) as Gamma_C
        ,sum(ThetaCNew) as ThetaCNew
    FROM [CMSandbox].[dbo].[tblPnLPositionHistory]
    Where SecurityType != 'S' and ThetaCNew is not null and account LIKE '%AKA%'
    group by LastUpdated, Account
    """
    
    frame = psql.read_sql(query, connection)[['dt', 'Account', 'Gamma_C', 'ThetaCNew']]
    frame.rename(columns = {'dt': 'TradeDate',
                            'Account': 'Account',
                            'Gamma_C' : 'Gamma_C',
                            'ThetaCNew' : 'Theta_C_New'}, inplace=True)
    frame.drop_duplicates(['TradeDate', 'Account', 'Gamma_C', 'Theta_C_New'], inplace=True)
    frame.reset_index(drop=True, inplace=True)
    return frame 


def pull_industries():
    sector_1 = ['Communications', 'Technology', 'Energy', 'Industrials', 'Materials']
    sector_2 = ['Consumer Discretionary', 'Consumer Staples', 'Financial', 'Healthcare', 'Utilities']
    
    connstr = 'DRIVER={SQL Server Native Client 10.0}; SERVER=pvwchi6psql1; Database=master; Trusted_Connection=yes'
    connection = pyodbc.connect(connstr)

    query = """
     select StockSymbol, SectorName
     from companies.dbo.tblStockBeta
    """
    
    frame = psql.read_sql(query, connection)[['StockSymbol', 'SectorName']]
    frame.rename(columns = {'StockSymbol': 'Symbol', 
                            'SectorName': 'Sector'}, inplace=True)
    frame['Symbol'] = frame['Symbol'].str.strip() 
    frame['Sector'] = frame['Sector'].str.strip()
    s1 = frame.loc[frame['Sector'].isin(sector_1)]
    s2 = frame.loc[frame['Sector'].isin(sector_2)]
    
    return s1, s2


def get_stocks(startdt, enddt):

    connstr = 'DRIVER={SQL Server Native Client 10.0}; SERVER=pvwchi6psql1; Database=master; Trusted_Connection=yes'
    connection = pyodbc.connect(connstr)

    query = """
    SELECT CAST(TradeDate AS DATE) TradeDate,
        Symbol,
        [AdjClose] AS "Close"
    FROM [igtdev].[dbo].[tblStockHistory]
    WHERE TradeDate BETWEEN '{0}' AND '{1}' and symbol = 'AAPL'
    ORDER BY TradeDate ASC
    """.format(startdt, enddt)

    frame = psql.read_sql(query, connection)
    frame['Symbol'] = frame['Symbol'].str.strip()
    return frame


def fix_earns(gp):

    gp.sort(['TradeDate'], ascending=True, inplace=True)
    inds = np.array(np.where(gp['Earn'].values==1))
    preearn = inds - 1
    preearn = preearn[preearn >= 0]
    postearn = inds + 1
    postearn = postearn[postearn < len(gp)]

    gp['Earn'].iloc[preearn] = 1
    gp['Earn'].iloc[postearn] = 1

    gp['LogRets'] = np.log(gp['Close'] / gp['Close'].shift(1))
    return gp


def calc_log_vols(ser):
    stk = ser.values

    daily = stk[-period:]
    print daily 
    daily = np.std(daily, ddof=1) * np.sqrt(252.0)

    weekly = pd.rolling_sum(stk[-(period+5):], 5) #gets the rolling sum for every 5 days 
    print weekly 
    weekly = weekly[-period:] #gets that number of days back
    weekly = weekly[4::5] #starting from the 
    print weekly 
    weekly = np.std(weekly, ddof=1) * np.sqrt(52)
    return pd.Series([daily, weekly], index=['daily', 'weekly'])


if __name__ == '__main__':

#    leaps, convexity = pull_traders()
#    leaps.reset_index(drop=True, inplace=True)
#    convexity.reset_index(drop=True, inplace=True)
#
#    catalyst = pull_cat()
#    sector_1, sector_2 = pull_industries()
#    sector_1.reset_index(drop=True, inplace=True)
#    sector_2.reset_index(drop=True, inplace=True)

    
    trades = pull_trade_pnl()
#    lg = trades.merge(leaps, on=['Account'], how='left')
#    cx = trades.merge(convexity, on=['Account'], how='left')
    
    print trades
#    cx = leaps.merge(convexity, on=['Account'], how='left')
#    print cx
#    s1 = cx.merge(sector_1, on=['Symbol'], how='left')
#    s2 = s1.merge(sector_2, on=['Symbol'], how='left')
#    
#    lg = lg.loc[lg['Group'] == 'leaps'] 
#    cx = cx.loc[cx['Group'] == 'convex']
#    s1 = s1.loc[s1['Group'] ]
#    print s2
#    



#
#    merged = stocks.merge(earns, on=['TradeDate', 'Symbol'], how='left'); 
#    grouped = merged.groupby(['Symbol']).apply(fix_earns); 
#    grouped['Earn'].fillna(0, inplace=True)
#
#    censored = grouped[grouped['Earn']==0]
#
#    period = 20
#    vol20 = censored.groupby(['Symbol'])['LogRets'].apply(calc_log_vols).reset_index()
#    print vol20
#    vol20 = vol20.pivot('Symbol', 'level_1', 0)


