import pandas as pd, pandas.io.sql as psql
import numpy as np
import pyodbc
from datetime import date
from scipy import stats 
import statsmodels.api as sm
import matplotlib.pyplot as plt
import random 

def sample():
    connstr = 'DRIVER={SQL Server Native Client 10.0}; SERVER=pvwchi6psql1; Database=master; Trusted_Connection=yes'
    connection = pyodbc.connect(connstr)
    
    query = """
    SELECT CAST(convert(date, x.LastUpdated, 103) as datetime) as dt
    ,x.UnderlyingSymbol as Symbol 
    ,t.Liq_Rank
    ,SUM(Gamma_C) as Gamma_C
    ,SUM(case when x.ThetaCNew IS null then gg.thetaCNew else x.ThetaCNew END) as ThetaCNew
    ,sum(gg.blendedVega) as Vega
    ,(CASE WHEN CHARINDEX('.', x.Account) = 0 
      then x.Account else LEFT(x.Account, CHARINDEX('.', x.Account) - 1) END) as Initials
    ,y.Name 
    ,b.SectorName
    ,DaysToExp as Exp
    FROM [CMSandbox].[dbo].[tblPnLPositionHistory] x
    left join [positions].[dbo].[tblPnLPositionHistory_NG] gg on gg.PosTradeID = x.PosTradeID
    left join CMSandbox.dbo.tblLiquidityRankings t on t.Symbol = x.UnderlyingSymbol and (x.LastUpdated between t.RowIn and t.RowOut)
    left join [trades].[dbo].[tblTraders] y on y.Initials = (CASE WHEN CHARINDEX('.', x.Account) = 0 
      then x.Account else LEFT(x.Account, CHARINDEX('.', x.Account) - 1) END)
    left join companies.dbo.tblStockBeta b on b.StockSymbol = x.UnderlyingSymbol and (b.ProcessDate = CAST(convert(date, x.LastUpdated, 103) as datetime))
    where (x.ThetaCNew is not null or gg.thetaCNew is not null) and x.SecurityType != 'S' and x.account not like '%SVN%'and x.account not like '%BBE%' and x.type = 'P' 
    group by x.LastUpdated, Liq_Rank, x.UnderlyingSymbol, (CASE WHEN CHARINDEX('.', x.Account) = 0 
      then x.Account else LEFT(x.Account, CHARINDEX('.', x.Account) - 1) END), name, SectorName, DaysToExp
    order by x.LastUpdated desc
    """
    frame = psql.read_sql(query, connection)[['dt', 'Symbol', 'Liq_Rank', 'Gamma_C', 'ThetaCNew', \
                                             'Vega', 'Initials', 'Name', 'SectorName', 'Exp']]
    frame.rename(columns = {'dt': 'TradeDate',
                            'Symbol' : 'Symbol',
                            'Liq_Rank': 'Liq',
                            'Gamma_C' : 'Gamma_C',
                            'ThetaCNew' : 'Theta_C_New',
                            'Vega': 'TWVega',
                            'Initials': 'Account',
                            'Name': 'Name',
                            'SectorName': 'Sector',
                            'Exp': 'Exp'}, inplace=True)
    frame['Symbol'] = frame['Symbol'].str.strip() 
    frame['Account'] = frame['Account'].str.strip()
    frame['Sector'] = frame['Sector'].str.strip() 
    frame.drop_duplicates(['TradeDate', 'Symbol', 'Liq', 'Gamma_C', 'Theta_C_New', \
                        'TWVega', 'Account', 'Name', 'Sector', 'Exp'], inplace=True)
    frame['TWVega'] = frame['TWVega'].shift(1)
    frame.reset_index(drop=True, inplace=True)
    return frame 
    

def pull_spy():
    
    connstr = 'DRIVER={SQL Server Native Client 10.0}; SERVER=pvwchi6psql1; Database=master; Trusted_Connection=yes'
    connection = pyodbc.connect(connstr)

    query = """
      select CAST(convert(date, TradeDate, 103) as datetime) as dt
          ,AdjClose 
     from [igtdev].[dbo].[tblStockHistory]
     where Symbol = 'SPY' and TradeDate > '2015-08-01'
    """
    
    frame = psql.read_sql(query, connection)[['dt', 'AdjClose']]
    frame.rename(columns = {'dt': 'TradeDate', 
                            'AdjClose': 'Close'}, inplace=True)
    
    frame['Ret'] = frame['Close'] / frame['Close'].shift(1) - 1

    return frame


def k(x, x_grid, band, spy, fran, vg, gp):
    kde = sm.nonparametric.KDEMultivariateConditional(endog = x, exog = x_grid, dep_type='c', indep_type='c', bw=[band, band])  
    endog = np.arange(-3.0, 3.0, 0.01)  
    ys = kde.pdf(exog_predict=[spy]*len(endog), endog_predict=endog)
    
    prob_ys = ys / np.nansum(ys)
    mean_val = np.nansum(prob_ys * endog)
    sd = np.sqrt(np.nansum(endog**2 * prob_ys) - mean_val**2)

    perc_ys = kde.cdf(exog_predict=[spy]*len(endog), endog_predict=endog)

    idx_25 = np.abs(perc_ys - 0.25).argmin()
    val_25 = endog[idx_25]
    med_idx = np.abs(perc_ys - 0.5).argmin()
    med_val = endog[med_idx]
    idx_75 = np.abs(perc_ys - 0.75).argmin()
    val_75 = endog[idx_75]
    
    print 'Group: %s' % gp 
    print 'Franchise: %s' % fran
    print 'Expected franchise: %s' % (mean_val * vg)
    print '1 SD: %s' % (sd * vg)
    print '25th percentile: %s' % (val_25 * vg)
    print '50th percentile: %s' % (med_val * vg)
    print '75th percentile: %s \n \n' % (val_75 * vg)
 
    return endog, ys
        

def pull_data():
    leaps = ['Chad Evans', 'Brian Muller', 'Marc Rothman', 'Ryo Saotome', 'Andy Grigus', \
            'Nic ' + "O'" + 'Connor', 'Stephen Landefeld', 'Ali Amjad', 'LPS']
    leaps_2 = ['FSF', 'FA2', 'FG2', 'FBR', 'FBS', 'FC1', 'FRM', 'FWS', 'FRO']
    
    convexity = ['Tom Simpson', 'Neel Shah', 'Steve Lockwood', 'Roxy Rong', 'Matt MacFarlane', 'CVX'] 
    convexity_2 = ['FMC', 'FN1', 'FN2', 'FN3', 'FN6', 'FNS', 'FRK', 'FT1', 'FT2', 'FT3', 'FTS', 'FSL']
                
    cat = ['FK2', 'AKA', 'CAT']
                
    sector_1 = ['Communications', 'Technology', 'Energy', 'Industrials', 'Materials', 'SG1']
    sector_2 = ['Consumer Discretionary', 'Consumer Staples', 'Financial', 'Healthcare', 'Utilities', 'SG2']
    
    liq = ['LIQ']
    illiq = ['ILQ']
  
    #all trade pnl, joined on trader name and ID as well as sectorName for each symbol   
    trades = sample()  
    
    #leaps, convexity group, catalyst group      
    lp = trades.loc[(trades['Name'].isin(leaps) | trades['Account'].isin(leaps_2)) & (trades['Exp'] < 200)]
    cx = trades.loc[(trades['Name'].isin(convexity) | trades['Account'].isin(convexity_2)) & (trades['Exp'] <= 270)]
    catalyst = trades.loc[(trades['Account'].isin(cat)) & (trades['Exp'] <= 90)] 
    

    #Sector and liquidity groups     
    s1 = trades.loc[(trades['Sector'].isin(sector_1) == True) & (trades['Exp'] <= 270) & \
        (trades['Liq'] <= 750) & ((trades['Name'].isin(leaps + convexity) == False) | (trades['Account'].isin(leaps_2 + convexity_2 + cat) == False))]
    s2 = trades.loc[(trades['Sector'].isin(sector_2) == True) & (trades['Exp'] <= 270) & \
        (trades['Liq'] <= 750) & ((trades['Name'].isin(leaps + convexity) == False) | (trades['Account'].isin(leaps_2 + convexity_2 + cat) == False))]


    l1 = trades.loc[(trades['Liq'] <= 125) & ((trades['Name'].isin(leaps + convexity) == False) | (trades['Account'].isin(leaps_2 + convexity_2 + cat) == False)) \
        & (trades['Exp'] <= 270) & (trades['Sector'].isin(sector_1 + sector_2) == False) & (trades['Account'].isin(illiq) == False)]
    l2 = trades.loc[(trades['Liq'] >= 750) & \
        ((trades['Name'].isin(leaps + convexity) == False) | (trades['Account'].isin(leaps_2 + convexity_2 + cat) == False)) \
        & (trades['Exp'] <= 270) & (trades['Sector'].isin(sector_1 + sector_2) == False) & (trades['Account'].isin(liq) == False)]

   # resetting the indices 
    lp.reset_index(drop=True, inplace=True)
    leaps = lp.groupby('TradeDate', as_index=False).sum()
    leaps.reset_index() 
    
    cvx = cx.groupby('TradeDate', as_index=False).sum()
    cvx.reset_index()
    
    catalyst = catalyst.groupby('TradeDate', as_index=False).sum()
    catalyst.reset_index() 
    
    s1 = s1.groupby('TradeDate', as_index=False).sum()
    s1.reset_index() 
    s2 = s2.groupby('TradeDate', as_index=False).sum()
    s2.reset_index() 
    
    l1 = l1.groupby('TradeDate', as_index=False).sum()
    l1.reset_index() 
    l2 = l2.groupby('TradeDate', as_index=False).sum()
    l2.reset_index() 

    #merging with SPY returns 
    spy = pull_spy()
    leaps = leaps.merge(spy, on=['TradeDate'], how='left')
    leaps['ROV'] = (leaps['Gamma_C'] + leaps['Theta_C_New']) / leaps['TWVega'] 
    
    cvx = cvx.merge(spy, on=['TradeDate'], how='left')
    cvx['ROV'] = (cvx['Gamma_C'] + cvx['Theta_C_New']) / cvx['TWVega'] 
    
    catalyst = catalyst.merge(spy, on=['TradeDate'], how='left')
    catalyst['ROV'] = (catalyst['Gamma_C'] + catalyst['Theta_C_New']) / catalyst['TWVega'] 
    
    s1 = s1.merge(spy, on=['TradeDate'], how='left')
    s1['ROV'] = (s1['Gamma_C'] + s1['Theta_C_New']) / s1['TWVega'] 
    s2 = s2.merge(spy, on=['TradeDate'], how='left')
    s2['ROV'] = (s2['Gamma_C'] + s2['Theta_C_New']) / s2['TWVega'] 

    l1 = l1.merge(spy, on=['TradeDate'], how='left')
    l1['ROV'] = (l1['Gamma_C'] + l1['Theta_C_New']) / l1['TWVega'] 
    l2 = l2.merge(spy, on=['TradeDate'], how='left')
    l2['ROV'] = (l2['Gamma_C'] + l2['Theta_C_New']) / l2['TWVega'] 

    return leaps, cvx, catalyst, s1, s2, l1, l2
    

def grab_group(group, s, franchise, vega, data, fig_num=1):
    name = group.upper()
    
    if name == 'LEAPS':
        x, y = k(data['ROV'].values.T.tolist(), data['Ret'].values.T.tolist(), .25, s, franchise, vega, group) 
    elif name == 'CONVEXITY':
        x, y = k(data['ROV'].values.T.tolist(), data['Ret'].values.T.tolist(), .3, s, franchise, vega, group) 
    elif name == 'CATALYST':
        x, y = k(data['ROV'].values.T.tolist(), data['Ret'].values.T.tolist(), .4, s, franchise, vega, group) 
    elif name == 'SECTOR 1':
        x, y = k(data['ROV'].values.T.tolist(), data['Ret'].values.T.tolist(), .3, s, franchise, vega, group) 
    elif name == 'SECTOR 2':
        x, y= k(data['ROV'].values.T.tolist(), data['Ret'].values.T.tolist(), .3, s, franchise, vega, group) 
    elif name == 'LIQUID':
        x, y = k(data['ROV'].values.T.tolist(), data['Ret'].values.T.tolist(), .25, s, franchise, vega, group) 
    elif name == 'ILLIQUID':
        x, y = k(data['ROV'].values.T.tolist(), data['Ret'].values.T.tolist(), .25, s, franchise, vega, group) 
    else:
        raise NameError('Please enter in one of the following: \
            leaps, convexity, catalyst, sector 1, sector 2, liquid, or illiquid as group names')
            
    ##Grab probability density function plotting of group 
    plt.figure(fig_num).suptitle(name, fontsize=12, fontweight='bold')
    plt.plot(x, y, color='k', linewidth=1.0)
    return 
        

if __name__ == '__main__':
    leaps, cvx, cat, s1, s2, l1, l2 = pull_data() 
#==============================================================================
#      Format for inputting in the data: 
#      name of group, 
#      SPY percentage change in decimal terms => .2% should be .002
#      total franchise
#      total vol time weighted vega/blended vega 
#      data array: one of the elemnts in {leaps, cvx, cat, s1, s2, l1, l2 }
#      figure number: try to keep it in sequential integers 
#==============================================================================
    
    #leaps 
    grab_group('leaps', .00385, (-62293.2398944268 + 11151.3673), 406028.563784956, leaps, 1)
    
    #convexity
    grab_group('convexity', .02, 10000, 50000, cvx, 2)
    
    #catalyst
    grab_group('catalyst', .02, 10000, 50000, cat, 3)
    
    #Sector 1
    grab_group('sector 1', .02, 10000, 50000, s1, 4)
    
    #Sector 2    
    grab_group('sector 2', .02, 10000, 50000, s2, 5)
    
    #Liquidity 
    grab_group('liquid', .02, 10000, 50000, l1, 6)
    
    #illiquidty 
    grab_group('illiquid', .02, 10000, 50000, l2, 7)
    