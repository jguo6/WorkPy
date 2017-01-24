import pandas as pd, pandas.io.sql as psql
import numpy as np
import pyodbc
from datetime import date
from scipy import stats 
import statsmodels.api as sm
import matplotlib.pyplot as pltI
import smtplib
from pyspark import pyspark as ps
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.MIMEBase import MIMEBase
from email import Encoders
import datetime
from sqlalchemy import create_engine
import pymmd 

c = pymmd.MMDConnection('pvlchi6ppymmd1', 9999) #connection string to the prints widget 
r = c.call('auth.auto', {'user':'jguo'}) 
            
sql1_host = 'PVWCHI6PSQL1'
sql1_user = 'TSPython'
sql1_pwd = 'W!nt3r2015'
db = 'CMSandbox'
    
def sample():
    connstr = 'DRIVER={SQL Server Native Client 11.0}; SERVER=pvwchi6psql1; Database=master; Trusted_Connection=yes'
    connection = pyodbc.connect(connstr)
    
    query = """
    SELECT CAST(convert(date, x.LastUpdated, 103) as datetime) as dt
    ,x.UnderlyingSymbol as Symbol 
    ,t.Liq_Rank
    ,SUM(x.ImpliedGamma_C) as Gamma_C
    ,SUM(case when x.ThetaCNew IS null then gg.thetaCNew else x.ThetaCNew END) as ThetaCNew
    ,sum(gg.ImpliedVolTimeWeightedVega * 100) as Vega
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
    where (gg.thetaCNew is not null) and x.SecurityType != 'S' and x.account not like '%SVN%'and x.account not like '%BBE%' and x.type = 'P' 
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
    
    connstr = 'DRIVER={SQL Server Native Client 11.0}; SERVER=pvwchi6psql1; Database=master; Trusted_Connection=yes'
    connection = pyodbc.connect(connstr)

    query = """
      select CAST(convert(date, TradeDate, 103) as datetime) as dt
          ,AdjClose 
     from [igtdev].[dbo].[tblStockHistory]
     where Symbol = 'SPX' and TradeDate > '2015-08-01'
    """
    
    frame = psql.read_sql(query, connection)[['dt', 'AdjClose']]
    frame.rename(columns = {'dt': 'TradeDate', 
                            'AdjClose': 'Close'}, inplace=True)
    
    frame['Ret'] = frame['Close'] / frame['Close'].shift(1) - 1

    return frame


def k(x, x_grid, band, spy, fran, vg, gp):

    kde = sm.nonparametric.KDEMultivariateConditional(endog = x, exog = x_grid, dep_type='c', indep_type='c', bw=[band, band])  
    endog = np.arange(-2.0, 2.0, 0.01)  
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
    
#    print 'Group: %s' % gp 
#    print 'Franchise: %s' % fran
#    print 'Expected franchise: %s' % (mean_val * vg)
#    print '1 SD: %s' % (sd * vg)
#    print '25th percentile: %s' % (val_25 * vg)
#    print '50th percentile: %s' % (med_val * vg)
#    print '75th percentile: %s \n \n' % (val_75 * vg)
#    
    msg = 'Group: {} \n Franchise: {:,.2f} \n Expected Franchise: {:,.2f} \n 1 SD: {:,.2f} \
        \n 25th percentile: {:,.2f} \n 50th percentile: {:,.2f} \n 75th percentile: {:,.2f} \n \n'.format(gp, float(fran), float(mean_val * vg), \
            float(sd * vg), float(val_25 * vg), float(med_val * vg), float(val_75 * vg))
 
    print msg 
    return msg
        

def pull_data():
    leaps = ['Chad Evans', 'Brian Muller', 'Marc Rothman', 'Ryo Saotome', 'Andy Grigus', \
            'Nic ' + "O'" + 'Connor', 'Stephen Landefeld', 'Ali Amjad', 'LEAPs']
    leaps_2 = ['FSF', 'FA2', 'FG2', 'FBR', 'FBS', 'FC1', 'FRM', 'FWS', 'FRO', 'LPS']
    
    msc = ['JAB']
    
    convexity = ['Tom Simpson', 'Neel Shah', 'Steve Lockwood', 'Roxy Rong', 'Matt MacFarlane', 'Convexity' ] 
    convexity_2 = ['FMC', 'FN1', 'FN2', 'FN3', 'FN6', 'FNS', 'FRK', 'FT1', 'FT2', 'FT3', 'FTS', 'FSL', 'CVX']
                
    cat = ['FK2', 'AKA', 'CAT']
                
    sector_1 = ['Communications', 'Technology', 'Energy', 'Industrials', 'Materials']
    sc1 = ['SG1']
    sc2 = ['SG2']
    sector_2 = ['Consumer Discretionary', 'Consumer Staples', 'Financial', 'Healthcare', 'Utilities']
    
    liq = ['LIQ']
    illiq = ['ILQ']
  
    #all trade pnl, joined on trader name and ID as well as sectorName for each symbol   
    trades = sample()  
    
    #leaps, convexity group, catalyst group      
    lp = trades.loc[(trades['Name'].isin(leaps) | trades['Account'].isin(leaps_2)) & (trades['Exp'] > 200)]  
    cvx = trades.loc[(trades['Name'].isin(convexity) | trades['Account'].isin(convexity_2)) & (trades['Exp'] <= 270)] 
    catalyst = trades.loc[(trades['Account'].isin(cat)) & (trades['Exp'] <= 90)]  
    

    #Sector and liquidity groups     
    s1 = trades.loc[(trades['Sector'].isin(sector_1) == True) & (trades['Exp'] <= 270) & \
        (trades['Liq'] <= 750) & ((trades['Name'].isin(leaps + convexity) == False) & \
        (trades['Account'].isin(leaps_2 + convexity_2 + cat + msc + illiq + sc2 + liq) == False))]
    s2 = trades.loc[(trades['Sector'].isin(sector_2) == True) & (trades['Exp'] <= 270) & \
        (trades['Liq'] <= 750) & ((trades['Name'].isin(leaps + convexity) == False) & \
        (trades['Account'].isin(leaps_2 + convexity_2 + cat + msc + sc1 + liq + illiq) == False))]


    l1 = trades.loc[(trades['Liq'] <= 125) & ((trades['Name'].isin(leaps + convexity) == False) | \
        (trades['Account'].isin(leaps_2 + convexity_2 + cat + illiq + msc + sc1 + sc2) == False)) \
        & (trades['Exp'] <= 270) & (trades['Sector'].isin(sector_1 + sector_2) == False)]
    l2 = trades.loc[(trades['Liq'] >= 750) & \
        ((trades['Name'].isin(leaps + convexity) == False) | (trades['Account'].isin(leaps_2 + \
        convexity_2 + cat + msc + liq + sc1 + sc2) == False)) \
        & (trades['Exp'] <= 270) & (trades['Sector'].isin(sector_1 + sector_2) == False)]

    # netting vega across symbols, date and then summing abs vega over date 
    lp = lp.groupby(['TradeDate', 'Symbol'], as_index=False).sum()
    lp.reset_index() 
    lp['TWVega'] = lp['TWVega'].abs()
    lp = lp.groupby('TradeDate', as_index=False).sum()
    lp.reset_index() 
    
    cvx = cvx.groupby(['TradeDate', 'Symbol'], as_index=False).sum()
    cvx.reset_index() 
    cvx['TWVega'] = cvx['TWVega'].abs()
    cvx = cvx.groupby('TradeDate', as_index=False).sum()
    cvx.reset_index() 
    
    catalyst = catalyst.groupby(['TradeDate', 'Symbol'], as_index=False).sum()
    catalyst.reset_index() 
    catalyst['TWVega'] = catalyst['TWVega'].abs()
    catalyst = catalyst.groupby('TradeDate', as_index=False).sum()
    catalyst.reset_index() 
    
    s1 = s1.groupby(['TradeDate', 'Symbol'], as_index=False).sum()
    s1.reset_index() 
    s1['TWVega'] = s1['TWVega'].abs()
    s1 = s1.groupby('TradeDate', as_index=False).sum()
    s1.reset_index() 
    
    s2 = s2.groupby(['TradeDate', 'Symbol'], as_index=False).sum()
    s2.reset_index() 
    s2['TWVega'] = s2['TWVega'].abs()
    s2 = s2.groupby('TradeDate', as_index=False).sum()
    s2.reset_index() 

    l1 = l1.groupby(['TradeDate', 'Symbol'], as_index=False).sum()
    l1.reset_index() 
    l1['TWVega'] = l1['TWVega'].abs()
    l1 = l1.groupby('TradeDate', as_index=False).sum()
    l1.reset_index() 
    
    l2 = l2.groupby(['TradeDate', 'Symbol'], as_index=False).sum()
    l2.reset_index() 
    l2['TWVega'] = l2['TWVega'].abs()
    l2 = l2.groupby('TradeDate', as_index=False).sum()
    l2.reset_index() 
    
    #merging with SPY returns 
    spy = pull_spy()
    leaps = lp.merge(spy, on=['TradeDate'], how='left')
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


def connect_to_sql(host, usr, pwd, db):
    conn_str = 'mssql+pymssql://%s:%s@%s/%s' % (usr, pwd, host, db)
    engine = create_engine(conn_str, pool_size=5, pool_recycle=10)
    return engine

    
def convert(s):
    s.refresh()
    frame = s.make_dataframe()
    return frame

    
def grab_gamma():
    #grabs the franchise number from sql 
    engine = connect_to_sql('SQL3', sql1_user, sql1_pwd, 'positions')
    
    query = """
      SELECT t.name,p.traderID, sum(impliedGammaC) + sum(thetaCNew) 'Franchise'
      FROM [positions].[dbo].[tblPnLPositions_NG] p with (NOLOCK)
        left join [PEAK6Clearing].[dbo].[vwTradersActive] t with (NOLOCK) on t.TraderID = p.traderID
      where convert(date, lastupdated) = convert(date, getdate())
      and t.Trader = 1
      group by t.name,  p.traderID
    """
    frame = psql.read_sql(query, engine)[['name', 'traderID', 'Franchise']]
    print 'This is the frame'
    print frame 
    frame.rename(columns = {'name': 'name',
                            'traderID': 'traderID',
                            'Franchise': 'franchise'}, inplace=True)
    frame['name'] = frame['name'].str.strip() 
    frame.drop_duplicates(['name', 'traderID', 'franchise'], inplace=True)
    frame.reset_index(drop=True, inplace=True)
    
    franchise = frame['franchise'].values
    group_names = frame['name'].values
    dict_franchise = {}
    for i in range(len(franchise)):
        dict_franchise[group_names[i]] = float(franchise[i])
    
    print 'This is the gamma dictionary'
    print dict_franchise 
    return dict_franchise
    

def grab_spy_today():
    ps.init(host='pvlchi6ppymmd1', auth=str(r.body), timeout = 30) 
    query = """(And 
                (in StockSymbol :SPX) 
                (ExportValues StockSymbol StockLast StockOpen))"""
    s = ps.option_script(query) 
    frame = convert(s)
    change = (float(frame['StockLast'][0]) - float(frame['StockOpen'][0])) / frame['StockOpen'][0]
    s.channel.close()
    ps.close() 
    print 'grabbed spy' 
    return change

    
def grab_vega():
    engine = connect_to_sql(sql1_host, sql1_user, sql1_pwd, db)
 #  connection = connect_to_SQ2('TSPython', 'W!nt3r2015', 'positions')
   
    query = """
    with stockVega as (
    SELECT lastupdated, (CASE WHEN CHARINDEX('/', underlyingSymbol) = 0 
          then underlyingSymbol else LEFT(underlyingSymbol, CHARINDEX('.', underlyingSymbol) - 1) END) as underlyingSymbol, (CASE WHEN CHARINDEX('.', Account) = 0 
          then Account else LEFT(Account, CHARINDEX('.', Account) - 1) END) as account, sum(impliedVolTimeWeightedVega) as vega
    FROM [positions].[dbo].[tblPnLPositionHistory_NG]
    where thetaCNew is not null and account not like '%F%' and account not like '%JAB%' and account not like '%SVN%' 
    group by lastUpdated, underlyingSymbol, account
    )
    select s.lastUpdated, s.account, SUM(abs(s.vega)) * 100 as AbsGroupVega, x.tday
    from stockVega s 
    left join (select lastupdated, rank() over (order by lastupdated desc) as tday 
    			from [positions].[dbo].[tblPnLPositionHistory_NG]
    			group by lastupdated) x on x.lastupdated = s.lastUpdated
    where s.account not like '%LCL%' and x.tday = 1
    group by s.lastUpdated, s.account, x.tday
    order by lastupdated desc 
    """
    frame = psql.read_sql(query, engine) #[['lastUpdated', 'account', 'AbsGroupVega', 'tday']]
    frame.rename(columns = {'lastUpdated' : 'LastUpdated',
                            'account': 'Group',
                            'AbsGroupVega' : 'AdjVega',
                            'tday' : 'tday'}, inplace=True)
    frame['Group'] = frame['Group'].str.strip() 
    frame.drop_duplicates(['LastUpdated', 'Group', 'AdjVega', 'tday'], inplace=True)
    frame.reset_index(drop=True, inplace=True)
    
    names_vega = frame['Group'].values
    vega_vals = frame['AdjVega'].values
    dict_vega = {}
    for i in range(len(names_vega)):
        dict_vega[names_vega[i]] = float(vega_vals[i])
        
    return dict_vega


def grab_group(group, s, franchise, vega, data, fig_num=None):
    if fig_num is None:
        raise NameError('Missing arguments, recheck that you have all inputs')

    if franchise is None or (type(franchise) is not int and type(franchise) is not float):
        raise NameError('Please enter in valid franchise number')
    elif vega is None or (type(vega) is not int and type(vega) is not float):
        raise NameError('Please enter in valid vega number')
        
    if abs(s) > 4.5: 
        err = 'WARNING: SMALL DATASET FOR THIS SPX MOVE, RANGES CALCULATED MAY BE UNRELIABLE' 
    else:
        err = ''
        
    name = group.upper()

    if name == 'LEAPS':
        m = k(data['ROV'].values.T.tolist(), data['Ret'].values.T.tolist(), .15, s, franchise, vega, group) 
    elif name == 'CONVEXITY':
        m = k(data['ROV'].values.T.tolist(), data['Ret'].values.T.tolist(), .2, s, franchise, vega, group) 
    elif name == 'CATALYST':
        m = k(data['ROV'].values.T.tolist(), data['Ret'].values.T.tolist(), .25, s, franchise, vega, group) 
    elif name == 'SECTOR 1':
        m = k(data['ROV'].values.T.tolist(), data['Ret'].values.T.tolist(), .15, s, franchise, vega, group) 
    elif name == 'SECTOR 2':
        m = k(data['ROV'].values.T.tolist(), data['Ret'].values.T.tolist(), .12, s, franchise, vega, group) 
    elif name == 'LIQUID':
        m = k(data['ROV'].values.T.tolist(), data['Ret'].values.T.tolist(), .15, s, franchise, vega, group) 
    elif name == 'ILLIQUID':
        m = k(data['ROV'].values.T.tolist(), data['Ret'].values.T.tolist(), .2, s, franchise, vega, group) 
    else:
        raise NameError('Please enter in one of the following: \
            leaps, convexity, catalyst, sector 1, sector 2, liquid, or illiquid as group names')
            
    #Grab probability density function plotting of group 
#    plt.figure(fig_num).suptitle(name, fontsize=12, fontweight='bold')
#    plt.plot(x, y, color='k', linewidth=1.0)
    return err + m
        
def email(subject, body, receivers=None, path=None):
    msg = MIMEMultipart('alternative')
    s = smtplib.SMTP('smtp.peak6.net')

    from_email = ['jguo@peak6.com'] 

    if receivers is not None:
        to_email = receivers
    else:
        to_email = ['jguo@peak6.com'] #, 'aflores@peak6.com', 'slin@peak6.com', 'pshively@peak6.com', 'riskcapman@peak6.com']
        # to_email = ['slin@peak6.com']
    msg['Subject'] = subject

    content = MIMEText(body, 'plain')
    msg.attach(content)

    if path is not None:
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(open(path, 'rb').read())
        Encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment; filename=attached.csv"')
        msg.attach(part)

    s.sendmail(from_email, to_email, msg.as_string())

        
if __name__ == '__main__':
    message = '' 
    vegas = grab_vega() 
    gammas = grab_gamma()
    
    spy_move = grab_spy_today()
    lp, cvx, cat, s1, s2, l1, l2 = pull_data() 

#==============================================================================
#      Format for inputting in the data: 
#      name of group  -UNCHANGED
#      spy change - UNCHANGED, pulled from NextGen
#      TOTAL FRANCHISE => Gamma_C [FROM NEXTGEN, UNCHANGED]+ ThetaCNew [USER CHANGE]
#      total vol time weighted vega - UNCHANGED, pulled from SQL 
#      data array: one of the elemnts in {leaps, cvx, cat, s1, s2, l1, l2 } - UNCHANGED
#      figure number: try to keep it in sequential integers - UNCHANGED
#==============================================================================
    #how else to frame it 
    #starting in mid march, I guess it could work out since could just visit every weekend 
    
    
    #leaps
    message += grab_group('Leaps', spy_move, gammas['LEAPs'], vegas['LPS'], lp, 1) 
    
    #convexity
    message += grab_group('Convexity', spy_move, gammas['Convexity'], vegas['CVX'], cvx, 2)

    #catalyst
    message += grab_group('Catalyst', spy_move, gammas['Catalyst'], vegas['CAT'], cat, 3)

    #Sector 1
    message += grab_group('Sector 1', spy_move, gammas['Sector Group 1'], vegas['SG1'], s1, 4)

    #Sector 2    
    message += grab_group('Sector 2', spy_move, gammas['Sector Group 2'], vegas['SG2'], s2, 5)
 
    #Liquidity 
    message += grab_group('Liquid', spy_move, gammas['Liquid'], vegas['LIQ'], l1, 6)

    #illiquidty 
    message += grab_group('Illiquid', spy_move, gammas['Illiquid'], vegas['ILQ'], l2, 7)
#    
    email('Trading Franchise Distribution For %s' % (str(datetime.date.today())), message)
#    
