import tickdb
import pandas as pd
import time
from sqlalchemy import create_engine
import numpy as np
import datetime
import argparse
pd.options.mode.chained_assignment = None  # default='warn'


#flight is at 6L15 pm...board at 5:45, so will need to get there by 5 pm...so need to leave at 4:00 pm to get there 
def connect_to_clone(usr, pwd, db):
    host = 'SQL10'
    conn_str = 'mssql+pymssql://%s:%s@%s/%s' % (usr, pwd, host, db)
    engine = create_engine(conn_str, pool_size=5, pool_recycle=10)
    return engine


def format_output(df,dt):
    df.printTime = list(map(lambda x: str(x)[6:].strip(), df.printTime))
    df.price = df.price / 1000if 
        if 'bid' in x or 'ask' in x:
            df[x] = df[x] / 1000

    df['date'] = datetime.datetime.strptime(dt, '%Y-%m-%d').date()
    return df[['date', 'seq', 'secid','price', 'printTime', 'symbol', 'dte', 'ordinal', 'expiration', 'strike', 'cp', 'ask', 'bid', 'quoteTm1S', 'bid1Sback','ask1Sback', 'quoteTm5S', 'bid5Sback', 'ask5Sback', 'quoteTm10S',
                'bid10Sback', 'ask10Sback', 'quoteTm1M', 'bid1Mback', 'ask1Mback']]


def get_allPrints_paths(dt):
    prints = tickdb.tickdb.find(dt, data_type='print', security_type='option', bar=None, internal=False)
    allprints = list(prints)
    dict = {k.name[:k.name.find(".")]: k.path for k in allprints}
    return dict


def process_Print_date(d):
    df = pd.DataFrame()
    for x in d:
        dft = pd.read_tickbin(d[x], chunk=False,columns=['price','secid','seq','tm'])
        df = df.append(dft)
    df.tm = pd.to_timedelta(df.tm)
    return df


def get_print_timewindow(df):
    max_Times = df[['symbol', 'tm']].groupby('symbol').max().reset_index()
    max_Times.tm = pd.to_timedelta(max_Times.tm)
    max_Times.set_index('symbol', inplace=True)
    min_Times = df[['symbol', 'tm']].groupby('symbol').min().reset_index()
    min_Times.tm = pd.to_timedelta(min_Times.tm)
    min_Times.set_index('symbol', inplace=True)
    return [max_Times,min_Times]


def get_quote_paths(dt,d):
    quotes = tickdb.tickdb.find(dt, data_type='nbbo', security_type='option', bar=None, internal=False)
    allquotes = list(quotes)
    dict = {k.name[:k.name.find(".")]: k.path for k in allquotes if k.name[:k.name.find(".")] in d}
    return dict


def process_quotes(d,max_time,min_time,printdata): #really similar to using dataframe method 
    df = pd.DataFrame()
    printdata['OrgTm'] = printdata.tm

    df2 = pd.DataFrame()
    df3 = pd.DataFrame()
    df4 = pd.DataFrame()
    df5 = pd.DataFrame()

    printdata['back1'] = printdata.tm - datetime.timedelta(seconds=1)
    printdata['back5'] = printdata.tm - datetime.timedelta(seconds=5)
    printdata['back10'] = printdata.tm - datetime.timedelta(seconds=10)
    printdata['back30'] = printdata.tm - datetime.timedelta(seconds=30)
    printdata['back1min'] = printdata.tm - datetime.timedelta(minutes=1)

    for x in d:
        min_time.loc[x, 'tm'] = min_time.loc[x,'tm'] - datetime.timedelta(minutes=10)
        try:
            qdft = pd.read_tickbin(d[x], chunk=False, columns=['ask', 'bid', 'tm','secid'],start=min_time.loc[x,'tm'],end=max_time.loc[x,'tm'])
        except:
            qdft = pd.read_tickbin(d[x], chunk=False, columns=['ask', 'bid', 'tm', 'secid'])

        qdft.tm = pd.to_timedelta(qdft.tm)

        df = df.append(pd.merge_asof(printdata[printdata.symbol == x].sort_values("tm"), qdft,on='tm',by='secid'))
        df2 = df2.append(pd.merge_asof(printdata[printdata.symbol == x][['price', 'secid', 'seq', 'symbol', 'dte', 'ordinal', 'expiration','strike', 'cp', 'back1', 'OrgTm']].sort_values("back1"), qdft[['ask', 'bid','secid','tm']],left_on='back1',right_on='tm',by='secid'))
        df3 = df3.append(pd.merge_asof(printdata[printdata.symbol == x][['price', 'secid', 'seq', 'symbol', 'dte', 'ordinal', 'expiration', 'strike', 'cp', 'back5', 'OrgTm']].sort_values("back5"), qdft[['ask', 'bid', 'secid', 'tm']], left_on='back5', right_on='tm', by='secid'))
        df4 = df4.append(pd.merge_asof(printdata[printdata.symbol == x][['price', 'secid', 'seq', 'symbol', 'dte', 'ordinal', 'expiration', 'strike', 'cp', 'back10', 'OrgTm']].sort_values("back10"), qdft[['ask', 'bid', 'secid', 'tm']], left_on='back10', right_on='tm', by='secid'))
        df5 = df5.append(pd.merge_asof(printdata[printdata.symbol == x][['price', 'secid', 'seq', 'symbol', 'dte', 'ordinal', 'expiration', 'strike', 'cp', 'back1min', 'OrgTm']].sort_values("back1min"), qdft[['ask', 'bid', 'secid', 'tm']], left_on='back1min', right_on='tm', by='secid'))

    df2.rename(index=str, columns={"ask": "ask1Sback", "bid": "bid1Sback", 'tm': 'quoteTm1S'}, inplace=True)
    df3.rename(index=str, columns={"ask": "ask5Sback", "bid": "bid5Sback", 'tm': 'quoteTm5S'}, inplace=True)
    df4.rename(index=str, columns={"ask": "ask10Sback", "bid": "bid10Sback", 'tm': 'quoteTm10S'}, inplace=True)
    df5.rename(index=str, columns={"ask": "ask1Mback", "bid": "bid1Mback", 'tm': 'quoteTm1M'}, inplace=True)

    output = pd.merge(df[['price', 'secid', 'seq', 'tm', 'symbol', 'dte', 'ordinal', 'expiration','strike', 'cp', 'OrgTm','ask','bid']],df2[['OrgTm','quoteTm1S','bid1Sback','ask1Sback','secid']],on=['OrgTm','secid'])
    output = pd.merge(output, df3[['OrgTm', 'quoteTm5S', 'bid5Sback', 'ask5Sback','secid']],on=['OrgTm','secid'])
    output = pd.merge(output, df4[['OrgTm', 'quoteTm10S', 'bid10Sback', 'ask10Sback','secid']],on=['OrgTm','secid'])
    output = pd.merge(output, df5[['OrgTm', 'quoteTm1M', 'bid1Mback', 'ask1Mback','secid']],on=['OrgTm','secid'])

    output.rename(index=str, columns={'tm': 'printTime'}, inplace=True)
    output.drop_duplicates(['secid', 'seq'],inplace=True)
    return output[['price', 'secid', 'seq', 'printTime', 'symbol', 'dte', 'ordinal', 'expiration','strike', 'cp', 'ask', 'bid', 'quoteTm1S'
                    , 'bid1Sback','ask1Sback', 'quoteTm5S', 'bid5Sback', 'ask5Sback', 'quoteTm10S','bid10Sback', 'ask10Sback', 'quoteTm1M', 'bid1Mback', 'ask1Mback']]


def main():
    ENGINE = connect_to_clone('sparky1', 'Sp@rk_users', 'SparkTools')
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', help='date to process')
    args = parser.parse_args()
    DT = args.date
    Security_data = tickdb.refdata.scidb_option_symbols(DT)
    print_dict = get_allPrints_paths(DT)
    print_df = process_Print_date(print_dict)
    print_df = print_df.merge(Security_data,on='secid',how='inner')
    times = get_print_timewindow(print_df)
    quote_dict = get_quote_paths(DT,print_dict)
    final_df = process_quotes(quote_dict,times[0],times[1],print_df)
    output = format_output(final_df,DT)
    output.drop_duplicates(['secid', 'seq'], inplace=True)
    output.to_sql('tblHistPrintData',ENGINE,index=False,if_exists='append')

if __name__ == '__main__':
    main()
