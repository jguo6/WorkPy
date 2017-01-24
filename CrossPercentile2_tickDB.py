import tickdb
import pandas as pd
import time
from sqlalchemy import create_engine
import numpy as np

def connect_to_clone(usr, pwd, db):
    host = 'SQL10'
    conn_str = 'mssql+pymssql://%s:%s@%s/%s' % (usr, pwd, host, db)
    engine = create_engine(conn_str, pool_size=5, pool_recycle=10)
    return engine

def Get_Dates(engine):
    query='''
        USE SparkTools;
        SELECT cast(c1.date as date) as date
        FROM tblCalendar c (nolock)
        JOIN tblCalendar c1 (nolock)
            ON c1.cardinal = c.cardinal - 1
        Where c.date = CAST(Getdate() as date)
    '''
    data=pd.read_sql(query,engine)
    return data

def getAllFills(engine,dt):
    query = '''
                declare @startdate smalldatetime;
                set @startdate='%s';

                select CONVERT(varchar(10), CAST(a.auctionExchangeTime as time), 20)as time,a.stockSymbol as symbol,a.strike,cast(a.expdate as date) as expiration,a.cps as cp,f.fillSize,f.fillprice,a.orderside,a.secBid,a.secAsk,a.auctionorderid,a.date
                ,CONVERT(varchar(10),CAST(DATEADD(minute,-1,a.auctionExchangeTime)  as time), 20)  as Time1MinAgo
                ,CONVERT(varchar(10),CAST(DATEADD(minute,-10,a.auctionExchangeTime)  as time), 20)  as Time10MinAgo
                ,case when CAST(a.auctionExchangeTime as time)> '09:30:00' then CONVERT(varchar(10),CAST(DATEADD(hour,-1,a.auctionExchangeTime)  as time), 20) else null end as Time1HRago
                ,CONVERT(varchar(10),CAST(DATEADD(hour,1,a.auctionExchangeTime)  as time), 20)  as Time1hrForward
                ,a.orderVol
                from SparkTools..tblAuctionFills f with (nolock)
                join SparkTools..tblAuctions a with (nolock)
                on a.auctionOrderId = f.auctionOrderId and f.date = a.date
                where a.date = @startdate

                union

                select CONVERT(varchar(10), CAST(a.auctionExchangeTime as time), 20)as time,a.stockSymbol as symbol,a.strike,cast(a.expdate as date) as expiration,a.cps as cp,f.fillSize,f.fillprice,a.orderside,a.secBid,a.secAsk,a.auctionorderid,a.date
                ,CONVERT(varchar(10),CAST(DATEADD(minute,-1,a.auctionExchangeTime)  as time), 20)  as Time1MinAgo
                ,CONVERT(varchar(10),CAST(DATEADD(minute,-10,a.auctionExchangeTime)  as time), 20)  as Time10MinAgo
                ,case when CAST(a.auctionExchangeTime as time)> '09:30:00' then CONVERT(varchar(10),CAST(DATEADD(hour,-1,a.auctionExchangeTime)  as time), 20) else null end as Time1HRago
                ,CONVERT(varchar(10),CAST(DATEADD(hour,1,a.auctionExchangeTime)  as time), 20)  as Time1hrForward
                ,a.orderVol
                from SparkTools..tblFollowPrintOrderFills f with (nolock)
                join SparkTools..tblFollowPrintOrders a with (nolock)
                on a.auctionOrderId = f.auctionOrderId and f.date = a.date
                where a.date = @startdate

                union

                select CONVERT(varchar(10), CAST(a.auctionExchangeTime as time), 20)as time,a.stockSymbol as symbol,a.strike,cast(a.expdate as date) as expiration,a.cps as cp,f.fillSize,f.fillprice,a.orderside,a.secBid,a.secAsk,a.auctionorderid,a.date
                ,CONVERT(varchar(10),CAST(DATEADD(minute,-1,a.auctionExchangeTime)  as time), 20)  as Time1MinAgo
                ,CONVERT(varchar(10),CAST(DATEADD(minute,-10,a.auctionExchangeTime)  as time), 20)  as Time10MinAgo
                ,case when CAST(a.auctionExchangeTime as time)> '09:30:00' then CONVERT(varchar(10),CAST(DATEADD(hour,-1,a.auctionExchangeTime)  as time), 20) else null end as Time1HRago
                ,CONVERT(varchar(10),CAST(DATEADD(hour,1,a.auctionExchangeTime)  as time), 20)  as Time1hrForward
                ,a.orderVol
                from SparkTools..tblHedgehogOrderFills f with (nolock)
                join SparkTools..tblHedgehogOrders a with (nolock)
                on a.auctionOrderId = f.auctionOrderId and f.date = a.date
                where a.date = @startdate
    '''%(dt)
    data = pd.read_sql(query, engine)
    return data

ENGINE = connect_to_clone('sparky1','Sp@rk_users','SparkTools')
start = time.time()
date = Get_Dates(ENGINE)
DT = str(date.date[0])
fills = getAllFills(ENGINE,DT)
idMap = tickdb.refdata.scidb_option_symbols(DT)
idMap.expiration = idMap.expiration.astype('str')

df = pd.merge(fills,idMap, how='inner', left_on=['symbol','expiration','strike','cp'],right_on=['symbol','expiration','strike','cp'])
final = df.copy()
d = {}

for x in range(len(final)):
    d[final.ix[x].auctionorderid] = {}
    try:

        min1 = tickdb.tickdb.read_df(final.ix[x].symbol,DT, data_type='greek', bar='30S', security_type='option',
                                     chunk=False, start=final.ix[x].Time1MinAgo, end=final.ix[x].time,
                                     columns=['secid', 'ivb_l', 'iva_l'])
        std = min1[min1.secid == final.ix[x].secid].groupby('secid').std()
        average = min1[min1.secid == final.ix[x].secid].groupby('secid').mean()

        if final.ix[x].orderside == 1:
            d[final.ix[x].auctionorderid].update({'1minCrossPerc':float(((final.ix[x].orderVol/100) - average.ivb_l.values[0]) / (average.iva_l.values[0]- average.ivb_l.values[0]))})
        else:
            d[final.ix[x].auctionorderid].update({'1minCrossPerc': float((average.iva_l.values[0] - (final.ix[x].orderVol/100)) / (average.iva_l.values[0] - average.ivb_l.values[0]))})

        d[final.ix[x].auctionorderid].update({'1minBid':average.ivb_l.values[0]})
        d[final.ix[x].auctionorderid].update({'1minAsk':average.iva_l.values[0]})
        d[final.ix[x].auctionorderid].update({'1minBidSTD':std.ivb_l.values[0]})
        d[final.ix[x].auctionorderid].update({'1minAskSTD':std.iva_l.values[0]})

    except:
        pass

    try:
        min10 = tickdb.tickdb.read_df(final.ix[x].symbol, DT, data_type='greek', bar='30S', security_type='option',
                                      chunk=False, start=final.ix[x].Time10MinAgo, end=final.ix[x].time,
                                      columns=['secid', 'ivb_l', 'iva_l'])
        std = min10[min10.secid == final.ix[x].secid].groupby('secid').std()
        average = min10[min10.secid == final.ix[x].secid].groupby('secid').mean()

        if final.ix[x].orderside == 1:
            d[final.ix[x].auctionorderid].update({'10minCrossPerc':float(((final.ix[x].orderVol/100) - average.ivb_l.values[0]) / (average.iva_l.values[0]- average.ivb_l.values[0]))})
        else:
            d[final.ix[x].auctionorderid].update({'10minCrossPerc': float((average.iva_l.values[0] - (final.ix[x].orderVol/100)) / (average.iva_l.values[0] - average.ivb_l.values[0]))})

        d[final.ix[x].auctionorderid].update({'10minBid':average.ivb_l.values[0]})
        d[final.ix[x].auctionorderid].update({'10minAsk':average.iva_l.values[0]})
        d[final.ix[x].auctionorderid].update({'10minBidSTD':std.ivb_l.values[0]})
        d[final.ix[x].auctionorderid].update({'10minAskSTD':std.iva_l.values[0]})

    except:
        pass

    try:
        hr1 = tickdb.tickdb.read_df(final.ix[x].symbol, DT, data_type='greek', bar='30S', security_type='option',
                                      chunk=False, start=final.ix[x].Time10MinAgo, end=final.ix[x].time,
                                      columns=['secid', 'ivb_l', 'iva_l'])
        std = hr1[hr1.secid == final.ix[x].secid].groupby('secid').std()
        average = hr1[hr1.secid == final.ix[x].secid].groupby('secid').mean()

        if final.ix[x].orderside == 1:
            d[final.ix[x].auctionorderid].update({'1hrCrossPerc':float(((final.ix[x].orderVol/100) - average.ivb_l.values[0]) / (average.iva_l.values[0]- average.ivb_l.values[0]))})
        else:
            d[final.ix[x].auctionorderid].update({'1hrCrossPerc': float((average.iva_l.values[0] - (final.ix[x].orderVol/100)) / (average.iva_l.values[0] - average.ivb_l.values[0]))})

        d[final.ix[x].auctionorderid].update({'1hrBid':average.ivb_l.values[0]})
        d[final.ix[x].auctionorderid].update({'1hrAsk':average.iva_l.values[0]})
        d[final.ix[x].auctionorderid].update({'1hrBidSTD':std.ivb_l.values[0]})
        d[final.ix[x].auctionorderid].update({'1hrAskSTD':std.iva_l.values[0]})

    except:
        pass

    try:
        plushr1 = tickdb.tickdb.read_df(final.ix[x].symbol, DT, data_type='greek', bar='30S',
                                        security_type='option',
                                        chunk=False, start=final.ix[x].time, end=final.ix[x].Time1hrForward,
                                        columns=['secid', 'ivb_l', 'iva_l'])
        std = plushr1[plushr1.secid == final.ix[x].secid].groupby('secid').std()
        average = plushr1[plushr1.secid == final.ix[x].secid].groupby('secid').mean()

        if final.ix[x].orderside == 1:
            d[final.ix[x].auctionorderid].update({'Foward1hrCrossPerc':float(((final.ix[x].orderVol/100) - average.ivb_l.values[0]) / (average.iva_l.values[0]- average.ivb_l.values[0]))})
        else:
            d[final.ix[x].auctionorderid].update({'Foward1hrCrossPerc': float((average.iva_l.values[0] - (final.ix[x].orderVol/100)) / (average.iva_l.values[0] - average.ivb_l.values[0]))})

        d[final.ix[x].auctionorderid].update({'Foward1hrBid':average.ivb_l.values[0]})
        d[final.ix[x].auctionorderid].update({'Foward1hrAsk':average.iva_l.values[0]})
        d[final.ix[x].auctionorderid].update({'Foward1hrBidSTD':std.ivb_l.values[0]})
        d[final.ix[x].auctionorderid].update({'Foward1hrAskSTD':std.iva_l.values[0]})

    except:
        pass

    try:
        day = tickdb.tickdb.read_df(final.ix[x].symbol, DT, data_type='greek', bar='30S',
                                    security_type='option', chunk=False, columns=['secid', 'ivb_l', 'iva_l'])
        std = day[day.secid == final.ix[x].secid].groupby('secid').std()
        average = day[day.secid == final.ix[x].secid].groupby('secid').mean()

        if final.ix[x].orderside == 1:
            d[final.ix[x].auctionorderid].update({'AllDayCrossPerc':float(((final.ix[x].orderVol/100) - average.ivb_l.values[0]) / (average.iva_l.values[0]- average.ivb_l.values[0]))})
        else:
            d[final.ix[x].auctionorderid].update({'AllDayCrossPerc': float((average.iva_l.values[0] - (final.ix[x].orderVol/100)) / (average.iva_l.values[0] - average.ivb_l.values[0]))})

        d[final.ix[x].auctionorderid].update({'AllDayBid':average.ivb_l.values[0]})
        d[final.ix[x].auctionorderid].update({'AllDayAsk':average.iva_l.values[0]})
        d[final.ix[x].auctionorderid].update({'AllDayBidSTD':std.ivb_l.values[0]})
        d[final.ix[x].auctionorderid].update({'AllDayAskSTD':std.iva_l.values[0]})

    except:
        pass

output = pd.DataFrame.from_dict(d,orient='index')
output.index.rename('AuctionOrderID',inplace=True)
output = output.reset_index(drop=False)
output = output.replace([np.inf, -np.inf], np.nan)
output['date'] = final.date[0]
output.to_sql("tblCrossPercentile",con=ENGINE,if_exists='append',index=False)
