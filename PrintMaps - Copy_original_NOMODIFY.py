"""
Created on Jul 23 10:32:17 2015
@author: slin
"""

import pandas as pd
import numpy as np
import datetime
from multiprocessing import Pool

from pyspark import pyspark as ps
import pymmd

import pycake
#from pympler import summary, muppy, tracker

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from py7 import logging 


# c = pymmd.MMDConnection('delphi', 9999)
c = pymmd.MMDConnection('pvlchi6ppymmd1', 9999)
r = c.call('auth.auto', {'user':'jguo'})
log = logging.getLogger(__name__)

def get_opts(s):

    s.refresh()
    keys = s.make_dataframe().index

    #s.channel.close()
    #ps.close()

    return keys.values



def fix(x):

    split = x.split(':')
    strike = float(split[2]) / 10000
    strike = "{0:.2f}".format(strike)

    key = 'O:' + split[0] + ':' + split[1] + ':' + strike + ':' + split[3]

    return key


def fix_opts(keys):
    
    return map(fix, keys)


def get_prints(sec_key, ts=np.nan):

    if np.isnan(ts):
        start = datetime.datetime.now().date()
        mktopen = datetime.time(13, 30, 0)
        dtm = datetime.datetime.combine(start, mktopen)

        timestamp = (dtm - datetime.datetime(1970, 1, 1)).total_seconds()
        ts = int(timestamp*1000)

    params = {
                "securitykey" : sec_key,
                "columnmodel" : ["TIME", "PRICE", "SIZE", "BID", "ASK", "EXCH", "UPRICE", "IVOL_BID", "IVOL_ASK", "FLAGS"],
                "interval" : 0,
                "starttime" : ts
             }

    try:
        opt_dict = c.call('printsservice', body = params).body
        frame = pd.DataFrame.from_dict(opt_dict).T

    except Exception as err:
        print sec_key
        print err
        frame = pd.DataFrame()

    if not frame.empty:
        frame.columns = ['time', 'price', 'size', 'bid', 'ask', 'exch', 'uprice', 'ivol_sell', 'ivol_buy', 'flags']
        frame['security_id'] = sec_key

        frame = frame.reset_index(drop = True)
    else:
        log.info('Pulled from NextGen yet not a valid option: %s' % sec_key)

    return frame


def update_VWAV(ser):

    try:
        if np.isnan(ser['qty']):
            prints = get_prints(ser.name)
            if prints.empty:
                return ser

            ser['ts'] = max(prints['time']) + 1
            key = (ser.name).split(':')
            ser['StockSymbol'] = key[1]
            ser['ExpYear'] = int(key[2][:4])
            ser['ExpMonth'] = int(key[2][4:6])
            ser['ExpDay'] = int(key[2][6:])
            ser['Strike'] = float(key[3])
            ser['Cp'] = key[4]

            prints = prints[~pd.isnull(prints['ivol_sell'])]
            prints = prints[~pd.isnull(prints['ivol_buy'])]
            if prints.empty:
                return ser
            # prints['ivol_sell'].fillna(0.0, inplace=True)
            # prints['ivol_buy'].fillna(0.0, inplace=True)

            ser['qty'] = sum(abs(prints['size']))
            x = 1

            ser['VWAV_sell'] = sum(prints['size'] * prints['ivol_sell']) / sum(prints['size'])
            ser['VWAV_buy'] = sum(prints['size'] * prints['ivol_buy']) / sum(prints['size'])

            ser['max_vol_sell'] = max(prints['ivol_sell'])
            ser['max_vol_buy'] = max(prints['ivol_buy'])

            ser['min_vol_sell'] = min(prints['ivol_sell'])
            ser['min_vol_buy'] = min(prints['ivol_buy'])

            return ser

        else:
            prints = get_prints(ser.name, ser['ts'])
            if prints.empty:
                return ser

            ser['ts'] = max(prints['time']) + 1
            prints = prints[~pd.isnull(prints['ivol_sell'])]
            prints = prints[~pd.isnull(prints['ivol_buy'])]

            if prints.empty:
                return ser

            # prints['ivol_sell'].fillna(0.0, inplace=True)
            # prints['ivol_buy'].fillna(0.0, inplace=True)

            ser['qty'] = ser['qty'] + sum(prints['size'])

            ser['VWAV_sell'] = ((ser['VWAV_sell'] * ser['qty']) + sum(prints['ivol_sell'] * prints['size'])) / (ser['qty'] + sum(prints['size']))
            ser['VWAV_buy'] = ((ser['VWAV_buy'] * ser['qty']) + sum(prints['ivol_buy'] * prints['size'])) / (ser['qty'] + sum(prints['size']))

            ser['max_vol_sell'] = max([ser['max_vol_sell'], max(prints['ivol_sell'])])
            ser['max_vol_buy'] = max([ser['max_vol_buy'], max(prints['ivol_sell'])])

            ser['min_vol_sell'] = min([ser['min_vol_sell'], min(prints['ivol_sell'])])
            ser['min_vol_buy'] = min([ser['min_vol_buy'], min(prints['ivol_buy'])])

            #ser['ts'] = max(prints['time']) + 1
            return ser

    except Exception as err:
        print err
        return ser


def retro_dictify(frame):
    d = {}
    for row in frame.values:
        here = d
        for elem in row[:-2]:
            if elem not in here:
                here[elem] = {}
            here = here[elem]
        here[row[-2]] = row[-1]
    return d


def to_datamap(frame):

    temp = frame[['StockSymbol', 'ExpYear', 'ExpMonth', 'ExpDay', 'Cp', 'Strike', 'VWAV_sell']]
    temp['Strike'] = (temp['Strike']*1000.0)

    temp_dict = retro_dictify(temp)

    data_map = pycake.DataMap('SL_VWAV_SELL', True)
    data_map.clear()
    data_map.notify(data = temp_dict)
    data_map.close()


def frame_update(frame):
    return frame.apply(update_VWAV, axis=1)


def email(subject, body):
    msg = MIMEMultipart('alternative')
    s = smtplib.SMTP('smtp.peak6.net')

    from_email = 'jguo@peak6.com'
    to_email = 'jguo@peak6.com'

    msg['Subject'] = subject

    content = MIMEText(body, 'plain')
    msg.attach(content)

    s.sendmail(from_email, to_email, msg.as_string())


if __name__ == '__main__':

    logging.init('printmaps')
    log.info('Hello, this is the start of the program')
    ps.init(host='pvlchi6sweb1', auth=str(r.body), timeout = 30)

    maxV = 1000000
    minV = 0
    query = """(And (> OptionVolume 0) (< OptionVolume 10000000) (ExportValues OptionShortDesc OptionVolume))"""
    s = ps.option_script(query)

    cores = 5

    start = datetime.datetime.now().date()
    mktopen = datetime.time(13, 30, 0)
    dtm = datetime.datetime.combine(start, mktopen)

    timestamp = (dtm - datetime.datetime(1970, 1, 1)).total_seconds()
    ts = int(timestamp*1000)

    begin = datetime.datetime.now()
    lastupdated = begin

    keys = fix_opts(get_opts(s))

    frame = pd.DataFrame(index=keys, columns=['StockSymbol', 'ExpYear', 'ExpMonth', 'ExpDay', 'Strike', 'Cp', 'VWAV_sell', 'VWAV_buy', 'ts', 'qty', 'max_vol_sell', 'max_vol_buy', 'min_vol_sell', 'min_vol_buy'])
    frame['ts'].fillna(ts, inplace=True)
    frame['lastupdated'] = str(datetime.datetime.now())

    vpool = Pool(cores)
    #results = frame_update(frame)
    results = vpool.map(frame_update, np.array_split(frame, cores))
    # vpool.close()
    # vpool.join()
    frame = pd.concat(results)
    frame = frame[frame['ts'] != ts]
    frame.to_csv('check.csv')

    def add_level(x):
        if np.isnan(x):
            return 0
        if x < 500:
            return 1
        if x < 2500:
            return 2
        return 3

    def add_adj_level(x):
        day_frac = (((datetime.datetime.now() - (datetime.datetime.combine(datetime.date.today(), datetime.time(8, 30)))).total_seconds())/60.0) / (6.5*60)
        day_frac = min(1, day_frac)
        if np.isnan(x):
            return 0
        if x < (500 * day_frac):
            return 1
        if x < (2500 * day_frac):
            return 2
        return 3

    frame['level'] = frame['qty'].map(add_level)
    frame['adj_level'] = frame['qty'].map(add_adj_level)

    data_map = pycake.DataMap('JG_PRINTVOLS_NEW', True)
    #data_map.svr1 = c
    data_map.clear()

    temp = frame[['StockSymbol', 'ExpYear', 'ExpMonth', 'ExpDay', 'Strike', 'Cp', 'VWAV_sell', 'VWAV_buy', 'max_vol_sell', 'max_vol_buy', 'min_vol_sell', 'min_vol_buy', 'level', 'adj_level', 'lastupdated']]
    temp = pd.melt(temp, id_vars=['StockSymbol', 'ExpYear', 'ExpMonth', 'ExpDay', 'Strike', 'Cp']) 
    temp['variable'] = temp['variable'].apply(lambda x: x.upper())
    temp['Strike'] = (temp['Strike']*1000.0)
    temp_dict = retro_dictify(temp)
    data_map.notify(data = temp_dict)

    # tr = tracker.SummaryTracker()
    # tr.print_diff()
 
    frame.sort(['ts'], ascending = False, inplace=True)
    frame = frame[frame['ts'] != ts]
    lastStock = frame['StockSymbol'][0]
    lastPrintTime = frame['ts'][0]
    lastStrike = frame['Strike'][0]
    frame.sort(['qty'], ascending = False, inplace = True) 
    while datetime.datetime.now() < datetime.datetime.combine(datetime.date.today(), datetime.time(15, 0)):
        try:
            currentTime = datetime.datetime.now() 
            
            #freeze time check, will have to run it on the full dataset to see 
            if (currentTime - lastupdated).total_seconds() > 700:
                log.error('Long DM update. Last update occured ' + str(lastupdated))
            else:
                lastupdated = currentTime

            keys = get_opts(s)
            keys = fix_opts(keys)
            
            frame = frame.reindex(keys)
            frame['ts'].fillna(ts, inplace=True)

            results = vpool.map(frame_update, np.array_split(frame, cores))

            frame = pd.concat(results)
            frame = frame[frame['ts'] != ts]
            frame['level'] = frame['qty'].map(add_level)
            frame['adj_level'] = frame['qty'].map(add_adj_level)
            frame['lastupdated'] = str(datetime.datetime.now())

            frame.sort(['ts'], ascending = False, inplace=True)
            checkStock = frame['StockSymbol'][0]
            checkPrintTime = frame['ts'][0]
            checkStrike = frame['Strike'][0]

            #check to see if option has updated 
            if (checkStock == lastStock) and (checkPrintTime == lastPrintTime) and (checkStrike == lastStrike): 
                log.error('DM not updating. Latest option to be added was ' + lastStock)
            else:   
                lastStock = checkStock
                lastPrintTime = checkPrintTime
                lastStrike = checkStrike
                
            temp = frame[['StockSymbol', 'ExpYear', 'ExpMonth', 'ExpDay', 'Strike', 'Cp', 'VWAV_sell', 'VWAV_buy', 'max_vol_sell', 'max_vol_buy', 'min_vol_sell', 'min_vol_buy', 'level', 'adj_level', 'lastupdated']]
            temp = pd.melt(temp, id_vars=['StockSymbol', 'ExpYear', 'ExpMonth', 'ExpDay', 'Strike', 'Cp'])
            temp['variable'] = temp['variable'].apply(lambda x: x.upper())
            temp['Strike'] = (temp['Strike']*1000.0)
            temp_dict = retro_dictify(temp)
            data_map.notify(data = temp_dict)
            
        except Exception as err:
            email('VWAV MAP FAILED', str(err))