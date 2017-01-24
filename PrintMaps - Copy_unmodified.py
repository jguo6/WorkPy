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

#ways to check prints widget if what we're getting is directly from there...any manipulation would have to be checked
#look to see how to acess the datamap through nextgen 
#test it out with an example like O:AAPL:20170118:C' and see where it breaks 
#also think of test cases not ocvered yet 

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

c = pymmd.MMDConnection('pvlchi6ppymmd1', 9999) #connection string to the prints widget 
r = c.call('auth.auto', {'user':'jguo'}) 

def get_opts(s):
    s.refresh()
    oVol = s.make_dataframe()['OptionVolume'].values 
    keys = s.make_dataframe().index

    return keys.values, oVol 

def get_opts_volume(s):
    s.refresh()
    ovol = s.make_dataframe()['OptionVolume'].values
    return ovol 


def fix(x, y):
    split = x.split(':')
    strike = float(split[2]) / 10000
    strike = "{0:.2f}".format(strike)
    
    key = 'O:' + split[0] + ':' + split[1] + ':' + strike + ':' + split[3]
    return key, y

#return both the name as well as the volume back 
def fix_opts(keys, volume):
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
        opt_dict = c.call('printsservice', body = params).body #
        frame = pd.DataFrame.from_dict(opt_dict).T #transpose index and columns 
        
    except Exception as err:
        print 'Exception occured:'
        print err
        frame = pd.DataFrame()

    if not frame.empty:
        #could do an error handling message here for none values in the prints? 
        frame.columns = ['time', 'price', 'size', 'bid', 'ask', 'exch', 'uprice', 'ivol_sell', 'ivol_buy', 'flags'] #formats the prints widget for you 
        frame['security_id'] = sec_key
        frame = frame.reset_index(drop = True)

    print frame 
    return frame


def update_VWAV(ser):
    #ser.name is the option name ex: O:HYG:20170120:87.00:P 
    print 'this is the name'
    print ser.name
    try:
        if np.isnan(ser['qty']):
            prints = get_prints(ser.name) #getting the prints based on the option name 
            if prints.empty:
                return ser
                
            #create the final format table 
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

            ser['qty'] = sum(prints['size'])

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

            
            ser['qty'] = ser['qty'] + sum(prints['size'])

            ser['VWAV_sell'] = ((ser['VWAV_sell'] * ser['qty']) + sum(prints['ivol_sell'] * prints['size'])) / (ser['qty'] + sum(prints['size']))
            ser['VWAV_buy'] = ((ser['VWAV_buy'] * ser['qty']) + sum(prints['ivol_buy'] * prints['size'])) / (ser['qty'] + sum(prints['size']))

            ser['max_vol_sell'] = max([ser['max_vol_sell'], max(prints['ivol_sell'])])
            ser['max_vol_buy'] = max([ser['max_vol_buy'], max(prints['ivol_sell'])])

            ser['min_vol_sell'] = min([ser['min_vol_sell'], min(prints['ivol_sell'])])
            ser['min_vol_buy'] = min([ser['min_vol_buy'], min(prints['ivol_buy'])])

            return ser

    except Exception as err:
        print 'Error occured while updating VWAV:'
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

    ps.init(host='pvlchi6sweb1', auth=str(r.body), timeout = 30) 
    query = """(And (> OptionVolume 5000) (< OptionVolume 5500) (ExportValues OptionShortDesc OptionVolume))"""
    volume = """(ExportValues OptionShortDesc OptionVolume)"""
    s = ps.option_script(query) 
    v = ps.option_script(volume)

    cores = 5

    start = datetime.datetime.now().date()
    mktopen = datetime.time(13, 30, 0) 
    dtm = datetime.datetime.combine(start, mktopen)
    timestamp = (dtm - datetime.datetime(1970, 1, 1)).total_seconds()
    ts = int(timestamp*1000)
    begin = datetime.datetime.now()

    #error checking here...
    #not all of the names in the prints widget are being shown in the options list recieved here 
    #keys = fix_opts(get_opts(s)) 
    #print keys 

#    frame = pd.DataFrame(index=keys, columns=['StockSymbol', 'ExpYear', 'ExpMonth', 'ExpDay', 'Strike', 'Cp', 'VWAV_sell', 'VWAV_buy', 'ts', 'qty', 'max_vol_sell', 'max_vol_buy', 'min_vol_sell', 'min_vol_buy'])
#    frame['ts'].fillna(ts, inplace=True)
#
#    
#    results = frame_update(frame)
#    frame = pd.concat(results)
#    print results 
    #go through the prints widget to figure out how ot access certain elements at a time, and then do the checking to see what numbers these give you compared to prints....so do 
    #warning check of the prints widget 
    #print frame['StockSymbol'][0] #accessing the individual columns, do results[1][column name][number] to isolate the stock symbol name 
    #this frame should have somkething, although not fully sure of what it could be 

#    frame.to_csv('check.csv')
##
#    def add_level(x):
#        if np.isnan(x):
#            return 0
#        if x < 500:
#            return 1
#        if x < 2500:
#            return 2
#        return 3
#
#    def add_adj_level(x):
#        day_frac = (((datetime.datetime.now() - (datetime.datetime.combine(datetime.date.today(), datetime.time(8, 30)))).total_seconds())/60.0) / (6.5*60)
#        day_frac = min(1, day_frac)
#        if np.isnan(x):
#            return 0
#        if x < (500 * day_frac):
#            return 1
#        if x < (2500 * day_frac):
#            return 2
#        return 3
#
#    frame['level'] = frame['qty'].map(add_level)
#    frame['adj_level'] = frame['qty'].map(add_adj_level)
#    frame['last_updated'] = datetime.datetime.today() 
#    data_map = pycake.DataMap('JG_PRINTVOLS_NEW', True)
#    #data_map.svr1 = c
#    data_map.clear()
#
#    temp = frame[['StockSymbol', 'ExpYear', 'ExpMonth', 'ExpDay', 'Strike', 'Cp', 'VWAV_sell', 'VWAV_buy', 'max_vol_sell', 'max_vol_buy', 'min_vol_sell', 'min_vol_buy', 'level', 'adj_level', 'last_updated']]
#    temp = pd.melt(temp, id_vars=['StockSymbol', 'ExpYear', 'ExpMonth', 'ExpDay', 'Strike', 'Cp'])
#    temp['variable'] = temp['variable'].apply(lambda x: x.upper())
#    temp['Strike'] = (temp['Strike']*1000.0)
#    temp_dict = retro_dictify(temp) 
#    
##    print temp 
#    data_map.notify(data = temp_dict)

    # tr = tracker.SummaryTracker()
    # tr.print_diff()

#    while datetime.datetime.now() < datetime.datetime.combine(datetime.date.today(), datetime.time(15, 0)):
#        try:
#            keys = get_opts(s)
#            #tr.print_diff()
#            keys = fix_opts(keys)
#            #tr.print_diff()
#            frame = frame.reindex(keys)
#
#            frame['ts'].fillna(ts, inplace=True)
#
#            
#            results = vpool.map(frame_update, np.array_split(frame, cores))
#            # vpool.close()
#            # vpool.join()
#            frame = pd.concat(results)
#
#            frame['level'] = frame['qty'].map(add_level)
#            frame['adj_level'] = frame['qty'].map(add_adj_level)
#
#            temp = frame[['StockSymbol', 'ExpYear', 'ExpMonth', 'ExpDay', 'Strike', 'Cp', 'VWAV_sell', 'VWAV_buy', 'max_vol_sell', 'max_vol_buy', 'min_vol_sell', 'min_vol_buy', 'level', 'adj_level']]
#            temp = pd.melt(temp, id_vars=['StockSymbol', 'ExpYear', 'ExpMonth', 'ExpDay', 'Strike', 'Cp'])
#            temp['variable'] = temp['variable'].apply(lambda x: x.upper())
#            temp['Strike'] = (temp['Strike']*1000.0)
#            temp_dict = retro_dictify(temp)
#            data_map.notify(data = temp_dict)
#        except Exception as err:
#            email('VWAV MAP FAILED', err)