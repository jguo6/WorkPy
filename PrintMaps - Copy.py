"""
Created on Jul 23 10:32:17 2015
@author: slin
"""
#==============================================================================
# Tasks 
# Add in the last updated column to the datamap, then do more error handling (what could go wrong)
# Compare NextGen volume (checking same option specs, compare to volume) 
#==============================================================================
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

c = pymmd.MMDConnection('pvlchi6ppymmd1', 9999) #connection string to everything 
r = c.call('auth.auto', {'user':'jguo'})

def get_opts(s):
    try:
        s.refresh()
    except Exception as err: #TEST IT OUT 
        print 'ERR: Could not refresh table' 
        print err 

    keys = s.make_dataframe().index #just an object of the option names 
    s.channel.close()
    ps.close()
    return keys.values #Name, exp, and strike information of the options are string


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
             } #dictionary of values for each time 

    try:
        print 'Inside the get prints function' 
        opt_dict = c.call('printsservice', body = params).body #what is the object c....
        print c 
        print opt_dict
        frame = pd.DataFrame.from_dict(opt_dict).T #constructs the dataframe from opt_dict 
    except Exception as err:
        # print opt_dict
        print sec_key
        print err
        frame = pd.DataFrame() #error handling, gives back to you an empty frame 

    #creates the frame from the methods called above 
    if not frame.empty:
        frame.columns = ['time', 'price', 'size', 'bid', 'ask', 'exch', 'uprice', 'ivol_sell', 'ivol_buy', 'flags']
        frame['security_id'] = sec_key
        frame = frame.reset_index(drop = True)
    
    return frame


def update_VWAV(ser):
    print 'At the update_VWAV function' 
    try:
        if np.isnan(ser['qty']):
            print 'The column qty is Nan right now'
            print ser.name 
            prints = get_prints(ser.name) #feeds in the list of security names into the function 
            if prints.empty:
                return ser
            print prints

            ser['ts'] = max(prints['time']) + 1
            key = (ser.name).split(':')
            #get all parts of the option info from the name 
            ser['StockSymbol'] = 'AAPL'
            ser['ExpYear'] = '2017'
            ser['ExpMonth'] = int(key[2][4:6])
            ser['ExpDay'] = int(key[2][6:])
            ser['Strike'] = float(key[3])
            ser['Cp'] = key[4]
            
            prints = prints[~pd.isnull(prints['ivol_sell'])] #gets back to you the sequence but in reverse order 
            prints = prints[~pd.isnull(prints['ivol_buy'])]
            if prints.empty:
                return ser
            # prints['ivol_sell'].fillna(0.0, inplace=True)
            # prints['ivol_buy'].fillna(0.0, inplace=True)
            
            ser['qty'] = sum(prints['size']) #returns to you the sum of them 

            ser['VWAV_sell'] = sum(prints['size'] * prints['ivol_sell']) / sum(prints['size']) 
            ser['VWAV_buy'] = sum(prints['size'] * prints['ivol_buy']) / sum(prints['size'])

            ser['max_vol_sell'] = max(prints['ivol_sell'])
            ser['max_vol_buy'] = max(prints['ivol_buy'])

            ser['min_vol_sell'] = min(prints['ivol_sell'])
            ser['min_vol_buy'] = min(prints['ivol_buy'])

            #ser['ts'] = max(prints['time']) + 1
            return ser

        else:
            print 'The qty column is NOT nan'
            print ser.name 
            prints = get_prints(ser.name, ser['ts'])
            if prints.empty:
                return ser
            print prints 

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


#method unpackages the dictionary values, in a chain sequence  
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
    print 'At the frame_update process'
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


class LogExceptions(object):
    def __init__(self, callable):
        self.__calable = callable
        
    def __call__(self, *args, **kwargs):
        try:
            result = self.__callable(*args, **kwargs) 
        except Exception as e:
            error(traceback.format_exc())
            raise 
    
    
if __name__ == '__main__':
    # c = pymmd.MMDConnection('pvlchi6ppymmd1', 9999)

    ps.init(host='pvlchi6sweb1', auth=str(r.body), timeout = 30)

    query = """(And (> OptionVolume 5650) (< OptionVolume 5750) (ExportValues OptionShortDesc))"""
    s = ps.option_script(query) #gives back to you a pysark RuleEngineClient instance 
    
    cores = 5

    start = datetime.datetime.now().date() #gives back to you today's full date 
    mktopen = datetime.time(13, 30, 0) #gives back to you the time at 13:30:00 
    dtm = datetime.datetime.combine(start, mktopen) #gives to to you the sum of ^ date and time, so 2017-01-05 13:30:00
    
    timestamp = (dtm - datetime.datetime(1970, 1, 1)).total_seconds() #seconds since start of 1970 
    ts = int(timestamp*1000) #just integer version of the seconds since 1970 * 1000  
    
    begin = datetime.datetime.now() #time right now 
    
    keys = fix_opts(get_opts(s)) #applies a cleanup to each option name that you get from NextGen 

    frame = pd.DataFrame(index=keys, columns=['StockSymbol', 'ExpYear', 'ExpMonth', 'ExpDay', 'Strike', 'Cp', 'VWAV_sell', 'VWAV_buy', 'ts', 'qty', 'max_vol_sell', 'max_vol_buy', 'min_vol_sell', 'min_vol_buy'])
    frame['ts'].fillna(ts, inplace=True) #index is just the name of the option and then you have all the other columns as Nan ^, ts is today's timestamp 
    
    #Multiprocoessing of each different section 

    vpool = Pool(cores) #start 5 worker processes, parallelizes the exectuion of a function across multiple input values, distributing the data across processes...point of this? 
    print 'This is the line where you map frame_update to each of the 5 split chunks'     
    results = vpool.map(frame_update, np.array_split(frame, cores)) #splits the frame in 5 seperate chunks, applying frame update ot each one across all processes 
    # vpool.close()
    # vpool.join()
    frame = pd.concat(results) #actually gives back you the same thing as frame but with more populated data, since it called on the previous above methods 
    print ' This is the frame'
    print frame #somehow this does nothing....
#where is this coming from? 
#why isn't the frame update being populated? Or if it is, how since all my checks arent being passed through?  
#
#    frame.to_csv('check.csv') 
#
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
#        day_frac = (((datetime.datetime.now() - (datetime.datetime.combine(datetime.date.today(), datetime.time(8, 30)))).total_seconds())/60.0) / (6.5*60) #gets back to you how long it's been since market open in the day 
#        day_frac = min(1, day_frac)  #takes the minimum of where you are in the day
#        if np.isnan(x):
#            return 0
#        if x < (500 * day_frac):
#            return 1
#        if x < (2500 * day_frac):
#            return 2
#        return 3
#
#    frame['level'] = frame['qty'].map(add_level) #returns back to the level based on the quantitty
#    frame['adj_level'] = frame['qty'].map(add_adj_level) 
#
#    data_map = pycake.DataMap('JG_PRINTVOLS_NEW', True)
#    #data_map.svr1 = c
#    data_map.clear()
#    #frame 
#
#    temp = frame[['StockSymbol', 'ExpYear', 'ExpMonth', 'ExpDay', 'Strike', 'Cp', 'VWAV_sell', 'VWAV_buy', 'max_vol_sell', 'max_vol_buy', 'min_vol_sell', 'min_vol_buy', 'level', 'adj_level']]
#    temp = pd.melt(temp, id_vars=['StockSymbol', 'ExpYear', 'ExpMonth', 'ExpDay', 'Strike', 'Cp']) #unpivotes dataframe to longer dimension format 
#    temp['variable'] = temp['variable'].apply(lambda x: x.upper()) 
#    temp['Strike'] = (temp['Strike']*1000.0)
#    temp_dict = retro_dictify(temp) 
#    data_map.notify(data = temp_dict)
#
#    # tr = tracker.SummaryTracker()
#    # tr.print_diff()
#
#    #perpetual for loop that runs during market hours, repopulates the datamap you've already created 
#    while datetime.datetime.now() < datetime.datetime.combine(datetime.date.today(), datetime.time(15, 0)): 
#        try:
#            keys = get_opts(s) #gets you back the set of values 
#            #tr.print_diff()
#            keys = fix_opts(keys) #returns back all keys to you in O format 
#            #tr.print_diff()
#            frame = frame.reindex(keys) #remakes them the index 
#
#            frame['ts'].fillna(ts, inplace=True) 
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