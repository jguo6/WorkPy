import pandas as pd
pd.options.mode.chained_assignment = None

import tickdb
from pandas import HDFStore
from datetime import date, datetime
import numpy as np
import os

from multiprocessing import Pool
from functools import partial

from pandas.tseries.holiday import USFederalHolidayCalendar
from pandas.tseries.offsets import CustomBusinessDay
us_bd = CustomBusinessDay(calendar=USFederalHolidayCalendar())


def fit_nbbo(frame, dt_string):
    if frame.empty:
        return

    start = frame.copy()
    ticker = frame['ticker'].iloc[0]

    try:
        if '.' in ticker:
            print ticker
            return start

        cols = ["ask", "bid", "delta", "iva_l", "iva_s", "ivb_l", "ivb_s", "secid", "tm"]
        ticks = tickdb.read_df(ticker, dt_string, start="08:30:00", end="15:00:00", security_type="option",
                               data_type="greek", columns=cols, chunk=True)

        merge_list = []

        for tick in ticks:
            frame = frame[frame['print_tm'] >= tick['tm'].iloc[0]]
            merged = frame.merge(tick, on='secid', how='left')
            merged = merged[merged['tm'] <= merged['print_tm']]
            if merged.empty:
                continue

            merged = merged.groupby('sym_fillint', as_index=False, sort=False).tail(1)
            merge_list.append(merged)

        if len(merge_list) == 0:
            return pd.DataFrame()

        full = pd.concat(merge_list).reset_index(drop=True)
        full = full.groupby('sym_fillint', as_index=False, sort=False).tail(1)
        return full

    except Exception as err:
        print ticker
        print err


def fit_full(gp, dt):
    if gp.empty:
        return gp

    tick = gp['ticker'].iloc[0]

    try:
        if '.' in tick:
            print tick
            return

        greek_cols = ['calc_price', 'delta', 'vega', 'iva_l', 'iva_s', 'ivb_l', 'ivb_s', 'secid', 'tm']
        prints = tickdb.read_df(tick, dt.isoformat(), 'greek-print', 'option', columns=greek_cols, chunk=False)
        prints = gp.merge(prints, on='secid', how='left')

        if prints.empty:
            return

        # prints.rename(columns={'tm': 'greek_tm'}, inplace=True)

        sz_cols = ['secid', 'tm', 'price', 'sz']
        print_sz = tickdb.read_df(tick, dt.isoformat(), 'print', 'option', columns=sz_cols, chunk=False)

        prints = print_sz.merge(prints, on=['secid', 'tm'], how='left')
        prints.sort_values('tm', ascending=True, inplace=True)

        def fill_gp(gp):
            # gp.sort_values('tm', inplace=True)
            gp.fillna(method='ffill', inplace=True)
            return gp

        prints = prints.groupby('secid', as_index=False).apply(fill_gp)
        prints = prints[~pd.isnull(prints['ticker'])].reset_index(drop=True)

        if prints.empty:
            return

        # prints = prints[(abs(prints['delta']) >= 0.20) & (abs(prints['delta'] <= 0.80))].reset_index(drop=True)

        '''choose print iv based on bid and hedge direction'''
        prints['iv'] = np.nan

        # print "REACHED"
        # print prints.head(3)
        prints['iv'].loc[prints['Cp']=='P'] = prints['ivb_l'].loc[prints['Cp']=='P']
        prints['iv'].loc[prints['Cp']=='C'] = prints['ivb_s'].loc[prints['Cp']=='C']
        prints['calc_price'] = prints['calc_price'] / 1000.0
        prints['sym_fillint'] = xrange(len(prints))
        # print "RE-REACHED"

        prints.rename(columns={'tm': 'print_tm',
                               'delta': 'print_delta',
                               'vega': 'print_vega'},
                      inplace=True)

        prints.drop(['iva_l', 'ivb_l', 'iva_s', 'ivb_s'], axis=1, inplace=True)

        full = fit_nbbo(prints, dt.isoformat())
        return full

    except Exception as err:
        print tick
        print err


def fit_list(groups):
    if not len(groups)==0:
        try:
            return pd.concat([fit_func(g) for g in groups])
        except Exception as err:
            print err
            return pd.DataFrame()

def pull(start, end):
    hdf = HDFStore('/home/slin/alldata/leaps/liq.h5', mode='a')

    dt = start
    while dt <= end:
        print dt

        try:
            fit_func = partial(fit_full, dt=dt)

            cores = 48
            vpool = Pool(cores)

            ids = pd.read_hdf('/home/slin/alldata/leaps/secids.h5', 'secids')
            # ids = ids.iloc[:10]

            grouped = ids.groupby(['ticker'], as_index=False, sort=False)
            keys = grouped.groups.keys()
            split = [list(arr) for arr in np.array_split(keys, cores)]
            groups = [map(lambda x: grouped.get_group(x), s) for s in split]

            results = vpool.map(fit_list, groups)
            frame = pd.concat(results).reset_index(drop=True)
            print frame

            try:
                hdf.put('{0}'.format(dt.strftime('%Y%m%d')), frame, format='table', data_columns=True)
            except Exception as err:
                print err

        except Exception as err:
            print err

        finally:
            dt = (dt + us_bd).date()

if __name__ == '__main__':

    # start = date(2016, 7, 1)
    # end = date(2016, 7, 20)

    end = date.today()
    start = (end - 2*us_bd).date()
    hdf = HDFStore('/home/slin/alldata/leaps/liq.h5', mode='a')

    dt = start
    while dt <= end:
        print dt

        try:
            fit_func = partial(fit_full, dt=dt)

            cores = 48
            vpool = Pool(cores)

            ids = pd.read_hdf('/home/slin/alldata/leaps/secids.h5', 'secids')
            # ids = ids.iloc[:10]

            grouped = ids.groupby(['ticker'], as_index=False, sort=False)
            keys = grouped.groups.keys()
            split = [list(arr) for arr in np.array_split(keys, cores)]
            groups = [map(lambda x: grouped.get_group(x), s) for s in split]

            results = vpool.map(fit_list, groups)
            frame = pd.concat(results).reset_index(drop=True)
            print frame

            try:
                hdf.put('{0}'.format(dt.strftime('%Y%m%d')), frame, format='table', data_columns=True)
            except Exception as err:
                print err

        except Exception as err:
            print err

        finally:
            dt = (dt + us_bd).date()