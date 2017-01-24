'''
Created on Oct 21, 2013

@author: yqin
'''
import os
import sys
import pyodbc
import logging
import datetime
import miscutils
import csv
import pysftp
import psycopg2
import zipfile
from mspostgresutil_config import *

def init_logging():
    try:
        if not os.path.exists(LOG_FILE_ROOT):
            os.makedirs(LOG_FILE_ROOT)
        logfilename = 'EventStatsDailyImport_' + miscutils.replace_all(str(datetime.datetime.now()),\
                                                      {'-':'_', ':':'_', ' ':'_','.':'_'}) + '.log'
        logfilepath = os.path.join(LOG_FILE_ROOT, logfilename)
        logging.basicConfig(filename = logfilepath, level=LOG_LEVEL,\
                          format='%(asctime)s: %(levelname)s: %(message)s')
        logging.info('Logging Initialized')
        return logfilename
    except Exception, err:
        miscutils.handle_exception('Exception setting up logging', err, True)            
        return None

def get_order_clspx(startdt, enddt):
    '''
    query sql9 cross join sql3         
    [startdt, endde)
    '''    
    qry0 = """
        with trd_cte (tradedate, ClOrdId, root_id, InstrumentID, stocksymbol, root_symbol, strike, put_call, expdt, stock_instrumentid)
        as
        (
        select a.*, s.instrument_id stock_instrumentid
        from
        (
        select distinct cast(DATEADD(dd, 0, DATEDIFF(dd, 0, i.CreateDate))as DATE) tradedate, o.ClOrdID, SUBSTRING(o.ClordID, 0, 1+LEN(o.ClOrdID)-CHARINDEX('-', reverse(o.ClOrdID), 0)) root_id, i.InstrumentID, s.stocksymbol, s.root_symbol, s.strike, s.put_call, 
            case when expiration_year = 0 then '19000101' 
            else  CAST(RIGHT('000' + CAST(expiration_year AS VARCHAR(4)), 4) + RIGHT('0' + CAST(expiration_month AS VARCHAR(2)), 2) +  RIGHT('0' + CAST(expiration_day AS VARCHAR(2)), 2) AS DATE)
        end expdt
        from sql3.OMSDB.dbo.tblIdeasArchive i with (Nolock)
        join sql3.OMSDB.dbo.tblOrdersArchive o with (nolock)
        on i.IdeaID=o.IdeaID and i.LegID=o.LegID
        join sql3.OMSDB.dbo.tblSecurities s with (Nolock)
        on i.InstrumentID=s.instrument_id
        where i.CreateDate between '%s' and '%s'
        and i.Strategy='EXSVC'
        ) a
        left join sql3.OMSDB.dbo.tblSecurities s with (Nolock)
        on a.stocksymbol=s.stocksymbol and s.put_call='S'
        )
        select a.*, epx1.closing stock_closing
        from
        (
        select trdr.*, opx.Closing instrument_closing, opx.SecurityKey
        from
        trd_cte trdr
        left join DataMaster.dbo.tblOptionPrice opx
        on trdr.instrumentid=opx.instrumentid and trdr.tradedate=opx.TradeDate
        where opx.TradeDate between '%s' and '%s'
        union
        select trdr.*, epx.Closing instrument_closing, epx.SecurityKey
        from
        trd_cte trdr
        left join DataMaster.dbo.tblEquityPrice epx
        on trdr.instrumentid=epx.instrumentid and trdr.tradedate=epx.TradeDate
        where epx.TradeDate between '%s' and '%s'
        ) a
        left join
        DataMaster.dbo.tblEquityPrice epx1
        on a.stock_instrumentid=epx1.instrumentid and a.tradedate=epx1.TradeDate
        where epx1.TradeDate between '%s' and '%s'
        """ % (startdt, enddt, startdt, enddt, startdt, enddt, startdt, enddt)
        
    qry1 = """
        if OBJECT_ID('tempdb..#tmp1', 'U') IS NOT NULL drop table #tmp1
        """
    qry2 = """
         SELECT distinct transactiondate tradedate,  
         null instrument_id,
         RTRIM(stocksymbol) underlying,
         RTRIM(root) root,
         strike,
         cps,    
         CAST(CAST(exyear AS VARCHAR(4)) + RIGHT('0' + CAST(exmonth AS VARCHAR(2)), 2) + RIGHT('0' + CAST(FlexDayOfMonth AS VARCHAR(2)), 2) AS DATETIME) expdt,
         'S:'+RTRIM(StockSymbol) StockSymbol,
         case when CPS in ('C','P')
         then 'O:'+RTRIM(root)+':'+cast(ExYear as VARCHAR(4))+right('00'+cast(ExMonth as VARCHAR(2)),2)+RIGHT('00'+cast(flexdayofmonth as VARCHAR(2)), 2)+ ':'+ cast(strike as VARCHAR(16))+ ':'+ CPS
         else 'S:'+RTRIM(StockSymbol)
         end SecurityKey    
         into #tmp1  
         FROM sql3.RTPosition.dbo.tblactivities with (nolock)
         where TransactionDate between '%s' and '%s'
        """ % (startdt, enddt)
    qry3 = """
        select a.tradedate, null clordid, null root_id, null instrument_id,
            a.underlying, a.root, a.strike, a.cps, a.expdt, 
            null stock_instrument_id, a.instrument_closing, a.securitykey, epx1.closing stock_closing
        from
        (
        select trdr.*, opx.Closing instrument_closing 
        from
        #tmp1 trdr
        join DataMaster.dbo.tblOptionPrice opx with (nolock)
        on trdr.SecurityKey=opx.SecurityKey and trdr.tradedate=opx.TradeDate
        where opx.TradeDate between '%s' and '%s'
        union
        select trdr.*, epx.Closing instrument_closing
        from
        #tmp1 trdr
        join DataMaster.dbo.tblEquityPrice epx with (nolock)
        on trdr.SecurityKey=epx.SecurityKey and trdr.tradedate=epx.TradeDate
        where epx.TradeDate between '%s' and '%s'
        ) a
        left join
        DataMaster.dbo.tblEquityPrice epx1 with (nolock)
        on a.StockSymbol=epx1.SecurityKey and a.tradedate=epx1.TradeDate
        where epx1.TradeDate between '%s' and '%s'
        """ % (startdt, enddt, startdt, enddt, startdt, enddt)
    
    try:
        logging.info('Querying trade close price.')
        logging.info(qry1)
        logging.info(qry2)
        logging.info(qry3)
        connstr = "Driver={SQL Server};Server=%s;Database=%s;Uid=;Pwd=" % (SQL9, 'datamaster')
        conn = pyodbc.connect(connstr)
        cursor = conn.cursor()
        cursor.execute(qry1)
        cursor.execute(qry2)
        cursor.execute(qry3)        
        rows = cursor.fetchall()
        row_cnt= 0    
        with open(os.path.join(OUTPUT_DIR, OUTPUT_FILE_TRDCLSPX_TBL[0]),'wb') as csvfile:
            for row in rows:    
                csv.writer(csvfile, delimiter=',').writerow(row)
                row_cnt += 1
                if row_cnt % 10000 == 0:
                    csvfile.flush()     
        logging.info('%s rows returned' % (row_cnt))                           
    except Exception as err:
        miscutils.handle_exception('Exception querying trade close price', err, True)
    finally:                
        cursor.close()
        conn.close()    
        
def get_stk_clspx(startdt, enddt):
    '''
    query sql9         
    [startdt, endde]
    '''
    qry = """
    select distinct cast(DATEADD(dd, 0, DATEDIFF(dd, 0, tradedate))as DATE) tradedate, substring(Security_Key,3,LEN(Security_Key)-2) underlying, closing, i.instrument_id
    from DataMaster..tblEquityPrice e with (nolock)
    join DataMaster..tblInstrument i with (nolock)
    on e.InstrumentId=i.instrument_id and TradeDate between i.rowin and i.rowout
    where TradeDate between '%s' and '%s'
    """ % (startdt, enddt)
    try:
        logging.info('Querying trade close price.')
        logging.info(qry)
        connstr = "Driver={SQL Server};Server=%s;Database=%s;Uid=;Pwd=" % (SQL9, 'datamaster')
        conn = pyodbc.connect(connstr)
        cursor = conn.cursor()
        cursor.execute(qry)
        rows = cursor.fetchall()
        row_cnt= 0    
        with open(os.path.join(OUTPUT_DIR, OUTPUT_FILE_STKCLSPX_TBL[0]),'wb') as csvfile:
            for row in rows:    
                csv.writer(csvfile, delimiter=',').writerow(row)
                row_cnt += 1
                if row_cnt % 10000 == 0:
                    csvfile.flush()
        logging.info('%s rows returned' % (row_cnt))                                
    except Exception as err:
        miscutils.handle_exception('Exception querying trade close price', err, True)
    finally:                
        cursor.close()
        conn.close()

def get_opt_clspx(startdt, enddt):
    '''
    query sql9         
    [startdt, endde]
    '''
    qry = """
    select distinct cast(DATEADD(dd, 0, DATEDIFF(dd, 0, tradedate))as DATE) tradedate, Security_Key underlying, closing, i.instrument_id
    from DataMaster..tblOptionPrice e with (nolock)
    join DataMaster..tblInstrument i with (nolock)
    on e.InstrumentId=i.instrument_id and TradeDate between i.rowin and i.rowout
    where TradeDate between '%s' and '%s'
    and e.SecurityKey in 
    (
    select distinct 'O:'+RTRIM(root)+':'+cast(ExYear as VARCHAR(4))+right('00'+cast(ExMonth as VARCHAR(2)),2)+RIGHT('00'+cast(flexdayofmonth as VARCHAR(2)), 2)+ ':'+ cast(strike as VARCHAR(16))+ ':'+ CPS
    from sql3.RTPosition.dbo.tblactivities with (nolock)
    where TransactionDate between '%s' and '%s'
    )  
    """ % (startdt, enddt, startdt+datetime.timedelta(days=-40), enddt+datetime.timedelta(days=1))
    qry1 = """
    if OBJECT_ID('tempdb..#tmp2', 'U') IS NOT NULL drop table #tmp2
    """
    qry2 = """
    select distinct 'O:'+RTRIM(root)+':'+cast(ExYear as VARCHAR(4))+right('00'+cast(ExMonth as VARCHAR(2)),2)+RIGHT('00'+cast(flexdayofmonth as VARCHAR(2)), 2)+ ':'+ cast(strike as VARCHAR(16))+ ':'+ CPS SecurityKey
    into #tmp2
    from sql3.RTPosition.dbo.tblactivities with (nolock)
    where TransactionDate between '%s' and '%s'
    order by 1 
    """ % (startdt+datetime.timedelta(-40), enddt+datetime.timedelta(1))
    qry3 = """
    create clustered index cidx_temp2_key on #tmp2 (securitykey)
    """
    qry4 = """
    select cast(DATEADD(dd, 0, DATEDIFF(dd, 0, tradedate))as DATE) tradedate, Security_Key underlying, closing, i.instrument_id
    from DataMaster..tblOptionPrice e with (nolock)
    join #tmp2 t
            on e.SecurityKey=t.SecurityKey
    join DataMaster..tblInstrument i with (nolock)    
            on e.InstrumentId=i.instrument_id and TradeDate between i.rowin and i.rowout
    where e.TradeDate between '%s' and '%s'
    """ % (startdt, enddt)     
    try:
        logging.info('Querying trade close price.')
        logging.info(qry1)
        logging.info(qry2)
        logging.info(qry3)
        logging.info(qry4)
        connstr = "Driver={SQL Server};Server=%s;Database=%s;Uid=;Pwd=" % (SQL9, 'datamaster')
        conn = pyodbc.connect(connstr)
        cursor = conn.cursor()
        cursor.execute(qry1)
        cursor.execute(qry2)
        cursor.execute(qry3)
        cursor.execute(qry4)
        rows = cursor.fetchall()
        row_cnt= 0    
        with open(os.path.join(OUTPUT_DIR, OUTPUT_FILE_OPTCLSPX_TBL[0]),'wb') as csvfile:
            for row in rows:    
                csv.writer(csvfile, delimiter=',').writerow(row)
                row_cnt += 1
                if row_cnt % 10000 == 0:
                    csvfile.flush()
        logging.info('%s rows returned' % (row_cnt))                                
    except Exception as err:
        miscutils.handle_exception('Exception querying trade close price', err, True)
    finally:                
        cursor.close()
        conn.close()
    
    
def get_stk_medszwdthadv(enddt):
    '''
    query sql9, sql3        
    [startdt]
    '''
    qry = """
    with ivy_cte(securityid, stocksymbol) as 
    (
    select securityid
          ,RTRIM(s.Ticker) + 
                    CASE WHEN s.class IS NOT NULL AND LTRIM(RTRIM(s.class)) != ''
                            THEN '.' + LTRIM(s.Class)
                            ELSE ''
                    END  StockSymbol
    from [ppi-sql9].ivydb.dbo.Security s with (nolock)
    ),
    hedgingetc_cte(stocksymbol, mediandailyvlm, startdate) as
    (
    select distinct replace(LTRIM(RTRIM(stocksymbol)),'/','.'), MedianDailyVlm, StartDate 
    from StockTrading..tblHedgingETC with (nolock)
    where EndDate is null    
    )
    select tc.PreviousTradeDate, c.stocksymbol, c.MedianDailyVlm, NULL, NULL --, a.medianSize, a.medianWidthBPS
    from 
    (
    select c1.stocksymbol, c1.mediandailyvlm, CAST(c2.startdate as DATE) startdate
    from hedgingetc_cte c1
    join
    (
    select stocksymbol, MAX(startdate) startdate
    from hedgingetc_cte
    group by stocksymbol
    ) c2
    on c1.stocksymbol=c2.stocksymbol and c1.startdate=c2.startdate
    ) c
    join PEAK6Research..tblCalendar tc with (nolock)
    on c.startdate=tc.CalendarDate
    where tc.CalendarDate='%s' and tc.IsHoliday=0
    """ % (enddt)
    try:
        logging.info('Querying trade median size width and ADV.')
        logging.info(qry)
        connstr = "Driver={SQL Server};Server=%s;Database=%s;Uid=;Pwd=" % (SQL3, 'stocktrading')
        conn = pyodbc.connect(connstr)
        cursor = conn.cursor()
        cursor.execute(qry)
        rows = cursor.fetchall()
        row_cnt= 0    
        with open(os.path.join(OUTPUT_DIR, OUTPUT_FILE_MEDSZWDTHADV_TBL[0]),'wb') as csvfile:
            for row in rows:    
                csv.writer(csvfile, delimiter=',').writerow(row)
                row_cnt += 1
                if row_cnt % 10000 == 0:
                    csvfile.flush()
        logging.info('%s rows returned' % (row_cnt))                    
    except Exception as err:
        miscutils.handle_exception('Exception querying stock median size width and ADV', err, True)
    finally:                
        cursor.close()
        conn.close()

def get_oi(startdt, enddt):
    '''
    query sql9         
    [startdt, endde]
    '''
    qry = """
    select TradeDate, 'O:'+OptionSymbol+':'+ convert(varchar, ExpDate, 112) +':'+ CAST(strikePrice as varchar(20))+':'+CallPut,
    cast(Volume as integer), OpenInterest, Delta, ImpliedVolatility, ExpDate
    from TraderCubes..tblCubeOptionVolumeFact with (nolock)
    where TradeDate between '%s' and '%s'
    and OpenInterest>0    
    """ % (startdt, enddt)
    try:
        logging.info('Querying trade close price.')
        logging.info(qry)
        connstr = "Driver={SQL Server};Server=%s;Database=%s;Uid=;Pwd=" % (SQL9, 'datamaster')
        conn = pyodbc.connect(connstr)
        cursor = conn.cursor()
        cursor.execute(qry)
        rows = cursor.fetchall()
        row_cnt= 0    
        with open(os.path.join(OUTPUT_DIR, OUTPUT_FILE_OI_TBL[0]),'wb') as csvfile:
            for row in rows:    
                csv.writer(csvfile, delimiter=',').writerow(row)
                row_cnt += 1
                if row_cnt % 10000 == 0:
                    csvfile.flush()
        logging.info('%s rows returned' % (row_cnt))                                
    except Exception as err:
        miscutils.handle_exception('Exception querying trade oi', err, True)
    finally:                
        cursor.close()
        conn.close()    
		

def get_penny_symbol():
    '''
    query sql9
    '''
    qry = """
    select RTRIM(LTRIM(ticker)), rowin, rowout
    from DataMaster..tblPennyWideQuote with (NOLOCK)    
    """
    try:
        logging.info('Querying penny symbol.')
        logging.info(qry)
        connstr = "Driver={SQL Server};Server=%s;Database=%s;Uid=;Pwd=" % (SQL9, 'datamaster')
        conn = pyodbc.connect(connstr)
        cursor = conn.cursor()
        cursor.execute(qry)
        rows = cursor.fetchall()
        row_cnt= 0    
        with open(os.path.join(OUTPUT_DIR, OUTPUT_FILE_PENNY_TBL[0]),'wb') as csvfile:
            for row in rows:    
                csv.writer(csvfile, delimiter=',').writerow(row)
                row_cnt += 1
                if row_cnt % 10000 == 0:
                    csvfile.flush()
        logging.info('%s rows returned' % (row_cnt))                                
    except Exception as err:
        miscutils.handle_exception('Exception querying trade close price', err, True)
    finally:                
        cursor.close()
        conn.close()    

def get_stkimpact(dt):
    '''
    query sql3
    '''
    qry = """
    select ticker, shares, BPS
    from StockTrading..tblTickerSharesBPS with (nolock)
    """
    try:
        logging.info('Querying stock impact.')
        logging.info(qry)
        connstr = "Driver={SQL Server};Server=%s;Database=%s;Uid=;Pwd=" % (SQL3, 'StockTrading')
        conn = pyodbc.connect(connstr)
        cursor = conn.cursor()
        cursor.execute(qry)
        rows = cursor.fetchall()
        row_cnt= 0    
        with open(os.path.join(OUTPUT_DIR, OUTPUT_FILE_IMPACT[0]),'wb') as csvfile:
            for row in rows:
                l = [dt]
                l.extend(row)    
                csv.writer(csvfile, delimiter=',').writerow(l)
                row_cnt += 1
                if row_cnt % 10000 == 0:
                    csvfile.flush()
        logging.info('%s rows returned' % (row_cnt))                                
    except Exception as err:
        miscutils.handle_exception('Exception querying stock impact', err, True)
    finally:                
        cursor.close()
        conn.close()
		
def get_bvol_order(startdt, enddt, dbs):
    '''
    query postgres
    '''
    qry = """
    select distinct date_trunc('day', nw.create_time) dt, nw.account, lg.security_id, 1
    from events.new nw
    join events.legs lg
    on nw.session_id=lg.session_id and nw.root_id=lg.root_id and nw.pid=lg.pid
    where nw.create_time between '%s' and '%s'
    and nw.root_id like '%%.%%' and nw.from_pid=0
    and nw.actor_name='VolActor'
    """ % (startdt, enddt+datetime.timedelta(1))
    
    logging.info('Querying bulk vol order info from Postgres')
    logging.info(qry)
    
    try:
        rows = set()
        row_cnt = 0
        for (server, prt) in dbs:
            conn_sql = psycopg2.connect(host=server, user='execution', password='execution', port=prt, database='execution')
            cur = conn_sql.cursor()    
            cur.execute (qry)
            conn_sql.commit()
            for row in cur.fetchall():
                rows.add(row)     
        with open(os.path.join(OUTPUT_DIR, OUTPUT_FILE_BVOLORDER_TBL[0]),'wb') as csvfile:
            for row in rows:    
                csv.writer(csvfile, delimiter=',').writerow(row)
                row_cnt += 1
                if row_cnt % 10000 == 0:
                    csvfile.flush()        
    except Exception as err:
        miscutils.handle_exception('Exception querying fills in postgres', err, True)
    finally:                            
        cur.close()
        conn_sql.close()
        return row_cnt

def get_batsrisk(dt):
    '''
    query postgres
    trading server
    '''
    qry = '''
    select id, created_by, created_dt, modified_by, modified_dt, firm, underlyer, root, rule, value, time_period
    from trading.bats_riskcheck_overrides
    '''
    
    logging.info('Querying bulk vol order info from Postgres')
    logging.info(qry)
    
    try:
        rows = list()
        row_cnt = 0
        server = DB_TRADING[0]
        prt = DB_TRADING[1]
        
        conn_sql = psycopg2.connect(host=server, user='trading', password='trading', port=prt, database='trading')
        cur = conn_sql.cursor()    
        cur.execute (qry)
        conn_sql.commit()
        for row in cur.fetchall():
            l = [dt]
            l.extend(row)
            rows.append(l)     
        with open(os.path.join(OUTPUT_DIR, OUTPUT_FILE_BATS_RISK[0]),'wb') as csvfile:
            for row in rows:    
                csv.writer(csvfile, delimiter=',').writerow(row)
                row_cnt += 1
                if row_cnt % 10000 == 0:
                    csvfile.flush()        
    except Exception as err:
        miscutils.handle_exception('Exception querying fills in postgres', err, True)
    finally:                            
        cur.close()
        conn_sql.close()
        return row_cnt
    
def get_subjective_ratings(dt, db):
    '''
    query postgres
    prod
    '''
    qry = '''
    select session_id, root_id, dt, rating, reason, exectype
    from eventstats.bulk_fill_ratings
    where dt='%s' 
    ''' % (dt)
    
    logging.info('Querying subjective ratings from Postgres')
    logging.info(qry)
    
    try:
        rows = list()
        row_cnt = 0
        server = db[0]
        prt = db[1]
        
        conn_sql = psycopg2.connect(host=server, user='execution', password='execution', port=prt, database='execution')
        cur = conn_sql.cursor()    
        cur.execute (qry)
        conn_sql.commit()
        for row in cur.fetchall():
            rows.append(row)
        with open(os.path.join(OUTPUT_DIR, OUTPUT_FILE_SUBJECTIVE_RATINGS[0]),'wb') as csvfile:
            for row in rows:    
                csv.writer(csvfile, delimiter=',').writerow(row)
                row_cnt += 1
                if row_cnt % 10000 == 0:
                    csvfile.flush()        
    except Exception as err:
        miscutils.handle_exception('Exception querying fills in postgres', err, True)
    finally:                            
        cur.close()
        conn_sql.close()
        return row_cnt
    
def get_subjective_cache(dt, db):
    '''
    query postgres
    prod
    '''
    qry = '''
    select *
    from eventstats.bsr_scorecard_cache 
    where dt='%s' 
    ''' % (dt)
    
    logging.info('Querying subjective ratings from Postgres')
    logging.info(qry)
    
    try:
        rows = list()
        row_cnt = 0
        server = db[0]
        prt = db[1]
        
        conn_sql = psycopg2.connect(host=server, user='execution', password='execution', port=prt, database='execution')
        cur = conn_sql.cursor()    
        cur.execute (qry)
        conn_sql.commit()
        for row in cur.fetchall():            
            rows.append(row)
        with open(os.path.join(OUTPUT_DIR, OUTPUT_FILE_SUBJECTIVE_CACHE[0]),'wb') as csvfile:
            for row in rows:    
                csv.writer(csvfile, delimiter=',').writerow(row)
                row_cnt += 1
                if row_cnt % 10000 == 0:
                    csvfile.flush()        
    except Exception as err:
        miscutils.handle_exception('Exception querying fills in postgres', err, True)
    finally:                            
        cur.close()
        conn_sql.close()
        return row_cnt
    
def dump_sql_to_csv_dtrange(startdt, enddt):
    if OUTPUT_FILE_TRDCLSPX_TBL in OUTPUT_FILES_RANGE:
        get_order_clspx(str(startdt), str(enddt))
    if OUTPUT_FILE_STKCLSPX_TBL in OUTPUT_FILES_RANGE:
        get_stk_clspx(str(startdt), str(enddt))
    if OUTPUT_FILE_OPTCLSPX_TBL in OUTPUT_FILES_RANGE:
        get_opt_clspx(startdt, enddt)
    if OUTPUT_FILE_OI_TBL in OUTPUT_FILES_RANGE:
        get_oi(startdt, enddt)
    if OUTPUT_FILE_BVOLORDER_TBL in OUTPUT_FILES_RANGE_SQL:
        get_bvol_order(startdt, enddt, DBS)
    if OUTPUT_FILE_SUBJECTIVE_RATINGS in OUTPUT_FILES_RANGE:
        get_subjective_ratings(enddt, DB_PROD)
    if OUTPUT_FILE_SUBJECTIVE_CACHE in OUTPUT_FILES_RANGE:
        get_subjective_cache(enddt, DB_PROD)
            

def dump_sql_to_csv_curdt(curdt):
    if OUTPUT_FILE_MEDSZWDTHADV_TBL in OUTPUT_FILES_CUR:
        # date is T-2, because TICKDATA provides T-1 for @T in the table, 
        # thus @T, we run the job before TICKDATA pushing job, the date column is T-2
        get_stk_medszwdthadv(str(curdt))     
    if OUTPUT_FILE_PENNY_TBL in OUTPUT_FILES_CUR:
        get_penny_symbol()
    if OUTPUT_FILE_IMPACT in OUTPUT_FILES_CUR:
        get_stkimpact(curdt)
		
    
def dump_postgres_to_csv_curdt(curdt):
    if OUTPUT_FILE_BATS_RISK in OUTPUT_FILE_POSTGRES_CUR:
        get_batsrisk(curdt)    
    
def copy_to_postres_via_scp(csv_fir, server, user, pwd, file_nm):
    conn = None    
    os.chdir(OUTPUT_DIR)
    try:     
        conn = pysftp.Connection(server, user , None, pwd)
        conn.put(file_nm)
        conn.close()
    except Exception as err:
        miscutils.handle_exception('Exception copying data to postgres', err, True)
    finally:
        if conn:
            conn.close()            
    
def copy_to_postgres(is_curdt, dbs=DBS):
    try:
        os.chdir(OUTPUT_DIR)
        for (server, prt) in dbs:
            logging.info('Processing server: %s, port %s' % (server, prt))
            #conn = pysftp.Connection(server, 'yqin', None, 'xxxxxx')
            #conn.chdir(OUTPUT_DBSRVR_DIR)
            #for filenm in OUTPUT_FILES:
            #    conn.put(filenm)
                
            conn_sql = psycopg2.connect(host=server, user='execution', password='execution', port=prt, database='execution')
            cur = conn_sql.cursor()
            #qry = """COPY eventstats.trade_clspx from '/home/yqin/trdclspx.csv' DELIMITER ',' CSV;"""            
            #cur.execute (qry)
            #conn_sql.commit()          
            if is_curdt:
                for filenm_tbl in OUTPUT_FILES_CUR:
                    logging.info('Processing file: %s, table: %s' % (filenm_tbl[0], filenm_tbl[1]))
                    copy_to_postres_via_scp(OUTPUT_DIR, server, FTPUSERNAME, FTPPASSWD, filenm_tbl[0])
                    qry = """COPY """ + filenm_tbl[1] + """ from '/home/labsuser/""" + filenm_tbl[0] +"""' DELIMITER ',' NULL '' CSV;"""
                    cur.execute (qry)
                    conn_sql.commit()
                    #f = open(os.path.join(OUTPUT_DIR, filenm_tbl[0]), 'rb')
                    #cur.copy_from(f, filenm_tbl[1], sep=',')
                    #conn_sql.commit()
                    #f.close()
            else:
                for filenm_tbl in OUTPUT_FILES_RANGE:
                    logging.info('Processing file: %s, table: %s' % (filenm_tbl[0], filenm_tbl[1]))
                    copy_to_postres_via_scp(OUTPUT_DIR, server, FTPUSERNAME, FTPPASSWD, filenm_tbl[0])
                    qry = """COPY """ + filenm_tbl[1] + """ from '/home/labsuser/""" + filenm_tbl[0] +"""' DELIMITER ',' CSV;"""
                    cur.execute (qry)
                    conn_sql.commit()
            cur.close()
            conn_sql.close()
    except Exception as err:
        miscutils.handle_exception('Exception copying data to postgres', err, True)
    finally:
        conn_sql.close()


def adhoc_copy_to_postgres(dbs=DBS, output_files=ADHOC_OUTPUT_FILES):
    '''
    insert into the table based on the csv
    both are in configure file
    '''
    try:
        os.chdir(OUTPUT_DIR)
        for (server, prt) in dbs:
            logging.info('Processing server: %s, port %s' % (server, prt))
            conn_sql = psycopg2.connect(host=server, user='execution', password='execution', port=prt, database='execution')
            cur = conn_sql.cursor()
            for filenm_tbl in output_files:
                logging.info('Processing file: %s, table: %s' % (filenm_tbl[0], filenm_tbl[1]))
                #f = open(os.path.join(OUTPUT_DIR, filenm_tbl[0]), 'rb')
                #cur.copy_from(f, filenm_tbl[1], sep=',')                
                copy_to_postres_via_scp(OUTPUT_DIR, server, FTPUSERNAME, FTPPASSWD, filenm_tbl[0])
                qry = """COPY """ + filenm_tbl[1] + """ from '/home/labsuser/""" + filenm_tbl[0] +"""' DELIMITER ',' CSV;"""
                cur.execute (qry)
                conn_sql.commit()
                #f.close()
            cur.close()
            conn_sql.close()
    except Exception as err:
        miscutils.handle_exception('Exception copying data to postgres', err, True)
    finally:
        conn_sql.close()

def delete_postgres_tbl(db, filenm_tbl, startdt=None, enddt=None):
    '''
    delete the rows in table based on date(s)
    '''
    try:
        server = db[0]
        prt = db[1]        
        logging.info('Processing server: %s, port %s' % (server, prt))            
                
        conn_sql = psycopg2.connect(host=server, user='execution', password='execution', port=prt, database='execution')
        cur = conn_sql.cursor()                
        logging.info('removing date: %s from table %s' % (enddt, filenm_tbl[1]))                    
        cur.execute('''delete from %s where dt between '%s' and '%s' ''' % (filenm_tbl[1], startdt, enddt))
        conn_sql.commit()   
        cur.close()
        conn_sql.close()
    except Exception as err:
        miscutils.handle_exception('Exception deleting data to postgres', err, True)
    finally:
        conn_sql.close()
    

def delete_postgres(is_curdt, startdt=None, enddt=None):
    '''
    delete the rows in table based on date(s)
    '''
    try:
        for (server, prt) in DBS:
            logging.info('Processing server: %s, port %s' % (server, prt))            
                
            conn_sql = psycopg2.connect(host=server, user='execution', password='execution', port=prt, database='execution')
            cur = conn_sql.cursor()                      
            if is_curdt:
                for filenm_tbl in OUTPUT_FILES_CUR:
                    logging.info('removing date: %s from table %s' % (enddt, filenm_tbl[1]))                    
                    cur.execute('''delete from %s where dt=%s''' % (filenm_tbl[1], enddt))
                    conn_sql.commit()                     
            else:
                for filenm_tbl in OUTPUT_FILES_RANGE:
                    logging.info('removing date from %s to %s from table: %s' % (startdt, enddt, filenm_tbl[1]))                    
                    cur.execute('''delete from %s where tradedate between %s and %s''' % (filenm_tbl[1], startdt, enddt))
                    conn_sql.commit()                    
            cur.close()
            conn_sql.close()
    except Exception as err:
        miscutils.handle_exception('Exception deleting data to postgres', err, True)
    finally:
        conn_sql.close()


def delete_all_postgres():
    '''
    delete the whole table
    '''  
    try:
        for (server, prt) in DBS:
            logging.info('Processing server: %s, port %s' % (server, prt))        
                
            conn_sql = psycopg2.connect(host=server, user='execution', password='execution', port=prt, database='execution')
            cur = conn_sql.cursor()
            for filenm_tbl in OUTPUT_FILES_DELETE:
                logging.info('removing all records from table %s' % (filenm_tbl[1]))                    
                cur.execute('''delete from %s ''' % (filenm_tbl[1]))
                conn_sql.commit()
            cur.close()
            conn_sql.close()
    except Exception as err:
        miscutils.handle_exception('Exception deleting data to postgres', err, True)
    finally:
        conn_sql.close()
        
def copy_to_mssql():
    '''
    '''
    if OUTPUT_FILE_BVOLORDER_TBL in OUTPUT_FILES_RANGE_SQL:
        sqlconn = "Driver={SQL Server};Server=%s;Database=%s;Uid=;Pwd=" % ('pswchi6psql2', 'Labsprojects')    
        qry = miscutils.gen_bulk_insert_sql_stmt(OUTPUT_FILE_BVOLORDER_TBL[1],OUTPUT_FILE_BVOLORDER_TBL[2],OUTPUT_FILE_BVOLORDER_TBL[3],os.path.join(OUTPUT_DIR, OUTPUT_FILE_BVOLORDER_TBL[0]),',','\\n')
        logging.info('Bulk Inserting to MSSQL')
        logging.info(qry)     
        conn = pyodbc.connect(sqlconn)
        try:            
            cursor = conn.cursor()
            cursor.execute(qry)
            conn.commit()         
        finally:            
            conn.close()
        
def exec_sql(qry):
    '''
    execute postgres query
    '''    
    try:
        server = DB_PROD_ARCHIVE[0]
        prt = DB_PROD_ARCHIVE[1]    
        logging.info('Processing server: %s, port %s' % (server, prt))
        logging.info(qry)
        conn_sql = psycopg2.connect(host=server, user='execution', password='execution', port=prt, database='execution')
        cur = conn_sql.cursor()
        cur.execute(qry)
        conn_sql.commit()
    except Exception as err:
        miscutils.handle_exception('Exception deleting data to postgres', err, True)    
    finally:
        conn_sql.close()

def exec_sql_w_svr(server, prt, qry):
    '''
    execute postgres query
    '''    
    try:            
        logging.info('Processing server: %s, port %s' % (server, prt))
        logging.info(qry)
        conn_sql = psycopg2.connect(host=server, user='execution', password='execution', port=prt, database='execution')
        cur = conn_sql.cursor()
        cur.execute(qry)
        conn_sql.commit()
    except Exception as err:
        miscutils.handle_exception('Exception deleting data to postgres', err, True)    
    finally:
        conn_sql.close()
        
def archive_phx_log_files(start_date, end_date, servers, archivebasepath, user, passwd):
    '''
    '''
    logging.info('archive_phx_log_files %s %s', str(start_date), str(end_date))
    cwd = os.getcwd()
    try:
        archivebasepath = os.path.abspath(archivebasepath)
        sestgtpath = os.path.join(archivebasepath, 'PHX')
        if not os.path.exists(sestgtpath):
            os.makedirs(sestgtpath)
        for server in servers:
            conn = None
            try:
                logging.info('archive_phx_log_files: processing server: %s', server)
                #connect remote
                conn = pysftp.Connection(server, user, None, passwd)
                remotebasepath = '/plogs/sunzi'
                conn.chdir(remotebasepath)
                #create local path           
                svrtgtpath = os.path.join(sestgtpath, server)
                if not os.path.exists(svrtgtpath):
                    os.makedirs(svrtgtpath)                       
                currdt = start_date
                while currdt <= end_date:               
                    try:                  
                        logging.info('archive_phx_log_files: processing date: %s', str(currdt))
                        remotepath = remotebasepath
                        conn.chdir(remotepath)
                        tgtpath = os.path.join(svrtgtpath, str(currdt))
                        if not os.path.exists(tgtpath): 
                           os.makedirs(tgtpath)
                        os.chdir(tgtpath)
                        for f in conn.listdir():
                            if str(currdt) in f and 'jvmgc' in f:
                                logging.debug('archive_phx_log_files: downloading: %s%s => %s', remotepath, f, tgtpath)
                                try:
                                    conn.get(f)
                                    tgtfile = os.path.join(tgtpath, f)
                                    if os.path.exists(tgtfile + '.zip'):
                                        os.remove(tgtfile + '.zip')
                                    z = zipfile.ZipFile(tgtfile + '.zip', 'w', zipfile.ZIP_DEFLATED)
                                    z.write(tgtfile, f)
                                    z.close()
                                    if os.path.exists(tgtfile):
                                        os.remove(tgtfile)
                                except Exception, err:
                                    logging.info('archive_phx_log_files: file skipped %s', str(f))
                                    miscutils.handle_exception('exception', err, False)
                    except Exception, err:
                        logging.info('archive_phx_log_files: date skipped %s', str(currdt))
                        miscutils.handle_exception('exception', err, False)
                        pass
                    finally:
                        currdt = currdt + datetime.timedelta(days=1)
            except Exception, err:
                logging.info('archive_pez_fix_client_log_files: server skipped %s', str(server))
                miscutils.handle_exception('exception', err, False) 
                pass
            finally:
                if not conn == None:
                    conn.close()
    finally:
        if not conn == None:
            conn.close()
                                                   
def archive_fix_log_files(start_date, end_date, servers, archivebasepath, user, passwd):
    '''
    '''
    logging.info('archive_fix_log_files %s %s', str(start_date), str(end_date))
    cwd = os.getcwd()
    try:
        archivebasepath = os.path.abspath(archivebasepath)
        sestgtpath = os.path.join(archivebasepath, 'OR/gc')
        if not os.path.exists(sestgtpath):
            os.makedirs(sestgtpath)
        for server in servers:
            conn = None
            try:
                logging.info('archive_fix_log_files: processing server: %s', server)
                #connect remote
                conn = pysftp.Connection(server, user, None, passwd)
                remotebasepath = '/plogs/fixgateway'
                conn.chdir(remotebasepath)
                #create local path           
                svrtgtpath = os.path.join(sestgtpath, server)
                if not os.path.exists(svrtgtpath):
                    os.makedirs(svrtgtpath)                       
                currdt = start_date
                while currdt <= end_date:               
                    try:                  
                        logging.info('archive_fix_log_files: processing date: %s', str(currdt))
                        remotepath = remotebasepath
                        conn.chdir(remotepath)
                        tgtpath = os.path.join(svrtgtpath, str(currdt))
                        if not os.path.exists(tgtpath): 
                           os.makedirs(tgtpath)
                        os.chdir(tgtpath)
                        for f in conn.listdir():
                            if str(currdt) in f and 'jvmgc' in f:
                                logging.debug('archive_fix_log_files: downloading: %s%s => %s', remotepath, f, tgtpath)
                                try:
                                    conn.get(f)
                                    tgtfile = os.path.join(tgtpath, f)
                                    if os.path.exists(tgtfile + '.zip'):
                                        os.remove(tgtfile + '.zip')
                                    z = zipfile.ZipFile(tgtfile + '.zip', 'w', zipfile.ZIP_DEFLATED)
                                    z.write(tgtfile, f)
                                    z.close()
                                    if os.path.exists(tgtfile):
                                        os.remove(tgtfile)
                                except Exception, err:
                                    logging.info('archive_fix_log_files: file skipped %s', str(f))
                                    miscutils.handle_exception('exception', err, False)
                    except Exception, err:
                        logging.info('archive_fix_log_files: date skipped %s', str(currdt))
                        miscutils.handle_exception('exception', err, False)
                        pass
                    finally:
                        currdt = currdt + datetime.timedelta(days=1)
            except Exception, err:
                logging.info('archive_pez_fix_client_log_files: server skipped %s', str(server))
                miscutils.handle_exception('exception', err, False) 
                pass
            finally:
                if not conn == None:
                    conn.close()
    finally:
        if not conn == None:
            conn.close()

if __name__ == "__main__":
    init_logging()
    start_day = datetime.date.today() - datetime.timedelta(OFF_DAY)
    end_day =  datetime.date.today() - datetime.timedelta(OFF_DAY)
    cur_day = datetime.date.today()
    if sys.argv[1] == '0':
        dump_sql_to_csv_dtrange(start_day, end_day)        
        copy_to_postgres(int(sys.argv[1])) 
        copy_to_mssql()
        # subjective scorecard
        delete_postgres_tbl(DB_PROD_ARCHIVE, OUTPUT_FILE_SUBJECTIVE_RATINGS, start_day, end_day)
        delete_postgres_tbl(DB_PROD_ARCHIVE, OUTPUT_FILE_SUBJECTIVE_CACHE, start_day, end_day)
        adhoc_copy_to_postgres([DB_PROD_ARCHIVE], [OUTPUT_FILE_SUBJECTIVE_RATINGS])
        adhoc_copy_to_postgres([DB_PROD_ARCHIVE], [OUTPUT_FILE_SUBJECTIVE_CACHE])                
    elif sys.argv[1] == '4':        
        archive_phx_log_files(cur_day, cur_day, PHXSERVERS, ARCHIVEDIR, FTPUSERNAME, FTPPASSWD)
        archive_fix_log_files(cur_day, cur_day, FIXSERVERS, ARCHIVEDIR, FTPUSERNAME, FTPPASSWD)
    elif sys.argv[1] == '1':
        dump_sql_to_csv_curdt(cur_day)
        delete_all_postgres()           
        copy_to_postgres(int(sys.argv[1]), DBS_ALL)        
        for (svr, prt) in [DB_UAT, DB_PROD]:
            exec_sql_w_svr(svr, prt, 'insert into eventstats.diego_metric select * from get_diego_metrics(\''+cur_day.strftime('%Y%m%d')+'\', \''+cur_day.strftime('%Y%m%d')+'\')')
    elif sys.argv[1] == '2':
        dump_postgres_to_csv_curdt(cur_day)
        # bats risk
        delete_postgres_tbl(DB_PROD, OUTPUT_FILE_BATS_RISK, cur_day, cur_day)
        delete_postgres_tbl(DB_PROD_ARCHIVE, OUTPUT_FILE_BATS_RISK, cur_day, cur_day)
        adhoc_copy_to_postgres([DB_PROD, DB_PROD_ARCHIVE], [OUTPUT_FILE_BATS_RISK])
    elif sys.argv[1] == '3':
        adhoc_copy_to_postgres()    
    logging.info('Done')