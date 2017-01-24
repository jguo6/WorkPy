import smtplib
import logging
import traceback
import os
import itertools
import operator
import datetime
import time
import sys
import collections
import math
import psycopg2
import pyodbc
import pdb
import pandas as pd

from sqlalchemy import create_engine

from stkmedsprd_config import *


def replace_all(text, dic):
    for i, j in dic.items():
        text = text.replace(i, j)
    return text


def send_email(smtp_server, sender, receivers, subject, message):
    try:        
        emailmsg = "From: %s\r\nTo: %s\r\nSubject: %s\r\n\r\n%s"\
                 % (sender, ";".join(receivers), 'Stock Median Metrics Job: ' + subject, message)
        logging.debug('Sending email %s', emailmsg)        
        smtp = smtplib.SMTP(smtp_server)
        smtp.sendmail(sender, receivers, emailmsg)
    except Exception as err:
        logging.exception("Error sending email %s", err)            


def handle_exception(message, err, email=True, subject='Error'):
    logging.exception(message + " %s", err)
    if email:
        message = message + "\r\n" + traceback.format_exc(err)
        notify_thru_email(subject, message)

def notify_thru_email(subject, message):
    send_email(SMTP_SERVER, SENDER, RECEIVERS, subject, message) 


def connectToSQL(sql_user, sql_pwd, sql_host):
	sqlparams = {'usr':sql_usr,'pwd':sql_pwd,'host':sql_host}
	sqlstring = 'mssql+pymssql://%(usr)s:%(pwd)s@%(host)s' %  sqlparams
	ENGINE = create_engine(sqlstring, pool_size=5, pool_recycle=10)
	return ENGINE

def connectToPostgres(pg_user, pg_pwd, pg_host):
	pgparams = {'usr':pg_user,'pwd':pg_pwd,'host':pg_host}
	pgstring = 'postgresql://%(usr)s:%(pwd)s@%(host)s' %  pgparams
	ENGINE = create_engine(pgstring, pool_size=5, pool_recycle=10)
	return ENGINE

def exec_pgsql(db, qry):
    '''
    execute postgres query
    '''    
    try:            
        server = db[0]
        prt = db[1]
        usr = db[2]
        pwd = db[3]
        logging.info('Processing server: %s, port %s' % (server, prt))
        logging.info(qry)
        rows = None
        conn_sql = psycopg2.connect(host=server, user=usr, password=pwd, port=prt, database='execution')
        cur = conn_sql.cursor()
        cur.execute (qry)
        conn_sql.commit()
        rows = cur.fetchall()
    except Exception as err:
        handle_exception('Exception executing postgres', err, True)    
    finally:
        conn_sql.close()
        return rows

def fetch_mssql(constr, qry):
    '''                                                                                                                                                                                                                          fetch mssql query result into pandas dataframe                                                                                                                                                                               '''
    logging.info('Processing server: %s' % (constr))
    logging.info(qry)
    try:
        conn =  create_engine(constr)
        return pd.read_sql(qry, conn)
    except Exception as err:
        handle_exception('Exception fetching mssql', err, True)

def fetch_pgsql((server, prt, db, usr, pwd), qry, idx_col=None):
    '''                                                                                                                                                                                                                          fetch postgres query result into pandas dataframe                                                                                                                                                                            '''
    logging.info('Processing server: %s, port %s' % (server, prt))
    logging.info(qry)
    try:
        with psycopg2.connect(host=server, user=usr, password=pwd, port=prt, database=db) as conn_sql:
            return pd.read_sql(qry, conn_sql, index_col=idx_col)
    except Exception as err:
        handle_exception('Exception fetching postgres', err, True)


def exec_mssql(conn, qry):
    '''
    execute mssql query
    '''
    try:
        conn_sql = None
        conn_sql = pyodbc.connect(conn)
        logging.info(qry)
        cursor = conn_sql.cursor()
        cursor.execute(qry)
        conn_sql.commit()
    except Exception as err:
        handle_exception('Exception execution ms sql', err, True)
    finally:
        if conn_sql:                    
            conn_sql.close()

def copy_to_postgres_via_alch(df, conn, tbl):
    try:
        engine = create_engine(conn)
        df.to_sql(tbl, engine, if_exists='append', index=False)
    except Exception as err:
        miscutils.handle_exception('Exception copying data to postgres', err, True)
    finally:
        if engine:
            engine.close()

def copy_to_postgres_via_scp(csv_dir, dbs, file_tbl_map, delimited_char, user, pwd, local_dir='/home/labsuser'):
    '''
    copy the dumped file onto postgres local drive
    '''
    conn=None    
    try:                
        os.chdir(csv_dir)
        for (server, prt) in dbs:
            conn = pysftp.Connection(server, user , None, pwd)                
            for filenm_tbl in file_tbl_map:
                conn.put(filenm_tbl[0])
            conn.close()
    except Exception as err:
        miscutils.handle_exception('Exception copying data to postgres', err, True)
    finally:
        if conn:    
            conn.close()
        
    # copy to postgres
    conn_sql=None    
    try:        
        for (server ,prt) in dbs:
            logging.info('Inserting to server %s, port %s' %(server, prt))            
            conn_sql = psycopg2.connect(host=server, user='execution', password='execution', port=prt, database='execution')
            cur = conn_sql.cursor()
            for filenm_tbl in file_tbl_map:
                logging.info("COPY "+filenm_tbl[1]+" FROM '"+local_dir+"/"+filenm_tbl[0]+ "' DELIMITER '"+delimited_char+"' NULL ''")                  
                cur.execute("COPY "+filenm_tbl[1]+" FROM '"+local_dir+"/"+filenm_tbl[0]+ "' DELIMITER '"+delimited_char+"' NULL ''")
                conn_sql.commit()                
            cur.close()
            conn_sql.close()                    
    except Exception as err:
        miscutils.handle_exception('Exception copying data to postgres', err, True)
    finally:
        if conn_sql:                    
            conn_sql.close()


def gen_bulk_insert_sql_stmt(dbname, schema, tblname, filepath, fieldterm, rowterm):
    tblfullname = dbname + "." + schema + "." + tblname
    sql = """         
    BULK INSERT %s
    FROM '%s'
    WITH
    (
    FIELDTERMINATOR = '%s',
    ROWTERMINATOR = '%s'
    )""" % (tblfullname, filepath, fieldterm, rowterm)
    return sql
