import pandas as pd
import sqlite3
import snowflake.connector as sf
import os, sys

# get dag directory path
dag_path = os.getcwd()

def execute_query(connection,query):
    cursor = connection.cursor()
    cursor.execute(query)
    cursor.close()


def execute_sql_on_snow(sql_file):

    # snowflake arguments

    user= os.environ['SF_USERNAME'] 
    password= os.environ['SF_PASSWORD'] 
    account='onxieuu-we74620'
    warehouse='DEV_WH'
    database='load'

    try:
    
        conn=sf.connect(user=user,password=password,account=account)
        print('Connection successfull')

        sql = 'use warehouse {}'.format(warehouse)
        execute_query(conn,sql)

        sql = 'use role SYSADMIN'
        execute_query(conn,sql)

        fd = open('%s' %sql_file, 'r')

        sql = fd.read()
        fd.close()

        statements = sql.split(';')

        for sql in statements:

            print('executing query..\n', sql)
            
            execute_query(conn, sql)
    
    except Exception as e:
        print(e)
        print('exiting code..')
        sys.exit(-1)


# LOAD database #
def transactions_load():
    sql_file="C:/Users/Hp/Desktop/work/Markazapp/ETL/transactions_load.sql"
    execute_sql_on_snow(sql_file)

# STAGE database #
def transactions_stage():
    sql_file="C:/Users/Hp/Desktop/work/Markazapp/ETL/transactions_stage.sql"
    execute_sql_on_snow(sql_file)
  
# DW database #
def final_table():
    sql_file="final_table.sql"
    execute_sql_on_snow(sql_file)    

try:
    transactions_load()
    print('Transaction load table loaded in snowflake')

except Exception as e:
    print(e)
    print('exiting code..')
    sys.exit(-1)


try:
    transactions_stage()
    print('Transaction stage table loaded in snowflake')

except Exception as e:
    print(e)
    print('exiting code..')
    sys.exit(-1)

