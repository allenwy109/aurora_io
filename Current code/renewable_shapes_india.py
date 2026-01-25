# -*- coding: utf-8 -*
"""
Created on Tue Oct 25 10:32:05 2022

@author: david_kwa

Version 1.0
- uploads 8760 renewable shapes to SQL Server
"""

import getpass
import datetime as dt
import urllib
import pandas as pd
import xlwings as xw
from sqlalchemy import create_engine
import sqlalchemy

# <editor-fold desc="GLOBAL Variables">
params = urllib.parse.quote_plus("DRIVER={ODBC Driver 11 for SQL Server};"
                                 "SERVER=ANVDEVSQLVPM01;"
                                 "DATABASE=WM_POWER_RENEWABLES;"
                                 "Trusted_Connection=Yes")
engine_sqldb = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params, fast_executemany=True)
user = getpass.getuser()
timestamp = dt.datetime.now()
timestamp = timestamp - dt.timedelta(microseconds=timestamp.microsecond)

# </editor-fold>


# <editor-fold desc="Define Functions">
def sqlcoldict_dt(df):
    dtypedict = {}
    for i, j in zip(df.columns, df.dtypes):
        if "object" in str(j):
            dtypedict.update({i: sqlalchemy.types.NVARCHAR(length=255)})
        if "datetime" in str(j):
            dtypedict.update({i: sqlalchemy.types.DATETIME()})
        if "float" in str(j):
            dtypedict.update({i: sqlalchemy.types.Float(precision=6, asdecimal=True)})
        if "int" in str(j):
            dtypedict.update({i: sqlalchemy.types.INT()})
    return dtypedict


def upload_to_db(engine, dest_df, dest_tbl, existMethod):
    """

    Function that uploads df to SQL Server using Pandas' DataFrame.to_sql()

    Args:
        engine      : engine created from sqlalchemy that connects to MS SQL server
        dest_df          : the dataframe to be uploaded
        dest_tbl    : the destination table name in SQL Server
        existMethod : 'append' or 'replace', the method to use if table already exists in SQL Server

    Returns         : nothing
    """

    df_dict = sqlcoldict_dt(dest_df)
    dest_df.to_sql(dest_tbl, con=engine,
                   if_exists=existMethod, index=False,
                   chunksize=10000, dtype=df_dict)

    print('Upload complete')
    return


def execute_sqlcur(engine, sql):
    """

    Executes SQL statement via the SQL Cursor

    Args:
        engine  : engine created from sqlalchemy that connects to MS SQL server
        sql     : SQL statement to be executed

    Returns     : nothing
    """

    connx = engine.raw_connection()
    cursor = connx.cursor()
    cursor.execute(sql)
    connx.commit()
    cursor.close()

    return


def update_db_tbl(engine, dest_tbl, dest_df, by_col):
    """

    Function that REPLACES (by ID) Aurora tables with dataframes

        - Only applicable to Time Series Annual, Monthly & Weekly & any AID tables which uses ID as unique column
        - NOT applicable to Time Series Daily, Hourly & Generic
        - Deletes original records of dest_tbl by ID
        - Appends dest_df to dest_tbl (not null columns MUST be present, all other missing columns from dest_df will
        by appended as NULL

    Args:
        engine:
        dest_tbl (string): Aurora Input Database table name eg. tbl_AID_xxxxx
        dest_df (dataframe): df that looks like tbl_AID_xxxx
        by_col:

    Returns: nothing
    """

    unique_id = dest_df[by_col].unique()

    str_id = """('"""
    for item in unique_id:
        print(item)
        str_id += item + """','"""
    str_id = str_id[:-2] + ')'

    del_sql = """DELETE FROM """ + """[""" + dest_tbl + """] WHERE [""" + by_col + """] IN """ + str_id
    execute_sqlcur(engine=engine, sql=del_sql)
    upload_to_db(engine=engine, dest_df=dest_df, dest_tbl=dest_tbl, existMethod='append')

    return


def get_renewable_shape(src_type, sheet_name, shapetype, shapename,  level):
    """

    :param src_type:
    :param src_name:
    :return:
    """

    if src_type == 'excel':
        wb = xw.Book.caller()
        sht = wb.sheets[sheet_name]
        df = pd.DataFrame(sht.range('A1').expand().value)
        df.columns = df.iloc[0, :]
        df = df.iloc[1:, 0:].reset_index(drop=True) 
        #df['LevelName'] = levelname
        df=df[df['Level'] == level]
        df=df[df['ShapeType'] == shapetype]
        df=df[df['ShapeName'] == shapename]
        df['Date'] = pd.to_datetime(df['Date'])
        #df['electricity'] = pd.to_numeric(df['electricity'])
        #df.rename(columns={'electricity': 'Shape'}, inplace=True)
        df['Year'] = df['Date'].dt.year
        df['HourOfDay'] = df['Date'].dt.hour
        df['Month'] = df['Date'].dt.month
        df['DayOfMonth'] = df['Date'].dt.day
        df['DayOfWeek'] = df['Date'].dt.dayofweek + 1
        df['DayName'] = df['Date'].dt.day_name()
        df['HourOfYear'] = df.index + 1
        df['User'] = user
        df['TimeStamp'] = timestamp
    else:
        print('Source type is new; no specification on data transformation defined yet.')

    return df


# </editor-fold>


if __name__ == '__main__':
    active_excel = xw.books.active.name
    xw.Book(active_excel).set_mock_caller()
    df_solar = get_renewable_shape(src_type='excel', sheet_name='RE shape update', shapetype='Sun', shapename='NoTracking_Tilt22', 
                                   level='ModelZone')
    upload_to_db(engine=engine_sqldb, dest_df=df_solar, dest_tbl='APAC_Shapes_Raw8760_LIVE', existMethod='append')
    df_onshore = get_renewable_shape(src_type='excel', sheet_name='RE shape update', shapetype='Wind_Onshore', shapename='H90m_Vestas_V90 2000',
                                     level='ModelZone')
    upload_to_db(engine=engine_sqldb, dest_df=df_onshore, dest_tbl='APAC_Shapes_Raw8760_LIVE', existMethod='append')
    df_offshore = get_renewable_shape(src_type='excel', sheet_name='RE shape update', shapetype='Wind_Offshore', shapename='H100m_GE5_5 158',
                                        level='ModelZone')
    upload_to_db(engine=engine_sqldb, dest_df=df_offshore, dest_tbl='APAC_Shapes_Raw8760_LIVE', existMethod='append')
