import warnings
import getpass
from datetime import datetime
import urllib
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy



# <editor-fold desc="GLOBAL Variables">
params = urllib.parse.quote_plus("DRIVER={ODBC Driver 17 for SQL Server};"
                                 "SERVER=ANVDEVSQLVPM01;"
                                 "DATABASE=WM_POWER_RENEWABLES;"
                               
                                "Trusted_Connection=Yes")
engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params, fast_executemany=True)

# StartYr = 2000
user = getpass.getuser()
timestamp = datetime.now()

dest_tbl = 'APAC_LoadDurationCurves_HIST'
df= pd.read_excel("D:\WMData\China load curve upload for DB.xlsx")
# df = df[df['Area']=='Shandong']
df = df[['Area', 'DateTime', 'HourOfYear', 'Demand_MW', 'Year', 'Month',
       'DayOfMonth', 'DayOfWeek', 'DayName', 'HourOfDay']]

df['Username'] = user
df['timestamp'] = timestamp



def sqlcoldict_dt(df):
    dtypedict = {}
    for i,j in zip(df.columns, df.dtypes):
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
    return


upload_to_db(engine = engine, dest_df = df,dest_tbl= dest_tbl,existMethod='append')