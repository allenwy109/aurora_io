"""
Version 6.0
- Expand year to 2060

Version 5.0

- use the new template in DB


Version 4.0
============
- add the function to delete the old dataset in Origin and LIVE table by ID_List
- add if else to check the consistency between transmission line and the tariff
---To improve: 1 how to reload multiple countries data at the same time
               2 how to check the consistency when the txn line and tariff data not be load at the same time (only one)
                or the txn and tariff data is not loaded in pairs (eg. some lines updates separately)
               (add another if, if len(txn) >0,and len(tariff)>0, then compare the unique value.
               3 combine the 'delete_reload...'function into one.


Version 3.0
=============
- change the excel template
- add the update cycle in Excel
- add the country column
- add the update status column

Version 2.0:
=============
- use the functions and combine to a package





"""


print("\nWood Mackenzie APAC Power Transmission Uploader 6.0")
print("\nCopyright © 2020, Wood Mackenzie Limited. All rights reserved.\n")
print(f"Loading Python libraries and functions...")


import urllib
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy
import getpass
import datetime as dt
import xlwings as xw
import warnings

# <editor-fold desc="Change variables">
aurora_sqldb = 'ANVDEVSQLVPM01'
aurora_dbname = 'Aurora_APAC_DEV_'

path = r'C:\Users\I33421\Verisk Analytics\2021 H2 China Power Model - General\2022Q1\Inputs change\APAC_Transmission.xlsx'
sheetname_line = 'LiveUpdate'
sheetname_price = 'T&D Tariffs'

user = getpass.getuser()
timestamp = dt.datetime.now()
timestamp = timestamp - dt.timedelta(microseconds=timestamp.microsecond)
datasetname = str(timestamp)


# </editor-fold">

# <editor-fold desc="Read data from excel to dataframe">

def new_get_excel_transmission_line(path, sheetname_line):
    wb = xw.Book.caller()
    #wb = xw.Book(path)
    sht = wb.sheets[sheetname_line]
    cycle = sht.range('B1').value

    value = pd.DataFrame(sht.range('A2').expand().value)
    value.columns = value.iloc[0]
    value.drop(value.index[0], inplace=True)
    value = value.reset_index(drop=True)
    transmission_data = value[value['UpdatetoDB'] == 'Y']

    transmission_data.drop(['UpdatetoDB', 'UpdateStatus'], axis=1, inplace=True)
    transmission_data['TimeStamp'] = timestamp
    transmission_data['User'] = user
    transmission_line_origin = transmission_data
    country = ''.join(transmission_data['Country'].unique())
    #ID_list = str(list(transmission_line_origin['ID'])).replace('[', '(').replace(']', ')')

    return cycle, country, transmission_line_origin

def new_get_excel_transmission_price(path, sheetname_price):
    wb = xw.Book.caller()
    #wb = xw.Book(path)
    sht = wb.sheets[sheetname_price]
    value = sht.range('A1').expand().value
    price_origin = pd.DataFrame(value)
    price_origin.columns = price_origin.iloc[0]
    price_origin.drop(price_origin.index[0], inplace=True)
    price_origin = price_origin.reset_index(drop=True)
    price_origin = price_origin[price_origin['UpdatetoDB'] == 'Y']
    price_origin.drop('UpdatetoDB', axis=1, inplace=True)
    price_origin = pd.melt(price_origin,id_vars = ['Country',  'Power link name', 'Source province',  'Source regional grid',
            'Destination province', 'Destination regional grid'],var_name = 'Year',value_name = 'Value')
    price_origin['TimeStamp'] = timestamp
    price_origin['User'] = user
    price_origin_ts = price_origin.copy()
    #price_origin = price_data.copy()
    #ID_list_p = str(list(price_origin['Power link name'])).replace('[', '(').replace(']', ')')

    "use the same name ID_list, bc in other function, this is a parameter"
    return price_origin,price_origin_ts


def merge_txn_tariff(transmission_data, price_data):
    date_range = pd.DataFrame(range(1979, 2061))
    date_range.columns = ['Year']
    df = pd.DataFrame()
    for row in transmission_data.iterrows():
        data_row = row[1].to_frame().T
        df_row = data_row.merge(date_range, how='outer', left_on='Model Start Year', right_on='Year')
        df_row = df_row.fillna(method='ffill')
        df_row['Capacity'] = pd.np.where(
            (df_row['Year'] == df_row['Model Start Year']) & (df_row['Special label for profiling'] == 'F'),
            df_row['Adjusted Cap'] * 0.7,
            pd.np.where(
                (df_row['Year'] == df_row['Model Start Year'] + 1) & (df_row['Special label for profiling'] == 'F'),
                df_row['Adjusted Cap'] * 0.9,
                pd.np.where(
                    (df_row['Year'] == df_row['Model Start Year']) & (df_row['Special label for profiling'] == 'L'),
                    df_row['Adjusted Cap'] * 0.3,
                    pd.np.where((df_row['Year'] == df_row['Model Start Year'] + 1) & (
                            df_row['Special label for profiling'] == 'L'), df_row['Adjusted Cap'] * 0.6,
                                pd.np.where((df_row['Year'] == df_row['Model Start Year'] + 2) & (
                                        df_row['Special label for profiling'] == 'L'), df_row['Adjusted Cap'] * 0.8,
                                            pd.np.where((df_row['Year'] < df_row['Model Start Year']), 0,
                                                        pd.np.where(df_row['Special label for profiling'] == 'NA',
                                                                    df_row['Adjusted Cap'] * 1,
                                                                    df_row['Adjusted Cap'])))))))

        df = df.append(df_row)
    df['Unit'] = 'MW'
    df['Metric'] = 'Capacity'
    df['Type'] = 'Max'
    # min dataset
    df_min = df.copy()
    df_min['Type'] = 'Min'
    df_min['Capacity'] = df_min['Capacity'] * 0.5
    df_line = pd.concat([df, df_min], ignore_index=True)
    """ re-order and delete 'Capacity (MW)', 'Deduction coefficient', 'Adjusted Cap'"""
    df_line = df_line[['ID', 'Country', 'Transmission Line Name',  'Interconnection', 'From Province', 'From Region',
                 'To Province', 'To Region',  'Year', 'Capacity', 'Unit', 'Metric', 'Type',  'TimeStamp', 'User']]
    if country == 'India':
        df_line['From Province'] = df_line['From Region']
        df_line['To Province'] = df_line['To Region']
    else:
        df_line['From Province'] = df_line['From Province']
        df_line['To Province'] = df_line['To Province']



    df_line = df_line.groupby([ 'Country',  'Interconnection', 'From Province', 'From Region',
                 'To Province', 'To Region',  'Year',  'Unit', 'Metric', 'Type',  'TimeStamp', 'User'],as_index = False)['Capacity'].agg('sum')




    price_data['Interconnection'] = price_data['Source province'] + '-' + price_data['Destination province']
  #  price_data = pd.melt(price_data, id_vars=['Country', 'Power link name', 'Source province', 'Source regional grid',
  #                                            'Destination province',
  #                                            'Destination regional grid', 'Interconnection', 'TimeStamp', 'User'], var_name='Year', value_name='Value')


    price_data['Year'] = price_data['Year'].astype(int)

    price_data['Type'] = 'Flat'
    price_data['Unit'] = 'US$/MWh'
    price_data['Metric'] = 'Wheeling'
    price_data = price_data[['Country',  'Source province', 'Source regional grid',
                             'Destination province', 'Destination regional grid', 'Interconnection',
                             'Year', 'Value', 'Type', 'Unit', 'Metric', 'TimeStamp', 'User']]

    df_live = df_line.merge(price_data, how='left', on=['Interconnection', 'Year', 'TimeStamp', 'User', 'Country'])

    df_live = df_live[['Country', 'From Province','From Region', 'To Province','To Region',
                       'Interconnection', 'Year', 'Capacity', 'Metric_x', 'Type_x', 'Value','Metric_y','Type_y', 'TimeStamp']]
    df_live.columns = ['Country', 'From Province','From Region', 'To Province','To Region',
                       'Interconnection', 'Year', 'Capacity_MW', 'Metric_line', 'Type_line','Tariff_($/MWh)', 'Metric_tariff','Type_tariff', 'TimeStamp']

    df_live_new =df_live.groupby(['Country', 'From Province','From Region', 'To Province','To Region','Interconnection',
                                  'Year',  'Metric_line', 'Type_line','Tariff_($/MWh)',
                                  'Metric_tariff','Type_tariff', 'TimeStamp'], as_index = False)['Capacity_MW'].agg('sum')

    df_live_new = df_live_new[ ['Country', 'From Province', 'From Region', 'To Province', 'To Region',
                       'Interconnection', 'Year', 'Capacity_MW', 'Metric_line', 'Type_line', 'Tariff_($/MWh)',
                       'Metric_tariff', 'Type_tariff', 'TimeStamp']]
    return df_live_new


# </editor-fold >

# <editor-fold desc="Load data to SQL DB POWER&RENEWABLE">
def sqlcoldict_dt(df):
    """

    Functions that creates an sqlalchemy type dictionary for use when uploading to SQL Server

    Args:
        df  : dataframe

    Returns : sqlalchemy datatype dictionary
    """

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

def upload_sql(engine, df, dest_sql, existMethod):
    """

    Function that uploads df to SQL Server using Pandas' DataFrame.to_sql()

    Args:
        engine      : engine created from sqlalchemy that connects to MS SQL server
        df          : the dataframe to be uploaded
        dest_sql    : the destination table name in SQL Server
        existMethod : 'append' or 'replace', the method to use if table already exists in SQL Server

    Returns         : nothing
    """

    df_dict = sqlcoldict_dt(df)
    df.to_sql(dest_sql, con=engine,
              if_exists=existMethod, index=False,
              chunksize=10000, dtype=df_dict)
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

def delete_reload_sql_df(engine_src, df,dest_tbl, country):
    check_sql = """Select distinct (country) from """ + dest_tbl
    country_list = pd.read_sql_query(check_sql,engine_src)
    if country in country_list.values:
        delete_sql = """Delete FROM """ + dest_tbl + """ WHERE [Country] = """ + """'""" + country + """'"""
        execute_sqlcur(engine = engine_src,sql= delete_sql)
        upload_sql(engine=engine_src, df=df, dest_sql=dest_tbl, existMethod='append')
    else:
        upload_sql(engine=engine_src, df=df, dest_sql=dest_tbl, existMethod='append')
    return

# </editor-fold >


if __name__ == '__main__':
    warnings.filterwarnings('ignore')
    active_excel = xw.books.active.name
    xw.Book(active_excel).set_mock_caller()
    wb = xw.Book.caller()

    # connect to  power_renewable DB
    params_src = urllib.parse.quote_plus("DRIVER={ODBC Driver 17 for SQL Server};"
                                         "SERVER=ANVDEVSQLVPM01;"
                                         "DATABASE=WM_POWER_RENEWABLES;"
                                         "Trusted_Connection=Yes")
    engine_src = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params_src, fast_executemany=True)

    # read transmission_line data from excel
    cycle, country, transmission_origin = new_get_excel_transmission_line(path, sheetname_line)
    price_origin,price_origin_ts = new_get_excel_transmission_price(path, sheetname_price)
    print('Reading the original data from excel is completed')

    # check the consistency between transmission line and tariff
    #if country == 'India':
     #   interactionl = transmission_origin['From Province'] + "-" + transmission_origin['To Province']
    #else:
    interactionl = transmission_origin['Interconnection']
    if country == 'India':
        interactionp = price_origin['Source regional grid'] + "-" + price_origin['Destination regional grid']
    else:
        interactionp = price_origin['Source province'] + "-" + price_origin['Destination province']
    a = list(interactionl.unique())
    a.sort()
    b = list(interactionp.unique())
    b.sort()

    if a == b:
        print('txn line is consistent with tariff')
        # after read data from excel,  then connect the sql server, and then run the db interaction parts

        # Load(replace) data to LIVE table
        dest_tbl_line = 'APAC_PowerTransmission_InfrastructureProject_LIVE'  # change name from  origin to live
        dest_tbl_price = 'APAC_PowerTransmission_Tariff_LIVE'
        delete_reload_sql_df(engine_src=engine_src, df=transmission_origin, dest_tbl=dest_tbl_line, country=country)
        delete_reload_sql_df(engine_src=engine_src, df=price_origin, dest_tbl=dest_tbl_price, country=country)
        print('Uploading the LIVE data to DB is completed')

        # transform transmission line w/ the new origin dataset
        transmission_ts_live = merge_txn_tariff(transmission_data=transmission_origin,  price_data=price_origin_ts)
        print('Transforming the original data to time series format data is completed')
        dest_tbl_txn_ts_LIVE = 'APAC_PowerInfrastructure_AnnualTimeSeries'
        delete_reload_sql_df(engine_src=engine_src, df=transmission_ts_live, dest_tbl=dest_tbl_txn_ts_LIVE, country = country)
        print('Uploading the time series data to DB is completed')

        # LOAD (append) to Datasets table ----no need to compare difference, just upload it
        transmission_origin['UpdateCycle'] = cycle
        price_origin['UpdateCycle'] = cycle
        #transmission_origin['Dataset_Name'] = datasetname
        #price_origin['Dataset_Name'] = datasetname
        print('Transforming the origiission_Tariff_Dataset'
        print('Transforming the origiission_Tariff_Dataset'
        upload_sql(engine=engine_src, df=transmission_origin, dest_sql=dest_tbl_line_d, existMethod='append')
        upload_sql(engine=engine_src, df=price_origin, dest_sql=dest_tbl_price_d, existMethod='append')
        print('Finished upload the DATASET data to DB')

    else:
        print('something wrong, please go back to check the data')









