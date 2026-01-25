# -*- coding: utf-8 -*-yi-makespec --p
"""
Created on Mon Jun 3 13:49:05 2019

@author: kwadav

Version 2.0
- Expand year to 2060

Version 1.51
- Increased nvarchar length from 255 to 2000 for temp table used to do SQL Merge

Version 1.50
- Added new columns to match Global ETP DB & also GIS DB
- application no longer tied to a fixed excel file name; now works on the active excel

Version 1.41
Changes:
- round timestamp to nearest second

TODO:
- In "Preview Outlook" function, un-filter table before clear
- Map all province names to ISO3166-2 code when uploading & download from SQL server
"""


print("\nWood Mackenzie APAC Power Projects Uploader v2.0")
print("\nCopyright © 2019, Wood Mackenzie Limited. All rights reserved.\n")
print(f"Loading Python libraries and functions...\n")


import urllib
from sqlalchemy import create_engine
import pandas as pd
import sqlalchemy
import xlwings as xw
import getpass
import datetime as dt

# <editor-fold desc="Global Variables">
logFile = r'\\sinsrv0001\singapore\Power_Renewables\Inputs\UploadLog.log'

params = urllib.parse.quote_plus("DRIVER={ODBC Driver 17 for SQL Server};"
                                 "SERVER=ANVDEVSQLVPM01;"
                                 "DATABASE=WM_POWER_RENEWABLES;"
                                 "Trusted_Connection=Yes")
engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params, fast_executemany=True)

user = getpass.getuser()
timestamp = dt.datetime.now()
timestamp = timestamp - dt.timedelta(microseconds=timestamp.microsecond)

colExcel = ['PlantID', 'ACTION', 'Aggregate_ID', 'Unit_ID', 'Name_GIS_MapLabel', 'UnitName', 'PowerPlant_LocalName',
            'PowerPlant', 'Unit', 'Sector', 'PlantType', 'PlantTech', 'FuelPri',
            'FuelSec', 'Status', 'Start', 'End', 'Installed_MW', 'Available_MW',
            'HeatRate', 'Capacity_Coefficient', 'Province/State',
            'Lat', 'Lon', 'Owner_1', 'Share_1', 'Owner_2',
            'Share_2', 'Owner_3', 'Share_3', 'Owner_4', 'Share_4', 'Owner_5',
            'Share_5', 'Notes', 'GIS_Datum_Status', 'GIS_Datum_ID', 'GIS_Source_Type',
            'GIS_Source', 'GIS_Location_Accuracy']
col_real = ['Installed_MW', 'Available_MW', 'HeatRate', 'Capacity_Coefficient', 'Lat', 'Lon',
            'Share_1', 'Share_2', 'Share_3', 'Share_4', 'Share_5']
# </editor-fold>


# <editor-fold desc="Define Functions">
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
            dtypedict.update({i: sqlalchemy.types.NVARCHAR(length=2000)})
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
              chunksize=1000, dtype=df_dict)
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


def chk_refresh_excel():
    """

    Function that refreshes excel with latest data downloaded from SQL Server

    Returns: null
    """

    f = open(logFile, 'a+')
    wb = xw.Book.caller()
    sht = wb.sheets['PowerProjects']
    country = sht.range('selCountry_ISO').value

    # <editor-fold desc="Read Latest project list from SQL Server">
    f.write(str(dt.datetime.now()) + ':' + user + ': initiated Refresh script for: ' + country + '.\n')
    src_sql = 'APAC_PowerProjects_LIVE'
    sql_qry = """SELECT * FROM """ + src_sql + """ WHERE Country = '""" + country + """'"""
    # read data from sql server to df
    df = pd.read_sql_query(sql_qry, engine)
    df['ACTION'] = ''
    df = df[colExcel]
    f.write(str(dt.datetime.now()) + ':' + user + ': Retrieved ' + str(len(df)) + ' records from DB.\n')
    # </editor-fold>

    # <editor-fold desc="Write dataframe to Excel table">
    sht.range('PwrPrjList').clear_contents()
    sht.range('PwrPrjList').options(index=False, header=False).value = df
    f.write(str(dt.datetime.now()) + ':' + user + ': ' + str(len(df)) + ' records refreshed to Excel.\n')
    wb.save()
    # </editor-fold>

    return


def chk_upload_to_db():
    """

    Function that uploads excel data to SQL Server DB.

    3 seperate uploads are performed:
    1. "Update" as ACTION selected in excel
        - subsets entries which are indicated as "Update"
        - upload(replace) to temp table in SQL Server
        - executes SQL Merge against Main table(LIVE) in SQL Server
    2. "New" as ACTION selected in excel
        - subsets entries which are indicated as "New"
        - upload(append) to Main table(LIVE) in SQL Server
    3. Archive entire Country dataset
        - Reads entire latest dataset from LIVE table
        - Uses Timestamp as "Dataset_Name"
        - Given "Country_Archive" as UpdateCycle

    Returns: null
    """

    f = open(logFile, 'a+')
    wb = xw.Book.caller()
    sht = wb.sheets['PowerProjects']
    country = sht.range('selCountry_ISO').value

    # <editor-fold desc="Retrieve Latest project list from SQL Server for comparison">
    src_sql = 'APAC_PowerProjects_LIVE'
    sql_qry = """SELECT * FROM """ + src_sql + """ WHERE Country = '""" + country + """'"""
    df_sql = pd.read_sql_query(sql_qry, engine)
    # </editor-fold>

    # <editor-fold desc="Read Excel table to dataframe">
    f.write(str(dt.datetime.now()) + ':' + user + ': initiated Upload script for: ' + country + '.\n')
    df_pp = pd.DataFrame(sht.range('PwrPrjList').value)
    df_pp.columns = colExcel
    df_pp = df_pp.dropna(axis=0, how='all')
    df_pp['Country'] = country
    df_pp['Username'] = user
    df_pp['TimeStamp'] = timestamp
    df_pp[['Unit']] = df_pp[['Unit']].fillna(value=1).astype(int)
    df_pp[col_real] = df_pp[col_real].apply(pd.to_numeric, errors='coerce')
    changes = 0
    # </editor-fold>

    # <editor-fold desc="Update Power Project list using ID">
    # df_update (existing plants in db) check for duplicate plantID then load to temp tbl then SQL Merge
    df_update = df_pp[df_pp['ACTION'] == 'Update']
    if len(df_update) > 0:
        print('Found ' + str(len(df_update)) + ' records to be updated.')
        f.write(
            str(dt.datetime.now()) + ':' + user + ': Script found ' + str(len(df_update)) + ' records to be updated.\n')
        df_update = df_update.drop('ACTION', axis=1)
        df_update = df_update.astype({"PlantID": int})
        sql_merge = """MERGE APAC_PowerProjects_LIVE AS TARGET
                    USING tmpAPAC_PowerProjects_LIVE AS LATEST
                    ON (TARGET.PlantID = LATEST.PlantID)
                    WHEN MATCHED THEN UPDATE SET
                    TARGET.Aggregate_ID = LATEST.Aggregate_ID,
                    TARGET.Unit_ID = LATEST.Unit_ID,
                    TARGET.Name_GIS_MapLabel = LATEST.Name_GIS_MapLabel,
                    TARGET.UnitName = LATEST.UnitName,
                    TARGET.PowerPlant_LocalName = LATEST.PowerPlant_LocalName,
                    TARGET.PowerPlant = LATEST.PowerPlant,
                    TARGET.Unit = LATEST.Unit,
                    TARGET.Sector = LATEST.Sector,
                    TARGET.PlantType = LATEST.PlantType,
                    TARGET.PlantTech = LATEST.PlantTech,
                    TARGET.FuelPri = LATEST.FuelPri,
                    TARGET.FuelSec = LATEST.FuelSec,
                    TARGET.Status = LATEST.Status,
                    TARGET.Start = LATEST.Start,
                    TARGET.[End] = LATEST.[End],
                    TARGET.Installed_MW = LATEST.Installed_MW,
                    TARGET.Available_MW = LATEST.Available_MW,
                    TARGET.HeatRate = LATEST.HeatRate,
                    TARGET.Capacity_Coefficient = LATEST.Capacity_Coefficient,
                    TARGET.Country = LATEST.Country,
                    TARGET.[Province/State] = LATEST.[Province/State],
                    TARGET.Lat = LATEST.Lat,
                    TARGET.Lon = LATEST.Lon,
                    TARGET.Owner_1 = LATEST.Owner_1,
                    TARGET.Share_1 = LATEST.Share_1,
                    TARGET.Owner_2 = LATEST.Owner_2,
                    TARGET.Share_2 = LATEST.Share_2,
                    TARGET.Owner_3 = LATEST.Owner_3,
                    TARGET.Share_3 = LATEST.Share_3,
                    TARGET.Owner_4 = LATEST.Owner_4,
                    TARGET.Share_4 = LATEST.Share_4,
                    TARGET.Owner_5 = LATEST.Owner_5,
                    TARGET.Share_5 = LATEST.Share_5,
                    TARGET.Notes = LATEST.Notes,
                    TARGET.Username = LATEST.Username,
                    TARGET.TimeStamp = LATEST.TimeStamp ;"""
        sql_drop = """IF OBJECT_ID ('WM_POWER_RENEWABLES..tmpAPAC_PowerProjects_LIVE') IS NOT NULL 
                    DROP TABLE tmpAPAC_PowerProjects_LIVE"""
        # QC: Check if PK have been accidentally changed in excel
        if len(df_update['PlantID'].unique()) == len(df_update):
            print('QC passed')
            f.write(str(dt.datetime.now()) + ':' + user + ': QC Passed.\n')
            upload_sql(engine=engine, df=df_update, dest_sql='tmpAPAC_PowerProjects_LIVE', existMethod='replace')
            print('Uploaded to tmp table')
            f.write(str(dt.datetime.now()) + ':' + user + ': Uploaded to tmp table.\n')
            execute_sqlcur(engine=engine, sql=sql_merge)
            print('Updated Main table')
            f.write(str(dt.datetime.now()) + ':' + user + ': Updated Main table.\n')
            changes += 1
            execute_sqlcur(engine=engine, sql=sql_drop)
            print('Removed tmp table')
            f.write(str(dt.datetime.now()) + ':' + user + ': Removed tmp table.\n')
        else:
            print('QC failed: Duplicate PlantID detected')
            f.write(str(dt.datetime.now()) + ':' + user + ': QC failed: Duplicate PlantID detected!\n')
    else:
        print('NO records to be Updated')
        f.write(str(dt.datetime.now()) + ':' + user + ': NO records to be Updated.\n')
    # </editor-fold>

    # <editor-fold desc="Add NEW records">
    # df_new (new additions) check for duplicate plant name the append to main tbl
    df_new = df_pp[df_pp['ACTION'] == 'New']
    if len(df_new) > 0:
        print('Found ' + str(len(df_new)) + ' New records to be added.')
        f.write(str(dt.datetime.now()) + ':' + user + ': Script found ' + str(len(df_new)) + ' New records to be '
                                                                                           'added.\n')
        df_new = df_new.drop(['PlantID', 'ACTION'], axis=1)
        # QC: check if NEW plant name same as Existing plant name in SQL Server OR same as Updated Plant name
        if (len(set(df_new['PowerPlant']).intersection(set(df_update['PowerPlant']))) == 0) and \
                (len(set(df_new['PowerPlant']).intersection(set(df_sql['PowerPlant']))) == 0):
            print('QC Passed')
            f.write(str(dt.datetime.now()) + ':' + user + ': QC Passed.\n')
            upload_sql(engine=engine, df=df_new, dest_sql='APAC_PowerProjects_LIVE', existMethod='append')
            print('Added new plants to Main table')
            f.write(str(dt.datetime.now()) + ':' + user + ': Added new plants to Main table.\n')
            changes += 1
        else:
            print(
                'QC failed: Duplicate Plant Names detected. Update existing record instead? If New plant, please give a different name')
            f.write(str(
                dt.datetime.now()) + ':' + user + ': QC failed: Duplicate Plant Names detected. Update existing record instead? If New plant, please give a different name!\n')
    else:
        print('NO New records to be added')
        f.write(str(dt.datetime.now()) + ':' + user + ': NO New records to be added.\n')
    # </editor-fold>

    # <editor-fold desc="Archive on any change">
    # df_archive (entire list) load into archive
    if changes > 0:
        src_sql = 'APAC_PowerProjects_LIVE'
        sql_qry = """SELECT * FROM """ + src_sql + """ WHERE Country = '""" + country + """'"""
        df_archive = pd.read_sql_query(sql_qry, engine)
        df_archive['Dataset_Name'] = str(timestamp)
        df_archive['UpdateCycle'] = 'Country_Archive'
        df_archive['Username'] = user
        df_archive['TimeStamp'] = timestamp
        upload_sql(engine=engine, df=df_archive, dest_sql='APAC_PowerProjects_Datasets', existMethod='append')
        print('Added latest ' + country + ' dataset to Archive')
        f.write(str(dt.datetime.now()) + ':' + user + ': Added latest ' + country + ' dataset to Archive.\n')
    else:
        print('NO New/Updates found. NO archive added')
        f.write(str(dt.datetime.now()) + ':' + user + ': NO New/Updates found. NO archive added.\n')
    # </editor-fold>

    return


def chk_preview_outlook():
    """

    Function that generates aggregated capacity outlook by modelling zones by plant type

        - reads excel input instead of data from SQL Server, thus allows users to preview outlook before committing
        the changes/additions to the database.

    Returns: null
    """

    f = open(logFile, 'a+')
    wb = xw.Book.caller()
    sht = wb.sheets['PowerProjects']
    country = sht.range('selCountry_ISO').value

    f.write(str(dt.datetime.now()) + ':' + user + ': initiated Outlook Preview script for: ' + country + '.\n')
    df_pp = pd.DataFrame(sht.range('PwrPrjList').value)
    df_pp = df_pp.dropna(axis=0, how='all')
    if len(df_pp) > 0:
        # Read Excel data
        df_pp.columns = colExcel
        df_pp[col_real] = df_pp[col_real].apply(pd.to_numeric, errors='coerce')
        df_pp = df_pp[df_pp['Status'] != 'Cancelled']
        df_pp = df_pp.dropna(subset=['Status'])
        df_pp = df_pp.dropna(subset=['Start'])
        df_pp = df_pp.dropna(subset=['End'])
        df_pp['Country'] = country
        df_pp['Username'] = user
        df_pp['TimeStamp'] = timestamp
        df_pp[['Unit']] = df_pp[['Unit']].fillna(value=1).astype(int)
        df_pp.rename(columns={'Province/State': 'Province_State'}, inplace=True)

        # Find ModelZone to Province mapping & left join with df_pp
        src_sql = 'vLOC_Prov_State_ISO_ModelZone_Mapping'
        sql_qry = """SELECT Province_State, CustomZone_APACmodel FROM """ + src_sql + """ WHERE ISO3166_1_A3 = '""" + country + """'"""
        df_modelzone = pd.read_sql_query(sql_qry, engine)
        df_pp = pd.merge(df_pp, df_modelzone, on=['Province_State'], how='left')
        df_pp.rename(columns={'CustomZone_APACmodel': 'ModelZone'}, inplace=True)

        # Expand timeseries Annual
        df_ts = pd.concat([pd.DataFrame({
            'Year'     : pd.date_range(row.Start, row.End, freq='YS'),
            'Plant'    : row.PowerPlant,
            'PlantType': row.PlantType,
            'Zone'     : row.ModelZone,
            'MW'       : row.Available_MW},
            columns=['Year', 'Plant', 'PlantType', 'Zone', 'MW'])
            for i, row in df_pp.iterrows()], ignore_index=True)

        # Group by Country, ModelZone, PlantType, Start, End & Sum Available_MW
        df_existing = df_ts.groupby(['Zone', 'PlantType', 'Year'])['MW'].agg('sum').reset_index()
        df_existing = pd.pivot_table(df_existing, index=['Zone', 'Year'],
                                     columns='PlantType',
                                     values='MW',
                                     fill_value=0).reset_index()

        # Write to 'Capacity Outlook' sheet
        sht = wb.sheets['CapacityOutlook']
        sht.select()
        sht.range('A22').select()
        sht.range('A22').expand().clear_contents()
        sht.range('A22').options(index=False, header=True).value = df_existing
        tbl = sht.api.ListObjects.add()
        tbl.TableStyle = 'TableStyleMedium2'
        tbl.Name = 'OutlookPreview'
        # rngRef = sht.range('A22').expand(mode='table').address
        # wb.api.Names.Add(Name='OutlookPreview', RefersTo=rngRef)
        # Reset the chart data range & chart Type
        sht.charts['Chart 1'].set_source_data(sht.range('B22').expand(mode='table'))
        sht.charts['Chart 1'].chart_type = 'column_stacked'
        sht.charts['Chart 3'].set_source_data(sht.range('B22').expand(mode='table'))
        sht.charts['Chart 3'].chart_type = 'column_stacked_100'

        f.write(str(dt.datetime.now()) + ':' + user + ': Outlook Preview generated for: ' + country + '.\n')
    else:
        sht = wb.sheets['CapacityOutlook']
        sht.range('A22').expand().clear_contents()

        f.write(str(dt.datetime.now()) + ':' + user + ': No Outlook Preview generated for: ' + country + '.\n')

    wb.save()
    return


def click_submit():
    """

    Function that gets called 1st when user clicks the "Submit" button in excel

        - checks the True/False values of options available to user
        - executes respective functions if they are True

    Returns: null
    """

    wb = xw.Book.caller()
    sht = wb.sheets['PowerProjects']
    chkRefresh = sht.range('chkRefresh').value
    # chkPreview = sht.range('chkPreview').value
    chkPreview = False
    chkUpload = sht.range('chkUpload').value

    if chkRefresh is True:
        chk_refresh_excel()
    if chkPreview is True:
        # currently disabled
        print('Function currently not available.')
        # chk_preview_outlook()
    if chkUpload is True:
        chk_upload_to_db()

    wb.save()
    return
# </editor-fold>


if __name__ == '__main__':

    active_excel = xw.books.active.name
    xw.Book(active_excel).set_mock_caller()
    click_submit()
