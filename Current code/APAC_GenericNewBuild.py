# -*- coding: utf-8 -*
"""
Created on Thu Nov 28 10:17:35 2019

@author: kwadav

Version 2.0
- expand time range to 2060

Version 1.23
- add emission to the attribute list

Version 1.22
- excluded FixedCost Assumption assignment as it is an Annual TimeSeries assumption

Version 1.21
- made all generic new build to retire in 2060

Version 1.2
- fixed plant assumption assignment at Market & Zone level
"""

print("\nWood Mackenzie Generic New Build Uploader v2.0")
print("\nCopyright © 2019, Wood Mackenzie Limited. All rights reserved. \n")
print(f"Loading Python libraries and functions...\n")


import re
import getpass
import datetime as dt
import urllib
import pandas as pd
import xlwings as xw
from sqlalchemy import create_engine
import sqlalchemy

plant_att_tsAnnualAsumpt_list = ['FixedCost','EmissionRate_CO2', 'EmissionPrice_CO2']

# <editor-fold desc="GLOBAL Variables">

params = urllib.parse.quote_plus("DRIVER={ODBC Driver 17 for SQL Server};"
                                 "SERVER=ANVDEVSQLVPM01;"
                                 "DATABASE=WM_POWER_RENEWABLES;"
                                 "Trusted_Connection=Yes")
engine_sqldb = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params, fast_executemany=True)
user = getpass.getuser()
timestamp = dt.datetime.now()
timestamp = timestamp - dt.timedelta(microseconds=timestamp.microsecond)

# </editor-fold>EE

# <editor-fold desc="Define Functions">

def yes_or_no(question):
    while "the answer is invalid":
        reply = str(input(question+' (y/n): ')).upper().strip()
        if reply[:1] == 'Y':
            return True
        elif reply[:1] == 'N':
            return False
        else:
            print('Invalid response.')


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


def get_sql_topology_to_aid(src_sql, country):
    """

    Function that reads APAC model topology from SQL Server and Transforms it to 2 Aurora Topology Input tables.

    Args:
        src_sql (string): SQL View name from Source SQL Server that gives the APAC model topology
        country (string): country name

    Returns:
         dict_area (dictionary): dictionary that maps Zone Names to Aurora Zone IDs
         dict_zone_mkt (dictionary): dictionary that maps Zone names to Markets
         dict_mkt_country (dictionary): dictionary that maps Markets to Countries
         df (dataframe): df that gives AID_Topology_Zones
         df_area(dataframe): df that gives AID_Topology_Areas
    """

    sql_qry = """SELECT * FROM """ + src_sql + """ WHERE [System Name] = '""" + country + """'"""
    df = pd.read_sql_query(sql_qry, engine_sqldb)

    dict_zone_mkt = dict(df[['Zone Name', 'CustomMkt_APACmodel']].drop_duplicates().values.tolist())
    dict_mkt_country = dict(df[['CustomMkt_APACmodel', 'System Name']].drop_duplicates().values.tolist())
    dict_area = dict(df[['Zone Name', 'Zone ID']].drop_duplicates().values.tolist())

    # Topology_Zone
    df = df.drop(['CustomMkt_APACmodel'], 1)
    df = df.sort_values(by=['Zone ID']).reset_index(drop=True)

    # Topology_Area
    df_area = df[['Zone ID', 'Zone Name']].reset_index(drop=True)
    df_area.columns = ['Area Number', 'Area Name']
    df_area['Short Area Name'] = df_area['Area Name']
    df_area['Area Demand Number'] = df_area['Area Number']

    return dict_area, dict_zone_mkt, dict_mkt_country, df, df_area


def get_sql_plantasmpt(src_sql):
    """

    Function that reads APAC plant attribute assumptions from SQL Server

    Args:
        src_sql (string): SQL View name from Source SQL Server that gives the APAC plant attribute assumptions

    Returns:
         df (dataframe): df that gives plant attribute assumptions to be used if no actual data is available
    """

    sql_qry = """SELECT * FROM """ + src_sql
    df = pd.read_sql_query(sql_qry, engine_sqldb)
    df = df.drop(['Units'], 1)
    df['PlantType'] = pd.np.where(df['PlantTech'] == df['PlantType'],
                                  df['PlantType'],
                                  df['PlantType'] + '-' + df['PlantTech'])
    df = df[~df['PlantAttribute'].isin(plant_att_tsAnnualAsumpt_list)]

    return df


def assign_assumptions(df):
    """

    Function that applies Assumptions on Plant Attributes to a dataframe of Plant Projects.

    df is used in a series of left joins in the following order of sequence:
        1. By Resource_Group & StartYear
        2. By Country & Resource_Group & StartYear
        3. By Market & Resource_Group & StartYear
        4. By Zone & Resource_Group & StartYear
        5. By Project & Resource_Group & StartYear
    ffill is then used to find the most granular assumption.
    Project Level ACTUAL Attribute has the final overriding over assumption, ALWAYS.

    Args:
        df (dataframe): project list with 2 compulsory columns:
                        1. "Resource_Group"
                        2. "StartYear"

    Returns:
        df (dataframe): Project list with all plant attributes added as columns.
    """
    lst_attributes = list(Assumptions['PlantAttribute'].unique())
    lst_attributes.remove('StorageDuration')
    for attr in lst_attributes:
        # attr = lst_attributes[0]
        df_asmpt = Assumptions[Assumptions['PlantAttribute'] == attr].reset_index(drop=True)
        df_asmpt.rename(columns={'Value': attr,
                                 'PlantType': 'Resource_Group',
                                 'PlantOnlineYear': 'StartYear'}, inplace=True)
        # left join by RG, Yr
        df_asmpt_all = df_asmpt[df_asmpt['Level'] == 'All'].reset_index(drop=True)
        df_asmpt_all = df_asmpt_all[['Resource_Group', 'StartYear', attr]]
        df = pd.merge(df, df_asmpt_all, how='left', on=['Resource_Group', 'StartYear'])
        # left join by Country, RG, Yr
        df_asmpt_country = df_asmpt[df_asmpt['Level'] == 'Country'].reset_index(drop=True)
        if len(df_asmpt_country) > 0:
            df_asmpt_country = df_asmpt_country[['LevelName', 'Resource_Group', 'StartYear', attr]]
            df_asmpt_country.rename(columns={'LevelName': 'Country'}, inplace=True)
            df = pd.merge(df, df_asmpt_country, how='left', on=['Country', 'Resource_Group', 'StartYear'])
            df = df.fillna(method='ffill', axis=1)
            df.rename(columns={attr + '_y': attr}, inplace=True)
            df = df.drop([attr + '_x'], 1)
        # left join by Market, RG, Yr
        df_asmpt_mkt = df_asmpt[df_asmpt['Level'] == 'ModelMarket'].reset_index(drop=True)
        if len(df_asmpt_mkt) > 0:
            df_asmpt_mkt = df_asmpt_mkt[['LevelName', 'Resource_Group', 'StartYear', attr]]
            df_asmpt_mkt.rename(columns={'LevelName': 'Market'}, inplace=True)
            df = pd.merge(df, df_asmpt_mkt, how='left', on=['Market', 'Resource_Group', 'StartYear'])
            df = df.fillna(method='ffill', axis=1)
            df.rename(columns={attr + '_y': attr}, inplace=True)
            df = df.drop([attr + '_x'], 1)
        # left join by Zone, RG, Yr
        df_asmpt_zone = df_asmpt[df_asmpt['Level'] == 'ModelZone'].reset_index(drop=True)
        if len(df_asmpt_zone) > 0:
            df_asmpt_zone = df_asmpt_zone[['LevelName', 'Resource_Group', 'StartYear', attr]]
            df_asmpt_zone.rename(columns={'LevelName': 'Zone'}, inplace=True)
            df = pd.merge(df, df_asmpt_zone, how='left', on=['Zone', 'Resource_Group', 'StartYear'])
            df = df.fillna(method='ffill', axis=1)
            df.rename(columns={attr + '_y': attr}, inplace=True)
            df = df.drop([attr + '_x'], 1)
        # left join by plant, RG, Yr
        df_asmpt_plant = df_asmpt[df_asmpt['Level'] == 'Plant'].reset_index(drop=True)
        if len(df_asmpt_plant) > 0:
            df_asmpt_plant = df_asmpt_plant[['LevelName', 'Resource_Group', 'StartYear', attr]]
            df_asmpt_plant.rename(columns={'LevelName': 'PowerPlant'}, inplace=True)
            df = pd.merge(df, df_asmpt_plant, how='left', on=['PowerPlant', 'Resource_Group', 'StartYear'])
            df = df.fillna(method='ffill', axis=1)
            df.rename(columns={attr + '_y': attr}, inplace=True)
            df = df.drop([attr + '_x'], 1)

    df.rename(columns={'Resource_Group': 'PlantType-Tech', 'Technology': 'PlantTech'}, inplace=True)

    return df


def get_excel_newbuild(balancing_plant=False):
    """

    Function that reads Excel Generic New Build data and flattens them

    - Fuel mapping data is on sheet named "ref"
    - sheets that have "GNB" in their sheet name gets read
    - data starts in cell "A71"
    - data is incremental capacity build, so no ffill

    Args:
        xls_name (string): name of excel file that IS OPENED

    Returns:
        df (dataframe): fuel generic new build data by Model Zones from excel in flat format
    """

    wb = xw.Book.caller()

    print(f'\nretrieving default PriFuel for each PlantType from "ref" sheet...\n')
    # get PriFuel mapping to plant type
    sht = wb.sheets['ref']
    df_prjfuel = pd.DataFrame(sht.range('A1').expand().value)
    x = df_prjfuel.iloc[0, :]
    df_prjfuel.columns = x
    df_prjfuel = df_prjfuel.iloc[1:, :].reset_index(drop=True)
    df_prjfuel.rename(columns={'PrimaryFuel(GenericNewBuild)': 'FuelPri'}, inplace=True)
    dict_ptypefuel = dict(df_prjfuel.loc[:, ['PlantType', 'FuelPri']].values.tolist())


    # get Generic New Build Projects
    df_xls = pd.DataFrame()
    df_bal = pd.DataFrame()
    r_sht = re.compile("^(GNB_).*")
    xls_lst = [x.name for x in wb.sheets]
    lst_gnb = list(filter(r_sht.match, xls_lst))
    for zone in lst_gnb:
        # zone = lst_gnb[0]
        print(f'parsing incremental new build capacities for: {zone}')
        sht = wb.sheets[zone]
        # if zone == 'GNB_Liaoning':
        #     df = pd.DataFrame(sht.range('A152').expand().value)
        # else:
        df = pd.DataFrame(sht.range('A151').expand().value)
        x = df.iloc[0, :]
        df.columns = x
        df = df.iloc[1:, :].reset_index(drop=True)
        df = df.drop(['Column2'], axis=1)
        df = pd.melt(df, id_vars=['Zone', 'Resource_Group', 'Technology', 'Group'],
                     var_name='StartYear', value_name='Capacity')
        df = df[df['Capacity'].notnull()].reset_index(drop=True)
        df['StartYear'] = df['StartYear'].astype(int)
        df['FuelPri'] = df['Resource_Group'].map(dict_ptypefuel)
        df['PlantType'] = df['Resource_Group']
        df['Resource_Group'] = pd.np.where(df['Technology'].notnull(),
                                           df['Resource_Group'] + '-' + df['Technology'],
                                           df['Resource_Group'])
        df['PowerPlant'] = pd.np.where(df['StartYear'] <= dt.datetime.now().year,
                                       'Existing_' + df['Zone'] + '_' + df['Resource_Group'] + '_' + df['StartYear'].astype(str),
                                       'New_' + df['Zone'] + '_' + df['Resource_Group'] + '_' + df['StartYear'].astype(str))
        df['Capacity'] = pd.to_numeric(df['Capacity'])
        df['Market'] = df['Zone'].map(dict_zone_mkt)
        df['Country'] = df['Market'].map(dict_mkt_country)

        df_xls = df_xls.append(df, ignore_index=True)

        if balancing_plant == True:
            print(f'parsing cummulative balancing capacities for: {zone}')
            # capacity here is cummulative
            df1 = pd.DataFrame(sht.range('A578').expand().value)
            y = df1.iloc[0, :]
            df1.columns = y
            df1 = df1.iloc[1:, :].reset_index(drop=True)
            df1 = df1.drop(['Column2'], 1)
            df1 = pd.melt(df1, id_vars=['Zone', 'Resource_Group', 'Technology', 'Group'],
                         var_name='StartYear', value_name='BalancingCapacity_MW')
            df1['BalancingCapacity_MW'] = df1['BalancingCapacity_MW'].fillna(0)
            df1['StartYear'] = df1['StartYear'].astype(int)
            df1['FuelPri'] = df1['Resource_Group'].map(dict_ptypefuel)
            df1['PlantType'] = df1['Resource_Group']
            df1['Resource_Group'] = pd.np.where(df1['Technology'].notnull(),
                                               df1['Resource_Group'] + '-' + df1['Technology'],
                                               df1['Resource_Group'])
            # df1['PowerPlant'] = pd.np.where(df1['StartYear'] <= dt.datetime.now().year,
            #                                'Existing_' + df1['Zone'] + '_' + df1['Resource_Group'] + '_' + df1['StartYear'].astype(str),
            #                                'New_' + df1['Zone'] + '_' + df1['Resource_Group'] + '_' + df1['StartYear'].astype(str))
            df1['BalancingCapacity_MW'] = pd.to_numeric(df1['BalancingCapacity_MW'])
            df1['Market'] = df1['Zone'].map(dict_zone_mkt)
            df1['Country'] = df1['Market'].map(dict_mkt_country)

            df_bal = df_bal.append(df1, ignore_index=True)

    df_xls['Username'] = user
    df_xls['TimeStamp'] = timestamp
    if len(df_bal) > 0:
        df_bal['Username'] = user
        df_bal['TimeStamp'] = timestamp

    return df_xls, df_bal

# </editor-fold>


if __name__ == '__main__':

    active_excel = xw.books.active.name
    xw.Book(active_excel).set_mock_caller()

    # identify the Country
    wb = xw.Book.caller()
    print(f'Processing: {wb.name}...\n')
    sht = wb.sheets['MarketTotal']
    country = sht.range('rng_country').value
    # retrieve the Country's geographical setup (country, market, zone)
    print(f'retrieving geographical definition of {country} from SQL database...\n')
    dict_zone, dict_zone_mkt, dict_mkt_country, df_zone, df_area = get_sql_topology_to_aid(src_sql='vAID_Topology_Zones', country=country)
    print(f'retrieving plant attribute assumptions from SQL database...\n')
    Assumptions = get_sql_plantasmpt(src_sql='vAPAC_Plant_Attributes_Annual_LIVE')
    # ask user response
    res_bal = yes_or_no(f'Hi {user}, do you need to extract & upload balancing plant capacities?')
    # parse & compile excel data
    df_gnb, df_bal = get_excel_newbuild(balancing_plant=res_bal)

    # Add attribute assumptions
    print(f'\nassigning plant attribute assumptions on Generic New Build projects...\n')
    df_newbuild_list = assign_assumptions(df=df_gnb)

    # Transform it to look like Project List
    df_newbuild_list['PlantLife'] = df_newbuild_list['PlantLife'].astype(int)
    #df_newbuild_list['EndYear'] = df_newbuild_list['StartYear'] + df_newbuild_list['PlantLife']
    df_newbuild_list['EndYear'] = 2060
    df_newbuild_list['Day'] = 1
    df_newbuild_list['Month'] = 1
    df_newbuild_list['Year'] = df_newbuild_list['StartYear']
    df_newbuild_list['Start'] = pd.to_datetime(df_newbuild_list[['Day', 'Month', 'Year']])
    df_newbuild_list['Day'] = 31
    df_newbuild_list['Month'] = 12
    df_newbuild_list['Year'] = df_newbuild_list['EndYear']
    df_newbuild_list['End'] = pd.to_datetime(df_newbuild_list[['Day', 'Month', 'Year']])
    df_newbuild_list = df_newbuild_list.drop(['Day', 'Month', 'Year', 'StartYear', 'EndYear'], 1)

    # update SQL db with data
    updatecycle = 'Zonal_Archive'

    if len(df_newbuild_list) > 0:
        print(f'Uploading & updating SQL db with Incremental Generic New Builds...\n')
        update_db_tbl(engine=engine_sqldb, dest_tbl='APAC_PowerProjects_NewBuild_LIVE', dest_df=df_newbuild_list, by_col='Zone')
        df_newbuild_list['UpdateCycle'] = updatecycle
        df_newbuild_list['Dataset_Name'] = str(timestamp)
        print(f'Adding Incremental Generic New Builds to archives...\n')
        upload_to_db(engine=engine_sqldb, dest_tbl='APAC_PowerProjects_NewBuild_Datasets', dest_df=df_newbuild_list, existMethod='append')

    if len(df_bal) > 0:
        df_bal.rename(columns={'Resource_Group': 'PlantType-Tech', 'Technology': 'PlantTech'}, inplace=True)
        print(f'Uploading & updating SQL db with Cumulative Balancing capacities...\n')
        update_db_tbl(engine=engine_sqldb, dest_tbl='APAC_PowerProjects_Balancing_LIVE', dest_df=df_bal, by_col='Zone')
        df_bal['UpdateCycle'] = updatecycle
        df_bal['Dataset_Name'] = str(timestamp)
        print(f'Adding Cumulative Balancing capacities to archives...\n')
        upload_to_db(engine=engine_sqldb, dest_tbl='APAC_PowerProjects_Balancing_Datasets', dest_df=df_bal, existMethod='append')

    print(f'Script completed. Goodbye {user}!\n')