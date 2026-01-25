# -*- coding: utf-8 -*-
"""
Created on Mon Aug 19 12:12:52 2019

@author: david kwa

Version 3.00
=============
- expand time range to 2060

Version 2.90
=============
- updated modelling end year to yr_end=2060
- commented out last line of code in "def assign_tsannual_assumptions(df)" that drops columns 2061 to 2075
- added line of code that in "def get_sql_plantasmpt(src_sql)" drops annual assumptions (after retrieving if from SQL Server) that are after yr_end

Version 2.89
=============
- add a line code to change the GNB waste script (basded on Liyue's comments)

Version 2.88
=============
- Analysts made some changes on their side but not merge to bitbucket, this version just used to align with analysts' side latest version script
- change the DB name and change to ensure fixed costs loaded properly into Aurora, remove the wind offshore option of the last version changes.

Version 2.87:
=============
- Change wind data source-- only pick up onshore wind data from database, and offshore data will be loaded via GNB uploader by analysts


Version 2.86:
=============
- Applied storage duration to time series

Version 2.85:
=============
- Applied emission function to wind project list as well

Version 2.84:
=============
- Use 'CO2' for Emission Rate and Emission Price name columns

Version 2.83:
=============
- Use 'ALL' for Emission Price plant type column
- Comments out EV upload block

Version 2.82:
=============
- change the col order of Emission rate & Emission Price

Version 2.81:
=============
- modified the block for Emission Rate and Emission Price

Version 2.8:
=============
- added Annual Emission Rate & Annual Emission Price

Version 2.71:
============
- for India, get_sql_plant_existing() will now also grab Bhutan plants that are exporting to India (similar to Thailand & Vietnam)


Version 2.7:
============
- with LDC now accounting for leap years, get_sqldemand_to_aid function now also checks for leap years.
- added code to extract and load EV demand from Excel to SQL DB for Europe team
- added code to extract and load 24hr shape for EV from Excel to SQL DB for Europe team

Version 2.61:
============
- shorten weekly & monthly Wind/Solar shape names (Kalimantan & Timor) to keep within 50 characters for Aurora DB


Version 2.6:
============
- solar & wind shapes are now scaled annually to meet the required CF by plant type


Version 2.54:
============
- excluded Taiwan from using Wind project list

Version 2.53:
============
- added min Must Run Capacity constraints for India based on Resource Group

Version 2.52:
============
- added min run constraints for Australia
- changed SQL view to use for Transmission
- tested for Australia & India

Version 2.51:
============
- All resource groups are now at zone level
- fixed TS_Assumptions function

Version 2.5:
============
- added code apply FixedCost assumptions as TS_Annual
- modified code to subset Transmission data to take year >= 2000
- scaled to Australia & India

Version 2.42:
============
- added code to set "Must Run" flags for China
- added code to add "Others" fuel to AID_Fuel table for China
- added code to clone AID_Operating_Rules table between aurora DBs
- modified code to add overrides for Vietnam & Thailand Fuel assignment to plants
- modified code pick up transmission data from new table
- modified code to extract plant list by model market for Viet & Thai so as to include Laos/Cambodia plants


Version 2.3:
============
- modified code to add new overrides for China balancing capacities
- modified code to aggregate transmission capacities before pivoting for Aurora
- modified code to extract plant list by model market for Viet & Thai so as to include Laos/Cambodia plants

Version 2.2:
============
- modified code to concat Fuel Constraint ID as a csv string if it has BOTH min & max limit defined
- added code to set Hydro_PS as a battery storage but without a 'Storage ID' and with a 12hr (China 24hr) max storage capacity instead of 4hr.
- tested on South Korea, China, Philippines, Malaysia & Singapore

Version 2.1:
============
- modified code to apply Wind & Solar shape by PlantType instead of by Fuel
- modified code to apply capacity factor (by ModelZone & PlantType) to Wind/Solar Shapes (also by ModelZone & PlantType) before it gets converted
to Maintenance rate of Resource
- tested on South Korea

Version 2.0:
============
- modified code to apply Min or Max Fuel constraint
- script tested on South Korea, Singapore & Malaysia

Version 1.9:
============
- added code to edit Balancing capacities (split between Tech/Fuel) in Agnostic Power DB
- modified code to make sure AID_HydroVector & AID_HydroMonthly zones match
- script tested on Philippines, China & Japan

Version 1.81:
============
- fixed capacity aggregation function bug which is missing plants that start 1 year after the 5 year blocks.
- added PumpedStorage as as PS plant type in AID_Fuel table

Version 1.8:
============
- fixed Storage Maximum capacity to be a TS_Annual name if plants are aggregated

Version 1.71:
=============
- fixed the Storage table code to pick up the correct Charging Resource based on the new plant names (with/without aggregating)

Version 1.7:
=============
- added function to aggregate power projects (existing & generic new builds) by zone, plant type/tech, fuel
- fixed the Storage table code to pick up the correct Charging Resource based on the new plant names

Version 1.6
============
- add two transmission original tables into DB

Version 1.5:
=============
- added AID_Resource table's Pri & Sec Fuel matching to AID_Fuel table's entry
- added placeholder functions for Ying to input her code for Transmission DB
"""


from datetime import datetime
import urllib
import pandas as pd
import numpy as np
import xlwings as xw
from sqlalchemy import create_engine
import sqlalchemy
import getpass
import datetime as dt
import calendar
import re

country = 'China'
cycle = 'test2060'

# <editor-fold desc="Global Variables">
aid_country = re.sub(' ', '', country)
aurora_sqldb = 'ANVDEVSQLVPM01'
#aurora_sqldb = 'SIND8M3BR42\SINSQLINTDEV02'
#aurora_sqldb = 'BEID3LZ6132\BEISQLINTDEV01'
#aurora_dbname = 'Aurora_APAC_DEV_' + aid_country
aurora_dbname = 'Aurora_APAC_DEV_' + aid_country + '_test' #Allen test db
# aurora_dbname = 'Aurora_APAC_DEV'
path = r'L:\Power_Renewables\Inputs\APAC_Transmission_China.xlsx'
sheetname_line = 'LiveUpdate'
sheetname_price = 'T&D Tariffs'

ba_list = ['BA', 'BA_PV', 'BA_OW', 'BA_WT', 'Hydro_PS']
plant_att_tsAnnualAsumpt_list = ['FixedCost', 'EmissionRate_CO2', 'EmissionPrice_CO2']
yr_start = 2011
yr_end = 2060
yr_model = list(range(yr_start, yr_end +1))

user = getpass.getuser()
timestamp = dt.datetime.now()
timestamp = timestamp - dt.timedelta(microseconds=timestamp.microsecond)
datasetname = str(timestamp)            # For reloads during cycle
# datasetname = 'APAC Research Final'    # For FINAL cut in cycle
# </editor-fold>

# <editor-fold desc="Setup Source & Destination SQL Server connection">

# Source is the SQL Server where the APAC Power data is kept & updated
params_src = urllib.parse.quote_plus("DRIVER={ODBC Driver 17 for SQL Server};"
                                 "SERVER=ANVDEVSQLVPM01;"
                                 "DATABASE=WM_POWER_RENEWABLES;"
                                 "Trusted_Connection=Yes")
engine_src = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params_src, fast_executemany=True)

# Destination is SQL Server where the AURORA power model reads from/writes to
params_dest = urllib.parse.quote_plus("DRIVER={ODBC Driver 17 for SQL Server};"
                                 "SERVER=" + aurora_sqldb + ";"
                                 "DATABASE=" + aurora_dbname + ";"
                                 "Trusted_Connection=Yes")
engine_dest = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params_dest, fast_executemany=True)

# </editor-fold>

# <editor-fold desc="Define Functions">

def get8760map():
    """

    Function that generates a 8760 row dataframe for every hour in a non-leap year,

    Args:

    Returns:
        df (dataframe): 4 column Pandas DataFrame of 8760 rows indicating the
                        - 'Hr_Yr' (1 to 8760) and its corresponding
                        - 'Mth' (1 to 12),
                        - 'Day' (1 to 31) &
                        - 'Hr_Day' (0 to 23)

    """

    df = pd.DataFrame({'dtm': pd.date_range('2011-01-01', '2012-01-01', freq='1H', closed='left')})
    df['Hr_Yr'] = df.index + 1
    df['Mth'] = df['dtm'].dt.month
    df['Day'] = df['dtm'].dt.day
    df['Hr_Day'] = df['dtm'].dt.hour
    df = df.drop(['dtm'], 1)

    return df

def get168map():
    """

    Function that generates a 168 row dataframe for every hour in a week.

    Args:

    Returns:
        df (dataframe): 3 column pandas dataframe indicating the
                        - 'DayOfWeek' (1 to 7)
                        - 'HourOfDay' (0 to 23)
                        - 'HrOfWk' (1 to 168)

    """

    d = np.array(list(range(1, 8)))
    h = np.array(list(range(0, 24)))
    x, y = np.meshgrid(d, h)
    x = x.flatten()
    y = y.flatten()
    df = pd.DataFrame({'DayOfWeek': x, 'HourOfDay': y})
    df = df.sort_values(by=['DayOfWeek', 'HourOfDay']).reset_index(drop=True)
    df['HrOfWk'] = df.index + 1

    return df

def check_leap_yr(year):
    if (year % 4) == 0:
        if (year % 100) == 0:
            if (year % 400) == 0:
                print(f"{year} is a leap year")
                leap_yr = True
            else:
                print(f"{year} is not a leap year")
                leap_yr = False
        else:
            print(f"{year} is a leap year")
            leap_yr = True
    else:
        print(f"{year} is not a leap year")
        leap_yr = False

    return leap_yr

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
    lst_attributes = [i for i in lst_attributes if i not in plant_att_tsAnnualAsumpt_list]
    df['Country'] = country
    df.rename(columns={'Name': 'PowerPlant',
                       'zREM County': 'Zone',
                       'zREM State': 'Market'}, inplace=True)
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
            df[[attr + '_x', attr + '_y']] = df[[attr + '_x', attr + '_y']].fillna(method='ffill', axis=1)
            df.rename(columns={attr + '_y': attr}, inplace=True)
            df = df.drop([attr + '_x'], 1)
        # left join by Market, RG, Yr
        df_asmpt_mkt = df_asmpt[df_asmpt['Level'] == 'ModelMarket'].reset_index(drop=True)
        if len(df_asmpt_mkt) > 0:
            df_asmpt_mkt = df_asmpt_mkt[['LevelName', 'Resource_Group', 'StartYear', attr]]
            df_asmpt_mkt.rename(columns={'LevelName': 'Market'}, inplace=True)
            df = pd.merge(df, df_asmpt_mkt, how='left', on=['Market', 'Resource_Group', 'StartYear'])
            df[[attr + '_x', attr + '_y']] = df[[attr + '_x', attr + '_y']].fillna(method='ffill', axis=1)
            df.rename(columns={attr + '_y': attr}, inplace=True)
            df = df.drop([attr + '_x'], 1)
        # left join by Zone, RG, Yr
        df_asmpt_zone = df_asmpt[df_asmpt['Level'] == 'ModelZone'].reset_index(drop=True)
        if len(df_asmpt_zone) > 0:
            df_asmpt_zone = df_asmpt_zone[['LevelName', 'Resource_Group', 'StartYear', attr]]
            df_asmpt_zone.rename(columns={'LevelName': 'Zone'}, inplace=True)
            df = pd.merge(df, df_asmpt_zone, how='left', on=['Zone', 'Resource_Group', 'StartYear'])
            df[[attr + '_x', attr + '_y']] = df[[attr + '_x', attr + '_y']].fillna(method='ffill', axis=1)
            df.rename(columns={attr + '_y': attr}, inplace=True)
            df = df.drop([attr + '_x'], 1)
        # left join by plant, RG, Yr
        df_asmpt_plant = df_asmpt[df_asmpt['Level'] == 'Plant'].reset_index(drop=True)
        if len(df_asmpt_plant) > 0:
            df_asmpt_plant = df_asmpt_plant[['LevelName', 'Resource_Group', 'StartYear', attr]]
            df_asmpt_plant.rename(columns={'LevelName': 'PowerPlant'}, inplace=True)
            df = pd.merge(df, df_asmpt_plant, how='left', on=['PowerPlant', 'Resource_Group', 'StartYear'])
            df[[attr + '_x', attr + '_y']] = df[[attr + '_x', attr + '_y']].fillna(method='ffill', axis=1)
            df.rename(columns={attr + '_y': attr}, inplace=True)
            df = df.drop([attr + '_x'], 1)

    df.rename(columns={'PowerPlant': 'Name',
                       'Resource_Group': 'PlantType-Tech',
                       'Technology': 'PlantTech',
                       'Zone': 'zREM County',
                       'Market': 'zREM State'}, inplace=True)
    df = df.drop(['Country'], 1)

    return df

def assign_tsannual_assumptions(df):
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

    df['Country'] = country
    df.rename(columns={'Name': 'PowerPlant',
                       'PlantType-Tech': 'Resource_Group',
                       'zREM County': 'Zone',
                       'zREM State': 'Market'}, inplace=True)
    ts_asmpt = pd.DataFrame()
    for attr in plant_att_tsAnnualAsumpt_list:
        #attr = plant_att_tsAnnualAsumpt_list[0]
        if attr == 'EmissionPrice_CO2':
            df_asmpt = Assumptions[Assumptions['PlantAttribute'] == attr].reset_index(drop=True)
            df_asmpt = pd.pivot_table(df_asmpt,
                       index=['LevelName', 'Level', 'PlantType', 'PlantAttribute'],
                       columns='PlantOnlineYear', values='Value', fill_value=0).reset_index()
            df_asmpt.rename(columns={'PlantType': 'Resource_Group',
                                     'PlantAttribute': 'Use'}, inplace=True)
            # left join by RG, Yr
            df_asmpt_all = df_asmpt[df_asmpt['Level'] == 'All'].reset_index(drop=True)
            df_asmpt_all['ID'] = df_asmpt_all['Use'] + '_' + df_asmpt_all['Resource_Group']
            df_asmpt_all = df_asmpt_all.drop(['LevelName', 'Level'], 1)
            ts_asmpt_all = df_asmpt_all.copy()
            ts_asmpt_all['zREM Topology'] = 'All'
            ts_asmpt_all.rename(columns={'Resource_Group': 'zREM Type'}, inplace=True)
            df_asmpt_all = df_asmpt_all[['Resource_Group', 'ID']].reset_index(drop=True)
            df_asmpt_all[attr] = 'yr_' + df_asmpt_all['ID']
            df_asmpt_all = df_asmpt_all.drop(['ID'], 1)
            #df = pd.merge(df, df_asmpt_all, how='left', on=['Resource_Group'])
            df[attr] =  df_asmpt_all[attr]
            df[attr] = df[attr].fillna(method='ffill', axis=0)
            ts_asmpt = pd.concat([ts_asmpt, ts_asmpt_all], ignore_index=True, sort=True)

            # left join by Country, RG, Yr
            df_asmpt_country = df_asmpt[df_asmpt['Level'] == 'Country'].reset_index(drop=True)
            if len(df_asmpt_country) > 0:
                df_asmpt_country['ID'] = df_asmpt_country['Use'] + '_' + df_asmpt_country['LevelName'] + '_' + df_asmpt_country['Resource_Group']
                df_asmpt_country.drop(['Level'], 1,inplace = True)
                df_asmpt_country.rename(columns={'LevelName': 'Country'}, inplace=True)
                ts_asmpt_country = df_asmpt_country.copy()
                ts_asmpt_country.rename(columns={'Country': 'zREM Topology',
                                                 'Resource_Group': 'zREM Type'}, inplace=True)
                df_asmpt_country = df_asmpt_country[['Country', 'Resource_Group', 'ID']].reset_index(drop=True)
                df_asmpt_country[attr] = 'yr_' + df_asmpt_country['ID']
                df_asmpt_country = df_asmpt_country.drop(['ID'], 1)

                df = pd.merge(df, df_asmpt_country, how='left', on=['Country'])
                df[[attr + '_x', attr + '_y']] = df[[attr + '_x', attr + '_y']].fillna(method='ffill', axis=1)
                df.rename(columns={attr + '_y': attr,
                                   'Resource_Group_x':'Resource_Group'}, inplace=True)
                df = df.drop([attr + '_x','Resource_Group_y'], 1)

                ts_asmpt = pd.concat([ts_asmpt, ts_asmpt_country], ignore_index=True, sort=True)

        else:
            df_asmpt = Assumptions[Assumptions['PlantAttribute'] == attr].reset_index(drop=True)
            df_asmpt = pd.pivot_table(df_asmpt,
                                      index=['LevelName', 'Level', 'PlantType', 'PlantAttribute'],
                                      columns='PlantOnlineYear', values='Value', fill_value=0).reset_index()
            df_asmpt.rename(columns={'PlantType': 'Resource_Group',
                                     'PlantAttribute': 'Use'}, inplace=True)
            # left join by RG, Yr
            df_asmpt_all = df_asmpt[df_asmpt['Level'] == 'All'].reset_index(drop=True)
            df_asmpt_all['ID'] = df_asmpt_all['Use'] + '_' + df_asmpt_all['Resource_Group']
            df_asmpt_all = df_asmpt_all.drop(['LevelName', 'Level'], 1)
            ts_asmpt_all = df_asmpt_all.copy()
            ts_asmpt_all['zREM Topology'] = 'All'
            ts_asmpt_all.rename(columns={'Resource_Group': 'zREM Type'}, inplace=True)
            df_asmpt_all = df_asmpt_all[['Resource_Group', 'ID']].reset_index(drop=True)
            df_asmpt_all[attr] = 'yr_' + df_asmpt_all['ID']
            df_asmpt_all = df_asmpt_all.drop(['ID'], 1)
            df = pd.merge(df, df_asmpt_all, how='left', on=['Resource_Group'])
            ts_asmpt = pd.concat([ts_asmpt, ts_asmpt_all], ignore_index=True, sort=True)

            # left join by Country, RG, Yr
            df_asmpt_country = df_asmpt[df_asmpt['Level'] == 'Country'].reset_index(drop=True)
            if len(df_asmpt_country) > 0:
                df_asmpt_country['ID'] = df_asmpt_country['Use'] + '_' + df_asmpt_country['LevelName'] + '_' + \
                                         df_asmpt_country['Resource_Group']
                df_asmpt_country = df_asmpt_country.drop(['Level'], 1)
                df_asmpt_country.rename(columns={'LevelName': 'Country'}, inplace=True)
                ts_asmpt_country = df_asmpt_country.copy()
                ts_asmpt_country.rename(columns={'Country': 'zREM Topology',
                                                 'Resource_Group': 'zREM Type'}, inplace=True)
                df_asmpt_country = df_asmpt_country[['Country', 'Resource_Group', 'ID']].reset_index(drop=True)
                df_asmpt_country[attr] = 'yr_' + df_asmpt_country['ID']
                df_asmpt_country = df_asmpt_country.drop(['ID'], 1)

                df = pd.merge(df, df_asmpt_country, how='left', on=['Country', 'Resource_Group'])
                df[[attr + '_x', attr + '_y']] = df[[attr + '_x', attr + '_y']].fillna(method='ffill', axis=1)
                df.rename(columns={attr + '_y': attr}, inplace=True)
                df = df.drop([attr + '_x'], 1)

                ts_asmpt = pd.concat([ts_asmpt, ts_asmpt_country], ignore_index=True, sort=True)

            # left join by Market, RG, Yr
            df_asmpt_mkt = df_asmpt[df_asmpt['Level'] == 'ModelMarket'].reset_index(drop=True)
            if len(df_asmpt_mkt) > 0:
                df_asmpt_mkt['ID'] = df_asmpt_mkt['Use'] + '_' + df_asmpt_mkt['LevelName'] + '_' + df_asmpt_mkt[
                    'Resource_Group']
                df_asmpt_mkt = df_asmpt_mkt.drop(['Level'], 1)
                df_asmpt_mkt.rename(columns={'LevelName': 'Market'}, inplace=True)
                ts_asmpt_mkt = df_asmpt_mkt.copy()
                ts_asmpt_mkt.rename(columns={'Market': 'zREM Topology',
                                             'Resource_Group': 'zREM Type'}, inplace=True)
                df_asmpt_mkt = df_asmpt_mkt[['Market', 'Resource_Group', 'ID']].reset_index(drop=True)
                df_asmpt_mkt[attr] = 'yr_' + df_asmpt_mkt['ID']
                df_asmpt_mkt = df_asmpt_mkt.drop(['ID'], 1)

                df = pd.merge(df, df_asmpt_mkt, how='left', on=['Market', 'Resource_Group'])
                df[[attr + '_x', attr + '_y']] = df[[attr + '_x', attr + '_y']].fillna(method='ffill', axis=1)
                df.rename(columns={attr + '_y': attr}, inplace=True)
                df = df.drop([attr + '_x'], 1)

                ts_asmpt = pd.concat([ts_asmpt, ts_asmpt_mkt], ignore_index=True, sort=True)

            # left join by Zone, RG, Yr
            df_asmpt_zone = df_asmpt[df_asmpt['Level'] == 'ModelZone'].reset_index(drop=True)
            if len(df_asmpt_zone) > 0:
                df_asmpt_zone['ID'] = df_asmpt_zone['Use'] + '_' + df_asmpt_zone['LevelName'] + '_' + df_asmpt_zone[
                    'Resource_Group']
                df_asmpt_zone = df_asmpt_zone.drop(['Level'], 1)
                df_asmpt_zone.rename(columns={'LevelName': 'Zone'}, inplace=True)
                ts_asmpt_zone = df_asmpt_zone.copy()
                ts_asmpt_zone.rename(columns={'Zone': 'zREM Topology',
                                              'Resource_Group': 'zREM Type'}, inplace=True)
                df_asmpt_zone = df_asmpt_zone[['Zone', 'Resource_Group', 'ID']].reset_index(drop=True)
                df_asmpt_zone[attr] = 'yr_' + df_asmpt_zone['ID']
                df_asmpt_zone = df_asmpt_zone.drop(['ID'], 1)

                df = pd.merge(df, df_asmpt_zone, how='left', on=['Zone', 'Resource_Group'])
                df[[attr + '_x', attr + '_y']] = df[[attr + '_x', attr + '_y']].fillna(method='ffill', axis=1)
                df.rename(columns={attr + '_y': attr}, inplace=True)
                df = df.drop([attr + '_x'], 1)

                ts_asmpt = pd.concat([ts_asmpt, ts_asmpt_zone], ignore_index=True, sort=True)

            # left join by plant, RG, Yr
            df_asmpt_plant = df_asmpt[df_asmpt['Level'] == 'Plant'].reset_index(drop=True)
            if len(df_asmpt_plant) > 0:
                df_asmpt_plant['ID'] = df_asmpt_plant['Use'] + '_' + df_asmpt_plant['LevelName'] + '_' + df_asmpt_plant[
                    'Resource_Group']
                df_asmpt_plant = df_asmpt_plant.drop(['Level'], 1)
                df_asmpt_plant.rename(columns={'LevelName': 'PowerPlant'}, inplace=True)
                ts_asmpt_plant = df_asmpt_plant.copy()
                ts_asmpt_plant.rename(columns={'PowerPlant': 'zREM Topology',
                                               'Resource_Group': 'zREM Type'}, inplace=True)
                df_asmpt_plant = df_asmpt_plant[['PowerPlant', 'Resource_Group', 'ID']].reset_index(drop=True)
                df_asmpt_plant[attr] = 'yr_' + df_asmpt_plant['ID']
                df_asmpt_plant = df_asmpt_plant.drop(['ID'], 1)

                df = pd.merge(df, df_asmpt_plant, how='left', on=['PowerPlant', 'Resource_Group'])
                df[[attr + '_x', attr + '_y']] = df[[attr + '_x', attr + '_y']].fillna(method='ffill', axis=1)
                df.rename(columns={attr + '_y': attr}, inplace=True)
                df = df.drop([attr + '_x'], 1)

                ts_asmpt = pd.concat([ts_asmpt, ts_asmpt_plant], ignore_index=True, sort=True)

    df.rename(columns={'PowerPlant': 'Name',
                       'Resource_Group': 'PlantType-Tech',
                       'Technology': 'PlantTech',
                       'Zone': 'zREM County',
                       'Market': 'zREM State'}, inplace=True)
    df = df.drop(['Country'], 1)
    # ts_asmpt = ts_asmpt.drop([ 2061,            2062,            2063,
    #               2064,            2065,            2066,            2067,
    #               2068,            2069,            2070,            2071,
    #               2072,            2073,            2074,            2075],1)

    return df, ts_asmpt

def apply_emissions(df):
    df.rename(columns={'EmissionRate_CO2': 'EmissionRate',
                       'EmissionPrice_CO2': 'EmissionPrice',
                      }, inplace=True)
    df['EmissionRate'] = 'ER_'+ df['EmissionRate'].str.split('_',1,expand = True)[1]
    #comments out when the price data is ready with planttype
    df['EmissionPrice'] = 'ER_' + df['EmissionPrice'].str.split('_', 1, expand=True)[1]

    emission_rate = df[['Name', 'EmissionRate']] # can add other cols if needed
    emission_rate['ID'] =  emission_rate['EmissionRate']
    emission_rate['EmissionRate'] = 'yr_' + emission_rate['EmissionRate'].str.split('_',1,expand = True)[1]
    emission_rate.rename(columns ={'EmissionRate':'Rate'}, inplace = True)
    emission_rate['Name'] = 'CO2'
    emission_rate = emission_rate.drop_duplicates()
    emissionrate = emission_rate[['ID','Name', 'Rate']]



    emission_price = df[['Name', 'EmissionPrice']] # can add other cols if needed
    emission_price['ID'] = emission_price['EmissionPrice']
    emission_price['EmissionPrice'] = 'yr_' + emission_price['EmissionPrice'].str.split('_',1,expand = True)[1]
    emission_price.rename(columns ={'EmissionPrice':'Price'}, inplace = True)
    emission_price['Limit']=0 # can change to any
    emission_price['zREM Topology'] =''
    emission_price['Name'] = 'CO2'
    emission_price = emission_price.drop_duplicates()
    emissionprice = emission_price[['ID','Name', 'Price', 'Limit', 'zREM Topology']]

    return df, emissionrate, emissionprice

def match_aidResourceFuel_to_aidFuel(df_resource, df_fuel, colname):
    # we've chosen to make Fuel ID & Fuel Name both the same, ie, both are unique
    remap_fuel = df_fuel[['Fuel ID', 'Fuel Name', 'zREM Topology']].reset_index(drop=True)
    # rename 'Fuel ID' to 'Fuel' for left join with AID_Resource df
    remap_fuel.columns = [colname, 'AID_FuelName', 'Area']
    # add Zone, Market & Country columns for left join with Fuel df
    df_resource['Zone'] = df_resource['zREM County']
    df_resource['Market'] = df_resource['Zone'].map(dict_zone_mkt)
    df_resource['Country'] = df_resource['Market'].map(dict_mkt_country)

    # left join by Fuel only
    remap = remap_fuel[remap_fuel['Area'] == 'All'].reset_index(drop=True)
    remap = remap.drop(['Area'], 1)
    df_resource = pd.merge(df_resource, remap, how='left', on=colname)
    # check df_resource now; 'AID_FuelName' column that has nan indicates we cannot find the Plant's fuel in AID_Fuel table
    # so one level lower is Country, so according to agreed naming convention, we look for "Country_Fuel" now

    # left join Country & Fuel
    # df_resource['fuel_lookup'] = pd.np.where(df_resource['AID_FuelName'].isnull(),
    #                                           df_resource['Country'] + '_' + df_resource[colname],
    #                                           df_resource['AID_FuelName'])
    df_resource['fuel_lookup'] = df_resource['Country'] + '_' + df_resource[colname]
    remap = remap_fuel[remap_fuel['Area'] != 'All'].reset_index(drop=True)
    remap.columns = ['fuel_lookup', 'area_fuel', 'Country']
    df_resource = pd.merge(df_resource, remap, how='left', on=['Country', 'fuel_lookup'])
    df_resource.rename(columns={'area_fuel': 'country_fuel_found'}, inplace=True)

    # left join Market & Fuel
    # df_resource['fuel_lookup'] = pd.np.where(df_resource['AID_FuelName'].isnull(),
    #                                           df_resource['Market'] + '_' + df_resource[colname],
    #                                           df_resource['AID_FuelName'])
    df_resource['fuel_lookup'] = df_resource['Market'] + '_' + df_resource[colname]
    remap.columns = ['fuel_lookup', 'area_fuel', 'Market']
    df_resource = pd.merge(df_resource, remap, how='left', on=['Market', 'fuel_lookup'])
    df_resource.rename(columns={'area_fuel': 'market_fuel_found'}, inplace=True)

    # left join Zone & Fuel
    # df_resource['fuel_lookup'] = pd.np.where(df_resource['AID_FuelName'].isnull(),
    #                                           df_resource['Zone'] + '_' + df_resource[colname],
    #                                           df_resource['AID_FuelName'])
    df_resource['fuel_lookup'] = df_resource['Zone'] + '_' + df_resource[colname]
    remap.columns = ['fuel_lookup', 'area_fuel', 'Zone']
    df_resource = pd.merge(df_resource, remap, how='left', on=['Zone', 'fuel_lookup'])
    df_resource.rename(columns={'area_fuel': 'zone_fuel_found'}, inplace=True)

    # left join Plant & Fuel
    # df_resource['fuel_lookup'] = pd.np.where(df_resource['AID_FuelName'].isnull(),
    #                                           df_resource['Name'] + '_' + df_resource[colname],
    #                                           df_resource['AID_FuelName'])
    df_resource['fuel_lookup'] = df_resource['Name'] + '_' + df_resource[colname]
    remap.columns = ['fuel_lookup', 'area_fuel', 'Name']
    df_resource = pd.merge(df_resource, remap, how='left', on=['Name', 'fuel_lookup'])
    df_resource.rename(columns={'area_fuel': 'plant_fuel_found'}, inplace=True)

    # keep [AID_FuelName, country_fuel_found, market_fuel_found, zone_fuel_found, plant_fuel_found] then ffill to right then drop the rest
    df_resource = df_resource.drop(['fuel_lookup'], 1)
    df_resource[[colname, 'AID_FuelName', 'country_fuel_found', 'market_fuel_found', 'zone_fuel_found', 'plant_fuel_found']] = df_resource[[colname,
        'AID_FuelName', 'country_fuel_found', 'market_fuel_found', 'zone_fuel_found', 'plant_fuel_found']].fillna(method='ffill', axis=1)
    df_resource[colname] = df_resource['plant_fuel_found']
    df_resource = df_resource.drop(['AID_FuelName', 'country_fuel_found', 'market_fuel_found', 'zone_fuel_found', 'plant_fuel_found'], 1)

    return df_resource

def aggregate_plant_list(df_plantlist, yr_start, yr_end, step, name_prefix, offset_balancing_capacity=False,):
    # Existing plans block 1: projects that start <= 31/12/2010 --> group by prov & type-tech & year then cummulative sum
    df_plantlist.rename(columns={'Resource Begin Date': 'ResourceBeginDate',
                                   'Resource End Date'  : 'ResourceEndDate'}, inplace=True)

    df_plantlist['Day'] = 1
    df_plantlist['Month'] = 1
    df_plantlist['Year'] = df_plantlist['ResourceBeginDate'].dt.year
    df_plantlist['ResourceBeginDate'] = pd.to_datetime(df_plantlist[['Day', 'Month', 'Year']])
    df_plantlist['Day'] = 31
    df_plantlist['Month'] = 12
    df_plantlist['Year'] = df_plantlist['ResourceEndDate'].dt.year
    df_plantlist['ResourceEndDate'] = pd.to_datetime(df_plantlist[['Day', 'Month', 'Year']])
    df_plantlist = df_plantlist.drop(['Day', 'Month', 'Year'], 1)

    df_list = []
    yr_blk = list(range(yr_start, yr_end + 1, step))
    for idx, yr in enumerate(yr_blk):
        if idx == 0:
            startyr = '1900-01-01'
            endyr = str(yr_blk[idx]) + '-12-31'
            df_plant_blk = df_plantlist[(df_plantlist['ResourceBeginDate'] >= startyr) & (df_plantlist['ResourceBeginDate'] <= endyr)]
        else:
            startyr = str(yr_blk[idx-1]+1) + '-01-01'
            endyr = str(yr_blk[idx]) + '-12-31'
            df_plant_blk = df_plantlist[(df_plantlist['ResourceBeginDate'] >= startyr) & (df_plantlist['ResourceBeginDate'] <= endyr)]

        if len(df_plant_blk) > 0:
            print(f'Grouping {name_prefix} projects...block {idx + 1}... {startyr} : {endyr}')
            df_list.append(df_plant_blk)

    df_existing_cummulative = []
    for id, df_plant_blk in enumerate(df_list):
        # df_plant_blk = df_list[0]

        print(f'Aggregating {name_prefix} plants...block {id + 1}...')
        for col in ['Resource_Group', 'zREM Technology', 'Fuel', 'zREM County', 'zREM State', 'Resource Group']:
            df_plant_blk[col] = df_plant_blk[col].astype(str).str.strip()

        df_ppA = pd.concat([pd.DataFrame({
        'Yr'             : pd.date_range(row['ResourceBeginDate'], row['ResourceEndDate'], freq='Y'),
        'Name'           : row.Name,
        'Resource_Group' : row['Resource_Group'],
        'zREM Technology': row['zREM Technology'],
        'Fuel'           : row.Fuel,
        'Capacity'       : row['Capacity'],
        'zREM County'    : row['zREM County'],
        'zREM State'     : row['zREM State'],
        'Area'           : row['Area'],
        'Resource Group' : row['Resource Group']},
        columns=['Yr', 'Name', 'Resource_Group', 'zREM Technology', 'Fuel', 'Capacity', 'zREM County', 'zREM State',
                 'Area', 'Resource Group'])
        for i, row in df_plant_blk.iterrows()], ignore_index=True)
        df_ppA[['Capacity']] = df_ppA[['Capacity']].apply(pd.to_numeric, errors='coerce')
        df_ppA['Yr'] = df_ppA['Yr'].dt.year.astype('str')
        df_existing_cum = df_ppA.groupby(['Yr', 'Resource_Group', 'zREM Technology', 'Fuel', 'zREM County', 'zREM State',
                                                  'Area', 'Resource Group'], as_index=False)['Capacity'].agg('sum')

        if offset_balancing_capacity:
            if id == 0:
                print(f'Getting balancing capacities on block {id + 1}')
                Balancing_MW = get_sql_plant_balancing(src_sql='vAPAC_PowerCapacity_BalancingQty_LIVE', country='China')
                Balancing_MW = Balancing_MW[Balancing_MW['BalancingCapacity_MW'] != 0].reset_index(drop=True)
                Balancing_MW['zREM Technology'] = Balancing_MW['zREM Technology'].fillna(np.nan)

                Balancing_MW['zREM Technology'] = pd.np.where(Balancing_MW['Resource_Group'] == 'CHP_Gas',
                                                              'CCF', Balancing_MW['zREM Technology'])
                Balancing_MW['zREM Technology'] = pd.np.where(Balancing_MW['Resource_Group'] == 'CHP_Coal',
                                                              'SubC', Balancing_MW['zREM Technology'])
                Balancing_MW['Resource_Group'] = pd.np.where(Balancing_MW['Resource_Group'] == 'CHP_Gas',
                                                             Balancing_MW['Resource_Group'] + '-' + Balancing_MW['zREM Technology'],
                                                             Balancing_MW['Resource_Group'])
                Balancing_MW['Resource_Group'] = pd.np.where(Balancing_MW['Resource_Group'] == 'CHP_Coal',
                                                             Balancing_MW['Resource_Group'] + '-' + Balancing_MW['zREM Technology'],
                                                             Balancing_MW['Resource_Group'])

                Balancing_MW['zREM Technology'] = pd.np.where(Balancing_MW['Resource_Group'] == 'Nuclear',
                                                              'PWR', Balancing_MW['zREM Technology'])
                Balancing_MW['zREM Technology'] = pd.np.where(Balancing_MW['Resource_Group'] == 'STRenew',
                                                              'ST', Balancing_MW['zREM Technology'])
                Balancing_MW['zREM Technology'] = pd.np.where(Balancing_MW['Resource_Group'] == 'STFuelOil',
                                                              'ST', Balancing_MW['zREM Technology'])
                Balancing_MW['zREM Technology'] = pd.np.where(Balancing_MW['Resource_Group'] == 'Hydro_PS',
                                                              'Hydro', Balancing_MW['zREM Technology'])

                Balancing_MW['zREM Technology'] = pd.np.where(Balancing_MW['zREM Technology'].isnull(),
                                                              Balancing_MW['Resource_Group'], Balancing_MW['zREM Technology'])

                Balancing_MW['Resource Group'] = Balancing_MW['Resource_Group'].str.split('-').str[0]

                Balancing_MW['Fuel'] = Balancing_MW['Fuel'].str.strip()

                Balancing_MW.rename(columns={'BalancingCapacity_MW': 'Capacity'}, inplace=True)
                Balancing_MW = Balancing_MW.groupby(['Yr', 'Resource_Group', 'zREM Technology', 'Fuel', 'zREM County', 'zREM State',
                                                  'Area', 'Resource Group'], as_index=False)['Capacity'].agg('sum')
                Balancing_MW['Yr'] =Balancing_MW['Yr'].astype('str')

                df_existing_cum = pd.merge(df_existing_cum, Balancing_MW, how='outer', on=['Yr', 'Resource_Group', 'zREM Technology', 'Fuel',
                                                                                           'zREM County', 'zREM State', 'Area',
                                                                                           'Resource Group'], indicator=True).reset_index(drop=True)

                df_existing_cum['Capacity_x'] = df_existing_cum['Capacity_x'].fillna(0)
                df_existing_cum['Capacity_y'] = df_existing_cum['Capacity_y'].fillna(0)
                df_existing_cum['Capacity'] = df_existing_cum['Capacity_x'] + df_existing_cum['Capacity_y']

                df_existing_cum.to_csv(r'C:\Users\\' + user + '\downloads\ExistingBlock1_balancing_check.csv')

                df_existing_cum = df_existing_cum.drop(['Capacity_x', 'Capacity_y'], 1)
                df_existing_cum = df_existing_cum.groupby(['Yr', 'Resource_Group', 'zREM Technology', 'Fuel', 'zREM County', 'zREM State',
                                                           'Area', 'Resource Group'], as_index=False)['Capacity'].agg('sum')

        df_existing_cum['StartYear'] = df_existing_cum.groupby(['Resource_Group', 'zREM Technology', 'Fuel', 'zREM County', 'zREM State',
                                          'Area', 'Resource Group'], as_index=False)['Yr'].transform('min')
        df_existing_cum['endyr'] = df_existing_cum.groupby(['Resource_Group', 'zREM Technology', 'Fuel', 'zREM County', 'zREM State',
                                                     'Area', 'Resource Group'], as_index=False)['Yr'].transform('max')
        if len(df_existing_cum) > 0:
            df_existing_cummulative.append(df_existing_cum)

    df_existing_cummulative = pd.concat(df_existing_cummulative, ignore_index=True)
    df_existing_cummulative['Yr'] = df_existing_cummulative['Yr'].astype('int')
    df_existing_cummulative = df_existing_cummulative[(df_existing_cummulative['Yr'] >= 2000) & (df_existing_cummulative['Yr'] <= yr_end)]
    df_existing_cummulative = df_existing_cummulative[df_existing_cummulative['Capacity'] != 0]
    # 对于相同(grouping, StartYear)的记录取最大endyr，避免重复ID
    df_existing_cummulative['endyr'] = df_existing_cummulative.groupby(
        ['Resource_Group', 'zREM Technology', 'Fuel', 'zREM County', 'zREM State', 'Area', 'Resource Group', 'StartYear']
    )['endyr'].transform('max')

    # when aggregating, create Storage Max Annual_TS for battery types
    df_existing_cummulative['StorageMaxCap'] = pd.np.where(df_existing_cummulative['Resource Group'].isin(ba_list), df_existing_cummulative[
        'Capacity'] * 4, np.nan)
    df_existing_cummulative['StorageMaxCap'] = pd.np.where(df_existing_cummulative['Resource Group'] == 'Hydro_PS' , df_existing_cummulative[
        'Capacity'] * 24, df_existing_cummulative['StorageMaxCap'])
    df_storagemax_TS = pd.pivot_table(df_existing_cummulative,
                   index=['Resource_Group', 'zREM Technology', 'Fuel', 'zREM County', 'zREM State', 'Area', 'Resource Group', 'StartYear', 'endyr'],
                   columns='Yr', values='StorageMaxCap', fill_value=0).reset_index()
    if len(df_storagemax_TS) > 0 :
        df_storagemax_TS['StartYear'] = df_storagemax_TS['StartYear'].astype('int')
        df_storagemax_TS['StartYear'] = pd.np.where(df_storagemax_TS['StartYear'] < 2000, 2000, df_storagemax_TS['StartYear'])
        df_storagemax_TS = df_storagemax_TS[df_storagemax_TS['StartYear'] <= yr_end]
        df_storagemax_TS['Name'] = name_prefix + '_' + df_storagemax_TS['zREM County'] + '_' + df_storagemax_TS['Fuel'] \
                                          + '_' + df_storagemax_TS['Resource_Group'] + '_' + df_storagemax_TS['StartYear'].astype(str)
        df_storagemax_TS['ID'] = 'StorageMax_' + df_storagemax_TS['Name']
        df_storagemax_TS['Use'] = 'AggregatedStorageMax'
        df_storagemax_TS['zREM Type'] = 'StorageMaxCapacity'
        df_storagemax_TS = df_storagemax_TS.drop(['Name', 'Resource_Group', 'zREM Technology', 'Fuel', 'zREM County', 'zREM State', 'Area',
                                                  'Resource Group', 'StartYear', 'endyr'], 1)


    df_existing_cummulative = pd.pivot_table(df_existing_cummulative,
                   index=['Resource_Group', 'zREM Technology', 'Fuel', 'zREM County', 'zREM State', 'Area', 'Resource Group', 'StartYear', 'endyr'],
                   columns='Yr', values='Capacity', fill_value=0).reset_index()
    # x = df_existing_cummulative[df_existing_cummulative['zREM County'] == 'Sichuan']

    df_existing_cummulative['StartYear'] = df_existing_cummulative['StartYear'].astype('int')
    df_existing_cummulative['StartYear'] = pd.np.where(df_existing_cummulative['StartYear'] < 2000, 2000, df_existing_cummulative['StartYear'])
    df_existing_cummulative = df_existing_cummulative[df_existing_cummulative['StartYear'] <= yr_end]
    df_existing_cummulative['Name'] = name_prefix + '_' + df_existing_cummulative['zREM County'] + '_' + df_existing_cummulative['Fuel'] \
                                      + '_' + df_existing_cummulative['Resource_Group'] + '_' + df_existing_cummulative['StartYear'].astype(str)
    df_existing_cummulative['Day'] = 1
    df_existing_cummulative['Month'] = 1
    df_existing_cummulative['Year'] = df_existing_cummulative['StartYear']
    df_existing_cummulative['ResourceBeginDate'] = pd.to_datetime(df_existing_cummulative[['Day', 'Month', 'Year']])
    df_existing_cummulative['Day'] = 31
    df_existing_cummulative['Month'] = 12
    df_existing_cummulative['Year'] = df_existing_cummulative['endyr']
    df_existing_cummulative['ResourceEndDate'] = pd.to_datetime(df_existing_cummulative[['Day', 'Month', 'Year']])
    df_existing_cummulative = df_existing_cummulative.drop(['Day', 'Month', 'Year', 'endyr'], 1)
    df_existing_cummulative['Capacity'] = 'yr_Capacity_' + df_existing_cummulative['Name']
    df_plant_agg = df_existing_cummulative[['Name', 'Resource_Group', 'zREM Technology', 'Fuel', 'ResourceBeginDate', 'ResourceEndDate', 'Capacity',
       'zREM County', 'zREM State', 'Area', 'Resource Group', 'StartYear']].reset_index(drop=True)
    df_plant_agg['Second Fuel'] = np.nan
    df_plant_agg['Heat Rate'] = np.nan
    df_existing_cummulative['ID'] = 'Capacity_' + df_existing_cummulative['Name']
    df_existing_cummulative['Use'] = 'AggregatedCapacity'
    df_existing_cummulative['zREM Type'] = 'PlantCapacity'
    df_existing_cummulative = df_existing_cummulative.drop(['Name', 'Resource_Group', 'zREM Technology', 'Fuel', 'ResourceBeginDate',
                                                            'ResourceEndDate', 'Capacity', 'zREM County', 'zREM State', 'Area',
                                                            'Resource Group', 'StartYear'], 1)

    return df_plant_agg, df_existing_cummulative, df_storagemax_TS

# Functions to Write to SQL Server
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

def reload_tbl(engine, dest_tbl, dest_df):
    """

    Function that reloads Aurora Input tables with dataframes

        - Truncates original dest_tbl
        - Appends dest_df to dest_tbl (not null columns MUST be present, all other missing columns from dest_df will
        by appended as NULL

    Args:
        dest_tbl (string): Aurora Input Database table name eg. tbl_AID_xxxxx
        dest_df (dataframe): df that looks like tbl_AID_xxxx

    Returns: nothing
    """

    clear_sql = """TRUNCATE TABLE """ + dest_tbl
    execute_sqlcur(engine=engine, sql=clear_sql)
    upload_sql(engine=engine, df=dest_df, dest_sql=dest_tbl, existMethod='append')

    return

def update_aid_id(dest_tbl, dest_df):
    """

    Function that REPLACES (by ID) Aurora tables with dataframes

        - Only applicable to Time Series Annual, Monthly & Weekly & any AID tables which uses ID as unique column
        - NOT applicable to Time Series Daily, Hourly & Generic
        - Deletes original records of dest_tbl by ID
        - Appends dest_df to dest_tbl (not null columns MUST be present, all other missing columns from dest_df will
        by appended as NULL

    Args:
        dest_tbl (string): Aurora Input Database table name eg. tbl_AID_xxxxx
        dest_df (dataframe): df that looks like tbl_AID_xxxx

    Returns: nothing
    """

    str_id = """('"""
    for index, row in dest_df.iterrows():
        print(row.ID)
        str_id += row.ID + """','"""
    str_id = str_id[:-2] + ')'

    del_sql = """DELETE FROM """ + dest_tbl + """ WHERE [ID] IN """ + str_id
    execute_sqlcur(engine=engine_dest, sql=del_sql)
    upload_sql(engine=engine_dest, df=dest_df, dest_sql=dest_tbl, existMethod='append')

    return

def update_sqltbl_by(engine, dest_tbl, dest_df, by_col):
    """

    Function that REPLACES (by Country) data in SQL Server tables with dataframes

        - Deletes original records of dest_tbl by Country
        - Appends dest_df to dest_tbl (not null columns MUST be present, all other missing columns from dest_df will
        by appended as NULL

    Args:
        dest_tbl (string): Database table name
        dest_df (dataframe): flat format df that looks like database table

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
    upload_sql(engine=engine, df=dest_df, dest_sql=dest_tbl, existMethod='append')

    return


# Functions to Read Excel Data & Transform to Flat format for upload to SQL db
def get_excel_transmission(xls_name):
    """

    Function that reads Excel Power Transmission data and turns it into a flat format.

    Handles 3 types of data
    - Capacity MAX: Maximum transmission in MW
    - Capacity MIN: Minimum transmission in MW
    - Wheeling:     Transmission Cost in $/MWh

    Args:
        xls_name (string): name of excel file that IS OPENED

    Returns:
        df (dataframe): transmission data from excel in flat format
    """

    # read data from excel to dataframe
    xw.Book(xls_name).set_mock_caller()
    wb = xw.Book.caller()
    sht = wb.sheets['Links']
    df = pd.DataFrame(sht.range('tbl_Tx').offset(-1).expand().value)
    x = df.iloc[0, :]
    df.columns = x
    df = df.iloc[1:, :].reset_index(drop=True)

    # subset into different dataframes for different rules on fillna
    capacity = df[df['AURORA_Column'] == 'Capacity'].reset_index(drop=True)
    cap_max = capacity[capacity['Type'] == 'Max'].reset_index(drop=True)
    cap_min = capacity[capacity['Type'] == 'Min'].reset_index(drop=True)
    wheeling = df[df['AURORA_Column'] == 'Wheeling'].reset_index(drop=True)

    # fillna for each subset
    cap_max = cap_max.fillna(method='ffill', axis=1)
    cap_min = cap_min.fillna(0)
    wheeling = wheeling.fillna(method='ffill', axis=1)

    # concat back to single df after fillna
    df = pd.concat([cap_max, cap_min, wheeling], ignore_index=True)

    # flatten df to DB format
    df = pd.melt(df, id_vars=['Country', 'Area_From', 'Area_To', 'AURORA_Column', 'Unit', 'Type'], var_name='Year',
                 value_name='Qty')
    df.rename(columns={'AURORA_Column': 'Metric'}, inplace=True)
    df['User'] = user
    df['TimeStamp'] = timestamp
    df['TimeStamp'] = df['TimeStamp'].dt.round('1s')
    df['Year'] = df['Year'].astype(int)

    return df

def new_get_excel_transmission(path, sheetname_line,sheetname_price):
    # Ying to paste her code here

    transmission_data = pd.read_excel(path, sheet_name=sheetname_line)
    # max dataset
    date_range = pd.DataFrame(range(2000, yr_end+1))
    date_range.columns = ['Year']
    df = pd.DataFrame()
    for row in transmission_data.iterrows():
        data_row = row[1].to_frame().T
        df_row = data_row.merge(date_range, how='outer', left_on='Model Start Year', right_on='Year')
        df_row = df_row.fillna(method='ffill')
        df_row['Value'] = pd.np.where((df_row['Year'] == df_row['Model Start Year']) & (df_row['Special label for profiling'] == 'F'),df_row['Adjusted Cap'] * 0.7,
                        pd.np.where((df_row['Year'] == df_row['Model Start Year'] + 1) & (df_row['Special label for profiling'] == 'F'),df_row['Adjusted Cap'] * 0.9,
                        pd.np.where((df_row['Year'] == df_row['Model Start Year']) & (df_row['Special label for profiling'] == 'L'),df_row['Adjusted Cap'] * 0.3,
                        pd.np.where((df_row['Year'] == df_row['Model Start Year'] + 1) & (df_row['Special label for profiling'] == 'L'), df_row['Adjusted Cap'] * 0.6,
                        pd.np.where((df_row['Year'] == df_row['Model Start Year'] + 2) & (df_row['Special label for profiling'] == 'L'),df_row['Adjusted Cap'] * 0.8,
                        pd.np.where((df_row['Year'] < df_row['Model Start Year']),                                                        0,
                        pd.np.where( df_row['Special label for profiling'] == 'NA',    df_row['Adjusted Cap']*1,df_row['Adjusted Cap'])))))))

        df = df.append(df_row)
    df['Type'] = 'Max'
    df['Unit'] = 'MW'
    df['Metric'] = 'Capacity'

    # min dataset
    df_min = df.copy()
    df_min['Type'] = 'Min'
    df_min['Value'] = df_min['Value'] * 0.5

    # price data
    price_data = pd.read_excel(path, sheet_name=sheetname_price, usecols="BU:DX")
    price_data = price_data[7:]
    price_data.iloc[0, 5:] = price_data.iloc[1, 5:]
    price_data.columns = price_data.iloc[0]
    price_data = price_data[3:].reset_index(drop=True)
    price_data['Interconnection'] = price_data['Source province'] + '-' + price_data['Destination province']
    price_data = pd.melt(price_data, id_vars=['Power link name', 'Source province', 'Source regional grid', 'Destination province',
                                  'Destination regional grid', 'Interconnection'], var_name='Year', value_name='Value')
    price_data['Year'] = price_data['Year'].astype(int)
    price_line = df.merge(price_data, how='left', on=['Interconnection', 'Year'])
    price_line['Type'] = 'Flat'
    price_line['Unit'] = 'US$/MWh'
    price_line['Metric'] = 'Wheeling'
    wheeling = price_line[['ID', 'Transmission Line Name', 'Chinese Name', 'Status',
                           'Number of Loops', 'Interconnection', 'From Province', 'From Region',
                           'To Province', 'To Region', 'Primary Fuel',
                           'Special label for profiling', 'Length (km)', 'DC/AC', 'Voltage Grade',
                           'Capacity (MW)', 'Deduction coefficient', 'Adjusted Cap',
                           'Construction commencement', 'COD', 'COD year', 'Model Start Year',
                           'Capex (Mn RMB)', 'Commentaries', 'Year', 'Value_y', 'Type', 'Unit', 'Metric']]
    wheeling.rename(columns={'Value_y': 'Value'}, inplace=True)

    # original table prepared to upload to DB
    price_tariff_orig = price_data.copy()
    price_tariff_orig['TimeStamp'] = timestamp
    transmission_line_orig = transmission_data.copy()
    transmission_line_orig['TimeStamp'] = timestamp

    # concat back to single df after fillna
    df_all = pd.concat([df, df_min, wheeling], ignore_index=True)
    df_all['Year'] = df_all['Year'].astype(int)
    df_all['Country'] = country

    # re-order
    df_all = df_all[['ID', 'Country', 'Transmission Line Name', 'Chinese Name', 'Status',
                     'Number of Loops', 'Interconnection', 'From Province', 'From Region',
                     'To Province', 'To Region', 'Primary Fuel',
                     'Special label for profiling', 'Length (km)', 'DC/AC', 'Voltage Grade',
                     'Capacity (MW)', 'Deduction coefficient', 'Adjusted Cap',
                     'Construction commencement', 'COD', 'COD year', 'Model Start Year',
                     'Capex (Mn RMB)', 'Year', 'Metric', 'Value', 'Type', 'Unit', 'Commentaries', ]]
    df_all['User'] = user
    df_all['TimeStamp'] = timestamp
    return df_all, price_tariff_orig, transmission_line_orig

def get_excel_rawwindsolar8760shape(xls_name, shape_name, level, level_name, spec):
    """

    Function that reads Excel Wind & Solar Shape data (8760 data points).

    - Wind data is on sheet named "Wind"
    - Solar data is on sheets named "Solar"
    - data starts on cell "B4"

    Args:
        xls_name (string): name of excel file that IS OPENED
        type_name (string): name of the shape. (Wind, Solar)
        level (string): level of granularity (Country, Market, Zone, Plant)
        level_name (string): name of the country/model market/model zone/plant

    Returns:
        df (dataframe): raw(8760) wind and solar shape data from excel in flat format
    """

    xw.Book(xls_name).set_mock_caller()
    wb = xw.Book.caller()
    sht = wb.sheets[shape_name]
    df = pd.DataFrame(sht.range('B4').expand().value)
    x = df.iloc[0, :]
    df.columns = x
    df = df.iloc[1:, :].reset_index(drop=True)

    df['local_time'] = pd.to_datetime(df['local_time'])
    df['electricity'] = pd.to_numeric(df['electricity'])
    df.columns = ['Date', 'Shape']
    df['HourOfDay'] = df['Date'].dt.hour
    df['Year'] = df['Date'].dt.year
    df['Month'] = df['Date'].dt.month
    df['DayOfMonth'] = df['Date'].dt.day
    df['DayOfWeek'] = df['Date'].dt.dayofweek + 1
    df['DayName'] = df['Date'].dt.day_name()
    df['HourOfYear'] = df.index + 1
    df['ShapeType'] = shape_name
    df['ShapeName'] = spec
    df['Level'] = level
    df['LevelName'] = level_name
    df['User'] = user
    df['TimeStamp'] = timestamp

    return df

def get_excel_hydro12mthlyshape(xls_name):
    """

    Function that reads Excel Monthly shapes and flattens it

    - data is on sheet named "Monthly"
    - data starts on cell "A1"

    Args:
        xls_name (string): name of excel file that IS OPENED

    Returns:
        df (dataframe): hydro 12 monthly shape data from excel in flat format
        df_vec (dataframe): AID format Hydro Vectors
    """

    xw.Book(xls_name).set_mock_caller()
    wb = xw.Book.caller()

    sht = wb.sheets['Monthly']
    df = pd.DataFrame(sht.range('A1').expand().value)
    x = df.iloc[0, :]
    df.columns = x
    df = df.iloc[1:, :].reset_index(drop=True)
    df = pd.melt(df, id_vars=['LevelName', 'Level', 'ShapeName', 'Year'], var_name='Month', value_name='Shape')
    df['Year'] = df['Year'].astype(int)
    df['Month'] = df['Month'].astype(int)
    df['Day'] = 1
    df['Date'] = pd.to_datetime(df[['Day', 'Month', 'Year']])
    df = df.drop(['Day'], axis=1)
    df['User'] = user
    df['TimeStamp'] = timestamp

    sht = wb.sheets['Vector']
    df_vec = pd.DataFrame(sht.range('A1').expand().value)
    x = df_vec.iloc[0, :]
    df_vec.columns = x
    df_vec = df_vec.iloc[1:, :].reset_index(drop=True)

    return df, df_vec

def get_excel_plantasmpt(xls_name, shtname, units):
    """

    Function that reads Excel Plant Attribute Assumptions and flattens it

    - Sheets are named after their attributes
    - data starts in cell "A2"
    - ffill to the right

    Args:
        xls_name (string): name of excel file that IS OPENED

    Returns:
        df (dataframe): plant attribute data from excel in flat format
    """

    xw.Book(xls_name).set_mock_caller()
    wb = xw.Book.caller()

    sht = wb.sheets[shtname]
    df = pd.DataFrame(sht.range('A2').expand().value)
    x = df.iloc[0, :]
    df.columns = x
    df = df.iloc[1:, :].reset_index(drop=True)
    df = df.fillna(method='ffill', axis=1)
    if shtname == 'EmissionRate' or shtname == 'EmissionPrice':
        df = pd.melt(df, id_vars=['LevelName', 'Level', 'PlantType', 'PlantTech', 'Pollutant'], var_name='PlantOnlineYear', value_name='Value')
        df['PlantAttribute'] = shtname + '_' + df['Pollutant']
        df = df.drop(['Pollutant'], axis=1)
    else:
        df = pd.melt(df, id_vars=['LevelName', 'Level', 'PlantType', 'PlantTech'], var_name='PlantOnlineYear', value_name='Value')
        df['PlantAttribute'] = shtname
    df['PlantOnlineYear'] = df['PlantOnlineYear'].astype(int).astype(str)
    df['Value'] = pd.to_numeric(df['Value'])
    df['Units'] = units
    df['User'] = user
    df['TimeStamp'] = timestamp


    return df

def get_excel_hourlyshape(xls_name, shtname):
    """

    Function that reads Excel Plant Attribute Assumptions and flattens it

    - Sheets are named after their attributes
    - data starts in cell "A2"
    - ffill to the right

    Args:
        xls_name (string): name of excel file that IS OPENED

    Returns:
        df (dataframe): plant attribute data from excel in flat format
    """

    xw.Book(xls_name).set_mock_caller()
    wb = xw.Book.caller()

    sht = wb.sheets[shtname]
    df = pd.DataFrame(sht.range('A2').expand().value)
    x = df.iloc[0, :]
    df.columns = x
    df = df.iloc[1:, :].reset_index(drop=True)
    df = df.fillna(method='ffill', axis=1)
    df = pd.melt(df, id_vars=['LevelName', 'Level', 'ShapeName', 'DayOfWeek', 'HourOfDay'], var_name='Year', value_name='Shape')
    df['Year'] = df['Year'].astype(int)
    df['Shape'] = pd.to_numeric(df['Shape'])
    df['User'] = user
    df['TimeStamp'] = timestamp


    return df

def get_excel_fuel(xls_name):
    """

    Function that reads Excel Fuel Prices and Fuel Limits and flattens them

    - sheet is named "Fuel"
    - data starts in cell "A1"
    - ffill to the right

    Args:
        xls_name (string): name of excel file that IS OPENED

    Returns:
        df (dataframe): fuel annual data from excel in flat format
    """

    xw.Book(xls_name).set_mock_caller()
    wb = xw.Book.caller()

    sht = wb.sheets['Fuel']
    df = pd.DataFrame(sht.range('B1').expand().value)
    x = df.iloc[0, :]
    df.columns = x
    df = df.iloc[1:, :].reset_index(drop=True)
    df = df.fillna(method='ffill', axis=1)
    df = pd.melt(df, id_vars=['LevelName', 'Level', 'FuelName', 'Description', 'FuelType', 'Metric', 'Units'],
                 var_name='Year', value_name='Value')
    df['Year'] = df['Year'].astype(int)
    df['Value'] = pd.to_numeric(df['Value'])
    df['User'] = user
    df['TimeStamp'] = timestamp

    return df

def get_excel_newbuild(xls_name, balancing_plant=False):
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

    xw.Book(xls_name).set_mock_caller()
    wb = xw.Book.caller()

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
        sht = wb.sheets[zone]
        df = pd.DataFrame(sht.range('A151').expand().value)
        x = df.iloc[0, :]
        df.columns = x
        df = df.iloc[1:, :].reset_index(drop=True)
        df = df.drop(['Column2'], 1)
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
            # capacity here is cummulative
            df1 = pd.DataFrame(sht.range('A542').expand().value)
            y = df1.iloc[0, :]
            df1.columns = y
            df1 = df1.iloc[1:, :].reset_index(drop=True)
            df1 = df1.drop(['Column2'], 1)
            df1 = pd.melt(df1, id_vars=['Zone', 'Resource_Group', 'Technology', 'Group'],
                         var_name='StartYear', value_name='BalancingCapacity')
            df1['BalancingCapacity'] = df1['BalancingCapacity'].fillna(0)
            df1['StartYear'] = df1['StartYear'].astype(int)
            df1['FuelPri'] = df1['Resource_Group'].map(dict_ptypefuel)
            df1['FuelPri'].replace('Waste', 'Heat',inplace=True)  # make the fuel type of GNB STWTE type plant to Waste, but balancing STWTE type plant to Heat
            df1['PlantType'] = df1['Resource_Group']
            df1['Resource_Group'] = pd.np.where(df1['Technology'].notnull(),
                                               df1['Resource_Group'] + '-' + df1['Technology'],
                                               df1['Resource_Group'])
            # df1['PowerPlant'] = pd.np.where(df1['StartYear'] <= dt.datetime.now().year,
            #                                'Existing_' + df1['Zone'] + '_' + df1['Resource_Group'] + '_' + df1['StartYear'].astype(str),
            #                                'New_' + df1['Zone'] + '_' + df1['Resource_Group'] + '_' + df1['StartYear'].astype(str))
            df1['BalancingCapacity'] = pd.to_numeric(df1['BalancingCapacity'])
            df1['Market'] = df1['Zone'].map(dict_zone_mkt)
            df1['Country'] = df1['Market'].map(dict_mkt_country)

            df_bal = df_bal.append(df1, ignore_index=True)

    df_xls['Username'] = user
    df_xls['TimeStamp'] = timestamp
    if len(df_bal) > 0:
        df_bal['Username'] = user
        df_bal['TimeStamp'] = timestamp

    return df_xls, df_bal

def get_excel_distributed(xls_name):
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
    # read data from excel to dataframe
    xw.Book(xls_name).set_mock_caller()
    wb = xw.Book.caller()
    sht = wb.sheets['Solar(MWac)']
    df = pd.DataFrame(sht.range('A1').expand().value)
    x = df.iloc[0, :]
    df.columns = x
    df = df.iloc[1:, :].reset_index(drop=True)
    df = df.fillna(method='ffill', axis=1)

    # flatten df to DB format
    df = pd.melt(df, id_vars=['Country', 'Zone'], var_name='Year',
                 value_name='Capacity')
    df['User'] = user
    df['TimeStamp'] = timestamp
    df['TimeStamp'] = df['TimeStamp'].dt.round('1s')
    df['Year'] = df['Year'].astype(int)

    return df



# Functions to read SQL Server data & Transform to AID template
def get_sql_plantasmpt(src_sql):
    """

    Function that reads APAC plant attribute assumptions from SQL Server

    Args:
        src_sql (string): SQL View name from Source SQL Server that gives the APAC plant attribute assumptions

    Returns:
         df (dataframe): df that gives plant attribute assumptions to be used if no actual data is available
    """

    sql_qry = """SELECT * FROM """ + src_sql
    df = pd.read_sql_query(sql_qry, engine_src)
    df = df.drop(['Units'], 1)
    df['PlantType'] = pd.np.where(df['PlantTech'] == df['PlantType'],
                                  df['PlantType'],
                                  df['PlantType'] + '-' + df['PlantTech'])
    df['LevelName'] = df['LevelName'].str.strip()
    df['Level'] = df['Level'].str.strip()
    df['PlantType'] = df['PlantType'].str.strip()
    df['PlantTech'] = df['PlantTech'].str.strip()
    df['PlantAttribute'] = df['PlantAttribute'].str.strip()
    df = df[df['PlantOnlineYear'] <= yr_end]

    return df

def get_sql_inflation_to_aid(src_sql, country):
    """

    Function that reads APAC inflation (Consumer Price Index YoY%) from SQL Server.

    - 2% inflation is assumed for Years where there are no forecast from WM Econ team.
    - model years are assumed to be from 2000 to 2060

    Args:
        src_sql (string): SQL View name from Source SQL Server that gives the inflation
        country (string): country name

    Returns:
         df (dataframe): df that gives inflation for the country
    """

    # get latest YoY inflation numbers from WM Econ team
    sql_qry = """SELECT * FROM """ + src_sql + """ WHERE [Country] = '""" + country + """'"""
    df_econ = pd.read_sql_query(sql_qry, engine_src)
    df_econ = df_econ[['Country', 'PROFILE_DESC', 'Year', 'Inflation']].reset_index(drop=True)

    # generate all combinations of Country by modelling years
    yr = np.array(list(range(2000, yr_end+1)))
    area = np.array(country)
    x, y = np.meshgrid(yr, area)
    x = x.flatten()
    y = y.flatten()
    df = pd.DataFrame({'Country': y, 'Year': x})

    # left join to give inflation for all required modelling years
    df = pd.merge(df, df_econ, how='left', on=['Country', 'Year'])

    # when no inflation data from econ team, ASSUME it to be 2%
    df[['PROFILE_DESC']] = df[['PROFILE_DESC']].fillna(method='ffill', axis=0)
    df['Inflation'] = df['Inflation'].fillna(2)

    # pivot to get years as columns
    df = pd.pivot_table(df, index=['Country', 'PROFILE_DESC'], columns='Year', values='Inflation',
                        fill_value=0).reset_index()
    df.rename(columns={'Country': 'Use',
                       'PROFILE_DESC': 'zREM Type'}, inplace=True)
    df['ID'] = 'inflation'

    return df

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
    df = pd.read_sql_query(sql_qry, engine_src)
    if country == 'China':
        sql_qry2 = """SELECT * FROM """ + src_sql + """ WHERE [Zone Name] IN ('East Russia', 'North Myanmar', 'Laos')"""
        df2 = pd.read_sql_query(sql_qry2, engine_src)
        df = df.append(df2, ignore_index=True)

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

def get_sql_demand_to_aid(src_sql, country):
    """

    Function that reads forecasted LDC from SQL Server and Transforms it to 3 Aurora Demand Input tables.

    Args:
        src_sql (string): SQL View name from Source SQL Server that gives the Forecasted LDC
        country (string): country name
    Returns:
        df_ai1 (dataframe): Demand_Hourly_Shape - Normalised LDC against Max (8760 +  1st 192 hours at bottom) to give
        8952 shape
        df_ai2 (dataframe): Demand_Monthly_Peak - 12 rows per Yr: Monthly Peak MW(Max per month)  & Monthly Average MW (
        Month Total MWh / Total Month Hour)
        df_ai3 (dataframe): Demand_Monthly - 14 rows per Yr :   row 13 = Average of 12 Monthly Average MW
                                                                row 1 to 12 = Monthly Average MW/row 13
                                                                row 14 = Annual Peak Demand (MW)
    """

    # <editor-fold desc="Read LDC for all Zones of selected Country">
    sql_qry = """SELECT * FROM """ + src_sql + """ WHERE [Country] = '""" + country + """'"""
    df = pd.read_sql_query(sql_qry, engine_src)
    # </editor-fold>

    # <editor-fold desc="Transforming LDC to Demand_Hourly_Shapes">
    yr_now = datetime.now().year
    leap_yr = check_leap_yr(yr_now)
    if leap_yr == True:
        df_ai1 = df[df['Year'] == yr_now - 1].reset_index(drop=True)
    else:
        df_ai1 = df[df['Year'] == yr_now].reset_index(drop=True)
    df_ai1 = df_ai1[['Area', 'Hr_Yr', 'Normalised']]
    df_ai1['Area'] = df_ai1['Area'].map(dict_zone)
    df_ai1 = pd.pivot_table(df_ai1, index=['Hr_Yr'], columns='Area', values='Normalised', fill_value=0).reset_index()
    df_ai1 = df_ai1.sort_values(by=['Hr_Yr']).reset_index(drop=True)
    df_ai1 = df_ai1.append(df_ai1.iloc[:192, :], ignore_index=True)
    df_ai1['Hr_Yr'] = df_ai1.index + 1
    df_ai1.rename(columns={'Hr_Yr': 'Demand Hour'}, inplace=True)
    # </editor-fold>

    # <editor-fold desc="Transforming LDC to Demand_Monthly_Peak">
    df_mth_mean = df.groupby(['Area', 'Year', 'Mth'], as_index=False)['Demand_MW'].mean()
    df_mth_mean.rename(columns={'Demand_MW':'Month Average'}, inplace=True)
    df_mth_peak = df.groupby(['Area', 'Year', 'Mth'], as_index=False)['Demand_MW'].max()
    df_mth_peak.rename(columns={'Demand_MW':'Month Peak'}, inplace=True)
    df_mth = pd.merge(df_mth_peak, df_mth_mean, how='inner')
    df_mth[13] = df_mth.groupby(['Area', 'Year'], as_index=False)['Month Average'].transform('mean')
    df_mth['MthShape'] = df_mth['Month Average']/df_mth[13]
    df_ai2 = df_mth.loc[:, ['Area', 'Year', 'Mth', 'Month Average', 'Month Peak']].reset_index(drop=True)
    df_ai2['Area'] = df_ai2['Area'].map(dict_zone)
    df_ai2.rename(columns={'Area': 'ID', 'Mth': 'Month'}, inplace=True)
    # </editor-fold>

    # <editor-fold desc="Transforming LDC to Demand_Monthly">
    df_1 = df_mth.groupby(['Area', 'Year'], as_index=False)['Month Average'].agg('mean')
    df_1.rename(columns={'Month Average': 13}, inplace=True)
    df_2 = df.groupby(['Area', 'Year'], as_index=False)['Demand_MW'].agg('max')
    df_2.rename(columns={'Demand_MW': 14}, inplace=True)
    df_ai3 = df_mth.loc[:, ['Area', 'Year', 'Mth', 'MthShape']].reset_index(drop=True)
    df_ai3.rename(columns={'MthShape':'Qty'}, inplace=True)
    df_ai3_1 = pd.merge(df_1, df_2, how='inner')
    df_ai3_1 = pd.melt(df_ai3_1, id_vars=['Area', 'Year'], var_name='Mth', value_name='Qty')

    df_ai3 = df_ai3.append(df_ai3_1, ignore_index=True)
    df_ai3 = df_ai3.sort_values(by = ['Area', 'Year', 'Mth']).reset_index(drop=True)
    df_ai3['Area'] = df_ai3['Area'].map(dict_zone)
    df_ai3.rename(columns={'Year': 'Demand Year', 'Mth': 'Demand Month'}, inplace=True)
    df_ai3 = pd.pivot_table(df_ai3, index=['Demand Year', 'Demand Month'],
                                    columns='Area',
                                    values='Qty',
                                    fill_value=0).reset_index()
    # </editor-fold>

    return df_ai1, df_ai2, df_ai3

def get_sql_transmission_links_to_aid(src_sql, country):
    """

    Function that reads Power Transmission data from SQL Server and transforms it into 2 Aurora input tables

    Args:
        src_sql (string): SQL View name from Source SQL Server that gives the Power Transmission Data
        country (string): country name

    Returns:
        df_ai1 (dataframe): Transmission_Links
        df_ai2 (dataframe): Time_Series_Annual data for Transmission Capacity & Prices
    """

    sql_qry = """SELECT * FROM """ + src_sql + """ WHERE [Country] = '""" + country + """'"""
    df = pd.read_sql_query(sql_qry, engine_src)
    df.rename(columns={'Area_From': 'Area From Descrip',
                       'Area_To'  : 'Area To Descrip'}, inplace=True)
    df['Area From'] = df['Area From Descrip'].map(dict_zone)
    df['Area To'] = df['Area To Descrip'].map(dict_zone)
    df = df.drop(['Country', 'Type'], axis=1)
    df = df.dropna()
    df['Area From'] = df['Area From'].astype(int)
    df['Area To'] = df['Area To'].astype(int)

    df['ID'] = df['Area From'].astype(str) + '_' + df['Area To'].astype(str)
    df['Capacity'] = 'txn_' + df['ID']
    df['Wheeling'] = 'wheeling_' + df['ID']
    df = df[df['Year'] >= 2000].reset_index(drop=True)

    df_ai1 = df[['ID', 'Area From', 'Area To', 'Area From Descrip', 'Area To Descrip', 'Capacity', 'Wheeling']]
    df_ai1 = df_ai1.drop_duplicates().reset_index(drop=True)
    df_ai1['Capacity'] = 'yr_' + df_ai1['Capacity']
    df_ai1['Wheeling'] = 'yr_' + df_ai1['Wheeling']

    ai2_cap = df[df['Metric'] == 'Capacity'].reset_index(drop=True)
    ai2_cap = ai2_cap[['Capacity', 'Year', 'Qty']].reset_index(drop=True)
    ai2_cap.rename(columns={'Capacity': 'ID'}, inplace=True)
    ai2_cap['Use'] = 'Capacity'
    ai2_cap = ai2_cap.groupby(['ID', 'Year','Use'], as_index=False)['Qty'].agg('sum')

    ai2_wheel = df[df['Metric'] == 'Wheeling'].reset_index(drop=True)
    ai2_wheel = ai2_wheel[['Wheeling', 'Year', 'Qty']].reset_index(drop=True)
    ai2_wheel.rename(columns={'Wheeling': 'ID'}, inplace=True)
    ai2_wheel['Use'] = 'Wheeling'
    ai2_wheel = ai2_wheel.groupby(['ID', 'Year', 'Use'], as_index=False)['Qty'].agg('mean')

    df_ai2 = pd.concat([ai2_cap, ai2_wheel], ignore_index=True)
    df_ai2 = df_ai2.sort_values(by=['ID', 'Year']).reset_index(drop=True)
    df_ai2['zREM Type'] = 'Transmission'
    df_ai2 = pd.pivot_table(df_ai2, index=['ID', 'Use', 'zREM Type'],
                            columns='Year',
                            values='Qty',
                            fill_value=0).reset_index()
    return df_ai1, df_ai2

def new_get_sql_transmission_links_to_aid(src_sql, country):
    # Ying to paste code here
    sql_qry = """SELECT * FROM """ + src_sql + """ WHERE [Country] = '""" + country + """'"""
    df = pd.read_sql_query(sql_qry, engine_src)
    df.replace(to_replace='Macau', value='Guangdong', inplace=True)  # classify Macau into Guangdong
    df.rename(columns={'Area_From': 'Area From Descrip', 'Area_To': 'Area To Descrip'}, inplace=True)
    df['Area From'] = df['Area From Descrip'].map(dict_zone)
    df['Area To'] = df['Area To Descrip'].map(dict_zone)
    df = df.drop(['Country', 'Type'], axis=1)
    df['ID'] = df['Area From'].astype(int).astype(str) + '_' + df['Area To'].astype(int).astype(str)
    df['Capacity'] = 'txn_' + df['ID']
    df['Wheeling'] = 'wheeling_' + df['ID']
    df_ai1 = df[['ID', 'Area From', 'Area To', 'Area From Descrip', 'Area To Descrip', 'Capacity', 'Wheeling']]
    df_ai1 = df_ai1.drop_duplicates().reset_index(drop=True)
    df_ai1['Capacity'] = 'yr_' + df_ai1['Capacity']
    df_ai1['Wheeling'] = 'yr_' + df_ai1['Wheeling']
    ai2_cap = df[df['Metric'] == 'Capacity'].reset_index(drop=True)
    ai2_cap = ai2_cap[['Capacity', 'Year', 'Qty']].reset_index(drop=True)
    ai2_cap.rename(columns={'Capacity': 'ID'}, inplace=True)
    ai2_cap['Use'] = 'Capacity'
    ai2_wheel = df[df['Metric'] == 'Wheeling'].reset_index(drop=True)
    ai2_wheel = ai2_wheel[['Wheeling', 'Year', 'Qty']].reset_index(drop=True)
    ai2_wheel.rename(columns={'Wheeling': 'ID'}, inplace=True)
    ai2_wheel['Use'] = 'Wheeling'
    df_ai2 = pd.concat([ai2_cap, ai2_wheel], ignore_index=True)
    df_ai2 = df_ai2.sort_values(by=['ID', 'Year']).reset_index(drop=True)
    df_ai2['zREM Type'] = 'Transmission'
    df_ai2 = pd.pivot_table(df_ai2, index=['ID', 'Use', 'zREM Type'], columns='Year', values='Qty',fill_value=0).reset_index()
    df_ai1[['Link Monthly Shape', 'Link Capacity Shape', 'Losses', 'Wheeling Free Capacity', 'Constraint ID', 'Constraint Emission Rate', 'Report', 'zREM Topology']] = pd.DataFrame([['', '', '', '', '', '', '', '']],index=df_ai1.index)
    df_ai1['Primary Key'] = range(1, len(df_ai1) + 1)
    return df_ai1, df_ai2

def get_sql_distributed_capacity_to_aid(src_sql, country):
    """

    Function that reads Power Transmission data from SQL Server and transforms it into 2 Aurora input tables

    Args:
        src_sql (string): SQL View name from Source SQL Server that gives the Power Transmission Data
        country (string): country name

    Returns:
        df_ai1 (dataframe): Transmission_Links
        df_ai2 (dataframe): Time_Series_Annual data for Transmission Capacity & Prices
    """

    sql_qry = """SELECT * FROM """ + src_sql + """ WHERE [Country] = '""" + country + """'"""
    df = pd.read_sql_query(sql_qry, engine_src)
    df['ID'] = 'Capacity_Distributed_PV_' + df['Zone']
    dist_cap = df[['ID', 'Year', 'Capacity']].reset_index(drop=True)
    dist_cap['Use'] = 'Capacity'
    dist_cap = dist_cap.sort_values(by=['ID', 'Year']).reset_index(drop=True)
    dist_cap['zREM Type'] = 'Distributed PV Capacity'
    dist_cap['Capacity'] = pd.to_numeric(dist_cap['Capacity'],errors='coerce')
    dist_cap = pd.pivot_table(dist_cap, index=['ID', 'Use', 'zREM Type'],
                            columns='Year',
                            values='Capacity',
                            fill_value=0).reset_index()

    df_dist = df[['Country', 'Zone']].reset_index(drop=True)
    df_dist = df_dist.drop_duplicates(subset='Zone')
    df_dist['PowerPlant'] = df_dist['Zone'] + '_Distributed_PV'
    df_dist['Market'] = df_dist['Country']
    df_dist['PlantType'] = 'PV'
    df_dist['Group'] = 'Solar'
    df_dist['Resource_Group'] = 'PV'
    df_dist['FuelPri'] = 'Sun_Dist'
    df_dist['StartYear'] = 2000

    return dist_cap, df_dist

def get_sql_hydroshapes_to_aid(src_sql_vec, src_sql_mthly, country):
    """

    Function that reads APAC hydro vectors & monthly from SQL Server and Transforms it to 2 Aurora input tables.

    Args:
        src_sql_vec (string): SQL View name from Source SQL Server that gives the hydro vectors
        src_sql_mthly (string): SQL View name from Source SQL Server that gives the hydro monthly shapes
        country (string): country name

    Returns:
         df_vec (dataframe): df that gives AID_Hydro_Vectors
         df_hydro_mthly (dataframe): df that gives AID_Hydro_Monthly
    """

    # get Hydro Vectors from SQL db
    sql_qry_vec = """SELECT * FROM """ + src_sql_vec + """ WHERE [Country] = '""" + country + """'"""
    df_vec = pd.read_sql_query(sql_qry_vec, engine_src)

    # get Hydro Shape (12 month shape) from SQL db
    sql_qry_mth = """SELECT * FROM """ + src_sql_mthly + """ WHERE [Level] = 'ModelZone' AND [ShapeName] = 'Hydro'"""
    df_hydro = pd.read_sql_query(sql_qry_mth, engine_src)
    df_hydro['Country'] = df_hydro['Area'].map(dict_zone_mkt).map(dict_mkt_country)
    df_hydro.dropna(inplace=True)
    df_hydro = df_hydro.drop(['Country', 'Level', 'ShapeName'], 1)
    # pivot df to get 1 column per Month
    df_hydro = pd.pivot_table(df_hydro, index=['Area', 'Year'],
                              columns='Month',
                              values='Shape',
                              fill_value=0).reset_index()
    # left join to Hydro Vector df by Area to get "Maximum" to use as Month 13 in Hydro df
    df_hydro = pd.merge(df_hydro, df_vec, how='left', on='Area')
    df_hydro = df_hydro.drop(['Country', 'Minimum', 'Energy Shift Method', 'Sus Maximum', 'Sus Number'], 1)
    df_hydro.rename(columns={'Maximum': '13'}, inplace=True)

    # generate all combinations of Hydro Area by modelling years
    yr = np.array(list(range(2000, 2061)))
    area = np.array(df_hydro['Area'].unique())
    x, y = np.meshgrid(yr, area)
    x = x.flatten()
    y = y.flatten()
    df_hydro_mthly = pd.DataFrame({'Area': y, 'Year': x})

    # left join Hydro df to All Combo df, then sort by Area & Year, then ffill downwards to duplicate
    df_hydro_mthly = pd.merge(df_hydro_mthly, df_hydro, how='left', on=['Area', 'Year'])
    df_hydro_mthly = df_hydro_mthly.sort_values(by=['Area', 'Year']).reset_index(drop=True)
    df_hydro_mthly = df_hydro_mthly.fillna(method='ffill', axis=0)
    df_hydro_mthly.rename(columns={'Area': 'Name', 'Year': 'Hydro Year'}, inplace=True)

    # add in other assumptions for hydro vector df to match tbl_AID_xxxxx
    df_vec['Index'] = df_vec.index + 1
    df_vec['Demand Source'] = 'Area'
    df_vec['Hydro Shape Sets'] = df_vec['Area']
    df_vec['Shape Areas'] = df_vec['Area'].map(dict_zone)
    df_vec.rename(columns={'Area': 'Name', 'Country': 'zREM Topology'}, inplace=True)

    return df_vec, df_hydro_mthly

def get_sql_windsolarshapes_to_aid_ts_wek(src_sql, country):
    """

    Functions that reads the 8760 hourly shape data from Source SQL Server and transforms it to Aurora Weekly &
    corresponding monthly time series format.

    Args:
        src_sql (string): SQL View name from Source SQL Server that gives the 8760 hourly shapes

    Returns:
        df (dataframe): pivoted dataframe with 168 columns for AID_Time_Series_Weekly
        df_monthly (dataframe): pivoted dataframe with 12 monthly columns for AID_Time_Series_Monthly that references
        the AID_Time_Series_Weekly
    """

    # generate all weekly time series 168 map
    df_168map = get168map()

    # renew_cf = Assumptions[(Assumptions['PlantType'].isin(['PV', 'PV_D', 'PV_BA', 'Wind_Onshore', 'Wind_Offshore'])) & (Assumptions[
    #     'PlantAttribute'] == 'CapacityFactor') & (Assumptions['PlantOnlineYear'] == 2020)]
    renew_cf = Assumptions[(Assumptions['PlantType'].isin(['PV', 'PV_D', 'PV_BA', 'Wind_Onshore', 'Wind_Offshore'])) & (Assumptions[
        'PlantAttribute'] == 'CapacityFactor')]
    renew_cf = renew_cf[['LevelName', 'Level', 'PlantType', 'PlantOnlineYear', 'Value']].reset_index(drop=True)
    renew_cf.rename(columns={'PlantOnlineYear': 'Year'}, inplace=True)

    # get Wind & Solar shapes from SQL db
    # Find Zonal Shapes
    sql_qry = """SELECT * FROM """ + src_sql + """ WHERE [Country] = '""" + country + """'"""
    df = pd.read_sql_query(sql_qry, engine_src)

    # Change Sun_Dist to PV_D, Sun to PV, then duplicate PV to PV_BA
    df_pvba = df[df['ShapeType'] == 'Sun'].copy()
    df_pvba['ShapeType'] = 'PV_BA'
    df = pd.concat([df, df_pvba], ignore_index=True)
    df['ShapeType'] = df['ShapeType'].str.replace('Sun_Dist', 'PV_D')
    df['ShapeType'] = df['ShapeType'].str.replace('Sun', 'PV')
    df = df.sort_values(by=['Country', 'ModelZone', 'ShapeType', 'ShapeName', 'Year', 'Month', 'DayOfWeek',
                            'HourOfDay']).reset_index(drop=True)
    df['AnnualAverageShape'] = df.groupby(['Country', 'ModelZone', 'ShapeType', 'ShapeName', 'Year'], as_index=False)[
        'AID_WeeklyShape'].transform('mean')
    df.rename(columns={'ShapeType': 'PlantType', 'AID_WeeklyShape': 2019}, inplace=True)

    df = df.drop(['Year'], 1)
    shape_yrs = [2019]
    yr_lst = [item for item in yr_model if item not in shape_yrs]
    for yr in yr_lst:
        df[yr] = df[2019]

    df = pd.melt(df, id_vars=['Country', 'ModelZone', 'PlantType', 'ShapeName', 'Month', 'DayOfWeek', 'HourOfDay', 'AnnualAverageShape'],
                 var_name='Year', value_name='AID_WeeklyShape')

    # scale shape up or down to match CF assumptions
    cf_all = renew_cf[renew_cf['Level'] == 'All'].copy()
    cf_all = cf_all.drop(['LevelName', 'Level'], 1)
    df = pd.merge(df, cf_all, how='left', on=['PlantType', 'Year'])
    df.rename(columns={'Value': 'Global_CF'}, inplace=True)

    cf_country = renew_cf[renew_cf['Level'] == 'Country'].copy()
    if len(cf_country) > 0:
        cf_country = cf_country.drop(['Level'], 1)
        cf_country.rename(columns={'LevelName': 'Country'}, inplace=True)
        df = pd.merge(df, cf_country, how='left', on=['Country', 'PlantType', 'Year'])
        df.rename(columns={'Value': 'Country_CF'}, inplace=True)

    cf_zone = renew_cf[renew_cf['Level'] == 'ModelZone'].copy()
    if len(cf_zone) > 0:
        cf_zone = cf_zone.drop(['Level'], 1)
        cf_zone.rename(columns={'LevelName': 'ModelZone'}, inplace=True)
        df = pd.merge(df, cf_zone, how='left', on=['ModelZone', 'PlantType', 'Year'])
        df.rename(columns={'Value': 'Zone_CF'}, inplace=True)
    df['Final_CF'] = np.nan
    df[['Global_CF', 'Country_CF', 'Zone_CF', 'Final_CF']] = df[['Global_CF', 'Country_CF', 'Zone_CF', 'Final_CF']].fillna(method='ffill', axis=1)
    df['FinalAID_Shape'] = (df['AID_WeeklyShape'] / df['AnnualAverageShape']) * df['Final_CF']
    # debug CF annual changes
    df = df.sort_values(by=['Country', 'PlantType', 'ShapeName', 'Year', 'Month', 'DayOfWeek',
                            'HourOfDay']).reset_index(drop=True)
    df_debug = df[df['PlantType'] == 'Wind_Onshore'].reset_index(drop=True)
    df_debug = df_debug[df_debug['ModelZone'] == 'TAS'].reset_index(drop=True)

    # get weekly time series df with 168 map
    df = df.drop(['Country', 'Global_CF', 'Country_CF', 'Zone_CF', 'Final_CF', 'AnnualAverageShape', 'AID_WeeklyShape'], 1)
    df.rename(columns={'ModelZone': 'Area', 'FinalAID_Shape': 'AID_WeeklyShape'}, inplace=True)
    df = df.sort_values(by=['Area', 'PlantType', 'ShapeName', 'Year', 'Month', 'DayOfWeek',
                            'HourOfDay']).reset_index(drop=True)
    df = pd.merge(df, df_168map, how='left', on=['DayOfWeek', 'HourOfDay'])
    df = df.drop(['DayOfWeek', 'HourOfDay'], 1)
    df['AID_WeeklyShape'] = 100 * (1 - df['AID_WeeklyShape'])
    df = pd.pivot_table(df, index=['Year', 'Area', 'PlantType', 'ShapeName', 'Month'],
                                columns='HrOfWk',
                                values='AID_WeeklyShape',
                                fill_value=0).reset_index()
    df['Name_mth'] = df['Area'] + '_' + df['PlantType'] + '_' + df['Month'].apply(lambda x: calendar.month_abbr[x])
    df['Name'] = df['Year'].astype(str) + '_' + df['Name_mth']
    df['ShapeName_mth'] = df['PlantType'] + '_' + df['ShapeName']
    df['ShapeName'] = df['Year'].astype(str) + '_' + df['ShapeName_mth']


    # weekly to monthly time series
    df_monthly = df[['Year', 'Area', 'PlantType', 'ShapeName_mth', 'Month', 'Name']].reset_index(drop=True)
    df_monthly.rename(columns={'ShapeName_mth': 'ShapeName'}, inplace=True)
    df_monthly['Name'] = 'wk_' + df_monthly['Name']
    df_monthly = df_monthly.set_index(['Year', 'Area', 'PlantType', 'ShapeName'])
    df_monthly = df_monthly.pivot(columns='Month').reset_index()

    colname = df_monthly.columns.values.tolist()
    list1 = [x[0] for x in colname]
    list2 = [x[1] for x in colname]
    colname = list1[:4] + list2[4:]
    df_monthly.columns = colname
    df_monthly['Use'] = df_monthly['Area'] + '_' + df_monthly['PlantType']

    df['ID'] = df['Name']
    df = df.drop(['Year', 'PlantType', 'Name_mth', 'ShapeName_mth'], 1)
    #df_monthly['ID'] = df_monthly['Use']
    df_monthly.rename(columns={'Use': 'ID'}, inplace=True)
    df_monthly = df_monthly.drop(['PlantType'], 1)

    df_maint_mth = df_monthly[['Year', 'Area', 'ShapeName', 'ID']].reset_index(drop=True)
    df_maint_mth['ID'] = 'mn_' + df_maint_mth['ID']

    df_maint_wk = df[['Area', 'ShapeName', 'ID']].reset_index(drop=True)
    df_maint_wk['ID'] = 'wk_' + df_maint_wk['ID']

    #df = df.drop(['Level'], 1)
    #df_monthly = df_monthly.drop(['Level'], 1)

    # df_debug = df[df['ShapeName'].str.contains('Wind_Onshore')].reset_index(drop=True)
    # df_debug = df_debug[df_debug['Area'] == 'TAS'].reset_index(drop=True)

    return df, df_monthly, df_maint_mth, df_maint_wk

def get_sql_plant_existing(src_sql, country):
    """

    Function that reads Existing & Planned (Named) power projects from SQL Server and transform it to Aurora Resource
    table.

    Args:
        src_sql (string): SQL View name from Source SQL Server that gives the Existing/Planned power project list
        country (string): country name

    Returns:
         df (dataframe): df that gives AID_Resource table
    """

    #get conventional plant
    if country == 'Thailand' or country == 'Vietnam' or country == 'India':
        sql_qry = """SELECT * FROM """ + src_sql + """ WHERE [Market] = '""" + country + """' AND [Status] NOT IN ('Frozen', 'Temp Shut Down', 
            'Cancelled')"""
    else:
        sql_qry = """SELECT * FROM """ + src_sql + """ WHERE [Country] = '""" + country + """' AND [Status] NOT IN ('Frozen', 'Temp Shut Down', 
    'Cancelled')"""
    df = pd.read_sql_query(sql_qry, engine_src)
    df = df[['PowerPlant', 'PlantType', 'PlantTech', 'FuelPri', 'FuelSec', 'Start', 'End', 'Available_MW', 'HeatRate',
                 'Zone', 'Market']]
    df.rename(columns={'PowerPlant': 'Name',
                           'PlantTech': 'zREM Technology',
                           'PlantType': 'Resource_Group',
                           'FuelPri': 'Fuel',
                           'FuelSec': 'Second Fuel',
                           'Start': 'Resource Begin Date',
                           'End': 'Resource End Date',
                           'Available_MW': 'Capacity',
                           'HeatRate': 'Heat Rate',
                           'Zone': 'zREM County',
                           'Market': 'zREM State'}, inplace=True)
    df['Area'] = df['zREM County'].map(dict_zone)
    df['Resource Group'] = df['Resource_Group']
    df['Resource_Group'] = pd.np.where(df['Resource_Group'] == 'CC',
                                           df['Resource_Group'] + '-' + df['zREM Technology'],
                                           df['Resource_Group'])
    df['Resource_Group'] = pd.np.where(df['Resource_Group'] == 'CHP_Gas',
                                       df['Resource_Group'] + '-' + df['zREM Technology'],
                                       df['Resource_Group'])
    df['Resource_Group'] = pd.np.where(df['Resource_Group'] == 'STCoal',
                                           df['Resource_Group'] + '-' + df['zREM Technology'],
                                           df['Resource_Group'])
    df['Resource_Group'] = pd.np.where(df['Resource_Group'] == 'CHP_Coal',
                                       df['Resource_Group'] + '-' + df['zREM Technology'],
                                       df['Resource_Group'])
    df['Resource Begin Date'] = pd.to_datetime(df['Resource Begin Date'])
    df['Resource End Date'] = pd.to_datetime(df['Resource End Date'])
    df['StartYear'] = df['Resource Begin Date'].dt.year
    df['StartYear'] = pd.np.where(df['StartYear'] < 2000, 2000, df['StartYear'])

    return df

def get_sql_plant_newbuild(src_sql, country):
    """

    Function that reads New Build power projects from SQL Server and transform it to Aurora Resource
    table.

    Args:
        src_sql (string): SQL View name from Source SQL Server that gives the New Build project list
        country (string): country name

    Returns:
         df (dataframe): df that gives AID_Resource table
    """

    #get conventional plant
    sql_qry = """SELECT * FROM """ + src_sql + """ WHERE [Country] = '""" + country + """'"""
    df = pd.read_sql_query(sql_qry, engine_src)
    df = df[['PowerPlant', 'PlantType', 'PlantTech', 'FuelPri', 'Start', 'End', 'Capacity', 'HeatRate', 'VOM',
             'CapacityFactor', 'Zone', 'Market']]
    df.rename(columns={'PowerPlant': 'Name',
                       'PlantTech': 'zREM Technology',
                       'PlantType': 'Resource Group',
                       'FuelPri': 'Fuel',
                       'Start': 'Resource Begin Date',
                       'End': 'Resource End Date',
                       'HeatRate': 'Heat Rate',
                       'Zone': 'zREM County',
                       'Market': 'zREM State'}, inplace=True)
    df['Area'] = df['zREM County'].map(dict_zone)
    df['Resource_Group'] = df['Resource Group']
    df['Resource_Group'] = pd.np.where(df['Resource_Group'] == 'CC',
                                       df['Resource_Group'] + '-' + df['zREM Technology'],
                                       df['Resource_Group'])
    df['Resource_Group'] = pd.np.where(df['Resource_Group'] == 'CHP_Gas',
                                       df['Resource_Group'] + '-' + df['zREM Technology'],
                                       df['Resource_Group'])
    df['Resource_Group'] = pd.np.where(df['Resource_Group'] == 'STCoal',
                                       df['Resource_Group'] + '-' + df['zREM Technology'],
                                       df['Resource_Group'])
    df['Resource_Group'] = pd.np.where(df['Resource_Group'] == 'CHP_Coal',
                                       df['Resource_Group'] + '-' + df['zREM Technology'],
                                       df['Resource_Group'])
    df['Resource Begin Date'] = pd.to_datetime(df['Resource Begin Date'])
    df['Resource End Date'] = pd.to_datetime(df['Resource End Date'])

    return df

def get_sql_plant_balancing(src_sql, country):
    """

    Function that reads Balancing power capacities from SQL Server and transform it to Aurora Resource
    table format to be use to offset existing & new build capacities to match published numbers.

    Args:
        src_sql (string): SQL View name from Source SQL Server that gives the balancing capacities
        country (string): country name

    Returns:
         df (dataframe): df that gives cummulative balancing capacitites by province by tech by year
    """

    #get conventional plant
    sql_qry = """SELECT * FROM """ + src_sql + """ WHERE [Country] = '""" + country + """'"""
    df = pd.read_sql_query(sql_qry, engine_src)
    df = df[['PlantType-Tech', 'PlantTech', 'FuelPri', 'StartYear', 'BalancingCapacity_MW', 'Zone', 'Market']]
    df.rename(columns={'PlantTech': 'zREM Technology',
                       'PlantType-Tech': 'Resource_Group',
                       'FuelPri': 'Fuel',
                       'StartYear': 'Yr',
                       'HeatRate': 'Heat Rate',
                       'Zone': 'zREM County',
                       'Market': 'zREM State'}, inplace=True)
    df['Area'] = df['zREM County'].map(dict_zone)


    return df

def get_sql_wind_plant_existing(src_sql, country):
    """

    Function that reads Existing & Planned (Named) Wind projects from SQL Server and transform it to Aurora Resource
    table.

    Args:
        src_sql (string): SQL View name from Source SQL Server that gives the Existing/Planned Wind project list
        country (string): country name

    Returns:
         df (dataframe): df that gives AID_Resource table
    """

    #get Existing & Planned (mid-term) Wind plant
    sql_qry = """SELECT * FROM """ + src_sql + """ WHERE [Country] = '""" + country + """'"""
    df = pd.read_sql_query(sql_qry, engine_src)
    df = df[['FINAL_PROJECT_NAME', 'PROJECT_TYPE','WM_YEARSTART', 'WM_YEAREND', 'MW', 'Zone', 'Market']]
    df.rename(columns={'FINAL_PROJECT_NAME': 'Name',
                       'PROJECT_TYPE': 'Resource_Group',
                       'WM_YEARSTART': 'Resource Begin Date',
                       'WM_YEAREND': 'Resource End Date',
                       'MW': 'Capacity',
                       'Zone': 'zREM County',
                       'Market': 'zREM State'}, inplace=True)
    df['Area'] = df['zREM County'].map(dict_zone)
    df['Fuel'] = 'Wind'
    df['Resource_Group'] = 'Wind_' + df['Resource_Group']

    df['Resource Begin Date'] = df['Resource Begin Date'].astype(int)
    df['Day'] = 1
    df['Month'] = 1
    df['Year'] = df['Resource Begin Date']
    df['Resource Begin Date'] = pd.to_datetime(df[['Day', 'Month', 'Year']])
    df['Resource End Date'] = df['Resource End Date'].astype(int)
    df['Day'] = 31
    df['Month'] = 12
    df['Year'] = df['Resource End Date']
    df['Resource End Date'] = pd.to_datetime(df[['Day', 'Month', 'Year']])
    df = df.drop(['Day', 'Month', 'Year'], 1)

    df['StartYear'] = df['Resource Begin Date'].dt.year
    df['StartYear'] = pd.np.where(df['StartYear'] < 2000, 2000, df['StartYear'])

    return df

def get_sql_fuel_to_aid(src_sql):
    """

    Function that reads APAC Fuel Prices from SQL Server and Transforms it to 2 Aurora Input tables.

    Args:
        src_sql (string): SQL View name from Source SQL Server that gives the APAC Fuel Prices
        country (string): country name

    Returns:
         df (dataframe): df that gives the Fuel prices in AID_Time_Series_Annual format
         df_aid_fuel (dataframe): df that gives AID_Fuel table
    """

    sql_qry = """SELECT * FROM """ + src_sql
    df = pd.read_sql_query(sql_qry, engine_src)
    df = df.drop(['Metric'], 1)
    df = pd.pivot_table(df, index=['Level', 'LevelName', 'FuelName', 'Description', 'FuelType', 'Units'],
                              columns='Year', values='Value', fill_value=0).reset_index()
    df['FuelName'] = pd.np.where(df['LevelName'] == 'All',
                                 df['FuelName'],
                                 df['LevelName'] + '_' + df['FuelName'])
    df['Fuel ID'] = df['FuelName']
    df['Price'] = 'yr_Price_' + df['Fuel ID']
    df.rename(columns={'LevelName': 'zREM Topology',
                       'FuelName' : 'Fuel Name',
                       'FuelType' : 'Fuel Type'}, inplace=True)

    df_aid_fuel = df[['Fuel ID', 'Fuel Name', 'Fuel Type', 'Units', 'Price', 'zREM Topology']].reset_index(drop=True)

    return df, df_aid_fuel

def apply_solar_wind_shapes_to_plants(df_maint_mth, df_plant):
    """

    :param df_maint_mth:
    :param df_plant:
    :return:
    """

    maint_rate = df_maint_mth[['Area', 'Resource Group', 'ID']].reset_index(drop=True)
    maint_rate.rename(columns={'ID': 'Maintenance Rate'}, inplace=True)
    maint_rate = maint_rate.drop_duplicates()

    if df_maint_mth['Level'].unique()[0] == 'ModelZone':
        df_plant = pd.merge(df_plant, maint_rate, how='left', left_on=['zREM County', 'Resource Group'], right_on=['Area',
                                                                                                         'Resource Group'])
    elif df_maint_mth['Level'].unique()[0] == 'ModelMkt':
        df_plant = pd.merge(df_plant, maint_rate, how='left', left_on=['zREM State', 'Resource Group'], right_on=['Area',
                                                                                                         'Resource Group'])
    else:
        df_plant = pd.merge(df_plant, maint_rate, how='left', left_on=['Resource Group'], right_on=['Resource Group'])

    df_plant = df_plant.drop(['Area_y'], 1)
    df_plant.rename(columns={'Area_x': 'Area'}, inplace=True)

    return df_plant

def get_sql_fuel_max_to_aid(src_sql):
    """

    Function that reads APAC Fuel Prices from SQL Server and Transforms it to 2 Aurora Input tables.

    Args:
        src_sql (string): SQL View name from Source SQL Server that gives the APAC Fuel Prices
        country (string): country name

    Returns:
         df (dataframe): df that gives the Fuel prices in AID_Time_Series_Annual format
         df_aid_fuel (dataframe): df that gives AID_Fuel table
    """
    sql_qry = """SELECT * FROM """ + src_sql
    df = pd.read_sql_query(sql_qry, engine_src)
    #df = df.drop(['Metric'], 1)
    df = pd.pivot_table(df, index=['Metric', 'Level', 'LevelName', 'FuelName', 'Description', 'FuelType', 'Units'],
                              columns='Year', values='Value', fill_value=0).reset_index()
    df['FuelName'] = pd.np.where(df['LevelName'] == 'All',
                                 df['FuelName'],
                                 df['LevelName'] + '_' + df['FuelName'])
    df['Set ID'] = df['Metric'] + '_' + df['FuelName']
    df['Limit'] = 'yr_' + df['Set ID']
    df.rename(columns={'LevelName': 'zREM Topology',
                       'Metric': 'Constraint Type',
                       'FuelName' : 'zREM Comment',
                       'FuelType': 'Fuel Type',
                       'Units' : 'Limit Units'}, inplace=True)
    df['Constraint Type'] = df['Constraint Type'].str.replace('_', ' ')
    df['Limit Type'] = 'Year'
    df['Chronological Method'] = 'Hourly Limit'
    df_aid_fuelmax = df[['Set ID', 'zREM Comment', 'Limit Type', 'Constraint Type', 'Chronological Method', 'Limit Units', 'Limit', 'zREM Topology']].reset_index(drop=True)

    return df, df_aid_fuelmax

#def append_unserved_energy_resources:
    #df_unserved = pd.DataFrame(columns=['ID','Name','Area','Resource Group','Resource Begin Date','Resource End Date','Capacity','Fuel'])
# </editor-fold>

# <editor-fold desc="Get Zone mappings & Plant Assumptions">
dict_zone, dict_zone_mkt, dict_mkt_country, df_zone, df_area = get_sql_topology_to_aid(src_sql='vAID_Topology_Zones',
                                                                                       country=country)
Assumptions = get_sql_plantasmpt(src_sql='vAPAC_Plant_Attributes_Annual_LIVE')
# </editor-fold>





# <editor-fold desc="Execute by Blocks: From Excel To Agnostic SQL DB">

# <editor-fold desc="OBSOLETE CODE ==> DO NOT USE/RUN">
# <editor-fold desc="OBSOLETE ==> Read & LOAD Transmission from EXCEL">

# READ data from excel
#df_txn = get_excel_transmission('APAC_Transmission.xlsx')
df_txn_all, df_txn_price,df_txn_line = new_get_excel_transmission(path, sheetname_line, sheetname_price)

# LOAD (append)/Reload to LIVE tbl
dest_tbl_all = 'APAC_PowerTransmission_LIVE'
dest_tbl_price = 'APAC_PowerTransmission_Tariffs_LIVE'
dest_tbl_line = 'APAC_PowerTransmission_InfrastructureProject_LIVE'

upload_sql(engine=engine_src, df=df_txn_all, dest_sql=dest_tbl_all, existMethod='append')
#reload_tbl(engine=engine_src,  dest_tbl=dest_tbl_all, dest_df=df_txn_all)
#reload_tbl(engine=engine_src,  dest_tbl=dest_tbl_price, dest_df=df_txn_price)
#reload_tbl(engine=engine_src,  dest_tbl=dest_tbl_line, dest_df=df_txn_line)


# LOAD (append) to Archive tbl
df_txn_all['UpdateCycle'] = cycle
df_txn_price['UpdateCycle'] = cycle
df_txn_line['UpdateCycle'] = cycle
df_txn_all['Dataset_Name'] = datasetname
df_txn_price['Dataset_Name'] = datasetname
df_txn_line['Dataset_Name'] = datasetname

dest_tbl_all_d = 'APAC_PowerTransmission_Datasets'
dest_tbl_price_d = 'APAC_PowerTransmission_Tariffs_Datasets'
dest_tbl_line_d = 'APAC_PowerTransmission_InfrastructureProject_Datasets'

upload_sql(engine=engine_src, df=df_txn_all, dest_sql=dest_tbl_all_d, existMethod='append')
upload_sql(engine=engine_src, df=df_txn_price, dest_sql=dest_tbl_price_d, existMethod='append')
upload_sql(engine=engine_src, df=df_txn_line, dest_sql=dest_tbl_line_d, existMethod='append')


# </editor-fold>

# <editor-fold desc="OBSOLETE ==> Read & LOAD RAW Wind and Solar 8760 shape from EXCEL">

# READ data from excel
df_shape_wind = get_excel_rawwindsolar8760shape(xls_name='RawWind&SolarData.xlsx',
                                                shape_name='Wind', level='Country', level_name='Philippines',
                                                spec='H120m_Vestas V112 3000')
df_shape_solar = get_excel_rawwindsolar8760shape(xls_name='RawWind&SolarData.xlsx',
                                                 shape_name='Solar', level='Country', level_name='Philippines',
                                                 spec='NoTracking_Tilt35')

# LOAD (append) to LIVE tbl
dest_tbl = 'APAC_Shapes_Raw8760_LIVE'
#upload_sql(engine=engine_src, df=df_shape_wind, dest_sql=dest_tbl, existMethod='append')
#upload_sql(engine=engine_src, df=df_shape_solar, dest_sql=dest_tbl, existMethod='append')

reload_tbl(engine=engine_src,  dest_tbl=dest_tbl, dest_df=df_shape_wind)
upload_sql(engine=engine_src, df=df_shape_solar, dest_sql=dest_tbl, existMethod='append')


# </editor-fold>

# <editor-fold desc="OBSOLETE ==> Read & LOAD Generic New Build from EXCEL">

# READ data from excel
df_newbuild, df_balancingplant = get_excel_newbuild(xls_name='APAC_NewBuilds_Central China.xlsm', balancing_plant=True)

# Add attribute assumptions
df_newbuild_list = assign_assumptions(df=df_newbuild)

# Transform it to look like Project List
df_newbuild_list['PlantLife'] = df_newbuild_list['PlantLife'].astype(int)
df_newbuild_list['EndYear'] = df_newbuild_list['StartYear'] + df_newbuild_list['PlantLife']
df_newbuild_list['Day'] = 1
df_newbuild_list['Month'] = 1
df_newbuild_list['Year'] = df_newbuild_list['StartYear']
df_newbuild_list['Start'] = pd.to_datetime(df_newbuild_list[['Day', 'Month', 'Year']])
df_newbuild_list['Day'] = 31
df_newbuild_list['Month'] = 12
df_newbuild_list['Year'] = df_newbuild_list['EndYear']
df_newbuild_list['End'] = pd.to_datetime(df_newbuild_list[['Day', 'Month', 'Year']])
df_newbuild_list = df_newbuild_list.drop(['Day', 'Month', 'Year', 'StartYear', 'EndYear'], 1)

# LOAD (append)/Reload to LIVE tbl
dest_tbl = 'APAC_PowerProjects_NewBuild_LIVE'
update_sqltbl_by(engine=engine_src, dest_tbl=dest_tbl, dest_df=df_newbuild_list, by_col='Zone')
#upload_sql(engine=engine_src, df=df_newbuild_list, dest_sql=dest_tbl, existMethod='append')
#reload_tbl(engine=engine_src,  dest_tbl=dest_tbl, dest_df=df_newbuild_list)

# LOAD (append) to ARCHIVE tbl
df_newbuild_list['UpdateCycle'] = cycle
df_newbuild_list['Dataset_Name'] = datasetname
dest_tbl = 'APAC_PowerProjects_NewBuild_Datasets'
upload_sql(engine=engine_src, df=df_newbuild_list, dest_sql=dest_tbl, existMethod='append')
# </editor-fold>

# <editor-fold desc="NOT IN USE ==> Read & LOAD Distributed Capacity from EXCEL">
# READ data from excel
# df_distributed = get_excel_distributed(xls_name='APAC_DistributedCapacity.xlsx')
#
# # LOAD (append)/Reload to LIVE tbl
# dest_tbl = 'APAC_DistributedCapacity_LIVE'
# #upload_sql(engine=engine_src, df=df_distributed, dest_sql=dest_tbl, existMethod='append')
# reload_tbl(engine=engine_src,  dest_tbl=dest_tbl, dest_df=df_distributed)
#
# # LOAD (append) to Archive tbl
# df_distributed['UpdateCycle'] = cycle
# df_distributed['Dataset_Name'] = datasetname
# dest_tbl = 'APAC_DistributedCapacity_Datasets'
# upload_sql(engine=engine_src, df=df_distributed, dest_sql=dest_tbl, existMethod='append')
# </editor-fold>
# </editor-fold>

# <editor-fold desc="NOT IN USE ==> EV">
#df_evShapes = get_excel_hourlyshape(xls_name='APAC_Assumptions.xlsx', shtname='HourlyShape')
#dest_tbl = 'APAC_Shapes_Hourly_LIVE'
#reload_tbl(engine=engine_src,  dest_tbl=dest_tbl, dest_df=df_evShapes)

# </editor-fold>

# <editor-fold desc="Read & LOAD Hydro 12 monthly shape from EXCEL">

# READ data from excel
df_shape_hydromth, df_AID_HydroVector = get_excel_hydro12mthlyshape(xls_name='APAC_Hydro.xlsx')

# LOAD (append) to LIVE tbl
dest_tbl = 'APAC_Shapes_Monthly_LIVE'
#upload_sql(engine=engine_src, df=df_shape_hydromth, dest_sql=dest_tbl, existMethod='append')
reload_tbl(engine=engine_src,  dest_tbl=dest_tbl, dest_df=df_shape_hydromth)

dest_tbl_1 = 'APAC_AID_HydroVectors_LIVE'
#upload_sql(engine=engine_src, df=df_AID_HydroVector, dest_sql=dest_tbl_1, existMethod='append')
reload_tbl(engine=engine_src,  dest_tbl=dest_tbl_1, dest_df=df_AID_HydroVector)

# </editor-fold>

# <editor-fold desc="Read & LOAD Plant Variable Assumptions from EXCEL">

# READ data from excel
df_plant_life = get_excel_plantasmpt(xls_name='APAC_Assumptions.xlsx', shtname='PlantLife', units='Years')
df_plant_vom = get_excel_plantasmpt(xls_name='APAC_Assumptions.xlsx', shtname='VOM', units='US$/MWh')
df_plant_heatrate = get_excel_plantasmpt(xls_name='APAC_Assumptions.xlsx', shtname='HeatRate', units='btu/kWh')
df_plant_cf = get_excel_plantasmpt(xls_name='APAC_Assumptions.xlsx', shtname='CapacityFactor', units='NA')
df_plant_fixcost = get_excel_plantasmpt(xls_name='APAC_Assumptions.xlsx', shtname='FixedCost', units='US$/MW week')
df_plant_emissionrate = get_excel_plantasmpt(xls_name='APAC_Assumptions.xlsx', shtname='EmissionRate', units='lb/mmbtu')
df_plant_emissionprice = get_excel_plantasmpt(xls_name='APAC_Assumptions.xlsx', shtname='EmissionPrice', units='US$/ton')
df_plant_storageDuration = get_excel_plantasmpt(xls_name='APAC_Assumptions.xlsx', shtname='StorageDuration', units='Hours')

# LOAD (append) to LIVE tbl
dest_tbl = 'APAC_PlantAttribute_AnnualAssumptions_LIVE'
reload_tbl(engine=engine_src,  dest_tbl=dest_tbl, dest_df=df_plant_life)
#upload_sql(engine=engine_src, df=df_plant_life, dest_sql=dest_tbl, existMethod='append')
upload_sql(engine=engine_src, df=df_plant_vom, dest_sql=dest_tbl, existMethod='append')
upload_sql(engine=engine_src, df=df_plant_heatrate, dest_sql=dest_tbl, existMethod='append')
upload_sql(engine=engine_src, df=df_plant_cf, dest_sql=dest_tbl, existMethod='append')
upload_sql(engine=engine_src, df=df_plant_fixcost, dest_sql=dest_tbl, existMethod='append')
upload_sql(engine=engine_src, df=df_plant_emissionrate, dest_sql=dest_tbl, existMethod='append')
upload_sql(engine=engine_src, df=df_plant_emissionprice, dest_sql=dest_tbl, existMethod='append')
upload_sql(engine=engine_src, df=df_plant_storageDuration, dest_sql=dest_tbl, existMethod='append')

# </editor-fold>

# <editor-fold desc="Read & LOAD Fuel annual data from EXCEL">

# READ data from excel
df_fuel = get_excel_fuel(xls_name='APAC_Fuels.xlsx')

# LOAD (append) to LIVE tbl
dest_tbl = 'APAC_PlantFuel_Annual_LIVE'
clear_sql = """TRUNCATE TABLE """ + dest_tbl
execute_sqlcur(engine=engine_src, sql=clear_sql)
upload_sql(engine=engine_src, df=df_fuel, dest_sql=dest_tbl, existMethod='append')

# </editor-fold>


# </editor-fold>

# <editor-fold desc="Execute by blocks: From Agnostic SQL DB To Aurora">

# <editor-fold desc="Reload: AID_Topology_Zones">
reload_tbl(engine=engine_dest, dest_tbl='tbl_AID_Topology_Zones', dest_df=df_zone)
reload_tbl(engine=engine_dest, dest_tbl='tbl_AID_Topology_Areas', dest_df=df_area)
# </editor-fold>

# <editor-fold desc="Reload: AID_Demand tables">
demand_hrlyshape, demand_mthlypeak, demand_mthly = get_sql_demand_to_aid(
    src_sql='vAPAC_LoadDurationCurve_Normalised_Forecast_LIVE', country=country)
demand_mthlypeak['Primary Key'] = demand_mthlypeak.index + 1

reload_tbl(engine=engine_dest, dest_tbl='tbl_AID_Demand_Hourly_Shapes', dest_df=demand_hrlyshape)
reload_tbl(engine=engine_dest, dest_tbl='tbl_AID_Demand_Monthly_Peak', dest_df=demand_mthlypeak)
reload_tbl(engine=engine_dest, dest_tbl='tbl_AID_Demand_Monthly', dest_df=demand_mthly)
# </editor-fold>

# <editor-fold desc="Reload: Inflation to TS_Annual">
inflation = get_sql_inflation_to_aid(src_sql='vAPAC_Inflation_YoY_Latest', country=country)
# for 2021 update cycle, can comments out if needs
inflation [[2000,        2001,        2002,
              2003,        2004,        2005,        2006,        2007,
              2008,        2009,        2010,        2011,        2012,
              2013,        2014,        2015,        2016,        2017,
              2018,        2019,        2020,        2021,        2022,
              2023,        2024,        2025,        2026,        2027,
              2028,        2029,        2030,        2031,        2032,
              2033,        2034,        2035,        2036,        2037,
              2038,        2039,        2040,        2041,        2042,
              2043,        2044,        2045,        2046,        2047,
              2048,        2049,        2050,        2051,        2052,
              2053,        2054,        2055,        2056,        2057,
              2058,        2059,        2060]] = 0
update_aid_id(dest_tbl='tbl_AID_Time_Series_Annual', dest_df=inflation)
# </editor-fold>

# <editor-fold desc="Reload: AID_Transmission & Update TS_Annual">
transmission_link, ts_annual_txlink = get_sql_transmission_links_to_aid(src_sql='vAPAC_Transmission_LIVE',
                                                                        country=country)

# ts_annual_txlink = ts_annual_txlink[ts_annual_txlink['ID'].isin(['wheeling_510_525', 'wheeling_510_517', 'wheeling_510_505',
#                                                                 'wheeling_523_525', 'wheeling_523_532', 'wheeling_523_503',
#                                                                 'wheeling_523_501', 'wheeling_523_517', 'wheeling_531_505'])].reset_index(drop=True)
reload_tbl(engine=engine_dest, dest_tbl='tbl_AID_Transmission_Links', dest_df=transmission_link)
update_aid_id(dest_tbl='tbl_AID_Time_Series_Annual', dest_df=ts_annual_txlink)
# </editor-fold>

# <editor-fold desc="NOT IN USE: Update TS_Annual for Distributed Capacity">
# READ data from excel
# df_ts_dist, df_resource_dist = get_sql_distributed_capacity_to_aid(src_sql = 'APAC_DistributedCapacity_LIVE', country=country)
#
# update_aid_id(dest_tbl='tbl_AID_Time_Series_Annual', dest_df=df_ts_dist)
# </editor-fold>

# <editor-fold desc="Reload: AID_Hydro_Vector">
hydro_vector, hydro_monthly = get_sql_hydroshapes_to_aid(src_sql_vec='APAC_AID_HydroVectors_LIVE',
                                                         src_sql_mthly='vAPAC_Shapes_Monthly_LatestYear',
                                                         country=country)
reload_tbl(engine=engine_dest, dest_tbl='tbl_AID_Hydro_Vectors', dest_df=hydro_vector)
reload_tbl(engine=engine_dest, dest_tbl='tbl_AID_Hydro_Monthly', dest_df=hydro_monthly)
# </editor-fold>



#Run these to update resource table

# <editor-fold desc="GET & Transform Maint rates & Fuel for Aurora">
# Shapes & Maintenance rates
TS_Weekly, TS_Monthly, Maint_mth, Maint_wk = get_sql_windsolarshapes_to_aid_ts_wek(src_sql='vAPAC_Shapes_8760_to_168_LatestYear_ModelZone',
                                                                                   country=country)
# df_debug = TS_Weekly[TS_Weekly['ShapeName'].str.contains('Wind_Onshore')].reset_index(drop=True)
# df_debug = df_debug[df_debug['Area'] == 'TAS'].reset_index(drop=True)

# get the Shape profile names
Maint_mth['Level'] = 'ModelZone'
Maint_mth['Resource Group'] = pd.np.where(Maint_mth['ShapeName'].str.contains('PV_D'), 'PV_D',
                                          pd.np.where(Maint_mth['ShapeName'].str.contains('PV_BA'), 'PV_BA',
                                            pd.np.where(Maint_mth['ShapeName'].str.contains('PV'), 'PV',
                                              pd.np.where(Maint_mth['ShapeName'].str.contains('Onshore'), 'Wind_Onshore',
                                                pd.np.where(Maint_mth['ShapeName'].str.contains('Offshore'), 'Wind_Offshore', 'NewShape')))))
# Rename relevant shape name IDs to keep within 50 characters
Maint_mth['ID'] = Maint_mth['ID'].str.replace('East Central South Kalimantan', 'ECS Kalimantan')
Maint_mth['ID'] = Maint_mth['ID'].str.replace('Papua Timor Maluku Nusa Tenggara', 'Timor')
TS_Weekly['ID'] = TS_Weekly['ID'].str.replace('East Central South Kalimantan', 'ECS Kalimantan')
TS_Weekly['ID'] = TS_Weekly['ID'].str.replace('Papua Timor Maluku Nusa Tenggara', 'Timor')
col_lst = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 'ID']
for col in col_lst:
    TS_Monthly[col] = TS_Monthly[col].str.replace('East Central South Kalimantan', 'ECS Kalimantan')
    TS_Monthly[col] = TS_Monthly[col].str.replace('Papua Timor Maluku Nusa Tenggara', 'Timor')

# Fuel table
ts_annual_fuel, Fuel = get_sql_fuel_to_aid(src_sql='vAPAC_Plant_Fuel_Price_Annual_LIVE')
# </editor-fold>

# <editor-fold desc="GET & Transform Existing Named Plants for Aurora">
# Existing plants to AID resource (left join attribute 1st)
Plant_Existing = get_sql_plant_existing(src_sql='vAPAC_PowerProjects_LIVE', country=country)

# option for aggregating plants balancing
if country == 'China':
    Plant_Existing, TS_Annual_PlantExisting, TS_StorageMax_Exist = aggregate_plant_list(df_plantlist=Plant_Existing, name_prefix='Existing',
                                                                                        yr_start=2020, yr_end=yr_end, step=1,
                                                                                        offset_balancing_capacity=True)
    Plant_Existing.rename(columns={'ResourceBeginDate': 'Resource Begin Date',
                                   'ResourceEndDate': 'Resource End Date'}, inplace=True)

# add assumptions
PlantExisting = assign_assumptions(df=Plant_Existing)
PlantExisting, ts_annual_assumptions = assign_tsannual_assumptions(df=PlantExisting)
PlantExisting, EmissionRate, EmissionPrice = apply_emissions(df=PlantExisting)
PlantExisting['Second Fuel'] = pd.np.where(PlantExisting['Second Fuel'] == PlantExisting['Fuel'],
                                           np.nan,
                                           PlantExisting['Second Fuel'])
PlantExisting[['HeatRate', 'Heat Rate']] = PlantExisting[['HeatRate', 'Heat Rate']].fillna(method='ffill', axis=1)
PlantExisting['Forced Outage'] = 100*(1-PlantExisting['CapacityFactor'])
PlantExisting = PlantExisting.drop(['HeatRate', 'PlantLife', 'CapacityFactor', 'PlantType-Tech', 'StartYear'], 1)
PlantExisting.rename(columns={'VOM': 'Variable O&M',
                              'FixedCost': 'Fix Cost Mod1',
                              'EmissionRate':'Emission Rate ID',
                              'EmissionPrice':'Emission Price ID'
                              }, inplace=True)
PlantExisting['ID'] = PlantExisting['Name']
# apply wind & solar shapes maintenance rate to existing plants
PlantExisting = apply_solar_wind_shapes_to_plants(df_maint_mth=Maint_mth, df_plant=PlantExisting)
PlantExisting['Forced Outage'] = pd.np.where(PlantExisting['Maintenance Rate'].isnull(),
                                             PlantExisting['Forced Outage'],
                                             np.nan)
# </editor-fold>

# <editor-fold desc="GET & Transform New Build Plants for Aurora">
# NewBuild plants to AID resource (left join solar & wind TS ref)
Plant_New = get_sql_plant_newbuild(src_sql='vAPAC_PowerProjects_NewBuild_LIVE', country=country)

# option for aggregating plants
if country == 'China':
    Plant_New, TS_Annual_PlantNew, TS_StorageMax_New = aggregate_plant_list(df_plantlist=Plant_New, name_prefix='NewBuild', yr_start=2000,
                                                                            yr_end=yr_end,  step=1, offset_balancing_capacity=False)
    Plant_New = assign_assumptions(df=Plant_New)
    Plant_New = Plant_New.drop(['PlantLife', 'Heat Rate', 'StartYear'], 1)
    Plant_New.rename(columns={'ResourceBeginDate': 'Resource Begin Date',
                              'ResourceEndDate': 'Resource End Date',
                              'HeatRate': 'Heat Rate',
                              'PlantType-Tech': 'Resource_Group'}, inplace=True)

Plant_New, tmp = assign_tsannual_assumptions(df=Plant_New)
Plant_New, tmp_EmissionRate, tmp_EmissionPrice = apply_emissions(df=Plant_New)

Plant_New['Forced Outage'] = 100*(1-Plant_New['CapacityFactor'])
Plant_New = Plant_New.drop(['CapacityFactor', 'PlantType-Tech'], 1)
Plant_New.rename(columns={'VOM': 'Variable O&M',
                          'FixedCost': 'Fix Cost Mod1',
                          'EmissionRate': 'Emission Rate ID',
                          'EmissionPrice': 'Emission Price ID'
                          }, inplace=True)
Plant_New['ID'] = Plant_New['Name']
# apply wind & solar shapes maintenance rate to new build plants
Plant_New = apply_solar_wind_shapes_to_plants(df_maint_mth=Maint_mth, df_plant=Plant_New)
Plant_New['Forced Outage'] = pd.np.where(Plant_New['Maintenance Rate'].isnull(),
                                         Plant_New['Forced Outage'],
                                         np.nan)
# </editor-fold>

# <editor-fold desc="GET & Transform Existing & planned mid term Wind plants for Aurora - Not Applicable for China & India & Australia & Taiwan & South Korea & Singapore & Malaysia">
# Existing & Planned (mid-term) Wind projects to AID resource
Plant_Wind = get_sql_wind_plant_existing(src_sql='vAPAC_WindProjectList_LIVE', country=country)
# add assumptions
PlantWind = assign_assumptions(df=Plant_Wind)
PlantWind, tmp = assign_tsannual_assumptions(df=PlantWind)
PlantWind, tmp_wind_EmissionRate, tmp_wind_EmissionPrice = apply_emissions(df=PlantWind)
PlantWind = PlantWind.drop(['CapacityFactor', 'PlantLife', 'StartYear'], 1)
PlantWind.rename(columns={'PlantType-Tech': 'Resource Group',
                          'HeatRate': 'Heat Rate',
                          'FixedCost': 'Fix Cost Mod1',
                          'VOM': 'Variable O&M',
                          'EmissionRate': 'Emission Rate ID',
                          'EmissionPrice': 'Emission Price ID'
                        }, inplace=True)
# apply wind & solar shapes maintenance rate to wind plants
PlantWind = apply_solar_wind_shapes_to_plants(df_maint_mth=Maint_mth, df_plant=PlantWind)
PlantWind['ID'] = PlantWind['Name']
# </editor-fold>


# <editor-fold desc="Reload: AID_TimeSeries_Weekly_Monthly_Solar and Wind Shapes">

TS_Weekly = TS_Weekly.drop(['Month'], 1)
TS_Weekly.rename(columns={'Area': 'zREM Topology', 'ShapeName': 'zREM Type'}, inplace=True)
TS_Monthly.rename(columns={'Area': 'zREM Topology', 'ShapeName': 'zREM Type'}, inplace=True)

reload_tbl(engine=engine_dest, dest_tbl='tbl_AID_Time_Series_Weekly', dest_df=TS_Weekly)
reload_tbl(engine=engine_dest, dest_tbl='tbl_AID_Time_Series_Monthly', dest_df=TS_Monthly)

#update_aid_id(dest_tbl='tbl_AID_Time_Series_Weekly', dest_df=TS_Weekly)
#update_aid_id(dest_tbl='tbl_AID_Time_Series_Monthly', dest_df=TS_Monthly)
# </editor-fold>

# <editor-fold desc="Reload: AID_Resource table">

if country == 'China':
    reload_tbl(engine=engine_dest, dest_tbl='tbl_AID_Resources', dest_df=PlantExisting)
    update_aid_id(dest_tbl='tbl_AID_Resources', dest_df=Plant_New)

    aid_table = 'tbl_AID_Time_Series_Annual'
    del_sql = """DELETE FROM """ + aid_table + """ WHERE [zREM Type] IN ('PlantCapacity', 'StorageMaxCapacity')"""
    execute_sqlcur(engine=engine_dest, sql=del_sql)
    update_aid_id(dest_tbl=aid_table, dest_df=TS_Annual_PlantExisting)
    update_aid_id(dest_tbl=aid_table, dest_df=TS_Annual_PlantNew)
    update_aid_id(dest_tbl=aid_table, dest_df=ts_annual_assumptions)
    if len(TS_StorageMax_Exist) > 0:
        update_aid_id(dest_tbl=aid_table, dest_df=TS_StorageMax_Exist)
    if len(TS_StorageMax_New) > 0:
        update_aid_id(dest_tbl=aid_table, dest_df=TS_StorageMax_New)
else:
    reload_tbl(engine=engine_dest, dest_tbl='tbl_AID_Resources', dest_df=PlantExisting)
    #update_aid_id(dest_tbl='tbl_AID_Resources', dest_df=Plant_New)
    #update_aid_id(dest_tbl='tbl_AID_Resources', dest_df=PlantWind)
    upload_sql(engine=engine_dest, df=Plant_New, dest_sql='tbl_AID_Resources', existMethod='append')
    if country != 'Australia' and country != 'India'  and country != 'Taiwan'and country != 'Pakistan' and country != 'Japan' and country != 'South Korea' and country !='Indonesia' and country !='Malaysia' and country !='Philippines' and country !='Singapore' and country !='New Zealand' and country !='Vietnam' and country !='New Zealand':
        upload_sql(engine=engine_dest, df=PlantWind, dest_sql='tbl_AID_Resources', existMethod='append')
    update_aid_id(dest_tbl='tbl_AID_Time_Series_Annual', dest_df=ts_annual_assumptions)

# </editor-fold>

# <editor-fold desc="Reload: AID_Emission table">

 # new plant also uploaded
EmissionPrice = pd.concat([EmissionPrice,tmp_EmissionPrice],axis = 0).drop_duplicates()
reload_tbl(engine=engine_dest, dest_tbl='tbl_AID_Emission_Prices', dest_df=EmissionPrice)
#upload_sql(engine=engine_dest, df=tmp_EmissionPrice, dest_sql='tbl_AID_Emission_Prices', existMethod='append')

EmissionRate = pd.concat([EmissionRate,tmp_EmissionRate],axis = 0).drop_duplicates()
reload_tbl(engine=engine_dest, dest_tbl='tbl_AID_Emission_Rates', dest_df=EmissionRate)
#upload_sql(engine=engine_dest, df=tmp_EmissionRate, dest_sql='tbl_AID_Emission_Rates', existMethod='append')



# </editor-fold>

# <editor-fold desc="NOT IN USE: Distributed Solar & Unserved Energy for Resource Table">
# Distributed solar to AID resource
# df_ts_dist, df_resource_dist = get_sql_distributed_capacity_to_aid(src_sql = 'APAC_DistributedCapacity_LIVE', country=country)
#
# # Add attribute assumptions
# Plant_Dist = assign_assumptions(df=df_resource_dist)
#
# Plant_Dist['Day'] = 1
# Plant_Dist['Month'] = 1
# Plant_Dist['Year'] = Plant_Dist['StartYear']
# Plant_Dist['Start'] = pd.to_datetime(Plant_Dist[['Day', 'Month', 'Year']])
# Plant_Dist['Day'] = 31
# Plant_Dist['Month'] = 12
# Plant_Dist['Year'] = 2100
# Plant_Dist['End'] = pd.to_datetime(Plant_Dist[['Day', 'Month', 'Year']])
#
# Plant_Dist = Plant_Dist[['PowerPlant', 'PlantType', 'FuelPri', 'Start', 'End', 'HeatRate', 'VOM', 'Zone', 'Market']]
# Plant_Dist.rename(columns={'PowerPlant': 'Name',
#                    'PlantType': 'Resource Group',
#                    'FuelPri': 'Fuel',
#                    'Start': 'Resource Begin Date',
#                    'End': 'Resource End Date',
#                    'HeatRate': 'Heat Rate',
#                    'Zone': 'zREM County',
#                    'Market': 'zREM State'}, inplace=True)
# Plant_Dist['Area'] = Plant_Dist['zREM County'].map(dict_zone)
# Plant_Dist['Resource Begin Date'] = pd.to_datetime(Plant_Dist['Resource Begin Date'])
# Plant_Dist['Resource End Date'] = pd.to_datetime(Plant_Dist['Resource End Date'])
#
# Plant_Dist.rename(columns={'VOM': 'Variable O&M'}, inplace=True)
# Plant_Dist['ID'] = Plant_Dist['Name']
# Plant_Dist['Capacity'] = 'yr_Capacity_Distributed_PV_' + Plant_Dist['zREM County']
#
# # get the Shape profile names
# Maint_mth['Fuel'] = pd.np.where(Maint_mth['ShapeName'].str.contains('Solar'), 'Sun_Dist', np.nan)
#
# Plant_Dist = apply_solar_wind_shapes_to_plants(df_maint_mth=Maint_mth, df_plant=Plant_Dist)
#
# update_aid_id(dest_tbl='tbl_AID_Resources', dest_df=Plant_Dist)

# Add unserved loads to AID resource
#Unserved_Energy_Columns = ['ID', 'Name', 'Area', 'Resource Group', 'zREM State', 'zREM County', 'Resource Begin Date', 'Resource End Date',
# 'Capacity', 'Heat Rate', 'Fuel']
#Unserved_Energy = pd.DataFrame(columns=Unserved_Energy_Columns)

#for province in df_area:
#    Unserved_Energy['Name'] = 'Unserved_Energy_' + df_area['Area Name']
#    Unserved_Energy['Area'] = df_area['Area Number']
#    Unserved_Energy['zREM County'] = df_area['Area Name']

#Unserved_Energy['Resource Group'] = 'DSM'
#Unserved_Energy['Resource Begin Date'] = '2000-01-01'
#Unserved_Energy['Resource End Date'] = '2100-12-31'
#Unserved_Energy['Capacity'] = 9999
#Unserved_Energy['Heat Rate'] = 3412
#Unserved_Energy['Fuel'] = 'UnservedLoad'
#Unserved_Energy['zREM State'] = country
#Unserved_Energy['ID'] = Unserved_Energy['Name']

#update_aid_id(dest_tbl='tbl_AID_Resources', dest_df=Unserved_Energy)
# </editor-fold>

# <editor-fold desc="Reload: AID_Fuel table">
#Fuel = Fuel.append({'Fuel ID': 'UnservedLoad',
#                    'Fuel Name': 'UnservedLoad',
#                    'Fuel Type': 'DSM',
#                    'Units' : 'US$/mmbtu',
#                    'Price' : 9999,
#                    'zREM Topology': 'All'}, ignore_index=True)

#Fuel = Fuel.append({'Fuel ID': 'DistributedSolar',
#                    'Fuel Name': 'DistributedSolar',
#                    'Fuel Type': 'DSM',
#                    'Units' : 'US$/mmbtu',
#                    'Price' : 'yr_Price_Sun',
#                    'zREM Topology': 'All'}, ignore_index=True)
FuelName = ['Storage', 'PumpedStorage', 'Sun_Dist']
FuelType = ['Storage', 'PS', 'Sun']
Topology = ['All', 'All', 'All']
Units = [np.nan, np.nan, 'US$/mmbtu']
Price = [np.nan, np.nan, 'yr_Price_Sun']
Fuel_Special = pd.DataFrame(list(zip(FuelName, FuelName, FuelType, Topology, Units, Price)),
                            columns=['Fuel ID', 'Fuel Name', 'Fuel Type', 'zREM Topology', 'Units', 'Price'])

reload_tbl(engine=engine_dest, dest_tbl='tbl_AID_Fuel', dest_df=Fuel)
upload_sql(engine=engine_dest, df=Fuel_Special, dest_sql='tbl_AID_Fuel', existMethod='append')

# Fuel Price Annual TS
TS_Annual_Price = pd.DataFrame(ts_annual_fuel)
TS_Annual_Price = TS_Annual_Price.drop(['Level', 'Fuel Name', 'Description', 'Fuel Type', 'Units', 'Price'], 1)
TS_Annual_Price.rename(columns={'Fuel ID': 'ID'}, inplace=True)
TS_Annual_Price['Use'] = 'Price'
TS_Annual_Price['ID'] = 'Price_' + TS_Annual_Price['ID']

update_aid_id(dest_tbl='tbl_AID_Time_Series_Annual', dest_df=TS_Annual_Price)
# </editor-fold>

# <editor-fold desc="***UPDATE AID_Resource table with matching AID_FuelID, HydroNumber, Storage & Battery Parameters">
# Re-match Fuel Prices to Resource table
src_sql = 'tbl_AID_Resources'
sql_qry = """SELECT * FROM """ + src_sql
df_aidResource = pd.read_sql_query(sql_qry, engine_dest)
df_aidResource['Fuel'] = df_aidResource['Fuel'].str.strip()
df_aidResource['zREM Primary'] = df_aidResource['Resource Group']
df_aidResource['zREM Secondary'] = df_aidResource['Fuel']
# Special ResourceGroups: Sun_Dist, Wind_Offshore, OW_BA, Wind_Onshore
df_aidResource['zREM Secondary'] = pd.np.where(df_aidResource['zREM Primary'] == 'PV_D', 'Sun_Dist', df_aidResource['zREM Secondary'])
df_aidResource['zREM Secondary'] = pd.np.where(df_aidResource['zREM Primary'].isin(['Wind_Onshore', 'WT_BA']),
                                               'Wind_Onshore', df_aidResource['zREM Secondary'])
df_aidResource['zREM Secondary'] = pd.np.where(df_aidResource['zREM Primary'].isin(['Wind_Offshore', 'OW_BA']),
                                               'Wind_Offshore', df_aidResource['zREM Secondary'])
if country != 'China':
    df_aidResource['zREM Secondary'] = pd.np.where(df_aidResource['zREM Secondary'].str.contains('LNG'), 'Gas', df_aidResource['zREM Secondary'])

#if (country == 'China') or (country == 'Malaysia'):
df_aidResource['Resource Group'] = df_aidResource['zREM Secondary'] + "_" + df_aidResource['zREM County']
#else:
#    df_aidResource['Resource Group'] = df_aidResource['zREM Secondary']


df_aidResource = match_aidResourceFuel_to_aidFuel(df_resource=df_aidResource, df_fuel=Fuel, colname='Fuel')
# Add secondary fuel for gas if it is still null (eg. new build)
df_aidResource['Second Fuel'] = pd.np.where((df_aidResource['Fuel'].str.contains('Gas') & df_aidResource['Second Fuel'].isnull()), 'LNG', df_aidResource['Second Fuel'])
df_resource = match_aidResourceFuel_to_aidFuel(df_resource=df_aidResource, df_fuel=Fuel, colname='Second Fuel')


if country == 'Vietnam':
    sql_qry = """SELECT [PowerPlant], [Region] FROM [vAPAC_PowerProjects_LIVE] WHERE [Market] = '""" + country + """' AND [Status] NOT IN (
    'Frozen', 'Temp Shut Down', 'Cancelled') AND [Zone] = 'South Vietnam'"""
    df_special = pd.read_sql_query(sql_qry, engine_src)
    df_special.drop_duplicates(inplace=True)
    df_resource = pd.merge(df_resource, df_special, how='left', left_on='Name', right_on='PowerPlant')
    df_resource['zREM Status'] = df_resource['Region']
    df_resource = df_resource.drop(['PowerPlant', 'Region'], 1)
    df_resource['Fuel'] = pd.np.where((df_resource['zREM Status'] == 'Southwest') & (df_resource['Fuel'] == 'South Vietnam_Gas'),
                                      'Southwest Vietnam_Gas', df_resource['Fuel'])
    df_resource['Second Fuel'] = pd.np.where((df_resource['zREM County'] == 'North Vietnam') & (df_resource['Fuel'] == 'North Vietnam_Coal'),
                                      'Vietnam_Coal', df_resource['Second Fuel'])

if country == 'Thailand':
    # Laos coal plant list
    sql_qry = """SELECT [PowerPlant], [Region] FROM [vAPAC_PowerProjects_LIVE] WHERE [Market] = '""" + country + """' AND [Status] NOT IN (
        'Frozen', 'Temp Shut Down', 'Cancelled') AND [Country] = 'Laos' AND [PlantType] = 'STCoal'"""
    df_laos = pd.read_sql_query(sql_qry, engine_src)
    df_laos.drop_duplicates(inplace=True)
    lst_laos_coal = df_laos['PowerPlant'].values.tolist()
    # Thailand plants to get Myanmar gas
    MMR_plant = ['Ratchaburi CC', 'Ratchaburi ST', 'Ratchaburi Power', 'TriEnergy', 'North Bangkok CC1', 'North Bangkok CC2']

    df_resource['Fuel'] = pd.np.where((df_resource['Fuel'] == 'Northeast Thailand_Coal') & (~df_resource['Name'].isin(lst_laos_coal)),
                                      'Thailand_Coal', df_resource['Fuel'])
    df_resource['Second Fuel'] = pd.np.where((df_resource['Fuel'] == 'North Thailand_Coal'),
                                             'Thailand_Coal', df_resource['Second Fuel'])
    df_resource['Fuel'] = pd.np.where((df_resource['Fuel'] == 'Central Thailand_Gas') & (~df_resource['Name'].isin(MMR_plant)),
                                      'South Thailand_Gas', df_resource['Fuel'])

# overwrite Fuel column for Storage & DSM & PS
df_resource['Fuel'] = pd.np.where(df_resource['Fuel'].str.contains('Storage'), 'Storage', df_resource['Fuel'])
df_resource['Fuel'] = pd.np.where(df_resource['Fuel'].str.contains('DSM'), 'DSM', df_resource['Fuel'])
df_resource['Fuel'] = pd.np.where(df_resource['zREM Primary'] == 'Hydro_PS', 'PumpedStorage', df_resource['Fuel'])

# add Hydro number column to resource table
df_resource['Hydro Number'] = pd.np.where(df_resource['Fuel'].isin(['Water', 'PumpedStorage']),
                                          'Area Name', df_resource['Hydro Number'])
df_resource['Hydro Number'] = pd.np.where(df_resource['Fuel'].str.contains('Water'),
                                          'Area Name', df_resource['Hydro Number'])

# add unique ID column as required by Aurora
df_resource['ID'] = df_resource.index + 1
df_resource['ID'] = df_resource['ID'].astype(str)
df_resource['ID'] = df_resource['ID'] + '_' + df_resource['Name']

# Battery parameters
if country != 'China': #if aggregation not used
    df_resource['Capacity'] = pd.to_numeric(df_resource['Capacity'], errors='coerce')
for batt in ba_list:
    df_resource['Recharge Capacity'] = pd.np.where(df_resource['zREM Primary'] == batt,
                                      df_resource['Capacity'],
                                      df_resource['Recharge Capacity'])

    if country == 'China':
        df_resource['Maximum Storage'] = pd.np.where(df_resource['zREM Primary'] == batt,
                                                     'yr_StorageMax_' + df_resource['Name'],
                                                     df_resource['Maximum Storage'])
    else:
        if batt == 'Hydro_PS':
            df_resource['Maximum Storage'] = pd.np.where(df_resource['zREM Primary'] == batt,
                                                         df_resource['Capacity'] * 12,
                                                         df_resource['Maximum Storage'])
        else:
            df_resource['Maximum Storage'] = pd.np.where(df_resource['zREM Primary'] == batt,
                                                         df_resource['Capacity']*4,
                                                         df_resource['Maximum Storage'])

    df_resource['Initial Contents'] = pd.np.where(df_resource['zREM Primary'] == batt,
                                      0.5,
                                      df_resource['Initial Contents'])

    # change StorageControlType from Price to Demand for Singapore & Thailand only
    if country == 'Thailand' or country == 'Singapore':
        df_resource['Storage Control Type'] = pd.np.where(df_resource['zREM Primary'] == batt,
                                          'Demand',
                                          df_resource['Storage Control Type'])
    else:
        df_resource['Storage Control Type'] = pd.np.where(df_resource['zREM Primary'] == batt,
                                                          'Price',
                                                          df_resource['Storage Control Type'])

    df_resource['Storage ID'] = pd.np.where(df_resource['zREM Primary'] == batt,
                                      df_resource['Name'],
                                      df_resource['Storage ID'])

# add storage ID
df_resource['Storage ID'] = pd.np.where(df_resource['zREM Primary'].isin(['BA']),
                                        'Stand_Alone',
                                        df_resource['Storage ID'])
df_resource['Storage ID'] = pd.np.where(df_resource['zREM Primary'].isin(['Hydro_PS']),
                                        np.nan,
                                        df_resource['Storage ID'])

# change Hydro_PS storage control from Price to Demand
if country == 'South Korea' or country == 'China' or country == 'Australia' or country == 'India':
    df_resource['Storage Control Type'] = pd.np.where(df_resource['zREM Primary'] == 'BA',
                                                      'DemandNetMR',
                                                      df_resource['Storage Control Type'])
    df_resource['Storage Control Type'] = pd.np.where(df_resource['zREM Primary'] == 'Hydro_PS',
                                                      'DemandNetMR',
                                                      df_resource['Storage Control Type'])
else:
    df_resource['Storage Control Type'] = pd.np.where(df_resource['zREM Primary'] == 'BA',
                                                      'DemandNetMR',
                                                      df_resource['Storage Control Type'])
    df_resource['Storage Control Type'] = pd.np.where(df_resource['zREM Primary'] == 'Hydro_PS',
                                                      'DemandNetMR',
                                                      df_resource['Storage Control Type'])

# Change parameters for PV_D, BA_PV, BA_OW and BA_WT
df_resource['Fuel'] = pd.np.where(df_resource['zREM Primary'] == 'PV_D',
                                      'Sun_Dist',
                                      df_resource['Fuel'])

# change the battery storage to time series data, not applicable to China
if country != 'China':
    Assumptions_sd = Assumptions[Assumptions['PlantAttribute'] == 'StorageDuration']
    # if specify to plant type level,then should change the merge on cols, only country level here
    df_resource_sd = pd.merge(df_resource, Assumptions_sd, how='left', left_on='Country', right_on='LevelName')
    df_resource_sd['Maximum Storage'] = np.where(
        (df_resource_sd['Maximum Storage'].notna()) & (df_resource_sd['zREM Secondary'] != 'PS'),
        df_resource_sd['Capacity'].astype('float') * df_resource_sd['Value'], df_resource_sd['Maximum Storage'])
    # generate the time series df
    ts_annual_assumptions_sd = df_resource_sd[
        (df_resource_sd['Maximum Storage'].notna()) & (df_resource_sd['zREM Secondary'] != 'PS')]



    ts_annual_assumptions_sd = ts_annual_assumptions_sd[['Name', 'PlantAttribute', 'PlantType', 'Maximum Storage', 'PlantOnlineYear']]
    ts_annual_assumptions_sd['Maximum Storage'] = ts_annual_assumptions_sd['Maximum Storage'].astype('float')
    ts_annual_assumptions_sd = pd.pivot_table(ts_annual_assumptions_sd, index=['Name', 'PlantType', 'PlantAttribute'],
                                              columns='PlantOnlineYear', values='Maximum Storage').reset_index()
    ts_annual_assumptions_sd.rename(columns={'Name': 'ID', 'PlantType': 'zREM Type', 'PlantAttribute': 'Use'},
                                    inplace=True)
    ts_annual_assumptions_sd['ID'] = 'StorageMax_' + ts_annual_assumptions_sd['ID']
    # change the max storage of BA to point string
    df_resource['Maximum Storage'] = np.where(
        (df_resource['Maximum Storage'].notna()) & (df_resource['zREM Secondary'] != 'PS'),
        'yr_StorageMax_' + df_resource['Name'],
        df_resource['Maximum Storage'])
df_resource = df_resource.drop(['Primary Key', 'Zone', 'Market', 'Country'], 1)

# Ad-hoc runtime calibration rules on 'Must Run' or 'Minimum Capacity' - subject to test and change
if country == 'China':
    df_resource['Must Run'] = pd.np.where(
        (df_resource['zREM Technology'].isin(['PV', 'PV_D', 'PV_BA', 'WT_BA', 'OW_BA', 'Wind_Onshore', 'Wind_Offshore'])),
        1, df_resource['Must Run'])
    df_resource['Minimum Capacity'] = pd.np.where(
        (df_resource['zREM Technology'].isin(['PV', 'PV_D', 'PV_BA', 'WT_BA', 'OW_BA','Wind_Onshore', 'Wind_Offshore'])),
        10, df_resource['Minimum Capacity'])

if country == 'Indonesia':
    df_resource['Minimum Capacity'] = pd.np.where((df_resource['zREM Primary'].isin(['CC'])),
                                                  40, df_resource['Minimum Capacity'])
    df_resource['Must Run'] = pd.np.where((df_resource['zREM Primary'].isin(['CC'])),
                                                  1, df_resource['Must Run'])

if country == 'Vietnam':
    df_resource['Minimum Capacity'] = pd.np.where((df_resource['zREM Primary'].isin(['CC']) &
                                                   df_resource['zREM County'].isin(['South Vietnam'])),
                                                  55, df_resource['Minimum Capacity'])
    df_resource['Must Run'] = pd.np.where((df_resource['zREM Primary'].isin(['CC']) &
                                           df_resource['zREM County'].isin(['South Vietnam'])),
                                          1, df_resource['Must Run'])

if country == 'Australia':
    df_resource['Minimum Capacity'] = pd.np.where((df_resource['zREM Secondary'].isin(['Gas']) &
                                                   df_resource['zREM County'].isin(['WA', 'VIC'])),
                                                  10, df_resource['Minimum Capacity'])
    df_resource['Must Run'] = pd.np.where((df_resource['zREM Secondary'].isin(['Gas']) &
                                           df_resource['zREM County'].isin(['WA', 'VIC'])),
                                          1, df_resource['Must Run'])

    df_resource['Minimum Capacity'] = pd.np.where((df_resource['zREM Secondary'].isin(['Gas']) &
                                                   df_resource['zREM County'].isin(['SA'])),
                                                  20, df_resource['Minimum Capacity'])
    df_resource['Must Run'] = pd.np.where((df_resource['zREM Secondary'].isin(['Gas']) &
                                           df_resource['zREM County'].isin(['SA'])),
                                          1, df_resource['Must Run'])

    df_resource['Minimum Capacity'] = pd.np.where((df_resource['zREM Secondary'].isin(['Diesel']) &
                                                   df_resource['zREM County'].isin(['WA'])),
                                                  50, df_resource['Minimum Capacity'])
    df_resource['Must Run'] = pd.np.where((df_resource['zREM Secondary'].isin(['Diesel']) &
                                           df_resource['zREM County'].isin(['WA'])),
                                          1, df_resource['Must Run'])

if country == 'India':
    df_resource['Minimum Capacity'] = pd.np.where((df_resource['zREM Secondary'].isin(['Gas'])),
                                                  10, df_resource['Minimum Capacity'])
    df_resource['Must Run'] = pd.np.where((df_resource['zREM Secondary'].isin(['Gas'])),
                                          1, df_resource['Must Run'])

if country == 'Taiwan':
    df_resource['Minimum Capacity'] = pd.np.where((df_resource['zREM Secondary'].isin(['FuelOil'])),
                                                  10, df_resource['Minimum Capacity'])
    df_resource['Must Run'] = pd.np.where((df_resource['zREM Secondary'].isin(['FuelOil'])),
                                          1, df_resource['Must Run'])
    df_resource['Must Run'] = pd.np.where(
        (df_resource['zREM Technology'].isin(['PV', 'PV_D', 'Wind_Onshore', 'Wind_Offshore'])),
        1, df_resource['Must Run'])
    df_resource['Minimum Capacity'] = pd.np.where(
        (df_resource['zREM Technology'].isin(['PV', 'PV_D', 'Wind_Onshore', 'Wind_Offshore'])),
        10, df_resource['Minimum Capacity'])
if country == 'Pakistan':
    df_resource['Minimum Capacity'] = pd.np.where((df_resource['zREM Secondary'].isin(['FuelOil'])),
                                                  10, df_resource['Minimum Capacity'])
    df_resource['Must Run'] = pd.np.where((df_resource['zREM Secondary'].isin(['FuelOil'])),
                                          1, df_resource['Must Run'])
    df_resource['Must Run'] = pd.np.where(
        (df_resource['zREM Technology'].isin(['PV', 'PV_D', 'Wind_Onshore', 'Wind_Offshore'])),
        1, df_resource['Must Run'])
    df_resource['Minimum Capacity'] = pd.np.where(
        (df_resource['zREM Technology'].isin(['PV', 'PV_D', 'Wind_Onshore', 'Wind_Offshore'])),
        10, df_resource['Minimum Capacity'])

df_resourceGroup = df_resource[['Resource Group', 'Resource Group', 'zREM Secondary', 'zREM County']].reset_index(drop=True)
df_resourceGroup.columns = ['Number', 'Name', 'Technology Name', 'Zone Name']
if not ((country == 'China') or (country == 'Malaysia')):
    df_resourceGroup['Zone Name'] = country
df_resourceGroup.drop_duplicates(inplace=True)
df_resourceGroup['Report'] = 0

if country != 'China':
    update_aid_id(dest_tbl='tbl_AID_Time_Series_Annual', dest_df=ts_annual_assumptions_sd)
reload_tbl(engine=engine_dest, dest_tbl='tbl_AID_Resource_Groups', dest_df=df_resourceGroup)
reload_tbl(engine=engine_dest, dest_tbl='tbl_AID_Resources', dest_df=df_resource)
# </editor-fold>

# <editor-fold desc="Reload: AID_Storage table">
Storage_Table = df_resource[['ID', 'Storage ID']].dropna().reset_index(drop=True)
Storage_Table = Storage_Table[Storage_Table['Storage ID'] != 'Stand_Alone'].reset_index(drop=True)
Storage_Table['ChargingSource'] = Storage_Table['Storage ID']
Storage_Table['ChargingSource'] = Storage_Table['ChargingSource'].str.replace('_Storage_BA_PV_', '_Sun_PV_BA_')
Storage_Table['ChargingSource'] = Storage_Table['ChargingSource'].str.replace('_Storage_BA_OW_', '_Wind_OW_BA_')
Storage_Table['ChargingSource'] = Storage_Table['ChargingSource'].str.replace('_Storage_BA_WT_', '_Wind_WT_BA_')
Storage_Table['ChargingSource'] = Storage_Table['ChargingSource'].str.replace('_BA_PV_', '_PV_BA_')
Storage_Table['ChargingSource'] = Storage_Table['ChargingSource'].str.replace('_BA_OW_', '_OW_BA_')
Storage_Table['ChargingSource'] = Storage_Table['ChargingSource'].str.replace('_BA_WT_', '_WT_BA_')

resource_lookup = df_resource[['ID', 'Name']].reset_index(drop=True)
resource_lookup.rename(columns={'Name': 'ChargingSource', 'ID': 'Charging Resource'}, inplace=True)

Storage_Table = pd.merge(Storage_Table, resource_lookup, how='left')

Storage_Table = Storage_Table[['Storage ID', 'Charging Resource']].reset_index(drop=True)
Storage_Table['Efficiency'] = 0.8
Storage_Table['Shaping Method'] = 'Price'
Storage_Table['Charging Availability'] = 1
Storage_Table['Generating Availability'] = 1

# Ben hard-coded script that creates the Storage table, which does not work once we aggregate the plants by fuel by type (new dynamic plant names)
# Storage_Table= pd.DataFrame(columns=['Storage ID','Efficiency','Shaping Method','Charging Availability','Generating Availability','Charging Resource','Primary Key'])
# Storage_ID = df_resource['Storage ID'].unique()
# Storage_ID = Storage_ID.tolist()
# Storage_ID = Storage_ID[1:]
# Storage_ID.remove('Stand_Alone')
# n = 0
#
# for unique_ba in Storage_ID:
#     n = n + 1
#     temporary_zone = unique_ba[:-10]
#     temporary_year = unique_ba[-5:]
#     temporary_type = unique_ba[-7:-5]
#     temporary_id = temporary_zone + temporary_type + '_BA' + temporary_year
#     charge_source = df_resource.loc[df_resource['Name'] == temporary_id]
#     charge_source = df_resource.loc[df_resource['Name'] == temporary_id, 'ID'].iloc[0]
#     Storage_Table = Storage_Table.append({'Storage ID': unique_ba, 'Efficiency': 0.8, 'Shaping Method': 'Price', 'Charging Availability': 1, 'Generating Availability': 1, 'Charging Resource': charge_source, 'Primary Key': n}, ignore_index=True)

Storage_Table = Storage_Table.append({'Storage ID': 'Stand_Alone', 'Efficiency': 0.8, 'Shaping Method': 'Price', 'Charging Availability': 1, 'Generating Availability': 1, 'Charging Resource': np.nan}, ignore_index=True)
Storage_Table['Primary Key'] = Storage_Table.index + 1

reload_tbl(engine=engine_dest, dest_tbl='tbl_AID_Storage', dest_df=Storage_Table)

# </editor-fold>

# <editor-fold desc="Reload: AID_Constraint table & Fuel table with Constraint IDs">
ts_annual_fuelmax, FuelMax = get_sql_fuel_max_to_aid(src_sql='vAPAC_Plant_Fuel_MinMax_Annual_LIVE')

reload_tbl(engine=engine_dest, dest_tbl='tbl_AID_Constraint', dest_df=FuelMax)

# Fuel Max Annual TS
TS_Annual_Max = pd.DataFrame(ts_annual_fuelmax)
TS_Annual_Max.rename(columns={'Set ID': 'ID',
                              'Constraint Type': 'Use'}, inplace=True)
TS_Annual_Max = TS_Annual_Max.drop(['Level', 'zREM Comment', 'Description', 'Fuel Type', 'Limit Units', 'Limit Type', 'Chronological Method', 'Limit'], 1)

update_aid_id(dest_tbl='tbl_AID_Time_Series_Annual', dest_df=TS_Annual_Max)

#Rematch Fuel_AID Table
src_sql = 'tbl_AID_Fuel'
sql_qry = """SELECT [Fuel ID], [Fuel Name], [Fuel Type], [Units], [Price], [zREM Topology] FROM """ + src_sql
Fuel = pd.read_sql_query(sql_qry, engine_dest)

#ts_annual_fuel, Fuel = get_sql_fuel_to_aid(src_sql='vAPAC_Plant_Fuel_Price_Annual_LIVE')

# Add distributed solar fuel in fuel aid
# Fuel_copy = Fuel.copy()
# Fuel_copy = Fuel_copy[Fuel_copy['Fuel ID'] == 'Sun']
# Fuel_copy['Fuel ID'] = Fuel_copy['Fuel ID'].replace(['Sun'], 'Sun_Dist')
# Fuel_copy['Fuel Type'] = Fuel_copy['Fuel Type'].replace(['Sun'], 'DSM')
# Fuel_copy['Fuel Name'] = Fuel_copy['Fuel Name'].replace(['Sun'], 'Sun_Dist')
#
# Fuel = Fuel.append(Fuel_copy)
# Fuel = Fuel.append({'Fuel ID': 'Storage',
#                     'Fuel Name': 'Storage',
#                     'Fuel Type': 'Storage',
#                     'zREM Topology': 'All'}, ignore_index=True)
# Fuel = Fuel.reset_index(drop=True)

#Add Fuel Constraint ID to Fuel_AID Table
FuelMax.rename(columns={'zREM Comment': 'Fuel ID'}, inplace=True)
FuelMax = FuelMax[['Fuel ID', 'Set ID', 'Constraint Type']].reset_index(drop=True)
FuelLimits = FuelMax.pivot_table(index=['Fuel ID'], columns='Constraint Type', values='Set ID', aggfunc='first')
FuelLimits = FuelLimits.reset_index()
FuelLimits['Set ID'] = pd.np.where((FuelLimits['Fuel Min'].notnull() & FuelLimits['Fuel Max'].notnull()),
                                   FuelLimits['Fuel Min'] + ', ' + FuelLimits['Fuel Max'],
                                   pd.np.where(FuelLimits['Fuel Min'].isnull(), FuelLimits['Fuel Max'], FuelLimits['Fuel Min']))
FuelLimits = FuelLimits[['Fuel ID', 'Set ID']].reset_index(drop=True)
Fuel = pd.merge(Fuel, FuelLimits, how='left', on=['Fuel ID'])
Fuel.rename(columns={'Set ID': 'Fuel Constraint ID'}, inplace=True)
if country == 'China' or country == 'India' or country == 'Australia' or country == 'Taiwan':
    Fuel = Fuel.append({'Fuel ID': 'Others',
                        'Fuel Name': 'Others',
                        'Fuel Type': 'Others',
                        'Units': 'US$/mmbtu',
                        'Price': 0.1,
                        'zREM Topology': 'All'}, ignore_index=True)
    Fuel = Fuel.reset_index(drop=True)

reload_tbl(engine=engine_dest, dest_tbl='tbl_AID_Fuel', dest_df=Fuel)

# </editor-fold>

# </editor-fold>



# <editor-fold desc="Fix -ve Balancing Capacities in Agnostic SQL DB">

# get manual balancing capacity splits between Fuel (Gas & LNG) or PlantTech(SubC & SC)
xw.Book('negativebalancing_soln.xlsx').set_mock_caller()
wb = xw.Book.caller()
sht = wb.sheets['BalancingEdits']
df = pd.DataFrame(sht.range('A1').expand().value)
x = df.iloc[0, :]
df.columns = x
df = df.iloc[1:, :].reset_index(drop=True)
df = df.fillna(method='ffill', axis=1)
df['StartYear'] = df['StartYear'].astype(int)
df['BalancingCapacity_MW'] = pd.to_numeric(df['BalancingCapacity_MW'])
df['Username'] = user
df['TimeStamp'] = timestamp

# load manual edit to tmp table
upload_sql(engine_src, df, 'tmp_Balancing', existMethod='replace')
# sql merge to adjust existing balancing capacities and add new ones
sql_merge = """MERGE [APAC_PowerProjects_Balancing_LIVE] AS [Target] 
            USING [tmp_Balancing] AS [Source] 
            ON ([Target].[Zone] = [Source].[Zone] 
            AND [Target].[PlantType-Tech] = [Source].[PlantType-Tech] 
            AND [Target].[FuelPri] = [Source].[FuelPri] 
            AND [Target].[StartYear] = [Source].[StartYear]) 
            WHEN MATCHED THEN UPDATE SET [Target].[BalancingCapacity_MW] = [Source].[BalancingCapacity_MW], 
            [Target].[Username] = [Source].[Username], [Target].[TimeStamp] = [Source].[TimeStamp] 
            WHEN NOT MATCHED THEN INSERT ([Zone], [PlantType-Tech], [PlantTech], [Group], [StartYear], [BalancingCapacity_MW], [FuelPri], 
            [PlantType], [Market], [Country], [Username], [TimeStamp]) 
            VALUES ([Source].[Zone], [Source].[PlantType-Tech], [Source].[PlantTech], [Source].[Group], [Source].[StartYear], 
            [Source].[BalancingCapacity_MW], [Source].[FuelPri], [Source].[PlantType], [Source].[Market], [Source].[Country], 
            [Source].[Username], [Source].[TimeStamp]);"""
execute_sqlcur(engine_src, sql=sql_merge)
# remove the tmp table from SQL Server
drop_sql = """IF OBJECT_ID ('WM_POWER_RENEWABLES..tmp_Balancing') IS NOT NULL
                    DROP TABLE tmp_Balancing"""
execute_sqlcur(engine_src, sql=drop_sql)


# Guangdong & Fujian & Guizhou CC-CCF balancing to use LNG instead of default Gas
sql_update = """UPDATE [APAC_PowerProjects_Balancing_LIVE] 
                SET [FuelPri] = 'LNG' WHERE [ZONE] IN ('Guangdong', 'Fujian', 'Guizhou') AND 
                [PlantType-Tech] = 'CC-CCF'"""
execute_sqlcur(engine_src, sql=sql_update)

# Beijing Coal balancing to use CHP_Coal-SubC instead of STCoal-SubC
sql_update = """UPDATE [APAC_PowerProjects_Balancing_LIVE] 
                SET [PlantType-Tech] = 'CHP_Coal-SubC' WHERE [ZONE] IN ('Beijing') AND 
                [PlantType-Tech] = 'STCoal-SubC'"""
execute_sqlcur(engine_src, sql=sql_update)

# Beijing & Hainan Gas balancing to use CHP_Gas-CCF instead of CC-CCF
sql_update = """UPDATE [APAC_PowerProjects_Balancing_LIVE] 
                SET [PlantType-Tech] = 'CHP_Gas-CCF', [PlantType] = 'CHP_Gas' WHERE [ZONE] IN ('Beijing', 'Hainan') AND [PlantType-Tech] = 'CC-CCF'"""
execute_sqlcur(engine_src, sql=sql_update)

# Jiangsu Gas balancing to use CHP_Gas-CCF instead of CC-CCF from 2011 onwards
sql_update = """UPDATE [APAC_PowerProjects_Balancing_LIVE] 
                SET [PlantType-Tech] = 'CHP_Gas-CCF', [PlantType] = 'CHP_Gas' WHERE [ZONE] IN ('Jiangsu') AND [PlantType-Tech] = 'CC-CCF' AND 
                [StartYear] >= 2011 """
execute_sqlcur(engine_src, sql=sql_update)

# Shanxi Gas balancing to use CHP_Gas-CCF instead of CC-CCF from 2014 onwards
sql_update = """UPDATE [APAC_PowerProjects_Balancing_LIVE] 
                SET [PlantType-Tech] = 'CHP_Gas-CCF', [PlantType] = 'CHP_Gas' WHERE [ZONE] IN ('Shanxi') AND [PlantType-Tech] = 'CC-CCF' AND 
                [StartYear] >= 2014 """
execute_sqlcur(engine_src, sql=sql_update)

# Chongqing STRenew balancing to use Waste instead of default Biomass
# sql_update = """UPDATE [APAC_PowerProjects_Balancing_LIVE]
#                 SET [FuelPri] = 'Waste' WHERE [ZONE] IN ('Chongqing') AND
#                 [PlantType-Tech] = 'STRenew'"""
# execute_sqlcur(engine_src, sql=sql_update)

# </editor-fold>

# <editor-fold desc="temp - clone operating rules between Aurora DB">
src_sql = 'tbl_AID_Operating_Rules'
sql_qry = """SELECT * FROM """ + src_sql
df_op = pd.read_sql_query(sql_qry, engine_dest)
df_op = df_op.drop(['Primary Key'], 1)

str_id = """('"""
for index, row in df_op.iterrows():
    print(row['Rule Value'])
    str_id += row['Rule Value'][3:] + """','"""
str_id = str_id[:-2] + ')'

src_sql = 'tbl_AID_Time_Series_Annual'
sql_qry = """SELECT * FROM """ + src_sql + """ WHERE [ID] IN """ + str_id
df_op_values = pd.read_sql_query(sql_qry, engine_dest)
df_op_values = df_op_values.drop(['Primary Key'], 1)

upload_sql(engine=engine_dest, df=df_op, dest_sql='tbl_AID_Operating_Rules', existMethod='append')
upload_sql(engine=engine_dest, df=df_op_values, dest_sql='tbl_AID_Time_Series_Annual', existMethod='append')
# </editor-fold>
