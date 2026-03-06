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
import argparse
import urllib
import pandas as pd

try:
    pd.set_option("future.no_silent_downcasting", True)
except Exception:
    pass
import numpy as np
import polars as pl
import os
from sqlalchemy import create_engine, event
from sqlalchemy.exc import OperationalError
import sqlalchemy
import getpass
import datetime as dt
import calendar
import re
import time
import hashlib
import json

country = 'China'
cycle = 'test2060'
debug_mode = False
global_dry_run = False
debug_run_ts = None
force_update = True
SQL_CHUNKSIZE = 2000
SQL_CHUNKSIZE_LARGE = 10000
SQL_CHUNKSIZE_LARGE_ROWS = 50000
SQL_UPLOAD_MAX_RETRIES = 3
SQL_UPLOAD_RETRY_SLEEP = 5
_country_tag = re.sub(r"[^A-Za-z0-9]+", "", country).lower()
HASH_CACHE_PATH = os.path.join("cache", f"write_hashes_{_country_tag}.json")
READ_HASH_CACHE_PATH = os.path.join("cache", f"read_hashes_{_country_tag}.json")
TS_ANNUAL_IDS_CACHE_PATH = os.path.join("cache", f"ts_annual_ids_{_country_tag}.json")
TS_ANNUAL_TRUNCATED = False
TS_ANNUAL_IDS_THIS_RUN = {}

# <editor-fold desc="Global Variables">
aid_country = re.sub(' ', '', country)
aurora_sqldb = 'ANVDEVSQLVPM01'
#aurora_sqldb = 'SIND8M3BR42\SINSQLINTDEV02'
#aurora_sqldb = 'BEID3LZ6132\BEISQLINTDEV01'
#aurora_dbname = 'Aurora_APAC_DEV_' + aid_country
aurora_dbname = 'Aurora_APAC_DEV_' + aid_country + '_test' #Allen test db
# aurora_dbname = 'Aurora_APAC_DEV'
input_dir = r'L:\Power_Renewables\Inputs'
hydro_xls = input_dir + r'\APAC_Hydro.xlsx'
assumptions_xls = input_dir + r'\APAC_Assumptions.xlsx'
fuels_xls = input_dir + r'\APAC_Fuels.xlsx'

ba_list = ['BA', 'BA_PV', 'BA_OW', 'BA_WT', 'Hydro_PS']
plant_att_tsAnnualAsumpt_list = ['FixedCost', 'EmissionRate_CO2', 'EmissionPrice_CO2']
yr_start = 2011
yr_end = 2060
yr_model = list(range(yr_start, yr_end +1))

# runtime globals (initialized in init_environment)
user = None
timestamp = None
datasetname = None
engine_src = None
engine_dest = None
dict_zone = None
dict_zone_mkt = None
dict_mkt_country = None
df_zone = None
df_area = None
Assumptions = None
TS_Weekly = None
TS_Monthly = None
Maint_mth = None
Maint_wk = None
ts_annual_fuel = None
Fuel = None
PlantExisting = None
Plant_New = None
EmissionRate = None
EmissionPrice = None
tmp_EmissionRate = None
tmp_EmissionPrice = None
tmp_wind_EmissionRate = None
tmp_wind_EmissionPrice = None
ts_annual_assumptions = None
TS_Annual_PlantExisting = None
TS_Annual_PlantNew = None
TS_StorageMax_Exist = None
TS_StorageMax_New = None
df_resource = None
df_resourceGroup = None
df_resources_raw = None
storage_table = None


def reset_runtime_state():
    global TS_Weekly, TS_Monthly, Maint_mth, Maint_wk
    global ts_annual_fuel, Fuel, PlantExisting, Plant_New
    global EmissionRate, EmissionPrice, tmp_EmissionRate, tmp_EmissionPrice
    global tmp_wind_EmissionRate, tmp_wind_EmissionPrice
    global ts_annual_assumptions, TS_Annual_PlantExisting, TS_Annual_PlantNew
    global TS_StorageMax_Exist, TS_StorageMax_New, df_resource, df_resourceGroup
    global df_resources_raw, storage_table
    TS_Weekly = None
    TS_Monthly = None
    Maint_mth = None
    Maint_wk = None
    ts_annual_fuel = None
    Fuel = None
    PlantExisting = None
    Plant_New = None
    EmissionRate = None
    EmissionPrice = None
    tmp_EmissionRate = None
    tmp_EmissionPrice = None
    tmp_wind_EmissionRate = None
    tmp_wind_EmissionPrice = None
    ts_annual_assumptions = None
    TS_Annual_PlantExisting = None
    TS_Annual_PlantNew = None
    TS_StorageMax_Exist = None
    TS_StorageMax_New = None
    df_resource = None
    df_resourceGroup = None
    df_resources_raw = None
    storage_table = None


def step_ready(step_name):
    if step_name == "topology":
        return df_zone is not None and df_area is not None
    if step_name == "shapes":
        return TS_Weekly is not None and TS_Monthly is not None and Maint_mth is not None and Maint_wk is not None
    if step_name == "plants_existing":
        return PlantExisting is not None and TS_Annual_PlantExisting is not None and TS_StorageMax_Exist is not None
    if step_name == "plants_newbuild":
        return Plant_New is not None and TS_Annual_PlantNew is not None and TS_StorageMax_New is not None
    if step_name == "resources":
        return (
            PlantExisting is not None and Plant_New is not None and
            TS_Annual_PlantExisting is not None and TS_Annual_PlantNew is not None and
            ts_annual_assumptions is not None and
            TS_StorageMax_Exist is not None and TS_StorageMax_New is not None
        )
    if step_name == "emission":
        return (
            EmissionRate is not None and EmissionPrice is not None and
            tmp_EmissionRate is not None and tmp_EmissionPrice is not None
        )
    if step_name == "fuel":
        return Fuel is not None and ts_annual_fuel is not None
    if step_name == "postprocess_resource":
        return Fuel is not None and df_resource is not None and df_resourceGroup is not None
    if step_name == "storage":
        return df_resource is not None
    return True
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
    df = df.drop(['dtm'], axis=1)

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
                leap_yr = True
            else:
                leap_yr = False
        else:
            leap_yr = True
    else:
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
            df[[attr + '_x', attr + '_y']] = df[[attr + '_x', attr + '_y']].ffill(axis=1).infer_objects(copy=False)
            df.rename(columns={attr + '_y': attr}, inplace=True)
            df = df.drop([attr + '_x'], axis=1)
        # left join by Market, RG, Yr
        df_asmpt_mkt = df_asmpt[df_asmpt['Level'] == 'ModelMarket'].reset_index(drop=True)
        if len(df_asmpt_mkt) > 0:
            df_asmpt_mkt = df_asmpt_mkt[['LevelName', 'Resource_Group', 'StartYear', attr]]
            df_asmpt_mkt.rename(columns={'LevelName': 'Market'}, inplace=True)
            df = pd.merge(df, df_asmpt_mkt, how='left', on=['Market', 'Resource_Group', 'StartYear'])
            df[[attr + '_x', attr + '_y']] = df[[attr + '_x', attr + '_y']].ffill(axis=1).infer_objects(copy=False)
            df.rename(columns={attr + '_y': attr}, inplace=True)
            df = df.drop([attr + '_x'], axis=1)
        # left join by Zone, RG, Yr
        df_asmpt_zone = df_asmpt[df_asmpt['Level'] == 'ModelZone'].reset_index(drop=True)
        if len(df_asmpt_zone) > 0:
            df_asmpt_zone = df_asmpt_zone[['LevelName', 'Resource_Group', 'StartYear', attr]]
            df_asmpt_zone.rename(columns={'LevelName': 'Zone'}, inplace=True)
            df = pd.merge(df, df_asmpt_zone, how='left', on=['Zone', 'Resource_Group', 'StartYear'])
            df[[attr + '_x', attr + '_y']] = df[[attr + '_x', attr + '_y']].ffill(axis=1).infer_objects(copy=False)
            df.rename(columns={attr + '_y': attr}, inplace=True)
            df = df.drop([attr + '_x'], axis=1)
        # left join by plant, RG, Yr
        df_asmpt_plant = df_asmpt[df_asmpt['Level'] == 'Plant'].reset_index(drop=True)
        if len(df_asmpt_plant) > 0:
            df_asmpt_plant = df_asmpt_plant[['LevelName', 'Resource_Group', 'StartYear', attr]]
            df_asmpt_plant.rename(columns={'LevelName': 'PowerPlant'}, inplace=True)
            df = pd.merge(df, df_asmpt_plant, how='left', on=['PowerPlant', 'Resource_Group', 'StartYear'])
            df[[attr + '_x', attr + '_y']] = df[[attr + '_x', attr + '_y']].ffill(axis=1).infer_objects(copy=False)
            df.rename(columns={attr + '_y': attr}, inplace=True)
            df = df.drop([attr + '_x'], axis=1)

    df.rename(columns={'PowerPlant': 'Name',
                       'Resource_Group': 'PlantType-Tech',
                       'Technology': 'PlantTech',
                       'Zone': 'zREM County',
                       'Market': 'zREM State'}, inplace=True)
    df = df.drop(['Country'], axis=1)

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
            df_asmpt_all = df_asmpt_all.drop(['LevelName', 'Level'], axis=1)
            ts_asmpt_all = df_asmpt_all.copy()
            ts_asmpt_all['zREM Topology'] = 'All'
            ts_asmpt_all.rename(columns={'Resource_Group': 'zREM Type'}, inplace=True)
            df_asmpt_all = df_asmpt_all[['Resource_Group', 'ID']].reset_index(drop=True)
            df_asmpt_all[attr] = 'yr_' + df_asmpt_all['ID']
            df_asmpt_all = df_asmpt_all.drop(['ID'], axis=1)
            #df = pd.merge(df, df_asmpt_all, how='left', on=['Resource_Group'])
            df[attr] =  df_asmpt_all[attr]
            df[attr] = df[attr].ffill(axis=0).infer_objects(copy=False)
            ts_asmpt = pd.concat([ts_asmpt, ts_asmpt_all], ignore_index=True, sort=True)

            # left join by Country, RG, Yr
            df_asmpt_country = df_asmpt[df_asmpt['Level'] == 'Country'].reset_index(drop=True)
            if len(df_asmpt_country) > 0:
                df_asmpt_country['ID'] = df_asmpt_country['Use'] + '_' + df_asmpt_country['LevelName'] + '_' + df_asmpt_country['Resource_Group']
                df_asmpt_country.drop(['Level'], axis=1, inplace=True)
                df_asmpt_country.rename(columns={'LevelName': 'Country'}, inplace=True)
                ts_asmpt_country = df_asmpt_country.copy()
                ts_asmpt_country.rename(columns={'Country': 'zREM Topology',
                                                 'Resource_Group': 'zREM Type'}, inplace=True)
                df_asmpt_country = df_asmpt_country[['Country', 'Resource_Group', 'ID']].reset_index(drop=True)
                df_asmpt_country[attr] = 'yr_' + df_asmpt_country['ID']
                df_asmpt_country = df_asmpt_country.drop(['ID'], axis=1)

                df = pd.merge(df, df_asmpt_country, how='left', on=['Country'])
                df[[attr + '_x', attr + '_y']] = df[[attr + '_x', attr + '_y']].ffill(axis=1).infer_objects(copy=False)
                df.rename(columns={attr + '_y': attr,
                                   'Resource_Group_x':'Resource_Group'}, inplace=True)
                df = df.drop([attr + '_x','Resource_Group_y'], axis=1)

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
            df_asmpt_all = df_asmpt_all.drop(['LevelName', 'Level'], axis=1)
            ts_asmpt_all = df_asmpt_all.copy()
            ts_asmpt_all['zREM Topology'] = 'All'
            ts_asmpt_all.rename(columns={'Resource_Group': 'zREM Type'}, inplace=True)
            df_asmpt_all = df_asmpt_all[['Resource_Group', 'ID']].reset_index(drop=True)
            df_asmpt_all[attr] = 'yr_' + df_asmpt_all['ID']
            df_asmpt_all = df_asmpt_all.drop(['ID'], axis=1)
            df = pd.merge(df, df_asmpt_all, how='left', on=['Resource_Group'])
            ts_asmpt = pd.concat([ts_asmpt, ts_asmpt_all], ignore_index=True, sort=True)

            # left join by Country, RG, Yr
            df_asmpt_country = df_asmpt[df_asmpt['Level'] == 'Country'].reset_index(drop=True)
            if len(df_asmpt_country) > 0:
                df_asmpt_country['ID'] = df_asmpt_country['Use'] + '_' + df_asmpt_country['LevelName'] + '_' + \
                                         df_asmpt_country['Resource_Group']
                df_asmpt_country = df_asmpt_country.drop(['Level'], axis=1)
                df_asmpt_country.rename(columns={'LevelName': 'Country'}, inplace=True)
                ts_asmpt_country = df_asmpt_country.copy()
                ts_asmpt_country.rename(columns={'Country': 'zREM Topology',
                                                 'Resource_Group': 'zREM Type'}, inplace=True)
                df_asmpt_country = df_asmpt_country[['Country', 'Resource_Group', 'ID']].reset_index(drop=True)
                df_asmpt_country[attr] = 'yr_' + df_asmpt_country['ID']
                df_asmpt_country = df_asmpt_country.drop(['ID'], axis=1)

                df = pd.merge(df, df_asmpt_country, how='left', on=['Country', 'Resource_Group'])
                df[[attr + '_x', attr + '_y']] = df[[attr + '_x', attr + '_y']].ffill(axis=1).infer_objects(copy=False)
                df.rename(columns={attr + '_y': attr}, inplace=True)
                df = df.drop([attr + '_x'], axis=1)

                ts_asmpt = pd.concat([ts_asmpt, ts_asmpt_country], ignore_index=True, sort=True)

            # left join by Market, RG, Yr
            df_asmpt_mkt = df_asmpt[df_asmpt['Level'] == 'ModelMarket'].reset_index(drop=True)
            if len(df_asmpt_mkt) > 0:
                df_asmpt_mkt['ID'] = df_asmpt_mkt['Use'] + '_' + df_asmpt_mkt['LevelName'] + '_' + df_asmpt_mkt[
                    'Resource_Group']
                df_asmpt_mkt = df_asmpt_mkt.drop(['Level'], axis=1)
                df_asmpt_mkt.rename(columns={'LevelName': 'Market'}, inplace=True)
                ts_asmpt_mkt = df_asmpt_mkt.copy()
                ts_asmpt_mkt.rename(columns={'Market': 'zREM Topology',
                                             'Resource_Group': 'zREM Type'}, inplace=True)
                df_asmpt_mkt = df_asmpt_mkt[['Market', 'Resource_Group', 'ID']].reset_index(drop=True)
                df_asmpt_mkt[attr] = 'yr_' + df_asmpt_mkt['ID']
                df_asmpt_mkt = df_asmpt_mkt.drop(['ID'], axis=1)

                df = pd.merge(df, df_asmpt_mkt, how='left', on=['Market', 'Resource_Group'])
                df[[attr + '_x', attr + '_y']] = df[[attr + '_x', attr + '_y']].ffill(axis=1).infer_objects(copy=False)
                df.rename(columns={attr + '_y': attr}, inplace=True)
                df = df.drop([attr + '_x'], axis=1)

                ts_asmpt = pd.concat([ts_asmpt, ts_asmpt_mkt], ignore_index=True, sort=True)

            # left join by Zone, RG, Yr
            df_asmpt_zone = df_asmpt[df_asmpt['Level'] == 'ModelZone'].reset_index(drop=True)
            if len(df_asmpt_zone) > 0:
                df_asmpt_zone['ID'] = df_asmpt_zone['Use'] + '_' + df_asmpt_zone['LevelName'] + '_' + df_asmpt_zone[
                    'Resource_Group']
                df_asmpt_zone = df_asmpt_zone.drop(['Level'], axis=1)
                df_asmpt_zone.rename(columns={'LevelName': 'Zone'}, inplace=True)
                ts_asmpt_zone = df_asmpt_zone.copy()
                ts_asmpt_zone.rename(columns={'Zone': 'zREM Topology',
                                              'Resource_Group': 'zREM Type'}, inplace=True)
                df_asmpt_zone = df_asmpt_zone[['Zone', 'Resource_Group', 'ID']].reset_index(drop=True)
                df_asmpt_zone[attr] = 'yr_' + df_asmpt_zone['ID']
                df_asmpt_zone = df_asmpt_zone.drop(['ID'], axis=1)

                df = pd.merge(df, df_asmpt_zone, how='left', on=['Zone', 'Resource_Group'])
                df[[attr + '_x', attr + '_y']] = df[[attr + '_x', attr + '_y']].ffill(axis=1).infer_objects(copy=False)
                df.rename(columns={attr + '_y': attr}, inplace=True)
                df = df.drop([attr + '_x'], axis=1)

                ts_asmpt = pd.concat([ts_asmpt, ts_asmpt_zone], ignore_index=True, sort=True)

            # left join by plant, RG, Yr
            df_asmpt_plant = df_asmpt[df_asmpt['Level'] == 'Plant'].reset_index(drop=True)
            if len(df_asmpt_plant) > 0:
                df_asmpt_plant['ID'] = df_asmpt_plant['Use'] + '_' + df_asmpt_plant['LevelName'] + '_' + df_asmpt_plant[
                    'Resource_Group']
                df_asmpt_plant = df_asmpt_plant.drop(['Level'], axis=1)
                df_asmpt_plant.rename(columns={'LevelName': 'PowerPlant'}, inplace=True)
                ts_asmpt_plant = df_asmpt_plant.copy()
                ts_asmpt_plant.rename(columns={'PowerPlant': 'zREM Topology',
                                               'Resource_Group': 'zREM Type'}, inplace=True)
                df_asmpt_plant = df_asmpt_plant[['PowerPlant', 'Resource_Group', 'ID']].reset_index(drop=True)
                df_asmpt_plant[attr] = 'yr_' + df_asmpt_plant['ID']
                df_asmpt_plant = df_asmpt_plant.drop(['ID'], axis=1)

                df = pd.merge(df, df_asmpt_plant, how='left', on=['PowerPlant', 'Resource_Group'])
                df[[attr + '_x', attr + '_y']] = df[[attr + '_x', attr + '_y']].ffill(axis=1).infer_objects(copy=False)
                df.rename(columns={attr + '_y': attr}, inplace=True)
                df = df.drop([attr + '_x'], axis=1)

                ts_asmpt = pd.concat([ts_asmpt, ts_asmpt_plant], ignore_index=True, sort=True)

    df.rename(columns={'PowerPlant': 'Name',
                       'Resource_Group': 'PlantType-Tech',
                       'Technology': 'PlantTech',
                       'Zone': 'zREM County',
                       'Market': 'zREM State'}, inplace=True)
    df = df.drop(['Country'], axis=1)
    # ts_asmpt = ts_asmpt.drop([ 2061,            2062,            2063,
    #               2064,            2065,            2066,            2067,
    #               2068,            2069,            2070,            2071,
    #               2072,            2073,            2074,            2075], axis=1)

    return df, ts_asmpt

def apply_emissions(df):
    if df is None or df.empty:
        return df, pd.DataFrame(), pd.DataFrame()
    df.rename(columns={'EmissionRate_CO2': 'EmissionRate',
                       'EmissionPrice_CO2': 'EmissionPrice',
                      }, inplace=True)
    if 'EmissionRate' not in df.columns:
        df['EmissionRate'] = np.nan
    if 'EmissionPrice' not in df.columns:
        df['EmissionPrice'] = np.nan
    df['EmissionRate'] = 'ER_' + df['EmissionRate'].astype(str).str.split('_', n=1).str[-1]
    #comments out when the price data is ready with planttype
    df['EmissionPrice'] = 'ER_' + df['EmissionPrice'].astype(str).str.split('_', n=1).str[-1]

    emission_rate = df[['Name', 'EmissionRate']].copy() # can add other cols if needed
    emission_rate['ID'] =  emission_rate['EmissionRate']
    emission_rate['EmissionRate'] = 'yr_' + emission_rate['EmissionRate'].astype(str).str.split('_', n=1).str[-1]
    emission_rate.rename(columns ={'EmissionRate':'Rate'}, inplace = True)
    emission_rate['Name'] = 'CO2'
    emission_rate = emission_rate.drop_duplicates()
    emissionrate = emission_rate[['ID','Name', 'Rate']]



    emission_price = df[['Name', 'EmissionPrice']].copy() # can add other cols if needed
    emission_price['ID'] = emission_price['EmissionPrice']
    emission_price['EmissionPrice'] = 'yr_' + emission_price['EmissionPrice'].astype(str).str.split('_', n=1).str[-1]
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
    remap = remap.drop(['Area'], axis=1)
    df_resource = pd.merge(df_resource, remap, how='left', on=colname)
    # check df_resource now; 'AID_FuelName' column that has nan indicates we cannot find the Plant's fuel in AID_Fuel table
    # so one level lower is Country, so according to agreed naming convention, we look for "Country_Fuel" now

    # left join Country & Fuel
    # df_resource['fuel_lookup'] = np.where(df_resource['AID_FuelName'].isnull(),
    #                                           df_resource['Country'] + '_' + df_resource[colname],
    #                                           df_resource['AID_FuelName'])
    df_resource['fuel_lookup'] = df_resource['Country'] + '_' + df_resource[colname]
    remap = remap_fuel[remap_fuel['Area'] != 'All'].reset_index(drop=True)
    remap.columns = ['fuel_lookup', 'area_fuel', 'Country']
    df_resource = pd.merge(df_resource, remap, how='left', on=['Country', 'fuel_lookup'])
    df_resource.rename(columns={'area_fuel': 'country_fuel_found'}, inplace=True)

    # left join Market & Fuel
    # df_resource['fuel_lookup'] = np.where(df_resource['AID_FuelName'].isnull(),
    #                                           df_resource['Market'] + '_' + df_resource[colname],
    #                                           df_resource['AID_FuelName'])
    df_resource['fuel_lookup'] = df_resource['Market'] + '_' + df_resource[colname]
    remap.columns = ['fuel_lookup', 'area_fuel', 'Market']
    df_resource = pd.merge(df_resource, remap, how='left', on=['Market', 'fuel_lookup'])
    df_resource.rename(columns={'area_fuel': 'market_fuel_found'}, inplace=True)

    # left join Zone & Fuel
    # df_resource['fuel_lookup'] = np.where(df_resource['AID_FuelName'].isnull(),
    #                                           df_resource['Zone'] + '_' + df_resource[colname],
    #                                           df_resource['AID_FuelName'])
    df_resource['fuel_lookup'] = df_resource['Zone'] + '_' + df_resource[colname]
    remap.columns = ['fuel_lookup', 'area_fuel', 'Zone']
    df_resource = pd.merge(df_resource, remap, how='left', on=['Zone', 'fuel_lookup'])
    df_resource.rename(columns={'area_fuel': 'zone_fuel_found'}, inplace=True)

    # left join Plant & Fuel
    # df_resource['fuel_lookup'] = np.where(df_resource['AID_FuelName'].isnull(),
    #                                           df_resource['Name'] + '_' + df_resource[colname],
    #                                           df_resource['AID_FuelName'])
    df_resource['fuel_lookup'] = df_resource['Name'] + '_' + df_resource[colname]
    remap.columns = ['fuel_lookup', 'area_fuel', 'Name']
    df_resource = pd.merge(df_resource, remap, how='left', on=['Name', 'fuel_lookup'])
    df_resource.rename(columns={'area_fuel': 'plant_fuel_found'}, inplace=True)

    # keep [AID_FuelName, country_fuel_found, market_fuel_found, zone_fuel_found, plant_fuel_found] then ffill to right then drop the rest
    df_resource = df_resource.drop(['fuel_lookup'], axis=1)
    df_resource[[colname, 'AID_FuelName', 'country_fuel_found', 'market_fuel_found', 'zone_fuel_found', 'plant_fuel_found']] = df_resource[[colname,
        'AID_FuelName', 'country_fuel_found', 'market_fuel_found', 'zone_fuel_found', 'plant_fuel_found']].ffill(axis=1).infer_objects(copy=False)
    df_resource[colname] = df_resource['plant_fuel_found']
    df_resource = df_resource.drop(['AID_FuelName', 'country_fuel_found', 'market_fuel_found', 'zone_fuel_found', 'plant_fuel_found'], axis=1)

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
    df_plantlist = df_plantlist.drop(['Day', 'Month', 'Year'], axis=1)

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
            df_plant_blk = df_plant_blk.copy()
            print(f'Grouping {name_prefix} projects...block {idx + 1}... {startyr} : {endyr}')
            df_list.append(df_plant_blk)

    df_existing_cummulative = []
    for id, df_plant_blk in enumerate(df_list):
        # df_plant_blk = df_list[0]

        print(f'Aggregating {name_prefix} plants...block {id + 1}...')
        for col in ['Resource_Group', 'zREM Technology', 'Fuel', 'zREM County', 'zREM State', 'Resource Group']:
            df_plant_blk[col] = df_plant_blk[col].astype(str).str.strip()

        frames = [pd.DataFrame({
        'Yr'             : pd.date_range(row['ResourceBeginDate'], row['ResourceEndDate'], freq='YE'),
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
        for i, row in df_plant_blk.iterrows()]
        frames = [f for f in frames if not f.empty and not f.isna().all(axis=None)]
        df_ppA = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(
            columns=['Yr', 'Name', 'Resource_Group', 'zREM Technology', 'Fuel', 'Capacity', 'zREM County', 'zREM State',
                     'Area', 'Resource Group']
        )
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

                Balancing_MW['zREM Technology'] = np.where(Balancing_MW['Resource_Group'] == 'CHP_Gas',
                                                              'CCF', Balancing_MW['zREM Technology'])
                Balancing_MW['zREM Technology'] = np.where(Balancing_MW['Resource_Group'] == 'CHP_Coal',
                                                              'SubC', Balancing_MW['zREM Technology'])
                Balancing_MW['Resource_Group'] = np.where(Balancing_MW['Resource_Group'] == 'CHP_Gas',
                                                             Balancing_MW['Resource_Group'] + '-' + Balancing_MW['zREM Technology'],
                                                             Balancing_MW['Resource_Group'])
                Balancing_MW['Resource_Group'] = np.where(Balancing_MW['Resource_Group'] == 'CHP_Coal',
                                                             Balancing_MW['Resource_Group'] + '-' + Balancing_MW['zREM Technology'],
                                                             Balancing_MW['Resource_Group'])

                Balancing_MW['zREM Technology'] = np.where(Balancing_MW['Resource_Group'] == 'Nuclear',
                                                              'PWR', Balancing_MW['zREM Technology'])
                Balancing_MW['zREM Technology'] = np.where(Balancing_MW['Resource_Group'] == 'STRenew',
                                                              'ST', Balancing_MW['zREM Technology'])
                Balancing_MW['zREM Technology'] = np.where(Balancing_MW['Resource_Group'] == 'STFuelOil',
                                                              'ST', Balancing_MW['zREM Technology'])
                Balancing_MW['zREM Technology'] = np.where(Balancing_MW['Resource_Group'] == 'Hydro_PS',
                                                              'Hydro', Balancing_MW['zREM Technology'])

                Balancing_MW['zREM Technology'] = np.where(Balancing_MW['zREM Technology'].isnull(),
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

                df_existing_cum = df_existing_cum.drop(['Capacity_x', 'Capacity_y'], axis=1)
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
    # 对于相同(grouping, StartYear)的记录取最大endyr
    df_existing_cummulative['endyr'] = df_existing_cummulative.groupby(
        ['Resource_Group', 'zREM Technology', 'Fuel', 'zREM County', 'zREM State', 'Area', 'Resource Group', 'StartYear']
    )['endyr'].transform('max')

    # when aggregating, create Storage Max Annual_TS for battery types
    # Calculate StorageMax BEFORE filtering out Capacity==0 rows,
    # so that BA types with Capacity=0 still get a StorageMax row (with value 0)
    df_existing_cummulative['StorageMaxCap'] = np.where(df_existing_cummulative['Resource Group'].isin(ba_list), df_existing_cummulative[
        'Capacity'] * 4, np.nan)
    df_existing_cummulative['StorageMaxCap'] = np.where(df_existing_cummulative['Resource Group'] == 'Hydro_PS' , df_existing_cummulative[
        'Capacity'] * 24, df_existing_cummulative['StorageMaxCap'])
    df_storagemax_TS = pd.pivot_table(df_existing_cummulative,
                   index=['Resource_Group', 'zREM Technology', 'Fuel', 'zREM County', 'zREM State', 'Area', 'Resource Group', 'StartYear', 'endyr'],
                   columns='Yr', values='StorageMaxCap', fill_value=0).reset_index()

    # Filter out Capacity==0 rows AFTER StorageMax pivot (so BA types with 0 capacity still get StorageMax entries)
    # Keep BA types (BA, BA_PV, BA_WT, BA_OW, Hydro_PS) even if Capacity==0, as they serve as resource placeholders
    df_existing_cummulative = df_existing_cummulative[
        (df_existing_cummulative['Capacity'] != 0) | (df_existing_cummulative['Resource Group'].isin(ba_list))
    ]
    if len(df_storagemax_TS) > 0 :
        df_storagemax_TS['StartYear'] = df_storagemax_TS['StartYear'].astype('int')
        df_storagemax_TS['StartYear'] = np.where(df_storagemax_TS['StartYear'] < 2000, 2000, df_storagemax_TS['StartYear'])
        df_storagemax_TS = df_storagemax_TS[df_storagemax_TS['StartYear'] <= yr_end]
        df_storagemax_TS['Name'] = name_prefix + '_' + df_storagemax_TS['zREM County'] + '_' + df_storagemax_TS['Fuel'] \
                                          + '_' + df_storagemax_TS['Resource_Group'] + '_' + df_storagemax_TS['StartYear'].astype(str)
        df_storagemax_TS['ID'] = 'StorageMax_' + df_storagemax_TS['Name']
        df_storagemax_TS['Use'] = 'AggregatedStorageMax'
        df_storagemax_TS['zREM Type'] = 'StorageMaxCapacity'
        df_storagemax_TS = df_storagemax_TS.drop(['Name', 'Resource_Group', 'zREM Technology', 'Fuel', 'zREM County', 'zREM State', 'Area',
                                                  'Resource Group', 'StartYear', 'endyr'], axis=1)
        value_cols = [c for c in df_storagemax_TS.columns if c not in ('ID', 'Use', 'zREM Type')]
        df_storagemax_TS = df_storagemax_TS.groupby('ID', as_index=False)[value_cols].max()
        df_storagemax_TS['Use'] = 'AggregatedStorageMax'
        df_storagemax_TS['zREM Type'] = 'StorageMaxCapacity'


    df_existing_cummulative = pd.pivot_table(df_existing_cummulative,
                   index=['Resource_Group', 'zREM Technology', 'Fuel', 'zREM County', 'zREM State', 'Area', 'Resource Group', 'StartYear', 'endyr'],
                   columns='Yr', values='Capacity', fill_value=0).reset_index()
    # x = df_existing_cummulative[df_existing_cummulative['zREM County'] == 'Sichuan']

    df_existing_cummulative['StartYear'] = df_existing_cummulative['StartYear'].astype('int')
    df_existing_cummulative['StartYear'] = np.where(df_existing_cummulative['StartYear'] < 2000, 2000, df_existing_cummulative['StartYear'])
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
    df_existing_cummulative = df_existing_cummulative.drop(['Day', 'Month', 'Year', 'endyr'], axis=1)
    df_existing_cummulative['Capacity'] = 'yr_Capacity_' + df_existing_cummulative['Name']
    df_plant_agg = df_existing_cummulative[['Name', 'Resource_Group', 'zREM Technology', 'Fuel', 'ResourceBeginDate', 'ResourceEndDate', 'Capacity',
       'zREM County', 'zREM State', 'Area', 'Resource Group', 'StartYear']].reset_index(drop=True)
    df_plant_agg['Second Fuel'] = np.nan
    df_plant_agg['Heat Rate'] = np.nan
    # If the same Name appears with different end dates, keep the latest end date
    if 'Name' in df_plant_agg.columns and 'ResourceEndDate' in df_plant_agg.columns:
        df_plant_agg = df_plant_agg.sort_values('ResourceEndDate').drop_duplicates(subset=['Name'], keep='last').reset_index(drop=True)
    df_existing_cummulative['ID'] = 'Capacity_' + df_existing_cummulative['Name']
    df_existing_cummulative['Use'] = 'AggregatedCapacity'
    df_existing_cummulative['zREM Type'] = 'PlantCapacity'
    df_existing_cummulative = df_existing_cummulative.drop(['Name', 'Resource_Group', 'zREM Technology', 'Fuel', 'ResourceBeginDate',
                                                            'ResourceEndDate', 'Capacity', 'zREM County', 'zREM State', 'Area',
                                                            'Resource Group', 'StartYear'], axis=1)
    # Deduplicate annual TS rows by ID (keep max across years)
    if 'ID' in df_existing_cummulative.columns:
        value_cols = [c for c in df_existing_cummulative.columns if c not in ('ID', 'Use', 'zREM Type')]
        df_existing_cummulative = df_existing_cummulative.groupby('ID', as_index=False)[value_cols].max()
        df_existing_cummulative['Use'] = 'AggregatedCapacity'
        df_existing_cummulative['zREM Type'] = 'PlantCapacity'

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

def _strip_brackets(name):
    return name.replace('[', '').replace(']', '')

def _split_table_name(table_name):
    table_name = _strip_brackets(table_name)
    parts = [p for p in table_name.split('.') if p]
    if len(parts) >= 2:
        schema = parts[-2]
        table = parts[-1]
    else:
        schema = 'dbo'
        table = parts[0]
    return schema, table

def _get_identity_columns(engine, table_name):
    schema, table = _split_table_name(table_name)
    sql = """
    SELECT c.name AS column_name
    FROM sys.columns c
    JOIN sys.tables t ON c.object_id = t.object_id
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE t.name = ? AND s.name = ?
      AND COLUMNPROPERTY(c.object_id, c.name, 'IsIdentity') = 1;
    """
    try:
        df_cols = pd.read_sql_query(sql, engine, params=[table, schema])
        return df_cols['column_name'].tolist()
    except Exception:
        return []

def _get_identity_columns_inspector(engine, table_name):
    try:
        schema, table = _split_table_name(table_name)
        inspector = sqlalchemy.inspect(engine)
        cols = inspector.get_columns(table, schema=schema)
        identity_cols = []
        for col in cols:
            identity_info = col.get("identity")
            if identity_info or col.get("autoincrement") is True:
                identity_cols.append(col.get("name"))
        return identity_cols
    except Exception:
        return []

def _get_table_columns(engine, table_name):
    schema, table = _split_table_name(table_name)
    sql = f"SELECT TOP 0 * FROM [{schema}].[{table}]"
    try:
        df_cols = pd.read_sql_query(sql, engine)
        cols = df_cols.columns.tolist()
        if cols:
            return cols
    except Exception:
        cols = []
    # Fallback to INFORMATION_SCHEMA if TOP 0 failed or returned empty.
    try:
        sql_info = """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
        """
        df_info = pd.read_sql_query(sql_info, engine, params=[schema, table])
        return df_info["COLUMN_NAME"].tolist()
    except Exception:
        return []

def _align_to_table_columns(engine, table_name, df):
    table_cols = _get_table_columns(engine, table_name)
    if not table_cols:
        log(f"align cols for {table_name}: no table columns found; skip align")
        return df
    df = df.copy()
    df.columns = [str(c) for c in df.columns]
    table_cols = [str(c) for c in table_cols]
    extra_cols = [c for c in df.columns if c not in table_cols]
    if extra_cols:
        log(f"align cols for {table_name}: drop extras {extra_cols}")
        df = df.drop(columns=extra_cols)
    ordered_cols = [c for c in table_cols if c in df.columns]
    return df[ordered_cols]

def _filter_year_range(df, year_start, year_end):
    if df is None or (year_start is None and year_end is None):
        return df
    df = df.copy()

    def _in_range(series):
        s = pd.to_numeric(series, errors="coerce")
        if year_start is not None and year_end is not None:
            return s.isna() | ((s >= year_start) & (s <= year_end))
        if year_start is not None:
            return s.isna() | (s >= year_start)
        return s.isna() | (s <= year_end)

    year_cols = [
        "Year",
        "Yr",
        "Demand Year",
        "Hydro Year",
        "PlantOnlineYear",
        "StartYear",
        "Model Start Year",
    ]
    for col in year_cols:
        if col in df.columns:
            df = df[_in_range(df[col])]

    def _is_year_col(col):
        if isinstance(col, int):
            return 1900 <= col <= 2200
        if isinstance(col, str) and col.isdigit() and len(col) == 4:
            return 1900 <= int(col) <= 2200
        return False

    year_data_cols = [c for c in df.columns if _is_year_col(c)]
    if year_data_cols:
        keep_cols = []
        for c in year_data_cols:
            yr = int(c) if isinstance(c, (int,)) else int(str(c))
            if year_start is not None and yr < year_start:
                continue
            if year_end is not None and yr > year_end:
                continue
            keep_cols.append(c)
        drop_cols = [c for c in year_data_cols if c not in keep_cols]
        if drop_cols:
            df = df.drop(columns=drop_cols)

    return df

def _skip_year_filter_for_table(dest_sql):
    # Do not clip year columns for time series annual or plant list outputs.
    if not dest_sql:
        return False
    name = dest_sql.lower()
    return name in ("tbl_aid_time_series_annual", "tbl_aid_resources")

def _get_column_info(engine, table_name, column_name):
    schema, table = _split_table_name(table_name)
    sql = """
    SELECT c.name AS column_name,
           c.is_nullable AS is_nullable,
           COLUMNPROPERTY(c.object_id, c.name, 'IsIdentity') AS is_identity
    FROM sys.columns c
    JOIN sys.tables t ON c.object_id = t.object_id
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE t.name = ? AND s.name = ? AND c.name = ?;
    """
    try:
        df_cols = pd.read_sql_query(sql, engine, params=[table, schema, column_name])
        if df_cols.empty:
            return None
        row = df_cols.iloc[0]
        return {
            "column_name": row["column_name"],
            "is_nullable": bool(row["is_nullable"]),
            "is_identity": bool(row["is_identity"]),
        }
    except Exception:
        return None

def _ensure_primary_key(engine, table_name, df, exist_method):
    schema, table = _split_table_name(table_name)
    qualified = f"[{schema}].[{table}]"
    info = _get_column_info(engine, table_name, 'Primary Key')
    identity_cols = _get_identity_columns(engine, table_name)
    if not info:
        table_cols = _get_table_columns(engine, table_name)
        if 'Primary Key' not in table_cols:
            return df
        if 'Primary Key' in identity_cols:
            return df
    elif info["is_identity"]:
        return df

    df = df.copy()
    max_pk_db = 0
    if exist_method == 'append':
        try:
            sql_max_pk = f"SELECT ISNULL(MAX([Primary Key]), 0) AS max_pk FROM {qualified}"
            df_max_pk = pd.read_sql_query(sql_max_pk, engine)
            max_pk_db = int(df_max_pk['max_pk'].iloc[0]) if not df_max_pk.empty else 0
        except Exception:
            max_pk_db = 0

    if 'Primary Key' in df.columns:
        pk_numeric = pd.to_numeric(df['Primary Key'], errors='coerce')
        max_pk_df = int(pk_numeric.dropna().max()) if pk_numeric.notna().any() else 0
        start_pk = max(max_pk_db, max_pk_df) if exist_method == 'append' else max_pk_df
        if pk_numeric.isna().any():
            new_pks = range(start_pk + 1, start_pk + 1 + pk_numeric.isna().sum())
            pk_filled = pk_numeric.copy()
            pk_filled.loc[pk_numeric.isna()] = list(new_pks)
            df['Primary Key'] = pk_filled.astype(int)
        else:
            df['Primary Key'] = pk_numeric.astype(int)
    else:
        start_pk = max_pk_db if exist_method == 'append' else 0
        df['Primary Key'] = range(start_pk + 1, start_pk + 1 + len(df))

    return df

def execute_sqlcur(engine, sql):
    """

    Executes SQL statement via the SQL Cursor

    Args:
        engine  : engine created from sqlalchemy that connects to MS SQL server
        sql     : SQL statement to be executed

    Returns     : nothing
    """

    if debug_mode:
        log("debug: skip SQL execute (write) statement")
        return
    connx = engine.raw_connection()
    cursor = connx.cursor()
    cursor.execute(sql)
    connx.commit()
    cursor.close()
    return

def upload_sql(engine, df, dest_sql, existMethod, skip_hash_check=False, module_key=None, skip_id_delete=False):
    """

    Function that uploads df to SQL Server using Pandas' DataFrame.to_sql()

    Args:
        engine      : engine created from sqlalchemy that connects to MS SQL server
        df          : the dataframe to be uploaded
        dest_sql    : the destination table name in SQL Server
        existMethod : 'append' or 'replace', the method to use if table already exists in SQL Server

    Returns         : nothing
    """

    df_to_upload = df
    if not _skip_year_filter_for_table(dest_sql):
        df_to_upload = _filter_year_range(df, demand_year_start, demand_year_end)
    if debug_write_csv(df_to_upload, dest_sql):
        return

    ids = []
    if dest_sql == 'tbl_AID_Time_Series_Annual' and df_to_upload is not None and 'ID' in df_to_upload.columns:
        ids = [i for i in df_to_upload['ID'].dropna().unique().tolist() if str(i).strip() != ""]
        _register_ts_annual_ids(module_key, ids)

    if not skip_hash_check and should_skip_write(dest_sql, df_to_upload):
        return

    global TS_ANNUAL_TRUNCATED
    if dest_sql == 'tbl_AID_Time_Series_Annual' and force_update and not TS_ANNUAL_TRUNCATED:
        if debug_mode:
            log_step("upload_sql", "debug: skip truncate tbl_AID_Time_Series_Annual (first write)")
        else:
            log_step("upload_sql", "truncate tbl_AID_Time_Series_Annual (first write)")
            execute_sqlcur(engine=engine_dest, sql="TRUNCATE TABLE tbl_AID_Time_Series_Annual")
        TS_ANNUAL_TRUNCATED = True

    if dest_sql == 'tbl_AID_Time_Series_Annual' and not force_update and not debug_mode and not skip_id_delete:
        if ids:
            _delete_ids_in_chunks(dest_sql, ids)

    df_to_upload = _ensure_primary_key(engine, dest_sql, df_to_upload, existMethod)
    df_to_upload = _align_to_table_columns(engine, dest_sql, df_to_upload)
    identity_cols = _get_identity_columns(engine, dest_sql)
    if not identity_cols:
        identity_cols = _get_identity_columns_inspector(engine, dest_sql)
    if not identity_cols:
        pk_info = _get_column_info(engine, dest_sql, 'Primary Key')
        if pk_info and pk_info.get("is_identity"):
            identity_cols = ['Primary Key']
    if identity_cols:
        drop_cols = [c for c in identity_cols if c in df_to_upload.columns]
        if drop_cols:
            log(f"drop identity cols for {dest_sql}: {drop_cols}")
            df_to_upload = df_to_upload.drop(columns=drop_cols)

    df_dict = sqlcoldict_dt(df_to_upload)
    row_count = len(df_to_upload)
    chunksize = SQL_CHUNKSIZE_LARGE if row_count >= SQL_CHUNKSIZE_LARGE_ROWS else SQL_CHUNKSIZE
    if row_count >= SQL_CHUNKSIZE_LARGE_ROWS:
        log_step("upload_sql", f"{dest_sql}: rows={row_count} use chunksize={chunksize}")
    def _is_comm_failure(err):
        msg = str(err)
        return (
            "08S01" in msg
            or "Communication link failure" in msg
            or "TCP Provider" in msg
            or "10060" in msg
        )

    attempt = 0
    while True:
        try:
            with engine.begin() as conn:
                df_to_upload.to_sql(
                    dest_sql,
                    con=conn,
                    if_exists=existMethod,
                    index=False,
                    chunksize=chunksize,
                    dtype=df_dict,
                )
            break
        except OperationalError as exc:
            if _is_comm_failure(exc) and attempt < SQL_UPLOAD_MAX_RETRIES:
                attempt += 1
                log_step(
                    "upload_sql",
                    f"{dest_sql}: OperationalError (08S01) retry {attempt}/{SQL_UPLOAD_MAX_RETRIES} after {SQL_UPLOAD_RETRY_SLEEP}s",
                )
                try:
                    engine.dispose()
                except Exception:
                    pass
                time.sleep(SQL_UPLOAD_RETRY_SLEEP)
                continue
            try:
                engine.dispose()
            except Exception:
                pass
            raise
        except Exception:
            # ensure we don't leave a pending transaction on failures
            try:
                engine.dispose()
            except Exception:
                pass
            raise
    record_hash(dest_sql, df_to_upload)
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

    if should_skip_write(dest_tbl, dest_df):
        return
    clear_sql = """TRUNCATE TABLE """ + dest_tbl
    execute_sqlcur(engine=engine, sql=clear_sql)
    upload_sql(engine=engine, df=dest_df, dest_sql=dest_tbl, existMethod='append', skip_hash_check=True)

    return

def update_aid_id(dest_tbl, dest_df, module_key=None):
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

    if dest_df is None or dest_df.empty or 'ID' not in dest_df.columns:
        log_step("update_aid_id", f"skip update for {dest_tbl} (empty or missing ID)")
        return

    ids = [i for i in dest_df['ID'].dropna().unique().tolist() if str(i).strip() != ""]
    if not ids:
        log_step("update_aid_id", f"skip update for {dest_tbl} (no valid ID)")
        return

    if dest_tbl == 'tbl_AID_Time_Series_Annual':
        _register_ts_annual_ids(module_key, ids)

    if should_skip_write(dest_tbl, dest_df):
        return

    global TS_ANNUAL_TRUNCATED
    if dest_tbl == 'tbl_AID_Time_Series_Annual' and force_update and not TS_ANNUAL_TRUNCATED:
        if debug_mode:
            log_step("update_aid_id", "debug: skip truncate tbl_AID_Time_Series_Annual (first write)")
        else:
            log_step("update_aid_id", "truncate tbl_AID_Time_Series_Annual (first write)")
            execute_sqlcur(engine=engine_dest, sql="TRUNCATE TABLE tbl_AID_Time_Series_Annual")
        TS_ANNUAL_TRUNCATED = True

    if debug_mode:
        log_step("update_aid_id", f"debug: skip delete for {dest_tbl}")
        upload_sql(
            engine=engine_dest,
            df=dest_df,
            dest_sql=dest_tbl,
            existMethod='append',
            skip_hash_check=True,
            module_key=module_key,
            skip_id_delete=True,
        )
        return

    if dest_tbl == 'tbl_AID_Time_Series_Annual' and not force_update:
        _delete_ids_in_chunks(dest_tbl, ids)
    else:
        str_id = """('""" + """','""".join([str(i).replace("'", "''") for i in ids]) + """')"""
        del_sql = """DELETE FROM """ + dest_tbl + """ WHERE [ID] IN """ + str_id
        execute_sqlcur(engine=engine_dest, sql=del_sql)
    upload_sql(
        engine=engine_dest,
        df=dest_df,
        dest_sql=dest_tbl,
        existMethod='append',
        skip_hash_check=True,
        module_key=module_key,
        skip_id_delete=True,
    )

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

    df = pd.read_excel(xls_name, sheet_name='Monthly', header=0)
    df = pd.melt(df, id_vars=['LevelName', 'Level', 'ShapeName', 'Year'], var_name='Month', value_name='Shape')
    df['Year'] = df['Year'].astype(int)
    df['Month'] = df['Month'].astype(int)
    df['Day'] = 1
    df['Date'] = pd.to_datetime(df[['Day', 'Month', 'Year']])
    df = df.drop(['Day'], axis=1)
    df['User'] = user
    df['TimeStamp'] = timestamp
    df_vec = pd.read_excel(xls_name, sheet_name='Vector', header=0)

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

    df = pd.read_excel(xls_name, sheet_name=shtname, header=1)
    df = df.ffill(axis=1).infer_objects(copy=False)
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

    df = pd.read_excel(xls_name, sheet_name='Fuel', header=0, usecols='B:BQ')
    df = df.ffill(axis=1).infer_objects(copy=False)
    df = pd.melt(df, id_vars=['LevelName', 'Level', 'FuelName', 'Description', 'FuelType', 'Metric', 'Units'],
                 var_name='Year', value_name='Value')
    df['Year'] = df['Year'].astype(int)
    df['Value'] = pd.to_numeric(df['Value'])
    df['User'] = user
    df['TimeStamp'] = timestamp

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
    df = df.drop(['Units'], axis=1)
    df['PlantType'] = np.where(df['PlantTech'] == df['PlantType'],
                                  df['PlantType'],
                                  df['PlantType'] + '-' + df['PlantTech'])
    df['LevelName'] = df['LevelName'].str.strip()
    df['Level'] = df['Level'].str.strip()
    df['PlantType'] = df['PlantType'].str.strip()
    df['PlantTech'] = df['PlantTech'].str.strip()
    df['PlantAttribute'] = df['PlantAttribute'].str.strip()
    df = df[df['PlantOnlineYear'] <= yr_end]

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
        df = pd.concat([df, df2], ignore_index=True)

    dict_zone_mkt = dict(df[['Zone Name', 'CustomMkt_APACmodel']].drop_duplicates().values.tolist())
    dict_mkt_country = dict(df[['CustomMkt_APACmodel', 'System Name']].drop_duplicates().values.tolist())
    dict_area = dict(df[['Zone Name', 'Zone ID']].drop_duplicates().values.tolist())

    # Topology_Zone
    df = df.drop(['CustomMkt_APACmodel'], axis=1)
    df = df.sort_values(by=['Zone ID']).reset_index(drop=True)

    # Topology_Area
    df_area = df[['Zone ID', 'Zone Name']].reset_index(drop=True)
    df_area.columns = ['Area Number', 'Area Name']
    df_area['Short Area Name'] = df_area['Area Name']
    df_area['Area Demand Number'] = df_area['Area Number']

    return dict_area, dict_zone_mkt, dict_mkt_country, df, df_area

def get_sql_demand_to_aid(src_sql, country, year_start=None, year_end=None, base_year=None):
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
    def _map_area(df_pl, area_col="Area"):
        mapping = pl.DataFrame({
            area_col: list(dict_zone.keys()),
            "Area_id": list(dict_zone.values())
        })
        return df_pl.join(mapping, on=area_col, how="left")

    log_step("demand", "sql read for hourly shape start")
    if base_year is not None:
        target_year = int(base_year)
        leap_yr = check_leap_yr(target_year)
        log_step("demand", f"use base year for hourly: {target_year} (leap={leap_yr})")
    else:
        yr_now = datetime.now().year
        leap_yr = check_leap_yr(yr_now)
        target_year = yr_now - 1 if leap_yr else yr_now
    sql_qry_hourly = (
        "SELECT [Area], [Year], [Hr_Yr], [Normalised] "
        "FROM " + src_sql +
        " WHERE [Country] = '" + country + "' AND [Year] = " + str(target_year)
    )
    t0 = time.perf_counter()
    df_hourly = pd.read_sql_query(sql_qry_hourly, engine_src)
    # Stabilize hash for demand inputs
    if not df_hourly.empty:
        df_hourly = df_hourly.sort_values(by=["Area", "Year", "Hr_Yr"], kind="mergesort").reset_index(drop=True)
        if "Normalised" in df_hourly.columns:
            df_hourly["Normalised"] = df_hourly["Normalised"].round(8)
    log_step("demand", f"sql read for hourly shape done rows={len(df_hourly)} ({time.perf_counter()-t0:.1f}s)")
    log_step("demand", f"hourly shape target_year={target_year} (leap={leap_yr})")

    log_step("demand", "sql read for monthly data start")
    year_filter = ""
    if year_start is not None and year_end is not None:
        year_filter = f" AND [Year] BETWEEN {int(year_start)} AND {int(year_end)}"
        log_step("demand", f"apply year filter: {year_start}-{year_end}")
    elif year_start is not None:
        year_filter = f" AND [Year] >= {int(year_start)}"
        log_step("demand", f"apply year filter: >= {year_start}")
    elif year_end is not None:
        year_filter = f" AND [Year] <= {int(year_end)}"
        log_step("demand", f"apply year filter: <= {year_end}")

    sql_qry_monthly = (
        "WITH Monthly AS ("
        "    SELECT [Area], [Year], [Mth], "
        "           AVG([Demand_MW]) AS [Month_Average], "
        "           MAX([Demand_MW]) AS [Month_Peak] "
        "    FROM " + src_sql +
        "    WHERE [Country] = '" + country + "'" + year_filter + " "
        "    GROUP BY [Area], [Year], [Mth]"
        "), "
        "YearAgg AS ("
        "    SELECT [Area], [Year], "
        "           AVG([Month_Average]) AS [Year_Mean], "
        "           MAX([Month_Peak]) AS [Year_Peak] "
        "    FROM Monthly "
        "    GROUP BY [Area], [Year]"
        ") "
        "SELECT m.[Area], m.[Year], m.[Mth], "
        "       m.[Month_Average], m.[Month_Peak], "
        "       y.[Year_Mean], y.[Year_Peak] "
        "FROM Monthly m "
        "JOIN YearAgg y "
        "  ON m.[Area] = y.[Area] AND m.[Year] = y.[Year]"
    )
    t1 = time.perf_counter()
    df_monthly_agg = pd.read_sql_query(sql_qry_monthly, engine_src)
    if not df_monthly_agg.empty:
        df_monthly_agg = df_monthly_agg.sort_values(by=["Area", "Year", "Mth"], kind="mergesort").reset_index(drop=True)
        for col in ["Month_Average", "Month_Peak", "Year_Mean", "Year_Peak"]:
            if col in df_monthly_agg.columns:
                df_monthly_agg[col] = df_monthly_agg[col].round(8)
    log_step("demand", f"sql read for monthly data done rows={len(df_monthly_agg)} ({time.perf_counter()-t1:.1f}s)")
    if should_skip_read("demand", [df_hourly, df_monthly_agg]):
        log_step("demand", "read hash unchanged; skip compute/write")
        return None, None, None, True
    # </editor-fold>

    # <editor-fold desc="Transforming LDC to Demand_Hourly_Shapes">
    log_step("demand", "transform hourly shape start")
    df_hourly_pl = pl.from_pandas(df_hourly[['Area', 'Hr_Yr', 'Normalised']])
    df_hourly_pl = _map_area(df_hourly_pl, "Area").filter(pl.col("Area_id").is_not_null())
    log_df_info("demand hourly shape after map", df_hourly_pl)
    df_ai1_pl = (
        df_hourly_pl
        .pivot(values="Normalised", index="Hr_Yr", on="Area_id", aggregate_function="mean")
        .sort("Hr_Yr")
        .fill_null(0)
    )
    df_ai1_pl = pl.concat([df_ai1_pl, df_ai1_pl.head(192)], how="vertical")
    df_ai1_pl = df_ai1_pl.with_row_index("Demand Hour", offset=1).drop("Hr_Yr")
    log_df_info("demand hourly shape final", df_ai1_pl)
    df_ai1 = pl_to_pandas(df_ai1_pl)
    log_step("demand", "transform hourly shape done")
    # </editor-fold>

    # <editor-fold desc="Transforming LDC to Demand_Monthly_Peak">
    log_step("demand", "transform monthly peak/average start")
    df_mth_pl = pl.from_pandas(df_monthly_agg)
    df_mth_pl = df_mth_pl.rename({
        "Month_Average": "Month Average",
        "Month_Peak": "Month Peak",
        "Year_Mean": "Year Mean",
        "Year_Peak": "Year Peak",
    })
    df_mth_pl = df_mth_pl.with_columns(
        (pl.col("Month Average") / pl.col("Year Mean")).alias("MthShape")
    )
    df_ai2_pl = df_mth_pl.select(["Area", "Year", "Mth", "Month Average", "Month Peak"])
    df_ai2_pl = _map_area(df_ai2_pl, "Area").filter(pl.col("Area_id").is_not_null())
    df_ai2_pl = df_ai2_pl.drop("Area")
    df_ai2_pl = df_ai2_pl.rename({"Area_id": "ID", "Mth": "Month"})
    log_df_info("demand monthly peak final", df_ai2_pl)
    df_ai2 = pl_to_pandas(df_ai2_pl)
    log_step("demand", "transform monthly peak/average done")
    # </editor-fold>

    # <editor-fold desc="Transforming LDC to Demand_Monthly">
    log_step("demand", "transform monthly demand start")
    df_ai3_pl = df_mth_pl.select(["Area", "Year", "Mth", "MthShape"]).rename({"MthShape": "Qty"})
    df_year_pl = df_mth_pl.select(["Area", "Year", "Year Mean", "Year Peak"]).unique()
    df_1_pl = df_year_pl.select([
        "Area", "Year",
        pl.lit(13).alias("Mth"),
        pl.col("Year Mean").alias("Qty"),
    ])
    df_2_pl = df_year_pl.select([
        "Area", "Year",
        pl.lit(14).alias("Mth"),
        pl.col("Year Peak").alias("Qty"),
    ])
    df_ai3_pl = df_ai3_pl.with_columns(
        pl.col("Mth").cast(pl.Int64),
        pl.col("Year").cast(pl.Int64)
    )
    df_1_pl = df_1_pl.with_columns(
        pl.col("Mth").cast(pl.Int64),
        pl.col("Year").cast(pl.Int64)
    )
    df_2_pl = df_2_pl.with_columns(
        pl.col("Mth").cast(pl.Int64),
        pl.col("Year").cast(pl.Int64)
    )
    df_ai3_pl = pl.concat([df_ai3_pl, df_1_pl, df_2_pl], how="vertical")
    df_ai3_pl = df_ai3_pl.sort(["Area", "Year", "Mth"])
    df_ai3_pl = _map_area(df_ai3_pl, "Area").filter(pl.col("Area_id").is_not_null())
    df_ai3_pl = df_ai3_pl.drop("Area")
    df_ai3_pl = df_ai3_pl.rename({"Year": "Demand Year", "Mth": "Demand Month", "Area_id": "Area"})
    df_ai3_pl = (
        df_ai3_pl
        .pivot(
            values="Qty",
            index=["Demand Year", "Demand Month"],
            on="Area",
            aggregate_function="mean",
        )
        .fill_null(0)
    )
    log_df_info("demand monthly final", df_ai3_pl)
    df_ai3 = pl_to_pandas(df_ai3_pl)
    log_step("demand", "transform monthly demand done")
    # </editor-fold>

    return df_ai1, df_ai2, df_ai3, False

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
    df_hydro = df_hydro.drop(['Country', 'Level', 'ShapeName'], axis=1)
    # pivot df to get 1 column per Month
    df_hydro = pd.pivot_table(df_hydro, index=['Area', 'Year'],
                              columns='Month',
                              values='Shape',
                              fill_value=0).reset_index()
    # left join to Hydro Vector df by Area to get "Maximum" to use as Month 13 in Hydro df
    df_hydro = pd.merge(df_hydro, df_vec, how='left', on='Area')
    df_hydro = df_hydro.drop(['Country', 'Minimum', 'Energy Shift Method', 'Sus Maximum', 'Sus Number'], axis=1)
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
    df_hydro_mthly = df_hydro_mthly.ffill(axis=0).infer_objects(copy=False)
    df_hydro_mthly.rename(columns={'Area': 'Name', 'Year': 'Hydro Year'}, inplace=True)

    # add in other assumptions for hydro vector df to match tbl_AID_xxxxx
    df_vec['Index'] = df_vec.index + 1
    df_vec['Demand Source'] = 'Area'
    df_vec['Hydro Shape Sets'] = df_vec['Area']
    df_vec['Shape Areas'] = df_vec['Area'].map(dict_zone)
    df_vec.rename(columns={'Area': 'Name', 'Country': 'zREM Topology'}, inplace=True)

    return df_vec, df_hydro_mthly

def get_sql_windsolarshapes_to_aid_ts_wek(src_sql, country, year_start=None, year_end=None):
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
    year_filter = ""
    if year_start is not None and year_end is not None:
        year_filter = f" AND [Year] BETWEEN {int(year_start)} AND {int(year_end)}"
    elif year_start is not None:
        year_filter = f" AND [Year] >= {int(year_start)}"
    elif year_end is not None:
        year_filter = f" AND [Year] <= {int(year_end)}"
    sql_qry = """SELECT * FROM """ + src_sql + """ WHERE [Country] = '""" + country + """'""" + year_filter
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

    df = df.drop(['Year'], axis=1)
    shape_yrs = [2019]
    yr_lst = [item for item in yr_model if item not in shape_yrs]
    for yr in yr_lst:
        df[yr] = df[2019]

    df = pd.melt(df, id_vars=['Country', 'ModelZone', 'PlantType', 'ShapeName', 'Month', 'DayOfWeek', 'HourOfDay', 'AnnualAverageShape'],
                 var_name='Year', value_name='AID_WeeklyShape')

    # scale shape up or down to match CF assumptions
    cf_all = renew_cf[renew_cf['Level'] == 'All'].copy()
    cf_all = cf_all.drop(['LevelName', 'Level'], axis=1)
    df = pd.merge(df, cf_all, how='left', on=['PlantType', 'Year'])
    df.rename(columns={'Value': 'Global_CF'}, inplace=True)

    cf_country = renew_cf[renew_cf['Level'] == 'Country'].copy()
    if len(cf_country) > 0:
        cf_country = cf_country.drop(['Level'], axis=1)
        cf_country.rename(columns={'LevelName': 'Country'}, inplace=True)
        df = pd.merge(df, cf_country, how='left', on=['Country', 'PlantType', 'Year'])
        df.rename(columns={'Value': 'Country_CF'}, inplace=True)

    cf_zone = renew_cf[renew_cf['Level'] == 'ModelZone'].copy()
    if len(cf_zone) > 0:
        cf_zone = cf_zone.drop(['Level'], axis=1)
        cf_zone.rename(columns={'LevelName': 'ModelZone'}, inplace=True)
        df = pd.merge(df, cf_zone, how='left', on=['ModelZone', 'PlantType', 'Year'])
        df.rename(columns={'Value': 'Zone_CF'}, inplace=True)
    df['Final_CF'] = np.nan
    df[['Global_CF', 'Country_CF', 'Zone_CF', 'Final_CF']] = df[['Global_CF', 'Country_CF', 'Zone_CF', 'Final_CF']].ffill(axis=1).infer_objects(copy=False)
    df['FinalAID_Shape'] = (df['AID_WeeklyShape'] / df['AnnualAverageShape']) * df['Final_CF']
    # debug CF annual changes
    df = df.sort_values(by=['Country', 'PlantType', 'ShapeName', 'Year', 'Month', 'DayOfWeek',
                            'HourOfDay']).reset_index(drop=True)
    df_debug = df[df['PlantType'] == 'Wind_Onshore'].reset_index(drop=True)
    df_debug = df_debug[df_debug['ModelZone'] == 'TAS'].reset_index(drop=True)

    # get weekly time series df with 168 map
    df = df.drop(['Country', 'Global_CF', 'Country_CF', 'Zone_CF', 'Final_CF', 'AnnualAverageShape', 'AID_WeeklyShape'], axis=1)
    df.rename(columns={'ModelZone': 'Area', 'FinalAID_Shape': 'AID_WeeklyShape'}, inplace=True)
    df = df.sort_values(by=['Area', 'PlantType', 'ShapeName', 'Year', 'Month', 'DayOfWeek',
                            'HourOfDay']).reset_index(drop=True)
    df = pd.merge(df, df_168map, how='left', on=['DayOfWeek', 'HourOfDay'])
    df = df.drop(['DayOfWeek', 'HourOfDay'], axis=1)
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
    df = df.drop(['Year', 'PlantType', 'Name_mth', 'ShapeName_mth'], axis=1)
    #df_monthly['ID'] = df_monthly['Use']
    df_monthly.rename(columns={'Use': 'ID'}, inplace=True)
    df_monthly = df_monthly.drop(['PlantType'], axis=1)

    df_maint_mth = df_monthly[['Year', 'Area', 'ShapeName', 'ID']].reset_index(drop=True)
    df_maint_mth['ID'] = 'mn_' + df_maint_mth['ID']

    df_maint_wk = df[['Area', 'ShapeName', 'ID']].reset_index(drop=True)
    df_maint_wk['ID'] = 'wk_' + df_maint_wk['ID']

    #df = df.drop(['Level'], axis=1)
    #df_monthly = df_monthly.drop(['Level'], axis=1)

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

    # get conventional plant (China-only path)
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
    df['Resource_Group'] = np.where(df['Resource_Group'] == 'CC',
                                           df['Resource_Group'] + '-' + df['zREM Technology'],
                                           df['Resource_Group'])
    df['Resource_Group'] = np.where(df['Resource_Group'] == 'CHP_Gas',
                                       df['Resource_Group'] + '-' + df['zREM Technology'],
                                       df['Resource_Group'])
    df['Resource_Group'] = np.where(df['Resource_Group'] == 'STCoal',
                                           df['Resource_Group'] + '-' + df['zREM Technology'],
                                           df['Resource_Group'])
    df['Resource_Group'] = np.where(df['Resource_Group'] == 'CHP_Coal',
                                       df['Resource_Group'] + '-' + df['zREM Technology'],
                                       df['Resource_Group'])
    df['Resource Begin Date'] = pd.to_datetime(df['Resource Begin Date'])
    df['Resource End Date'] = pd.to_datetime(df['Resource End Date'])
    df['StartYear'] = df['Resource Begin Date'].dt.year
    df['StartYear'] = np.where(df['StartYear'] < 2000, 2000, df['StartYear'])

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
    df['Resource_Group'] = np.where(df['Resource_Group'] == 'CC',
                                       df['Resource_Group'] + '-' + df['zREM Technology'],
                                       df['Resource_Group'])
    df['Resource_Group'] = np.where(df['Resource_Group'] == 'CHP_Gas',
                                       df['Resource_Group'] + '-' + df['zREM Technology'],
                                       df['Resource_Group'])
    df['Resource_Group'] = np.where(df['Resource_Group'] == 'STCoal',
                                       df['Resource_Group'] + '-' + df['zREM Technology'],
                                       df['Resource_Group'])
    df['Resource_Group'] = np.where(df['Resource_Group'] == 'CHP_Coal',
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
    df = df.drop(['Day', 'Month', 'Year'], axis=1)

    df['StartYear'] = df['Resource Begin Date'].dt.year
    df['StartYear'] = np.where(df['StartYear'] < 2000, 2000, df['StartYear'])

    return df

def get_sql_fuel_to_aid(src_sql, country):
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
    china_zones = list(dict_zone_mkt.keys())
    mask = (
        (df['Level'] == 'All') |
        ((df['Level'] == 'Country') & (df['LevelName'] == country)) |
        ((df['Level'] == 'ModelZone') & (df['LevelName'].isin(china_zones)))
    )
    df = df[mask].copy()
    df = df.drop(['Metric'], axis=1)
    df = pd.pivot_table(df, index=['Level', 'LevelName', 'FuelName', 'Description', 'FuelType', 'Units'],
                              columns='Year', values='Value', fill_value=0).reset_index()
    df['FuelName'] = np.where(df['LevelName'] == 'All',
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

    df_plant = df_plant.drop(['Area_y'], axis=1)
    df_plant.rename(columns={'Area_x': 'Area'}, inplace=True)

    return df_plant

def get_sql_fuel_max_to_aid(src_sql, country):
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
    china_zones = list(dict_zone_mkt.keys())
    mask = (
        (df['Level'] == 'All') |
        ((df['Level'] == 'Country') & (df['LevelName'] == country)) |
        ((df['Level'] == 'ModelZone') & (df['LevelName'].isin(china_zones)))
    )
    df = df[mask].copy()
    #df = df.drop(['Metric'], axis=1)
    df = pd.pivot_table(df, index=['Metric', 'Level', 'LevelName', 'FuelName', 'Description', 'FuelType', 'Units'],
                              columns='Year', values='Value', fill_value=0).reset_index()
    df['FuelName'] = np.where(df['LevelName'] == 'All',
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

# ---- runtime helpers ----

def log(message):
    ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {message}")

def log_step(step, message):
    log(f"{step}: {message}")

def log_df_info(label, df):
    if df is None:
        log(f"{label}: df is None")
        return
    try:
        rows = len(df)
        cols = len(df.columns)
        log(f"{label}: rows={rows}, cols={cols}")
    except Exception:
        log(f"{label}: df stats unavailable")

def _df_content_hash(df):
    if df is None:
        return None
    hash_df = df.copy()
    drop_cols = [c for c in hash_df.columns if str(c).strip().lower() in ("timestamp", "time stamp")]
    if drop_cols:
        hash_df = hash_df.drop(columns=drop_cols, errors="ignore")
    if len(hash_df.columns) > 0:
        sorted_cols = sorted([c for c in hash_df.columns], key=lambda x: str(x))
        hash_df = hash_df[sorted_cols]
    if len(hash_df) > 1 and len(hash_df.columns) > 0:
        try:
            hash_df = hash_df.sort_values(by=list(hash_df.columns), kind="mergesort", na_position="last")
        except Exception:
            pass
    hasher = hashlib.sha256()
    cols = [str(c) for c in hash_df.columns]
    dtypes = [str(t) for t in hash_df.dtypes]
    hasher.update("|".join(cols).encode("utf-8"))
    hasher.update("|".join(dtypes).encode("utf-8"))
    if len(hash_df) > 0:
        values_hash = pd.util.hash_pandas_object(hash_df, index=False).values
        hasher.update(values_hash.tobytes())
    return hasher.hexdigest()

def _load_hash_cache():
    if not os.path.exists(HASH_CACHE_PATH):
        return {}
    try:
        with open(HASH_CACHE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}

def _load_hash_cache_path(path):
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}

def _write_hash_cache_path(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def _hash_inputs(dfs):
    hasher = hashlib.sha256()
    for df in dfs:
        h = _df_content_hash(df) or "none"
        hasher.update(h.encode("utf-8"))
    return hasher.hexdigest()

def _normalize_id_list(ids):
    if not ids:
        return []
    normed = []
    for item in ids:
        if item is None:
            continue
        s = str(item).strip()
        if not s:
            continue
        normed.append(s)
    return sorted(set(normed))

def _load_ts_annual_ids_cache():
    return _load_hash_cache_path(TS_ANNUAL_IDS_CACHE_PATH)

def _write_ts_annual_ids_cache(data):
    _write_hash_cache_path(TS_ANNUAL_IDS_CACHE_PATH, data)

def _register_ts_annual_ids(module_key, ids):
    if not module_key:
        module_key = "unknown"
    ids_norm = _normalize_id_list(ids)
    if not ids_norm:
        return
    current = TS_ANNUAL_IDS_THIS_RUN.get(module_key, [])
    merged = sorted(set(current).union(ids_norm))
    TS_ANNUAL_IDS_THIS_RUN[module_key] = merged

def _register_ts_annual_ids_from_cache(module_key):
    if not module_key:
        return
    data = _load_ts_annual_ids_cache()
    ids = data.get("modules", {}).get(module_key, {}).get("ids", [])
    _register_ts_annual_ids(module_key, ids)

def _delete_ids_in_chunks(dest_tbl, ids, chunk_size=500):
    ids = _normalize_id_list(ids)
    if not ids:
        return
    for i in range(0, len(ids), chunk_size):
        chunk = ids[i : i + chunk_size]
        safe_ids = [s.replace("'", "''") for s in chunk]
        str_id = "('" + "','".join(safe_ids) + "')"
        del_sql = f"DELETE FROM {dest_tbl} WHERE [ID] IN {str_id}"
        execute_sqlcur(engine=engine_dest, sql=del_sql)

def finalize_ts_annual_ids_cleanup():
    if force_update or not TS_ANNUAL_IDS_THIS_RUN:
        if TS_ANNUAL_IDS_THIS_RUN:
            data = _load_ts_annual_ids_cache()
            modules = data.get("modules", {})
            now = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            for key, ids in TS_ANNUAL_IDS_THIS_RUN.items():
                modules[key] = {"ids": ids, "updated": now}
            all_ids = sorted(set().union(*[set(m.get("ids", [])) for m in modules.values()])) if modules else []
            data["modules"] = modules
            data["all_ids"] = all_ids
            data["updated"] = now
            _write_ts_annual_ids_cache(data)
        return

    data = _load_ts_annual_ids_cache()
    modules = data.get("modules", {})
    now = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for key, ids in TS_ANNUAL_IDS_THIS_RUN.items():
        modules[key] = {"ids": ids, "updated": now}

    current_all = sorted(set().union(*[set(m.get("ids", [])) for m in modules.values()])) if modules else []
    previous_all = set(data.get("all_ids", []))
    stale_ids = sorted(previous_all.difference(set(current_all)))

    if stale_ids and not debug_mode:
        log_step("ts_annual_cleanup", f"delete stale IDs rows={len(stale_ids)}")
        _delete_ids_in_chunks("tbl_AID_Time_Series_Annual", stale_ids)

    data["modules"] = modules
    data["all_ids"] = current_all
    data["updated"] = now
    _write_ts_annual_ids_cache(data)

def should_skip_read(step_key, dfs, ts_annual_module_key=None):
    if force_update:
        return False
    h = _hash_inputs(dfs)
    data = _load_hash_cache_path(READ_HASH_CACHE_PATH)
    if data.get(step_key, {}).get("hash") == h:
        if ts_annual_module_key:
            _register_ts_annual_ids_from_cache(ts_annual_module_key)
        return True
    data[step_key] = {
        "hash": h,
        "rows": [int(len(df)) if df is not None else 0 for df in dfs],
        "updated": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    _write_hash_cache_path(READ_HASH_CACHE_PATH, data)
    return False

def should_skip_write(table_name, df):
    if debug_mode or force_update:
        return False
    h = _df_content_hash(df)
    if h is None:
        return False
    data = _load_hash_cache()
    if data.get(table_name, {}).get("hash") == h:
        log_step("write_hash", f"{table_name}: unchanged; skip SQL write")
        return True
    return False

def record_hash(table_name, df):
    if df is None:
        return
    os.makedirs(os.path.dirname(HASH_CACHE_PATH), exist_ok=True)
    data = _load_hash_cache()
    data[table_name] = {
        "rows": int(len(df)),
        "cols": int(len(df.columns)),
        "hash": _df_content_hash(df),
        "updated": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    with open(HASH_CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def _safe_table_name(name):
    if name is None:
        return "unknown_table"
    return _strip_brackets(name).replace(".", "_")

def _debug_csv_path(table_name):
    ts = debug_run_ts if debug_run_ts else dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    folder = f"debug_{ts}"
    os.makedirs(folder, exist_ok=True)
    base = _safe_table_name(table_name)
    filename = os.path.join(folder, base + ".csv")
    if not os.path.exists(filename):
        return filename
    idx = 1
    while True:
        filename = os.path.join(folder, f"{base}_{idx}.csv")
        if not os.path.exists(filename):
            return filename
        idx += 1

def debug_write_csv(df, dest_sql):
    if not debug_mode:
        return False
    if df is None:
        log(f"debug: skip CSV (df is None) for {dest_sql}")
        return True
    path = _debug_csv_path(dest_sql)
    df.to_csv(path, index=False, encoding="utf-8-sig")
    record_hash(dest_sql, df)
    log(f"debug: write CSV {path} rows={len(df)} cols={len(df.columns)} (skip SQL)")
    return True

def pl_to_pandas(df_pl):
    try:
        return df_pl.to_pandas()
    except ModuleNotFoundError as exc:
        if "pyarrow" not in str(exc):
            raise
        return pd.DataFrame(df_pl.to_numpy(), columns=df_pl.columns)

def _validate_columns(df, required, name):
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"{name}: missing columns: {missing}")

def _norm_col(name):
    return " ".join(str(name).strip().lower().split())

def _rename_by_normalized(df, required_cols, name):
    norm_map = {_norm_col(c): c for c in df.columns}
    rename_map = {}
    missing = []
    for req in required_cols:
        norm_req = _norm_col(req)
        if norm_req in norm_map:
            rename_map[norm_map[norm_req]] = req
        else:
            missing.append(req)
    if missing:
        raise ValueError(f"{name}: missing columns (normalized): {missing}")
    return df.rename(columns=rename_map)

def _rename_by_normalized_optional(df, cols):
    norm_map = {_norm_col(c): c for c in df.columns}
    rename_map = {}
    for col in cols:
        norm_col = _norm_col(col)
        if norm_col in norm_map:
            rename_map[norm_map[norm_col]] = col
    if rename_map:
        return df.rename(columns=rename_map)
    return df

def _apply_alias_columns(df, alias_map):
    if not alias_map:
        return df
    norm_map = {_norm_col(c): c for c in df.columns}
    for target, aliases in alias_map.items():
        norm_target = _norm_col(target)
        if norm_target in norm_map:
            continue
        for alias in aliases:
            norm_alias = _norm_col(alias)
            if norm_alias in norm_map:
                df[target] = df[norm_map[norm_alias]]
                norm_map[norm_target] = target
                break
    return df

def _coerce_int(df, cols, name):
    for c in cols:
        if c in df.columns:
            try:
                coerced = pd.to_numeric(df[c], errors="coerce")
                bad_mask = df[c].notna() & coerced.isna()
                if bad_mask.any():
                    bad_vals = df.loc[bad_mask, c].astype(str).unique()[:5]
                    raise ValueError(f"{name}: column '{c}' cannot be int (examples: {bad_vals})")
                df[c] = coerced.astype("Int64")
            except Exception as exc:
                raise ValueError(f"{name}: column '{c}' cannot be int") from exc

def _coerce_numeric(df, cols, name):
    for c in cols:
        if c in df.columns:
            try:
                df[c] = pd.to_numeric(df[c])
            except Exception as exc:
                raise ValueError(f"{name}: column '{c}' cannot be numeric") from exc


def validate_excel_hydro(xls_name):
    df_monthly = pd.read_excel(xls_name, sheet_name='Monthly', header=0)
    _validate_columns(df_monthly, ['LevelName', 'Level', 'ShapeName', 'Year'], "APAC_Hydro.xlsx:Monthly")
    _coerce_int(df_monthly, ['Year'], "APAC_Hydro.xlsx:Monthly")

    df_vector = pd.read_excel(xls_name, sheet_name='Vector', header=0)
    _validate_columns(df_vector, ['Area'], "APAC_Hydro.xlsx:Vector")

def validate_excel_assumptions(xls_name):
    required_common = ['LevelName', 'Level', 'PlantType', 'PlantTech']
    sheets = [
        'PlantLife', 'VOM', 'HeatRate', 'CapacityFactor', 'FixedCost',
        'EmissionRate', 'EmissionPrice', 'StorageDuration'
    ]
    for sheet in sheets:
        df = pd.read_excel(xls_name, sheet_name=sheet, header=1)
        req = list(required_common)
        if sheet in ('EmissionRate', 'EmissionPrice'):
            req.append('Pollutant')
        _validate_columns(df, req, f"APAC_Assumptions.xlsx:{sheet}")

def validate_excel_fuel(xls_name):
    df = pd.read_excel(xls_name, sheet_name='Fuel', header=0, usecols='B:BQ')
    _validate_columns(
        df,
        ['LevelName', 'Level', 'FuelName', 'Description', 'FuelType', 'Metric', 'Units'],
        "APAC_Fuels.xlsx:Fuel",
    )

def validate_excel_inputs():
    # NOTE: Transmission Excel validation is handled by APAC_Transmission_Uploader_cli.py
    validate_excel_hydro(hydro_xls)
    validate_excel_assumptions(assumptions_xls)
    validate_excel_fuel(fuels_xls)

def progress_bar(current, total, width=28):
    if total <= 0:
        return "[----------------------------] 0/0"
    filled = int(width * current / total)
    bar = "#" * filled + "-" * (width - filled)
    return f"[{bar}] {current}/{total}"


def init_environment():
    global user, timestamp, datasetname
    global engine_src, engine_dest
    global dict_zone, dict_zone_mkt, dict_mkt_country, df_zone, df_area, Assumptions
    global TS_ANNUAL_TRUNCATED, TS_ANNUAL_IDS_THIS_RUN

    log("init: start")
    TS_ANNUAL_TRUNCATED = False
    TS_ANNUAL_IDS_THIS_RUN = {}
    user = getpass.getuser()
    timestamp = dt.datetime.now().replace(microsecond=0)
    datasetname = str(timestamp)

    log("init: connect source/dest")
    params_src = urllib.parse.quote_plus(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=ANVDEVSQLVPM01;"
        "DATABASE=WM_POWER_RENEWABLES;"
        "Trusted_Connection=Yes"
    )
    engine_src = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params_src, fast_executemany=True)

    params_dest = urllib.parse.quote_plus(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=" + aurora_sqldb + ";"
        "DATABASE=" + aurora_dbname + ";"
        "Trusted_Connection=Yes"
    )
    engine_dest = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params_dest, fast_executemany=True)

    def _enable_fast_executemany(engine):
        try:
            @event.listens_for(engine, "before_cursor_execute")
            def _set_fast_executemany(conn, cursor, statement, parameters, context, executemany):
                if executemany:
                    try:
                        cursor.fast_executemany = True
                    except Exception:
                        pass
        except Exception:
            pass

    _enable_fast_executemany(engine_src)
    _enable_fast_executemany(engine_dest)

    log("init: load topology and assumptions")
    dict_zone, dict_zone_mkt, dict_mkt_country, df_zone, df_area = get_sql_topology_to_aid(
        src_sql='vAID_Topology_Zones', country=country
    )
    Assumptions = get_sql_plantasmpt(src_sql='vAPAC_Plant_Attributes_Annual_LIVE')
    log_df_info("init topology zones", df_zone)
    log_df_info("init topology areas", df_area)
    log_df_info("init assumptions", Assumptions)
    log("init: done")


# ---- step functions ----

def run_excel_imports():
    step = "excel_imports"
    log_step(step, "validate excel inputs")
    validate_excel_inputs()
    # NOTE: Transmission Excel upload is handled by APAC_Transmission_Uploader_cli.py
    # and should be run separately before this script.

    log_step(step, "read hydro excel")
    df_shape_hydromth, df_AID_HydroVector = get_excel_hydro12mthlyshape(xls_name=hydro_xls)
    log_df_info(f"{step} hydro monthly", df_shape_hydromth)
    log_df_info(f"{step} hydro vector", df_AID_HydroVector)
    if should_skip_read(f"{step}_hydro", [df_shape_hydromth, df_AID_HydroVector]):
        log_step(f"{step}_hydro", "read hash unchanged; skip compute/write")
    else:
        log_step(step, "write hydro tables start")
        reload_tbl(engine=engine_src, dest_tbl='APAC_Shapes_Monthly_LIVE', dest_df=df_shape_hydromth)
        reload_tbl(engine=engine_src, dest_tbl='APAC_AID_HydroVectors_LIVE', dest_df=df_AID_HydroVector)
        log_step(step, "write hydro tables done")

    log_step(step, "read assumptions excel")
    df_plant_life = get_excel_plantasmpt(xls_name=assumptions_xls, shtname='PlantLife', units='Years')
    df_plant_vom = get_excel_plantasmpt(xls_name=assumptions_xls, shtname='VOM', units='US$/MWh')
    df_plant_heatrate = get_excel_plantasmpt(xls_name=assumptions_xls, shtname='HeatRate', units='btu/kWh')
    df_plant_cf = get_excel_plantasmpt(xls_name=assumptions_xls, shtname='CapacityFactor', units='NA')
    df_plant_fixcost = get_excel_plantasmpt(xls_name=assumptions_xls, shtname='FixedCost', units='US$/MW week')
    df_plant_emissionrate = get_excel_plantasmpt(xls_name=assumptions_xls, shtname='EmissionRate', units='lb/mmbtu')
    df_plant_emissionprice = get_excel_plantasmpt(xls_name=assumptions_xls, shtname='EmissionPrice', units='US$/ton')
    df_plant_storageDuration = get_excel_plantasmpt(xls_name=assumptions_xls, shtname='StorageDuration', units='Hours')
    log_df_info(f"{step} assumptions PlantLife", df_plant_life)
    log_df_info(f"{step} assumptions VOM", df_plant_vom)
    log_df_info(f"{step} assumptions HeatRate", df_plant_heatrate)
    log_df_info(f"{step} assumptions CapacityFactor", df_plant_cf)
    log_df_info(f"{step} assumptions FixedCost", df_plant_fixcost)
    log_df_info(f"{step} assumptions EmissionRate", df_plant_emissionrate)
    log_df_info(f"{step} assumptions EmissionPrice", df_plant_emissionprice)
    log_df_info(f"{step} assumptions StorageDuration", df_plant_storageDuration)

    dest_tbl = 'APAC_PlantAttribute_AnnualAssumptions_LIVE'
    assumptions_all = pd.concat([
        df_plant_life,
        df_plant_vom,
        df_plant_heatrate,
        df_plant_cf,
        df_plant_fixcost,
        df_plant_emissionrate,
        df_plant_emissionprice,
        df_plant_storageDuration,
    ], ignore_index=True)
    log_df_info(f"{step} assumptions all", assumptions_all)
    if should_skip_read(f"{step}_assumptions", [assumptions_all]):
        log_step(f"{step}_assumptions", "read hash unchanged; skip compute/write")
    else:
        log_step(step, f"reload {dest_tbl} start")
        reload_tbl(engine=engine_src, dest_tbl=dest_tbl, dest_df=assumptions_all)
        log_step(step, f"reload {dest_tbl} done rows={len(assumptions_all)}")

    log_step(step, "read fuel excel")
    df_fuel = get_excel_fuel(xls_name=fuels_xls)
    log_df_info(f"{step} fuel", df_fuel)
    if should_skip_read(f"{step}_fuel", [df_fuel]):
        log_step(f"{step}_fuel", "read hash unchanged; skip compute/write")
    else:
        dest_tbl = 'APAC_PlantFuel_Annual_LIVE'
        clear_sql = """TRUNCATE TABLE """ + dest_tbl
        log_step(step, f"truncate {dest_tbl}")
        execute_sqlcur(engine=engine_src, sql=clear_sql)
        log_step(step, f"write {dest_tbl} start")
        upload_sql(engine=engine_src, df=df_fuel, dest_sql=dest_tbl, existMethod='append')
        log_step(step, f"write {dest_tbl} done rows={len(df_fuel)}")


def run_topology():
    step = "topology"
    log_df_info(f"{step} zones", df_zone)
    log_df_info(f"{step} areas", df_area)
    if should_skip_read(step, [df_zone, df_area]):
        log_step(step, "read hash unchanged; skip compute/write")
        return
    log_step(step, "write tbl_AID_Topology_Zones start")
    reload_tbl(engine=engine_dest, dest_tbl='tbl_AID_Topology_Zones', dest_df=df_zone)
    log_step(step, f"write tbl_AID_Topology_Zones done rows={len(df_zone)}")
    log_step(step, "write tbl_AID_Topology_Areas start")
    reload_tbl(engine=engine_dest, dest_tbl='tbl_AID_Topology_Areas', dest_df=df_area)
    log_step(step, f"write tbl_AID_Topology_Areas done rows={len(df_area)}")


def run_demand():
    step = "demand"
    log_step(step, "read demand from SQL start")
    demand_hrlyshape, demand_mthlypeak, demand_mthly, demand_skipped = get_sql_demand_to_aid(
        src_sql='vAPAC_LoadDurationCurve_Normalised_Forecast_LIVE',
        country=country,
        year_start=demand_year_start,
        year_end=demand_year_end,
        base_year=demand_base_year,
    )
    if demand_skipped:
        return
    log_df_info(f"{step} hourly shape", demand_hrlyshape)
    log_df_info(f"{step} monthly peak", demand_mthlypeak)
    log_df_info(f"{step} monthly", demand_mthly)
    if 'Primary Key' in demand_mthlypeak.columns:
        demand_mthlypeak = demand_mthlypeak.drop(columns=['Primary Key'])
        log_step(step, "drop Primary Key for tbl_AID_Demand_Monthly_Peak (identity)")
    log_step(step, "write demand tables start")
    reload_tbl(engine=engine_dest, dest_tbl='tbl_AID_Demand_Hourly_Shapes', dest_df=demand_hrlyshape)
    reload_tbl(engine=engine_dest, dest_tbl='tbl_AID_Demand_Monthly_Peak', dest_df=demand_mthlypeak)
    reload_tbl(engine=engine_dest, dest_tbl='tbl_AID_Demand_Monthly', dest_df=demand_mthly)
    log_step(step, "write demand tables done")


def run_transmission():
    step = "transmission"
    log_step(step, "read transmission from SQL start")
    transmission_link, ts_annual_txlink = get_sql_transmission_links_to_aid(
        src_sql='vAPAC_Transmission_LIVE', country=country
    )
    log_df_info(f"{step} links", transmission_link)
    log_df_info(f"{step} ts_annual", ts_annual_txlink)
    if should_skip_read(step, [transmission_link, ts_annual_txlink], ts_annual_module_key="transmission"):
        log_step(step, "read hash unchanged; skip compute/write")
        return
    log_step(step, "write transmission tables start")
    reload_tbl(engine=engine_dest, dest_tbl='tbl_AID_Transmission_Links', dest_df=transmission_link)
    update_aid_id(dest_tbl='tbl_AID_Time_Series_Annual', dest_df=ts_annual_txlink, module_key="transmission")
    log_step(step, "write transmission tables done")


def run_hydro():
    step = "hydro"
    log_step(step, "read hydro from SQL start")
    hydro_vector, hydro_monthly = get_sql_hydroshapes_to_aid(
        src_sql_vec='APAC_AID_HydroVectors_LIVE',
        src_sql_mthly='vAPAC_Shapes_Monthly_LatestYear',
        country=country
    )
    log_df_info(f"{step} vector", hydro_vector)
    log_df_info(f"{step} monthly", hydro_monthly)
    if should_skip_read(step, [hydro_vector, hydro_monthly]):
        log_step(step, "read hash unchanged; skip compute/write")
        return
    log_step(step, "write hydro tables start")
    reload_tbl(engine=engine_dest, dest_tbl='tbl_AID_Hydro_Vectors', dest_df=hydro_vector)
    reload_tbl(engine=engine_dest, dest_tbl='tbl_AID_Hydro_Monthly', dest_df=hydro_monthly)
    log_step(step, "write hydro tables done")

def run_shapes():
    global TS_Weekly, TS_Monthly, Maint_mth, Maint_wk

    step = "shapes"
    log_step(step, "read wind/solar shapes from SQL start")
    ts_weekly, ts_monthly, maint_mth, maint_wk = get_sql_windsolarshapes_to_aid_ts_wek(
        src_sql='vAPAC_Shapes_8760_to_168_LatestYear_ModelZone',
        country=country,
        year_start=demand_year_start,
        year_end=demand_year_end,
    )
    log_df_info(f"{step} TS_Weekly", ts_weekly)
    log_df_info(f"{step} TS_Monthly", ts_monthly)
    log_df_info(f"{step} Maint_mth", maint_mth)
    log_df_info(f"{step} Maint_wk", maint_wk)
    if should_skip_read(step, [ts_weekly, ts_monthly, maint_mth, maint_wk]):
        if step_ready(step):
            log_step(step, "read hash unchanged; reuse cached shapes")
            return
        log_step(step, "read hash unchanged but no cached shapes; continue")

    TS_Weekly, TS_Monthly, Maint_mth, Maint_wk = ts_weekly, ts_monthly, maint_mth, maint_wk

    log_step(step, "compute adjustments start")
    Maint_mth['Level'] = 'ModelZone'
    Maint_mth['Resource Group'] = np.where(Maint_mth['ShapeName'].str.contains('PV_D'), 'PV_D',
                                              np.where(Maint_mth['ShapeName'].str.contains('PV_BA'), 'PV_BA',
                                                np.where(Maint_mth['ShapeName'].str.contains('PV'), 'PV',
                                                  np.where(Maint_mth['ShapeName'].str.contains('Onshore'), 'Wind_Onshore',
                                                    np.where(Maint_mth['ShapeName'].str.contains('Offshore'), 'Wind_Offshore', 'NewShape')))))
    Maint_mth['ID'] = Maint_mth['ID'].str.replace('East Central South Kalimantan', 'ECS Kalimantan')
    Maint_mth['ID'] = Maint_mth['ID'].str.replace('Papua Timor Maluku Nusa Tenggara', 'Timor')
    TS_Weekly['ID'] = TS_Weekly['ID'].str.replace('East Central South Kalimantan', 'ECS Kalimantan')
    TS_Weekly['ID'] = TS_Weekly['ID'].str.replace('Papua Timor Maluku Nusa Tenggara', 'Timor')
    col_lst = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 'ID']
    for col in col_lst:
        TS_Monthly[col] = TS_Monthly[col].str.replace('East Central South Kalimantan', 'ECS Kalimantan')
        TS_Monthly[col] = TS_Monthly[col].str.replace('Papua Timor Maluku Nusa Tenggara', 'Timor')

    TS_Weekly = TS_Weekly.drop(['Month'], axis=1)
    TS_Weekly.rename(columns={'Area': 'zREM Topology', 'ShapeName': 'zREM Type'}, inplace=True)
    TS_Monthly.rename(columns={'Area': 'zREM Topology', 'ShapeName': 'zREM Type'}, inplace=True)

    log_step(step, "compute adjustments done")
    if debug_mode:
        maint_mask = TS_Monthly['zREM Type'].astype(str).str.contains('maint', case=False, na=False)
        if maint_mask.any():
            check_cols = [c for c in TS_Monthly.columns if c not in ('zREM Topology', 'zREM Type', 'ID', 'Year')]
            vals = TS_Monthly.loc[maint_mask, check_cols].apply(pd.to_numeric, errors='coerce')
            neg_mask = (vals < 0).any(axis=1)
            if neg_mask.any():
                neg_rows = TS_Monthly.loc[maint_mask].loc[neg_mask]
                sample_ids = neg_rows['ID'].astype(str).head(5).tolist() if 'ID' in neg_rows.columns else []
                log_step(step, f"debug WARNING: maintenance rate negative rows={len(neg_rows)} sample_ids={sample_ids}")
    # clamp maintenance rates to >= 0 before writing
    maint_mask = TS_Monthly['zREM Type'].astype(str).str.contains('maint', case=False, na=False)
    if maint_mask.any():
        clamp_cols = [c for c in TS_Monthly.columns if c not in ('zREM Topology', 'zREM Type', 'ID', 'Year')]
        if clamp_cols:
            TS_Monthly.loc[maint_mask, clamp_cols] = TS_Monthly.loc[maint_mask, clamp_cols].apply(
                pd.to_numeric, errors='coerce'
            ).clip(lower=0)
    # clamp weekly shape values (168-hour columns) to >= 0 as well
    hr_cols = [c for c in TS_Weekly.columns if isinstance(c, (int, float))]
    if hr_cols:
        TS_Weekly[hr_cols] = TS_Weekly[hr_cols].apply(pd.to_numeric, errors='coerce').clip(lower=0)
    log_step(step, "write weekly/monthly tables start")
    reload_tbl(engine=engine_dest, dest_tbl='tbl_AID_Time_Series_Weekly', dest_df=TS_Weekly)
    reload_tbl(engine=engine_dest, dest_tbl='tbl_AID_Time_Series_Monthly', dest_df=TS_Monthly)
    log_step(step, "write weekly/monthly tables done")


def run_plants_existing():
    global PlantExisting, ts_annual_assumptions, EmissionRate, EmissionPrice
    global TS_Annual_PlantExisting, TS_StorageMax_Exist

    step = "plants_existing"
    log_step(step, "read existing plants from SQL start")
    Plant_Existing = get_sql_plant_existing(src_sql='vAPAC_PowerProjects_LIVE', country=country)
    log_df_info(f"{step} raw", Plant_Existing)
    if should_skip_read(step, [Plant_Existing]):
        if PlantExisting is not None:
            log_step(step, "read hash unchanged; reuse cached plants_existing")
            return
        log_step(step, "read hash unchanged but no cached plants_existing; continue")
    log_step(step, "aggregate plant list start")
    Plant_Existing, TS_Annual_PlantExisting, TS_StorageMax_Exist = aggregate_plant_list(
        df_plantlist=Plant_Existing, name_prefix='Existing',
        yr_start=2020, yr_end=yr_end, step=1,
        offset_balancing_capacity=True
    )
    log_df_info(f"{step} aggregated", Plant_Existing)
    log_df_info(f"{step} ts_annual", TS_Annual_PlantExisting)
    log_df_info(f"{step} storage_max", TS_StorageMax_Exist)
    Plant_Existing.rename(columns={'ResourceBeginDate': 'Resource Begin Date',
                                   'ResourceEndDate': 'Resource End Date'}, inplace=True)

    log_step(step, "assign assumptions start")
    PlantExisting = assign_assumptions(df=Plant_Existing)
    PlantExisting, ts_annual_assumptions = assign_tsannual_assumptions(df=PlantExisting)
    log_df_info(f"{step} ts_annual_assumptions", ts_annual_assumptions)
    log_step(step, "apply emissions start")
    PlantExisting, EmissionRate, EmissionPrice = apply_emissions(df=PlantExisting)
    log_df_info(f"{step} emission_rate", EmissionRate)
    log_df_info(f"{step} emission_price", EmissionPrice)
    log_step(step, "apply emissions done")
    PlantExisting['Second Fuel'] = np.where(PlantExisting['Second Fuel'] == PlantExisting['Fuel'],
                                               np.nan,
                                               PlantExisting['Second Fuel'])
    PlantExisting[['HeatRate', 'Heat Rate']] = PlantExisting[['HeatRate', 'Heat Rate']].ffill(axis=1).infer_objects(copy=False)
    PlantExisting['Forced Outage'] = 100 * (1 - PlantExisting['CapacityFactor'])
    PlantExisting = PlantExisting.drop(['HeatRate', 'PlantLife', 'CapacityFactor', 'PlantType-Tech', 'StartYear'], axis=1)
    PlantExisting.rename(columns={'VOM': 'Variable O&M',
                                  'FixedCost': 'Fix Cost Mod1',
                                  'EmissionRate': 'Emission Rate ID',
                                  'EmissionPrice': 'Emission Price ID'}, inplace=True)
    PlantExisting['ID'] = PlantExisting['Name']
    log_step(step, "apply shapes/maintenance start")
    PlantExisting = apply_solar_wind_shapes_to_plants(df_maint_mth=Maint_mth, df_plant=PlantExisting)
    PlantExisting['Forced Outage'] = np.where(PlantExisting['Maintenance Rate'].isnull(),
                                                 PlantExisting['Forced Outage'],
                                                 np.nan)
    log_df_info(f"{step} final", PlantExisting)
    log_step(step, "apply shapes/maintenance done")


def run_plants_newbuild():
    global Plant_New, tmp_EmissionRate, tmp_EmissionPrice
    global TS_Annual_PlantNew, TS_StorageMax_New

    step = "plants_newbuild"
    log_step(step, "read newbuild plants from SQL start")
    Plant_New = get_sql_plant_newbuild(src_sql='vAPAC_PowerProjects_NewBuild_LIVE', country=country)
    log_df_info(f"{step} raw", Plant_New)
    if should_skip_read(step, [Plant_New]):
        if Plant_New is not None:
            log_step(step, "read hash unchanged; reuse cached plants_newbuild")
            return
        log_step(step, "read hash unchanged but no cached plants_newbuild; continue")
    log_step(step, "aggregate plant list start")
    Plant_New, TS_Annual_PlantNew, TS_StorageMax_New = aggregate_plant_list(
        df_plantlist=Plant_New, name_prefix='NewBuild',
        yr_start=2000, yr_end=yr_end, step=1,
        offset_balancing_capacity=False
    )
    log_df_info(f"{step} aggregated", Plant_New)
    log_df_info(f"{step} ts_annual", TS_Annual_PlantNew)
    log_df_info(f"{step} storage_max", TS_StorageMax_New)
    log_step(step, "assign assumptions start")
    Plant_New = assign_assumptions(df=Plant_New)
    Plant_New = Plant_New.drop(['PlantLife', 'Heat Rate', 'StartYear'], axis=1)
    Plant_New.rename(columns={'ResourceBeginDate': 'Resource Begin Date',
                              'ResourceEndDate': 'Resource End Date',
                              'HeatRate': 'Heat Rate',
                              'PlantType-Tech': 'Resource_Group'}, inplace=True)

    Plant_New, _tmp = assign_tsannual_assumptions(df=Plant_New)
    log_step(step, "apply emissions start")
    Plant_New, tmp_EmissionRate, tmp_EmissionPrice = apply_emissions(df=Plant_New)
    log_df_info(f"{step} emission_rate", tmp_EmissionRate)
    log_df_info(f"{step} emission_price", tmp_EmissionPrice)
    Plant_New['Forced Outage'] = 100 * (1 - Plant_New['CapacityFactor'])
    Plant_New = Plant_New.drop(['CapacityFactor', 'PlantType-Tech'], axis=1)
    Plant_New.rename(columns={'VOM': 'Variable O&M',
                              'FixedCost': 'Fix Cost Mod1',
                              'EmissionRate': 'Emission Rate ID',
                              'EmissionPrice': 'Emission Price ID'}, inplace=True)
    Plant_New['ID'] = Plant_New['Name']
    log_step(step, "apply shapes/maintenance start")
    Plant_New = apply_solar_wind_shapes_to_plants(df_maint_mth=Maint_mth, df_plant=Plant_New)
    Plant_New['Forced Outage'] = np.where(Plant_New['Maintenance Rate'].isnull(),
                                             Plant_New['Forced Outage'],
                                             np.nan)
    log_df_info(f"{step} final", Plant_New)
    log_step(step, "apply shapes/maintenance done")


def run_resources_load():
    global df_resources_raw
    step = "resources"
    if should_skip_read(step, [PlantExisting, Plant_New, ts_annual_assumptions], ts_annual_module_key="resources") and df_resources_raw is not None:
        log_step(step, "read hash unchanged; reuse cached resources")
        return
    log_df_info(f"{step} PlantExisting", PlantExisting)
    log_df_info(f"{step} Plant_New", Plant_New)
    frames = []
    if PlantExisting is not None:
        frames.append(PlantExisting)
    if Plant_New is not None:
        frames.append(Plant_New)
    df_resources_raw = pd.concat(frames, axis=0, ignore_index=True) if frames else pd.DataFrame()
    log_df_info(f"{step} combined resources raw", df_resources_raw)

    aid_table = 'tbl_AID_Time_Series_Annual'
    if debug_mode:
        def _warn_neg_ts(df, label):
            if df is None or len(df) == 0:
                return
            year_cols = [c for c in df.columns if str(c).isdigit()]
            if not year_cols:
                return
            vals = df[year_cols].apply(pd.to_numeric, errors='coerce')
            neg_mask = (vals < 0).any(axis=1)
            if neg_mask.any():
                neg = df.loc[neg_mask]
                details = []
                if 'ID' in neg.columns:
                    for _, r in neg.iterrows():
                        years = vals.loc[r.name][vals.loc[r.name] < 0].index.tolist()
                        for y in years:
                            details.append(f"{r['ID']}:{y}={vals.loc[r.name][y]}")
                log_step(step, f"debug WARNING: negative capacity in {label} rows={len(neg)} details={details}")
        _warn_neg_ts(TS_Annual_PlantExisting, "TS_Annual_PlantExisting")
        _warn_neg_ts(TS_Annual_PlantNew, "TS_Annual_PlantNew")
        _warn_neg_ts(TS_StorageMax_Exist, "TS_StorageMax_Exist")
        _warn_neg_ts(TS_StorageMax_New, "TS_StorageMax_New")
    if TS_Annual_PlantExisting is not None and len(TS_Annual_PlantExisting) > 0:
        log_step(step, "update time series annual start (existing)")
        update_aid_id(dest_tbl=aid_table, dest_df=TS_Annual_PlantExisting, module_key="resources")
        log_step(step, f"update time series annual done (existing) rows={len(TS_Annual_PlantExisting)}")
    if TS_Annual_PlantNew is not None and len(TS_Annual_PlantNew) > 0:
        log_step(step, "update time series annual start (newbuild)")
        update_aid_id(dest_tbl=aid_table, dest_df=TS_Annual_PlantNew, module_key="resources")
        log_step(step, f"update time series annual done (newbuild) rows={len(TS_Annual_PlantNew)}")
    if ts_annual_assumptions is not None and len(ts_annual_assumptions) > 0:
        log_step(step, "update time series annual start (assumptions)")
        update_aid_id(dest_tbl=aid_table, dest_df=ts_annual_assumptions, module_key="resources")
        log_step(step, f"update time series annual done (assumptions) rows={len(ts_annual_assumptions)}")
    if TS_StorageMax_Exist is not None and len(TS_StorageMax_Exist) > 0:
        log_step(step, "update time series annual start (storage max exist)")
        update_aid_id(dest_tbl=aid_table, dest_df=TS_StorageMax_Exist, module_key="resources")
        log_step(step, f"update time series annual done (storage max exist) rows={len(TS_StorageMax_Exist)}")
    if TS_StorageMax_New is not None and len(TS_StorageMax_New) > 0:
        log_step(step, "update time series annual start (storage max new)")
        update_aid_id(dest_tbl=aid_table, dest_df=TS_StorageMax_New, module_key="resources")
        log_step(step, f"update time series annual done (storage max new) rows={len(TS_StorageMax_New)}")


def run_emission():
    global EmissionPrice, EmissionRate
    global tmp_EmissionRate, tmp_EmissionPrice
    global tmp_wind_EmissionRate, tmp_wind_EmissionPrice
    step = "emission"
    log_step(step, "combine emission price/rate start")
    price_frames = [f for f in [EmissionPrice, tmp_EmissionPrice, tmp_wind_EmissionPrice] if f is not None]
    rate_frames = [f for f in [EmissionRate, tmp_EmissionRate, tmp_wind_EmissionRate] if f is not None]
    if not price_frames and not rate_frames:
        log_step(step, "no emission data available; skip")
        return
    EmissionPrice = pd.concat(price_frames, axis=0).drop_duplicates() if price_frames else None
    log_df_info(f"{step} price", EmissionPrice)
    EmissionRate = pd.concat(rate_frames, axis=0).drop_duplicates() if rate_frames else None
    log_df_info(f"{step} rate", EmissionRate)
    if should_skip_read(step, [EmissionPrice, EmissionRate]):
        if EmissionPrice is not None and EmissionRate is not None:
            log_step(step, "read hash unchanged; reuse cached emission")
            return
        log_step(step, "read hash unchanged but no cached emission; continue")
    log_step(step, "write tbl_AID_Emission_Prices start")
    reload_tbl(engine=engine_dest, dest_tbl='tbl_AID_Emission_Prices', dest_df=EmissionPrice)
    log_step(step, f"write tbl_AID_Emission_Prices done rows={len(EmissionPrice)}")
    log_step(step, "write tbl_AID_Emission_Rates start")
    reload_tbl(engine=engine_dest, dest_tbl='tbl_AID_Emission_Rates', dest_df=EmissionRate)
    log_step(step, f"write tbl_AID_Emission_Rates done rows={len(EmissionRate)}")


def run_fuel():
    global ts_annual_fuel, Fuel
    step = "fuel"
    log_step(step, "read fuel from SQL start")
    ts_annual_fuel, Fuel = get_sql_fuel_to_aid(src_sql='vAPAC_Plant_Fuel_Price_Annual_LIVE', country=country)
    log_df_info(f"{step} fuel", Fuel)
    log_df_info(f"{step} ts_annual", ts_annual_fuel)
    if should_skip_read(step, [ts_annual_fuel, Fuel], ts_annual_module_key="fuel"):
        if Fuel is not None and ts_annual_fuel is not None:
            log_step(step, "read hash unchanged; reuse cached fuel")
            return
        log_step(step, "read hash unchanged but no cached fuel; continue")

    FuelName = ['Storage', 'PumpedStorage', 'Sun_Dist']
    FuelType = ['Storage', 'PS', 'Sun']
    Topology = ['All', 'All', 'All']
    Units = [np.nan, np.nan, 'US$/mmbtu']
    Price = [np.nan, np.nan, 'yr_Price_Sun']
    Fuel_Special = pd.DataFrame(list(zip(FuelName, FuelName, FuelType, Topology, Units, Price)),
                                columns=['Fuel ID', 'Fuel Name', 'Fuel Type', 'zREM Topology', 'Units', 'Price'])

    Fuel = pd.concat([Fuel, Fuel_Special], ignore_index=True)
    log_df_info(f"{step} fuel (with special)", Fuel)

    TS_Annual_Price = pd.DataFrame(ts_annual_fuel)
    TS_Annual_Price = TS_Annual_Price.drop(['Level', 'Fuel Name', 'Description', 'Fuel Type', 'Units', 'Price'], axis=1)
    TS_Annual_Price.rename(columns={'Fuel ID': 'ID'}, inplace=True)
    TS_Annual_Price['Use'] = 'Price'
    TS_Annual_Price['ID'] = 'Price_' + TS_Annual_Price['ID']
    log_df_info(f"{step} ts_annual_price", TS_Annual_Price)
    log_step(step, "update tbl_AID_Time_Series_Annual start")
    update_aid_id(dest_tbl='tbl_AID_Time_Series_Annual', dest_df=TS_Annual_Price, module_key="fuel")
    log_step(step, f"update tbl_AID_Time_Series_Annual done rows={len(TS_Annual_Price)}")


def run_postprocess_resource():
    global df_resource, df_resourceGroup

    step = "postprocess_resource"
    if df_resources_raw is None:
        raise RuntimeError("df_resources_raw is None; run resources step before postprocess_resource")
    df_aidResource = df_resources_raw.copy()
    log_df_info(f"{step} raw (in-memory)", df_aidResource)
    df_aidResource['Fuel'] = df_aidResource['Fuel'].str.strip()
    df_aidResource['zREM Primary'] = df_aidResource['Resource Group']
    df_aidResource['zREM Secondary'] = df_aidResource['Fuel']
    df_aidResource['zREM Secondary'] = np.where(df_aidResource['zREM Primary'] == 'PV_D', 'Sun_Dist', df_aidResource['zREM Secondary'])
    df_aidResource['zREM Secondary'] = np.where(df_aidResource['zREM Primary'].isin(['Wind_Onshore', 'WT_BA']),
                                                   'Wind_Onshore', df_aidResource['zREM Secondary'])
    df_aidResource['zREM Secondary'] = np.where(df_aidResource['zREM Primary'].isin(['Wind_Offshore', 'OW_BA']),
                                                   'Wind_Offshore', df_aidResource['zREM Secondary'])

    df_aidResource['Resource Group'] = df_aidResource['zREM Secondary'] + "_" + df_aidResource['zREM County']

    df_aidResource = match_aidResourceFuel_to_aidFuel(df_resource=df_aidResource, df_fuel=Fuel, colname='Fuel')
    df_aidResource['Second Fuel'] = np.where(
        (df_aidResource['Fuel'].str.contains('Gas') & df_aidResource['Second Fuel'].isnull()),
        'LNG', df_aidResource['Second Fuel']
    )
    df_resource = match_aidResourceFuel_to_aidFuel(df_resource=df_aidResource, df_fuel=Fuel, colname='Second Fuel')

    df_resource['Fuel'] = np.where(df_resource['Fuel'].str.contains('Storage'), 'Storage', df_resource['Fuel'])
    df_resource['Fuel'] = np.where(df_resource['Fuel'].str.contains('DSM'), 'DSM', df_resource['Fuel'])
    df_resource['Fuel'] = np.where(df_resource['zREM Primary'] == 'Hydro_PS', 'PumpedStorage', df_resource['Fuel'])

    if 'Hydro Number' not in df_resource.columns:
        df_resource['Hydro Number'] = np.nan
    # Hydro Number should be the province name (zREM County), not the zone ID (Area)
    area_name_col = 'zREM County'
    if area_name_col not in df_resource.columns:
        if 'Area Name' in df_resource.columns:
            area_name_col = 'Area Name'
        elif 'Area' in df_resource.columns:
            area_name_col = 'Area'
        else:
            area_name_col = None
    if area_name_col is not None:
        fuel_series = df_resource['Fuel'].fillna('')
        df_resource['Hydro Number'] = np.where(
            fuel_series.isin(['Water', 'PumpedStorage']),
            df_resource[area_name_col],
            df_resource['Hydro Number']
        )
        df_resource['Hydro Number'] = np.where(
            fuel_series.str.contains('Water'),
            df_resource[area_name_col],
            df_resource['Hydro Number']
        )

    df_resource['ID'] = df_resource.index + 1
    df_resource['ID'] = df_resource['ID'].astype(str)
    df_resource['ID'] = df_resource['ID'] + '_' + df_resource['Name']

    if 'Recharge Capacity' not in df_resource.columns:
        df_resource['Recharge Capacity'] = np.nan
    if 'Maximum Storage' not in df_resource.columns:
        df_resource['Maximum Storage'] = np.nan
    if 'Initial Contents' not in df_resource.columns:
        df_resource['Initial Contents'] = np.nan
    if 'Storage Control Type' not in df_resource.columns:
        df_resource['Storage Control Type'] = np.nan
    if 'Storage ID' not in df_resource.columns:
        df_resource['Storage ID'] = np.nan
    if 'Must Run' not in df_resource.columns:
        df_resource['Must Run'] = 0
    if 'Minimum Capacity' not in df_resource.columns:
        df_resource['Minimum Capacity'] = 0

    for batt in ba_list:
        df_resource['Recharge Capacity'] = np.where(df_resource['zREM Primary'] == batt,
                                          df_resource['Capacity'],
                                          df_resource['Recharge Capacity'])
        df_resource['Maximum Storage'] = np.where(df_resource['zREM Primary'] == batt,
                                                     'yr_StorageMax_' + df_resource['Name'],
                                                     df_resource['Maximum Storage'])
        df_resource['Initial Contents'] = np.where(df_resource['zREM Primary'] == batt,
                                          0.5,
                                          df_resource['Initial Contents'])
        df_resource['Storage Control Type'] = np.where(df_resource['zREM Primary'] == batt,
                                                          'DemandNetMR',
                                                          df_resource['Storage Control Type'])
        df_resource['Storage ID'] = np.where(df_resource['zREM Primary'] == batt,
                                          df_resource['Name'],
                                          df_resource['Storage ID'])

    df_resource['Storage ID'] = np.where(df_resource['zREM Primary'].isin(['BA']),
                                            'Stand_Alone',
                                            df_resource['Storage ID'])
    df_resource['Storage ID'] = np.where(df_resource['zREM Primary'].isin(['Hydro_PS']),
                                            np.nan,
                                            df_resource['Storage ID'])

    df_resource['Fuel'] = np.where(df_resource['zREM Primary'] == 'PV_D',
                                          'Sun_Dist',
                                          df_resource['Fuel'])

    df_resource = df_resource.drop(['Zone', 'Market', 'Country'], axis=1)

    df_resource['Must Run'] = np.where(
        (df_resource['zREM Technology'].isin(['PV', 'PV_D', 'PV_BA', 'WT_BA', 'OW_BA', 'Wind_Onshore', 'Wind_Offshore'])),
        1, df_resource['Must Run'])
    df_resource['Minimum Capacity'] = np.where(
        (df_resource['zREM Technology'].isin(['PV', 'PV_D', 'PV_BA', 'WT_BA', 'OW_BA', 'Wind_Onshore', 'Wind_Offshore'])),
        10, df_resource['Minimum Capacity'])

    df_resourceGroup = df_resource[['Resource Group', 'Resource Group', 'zREM Secondary', 'zREM County']].reset_index(drop=True)
    df_resourceGroup.columns = ['Number', 'Name', 'Technology Name', 'Zone Name']
    df_resourceGroup.drop_duplicates(inplace=True)
    df_resourceGroup['Report'] = 0

    log_df_info(f"{step} resource_group", df_resourceGroup)
    log_df_info(f"{step} resources", df_resource)

def run_storage():
    global storage_table
    step = "storage"
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

    Storage_Table = pd.concat([
        Storage_Table,
        pd.DataFrame([{
            'Storage ID': 'Stand_Alone',
            'Efficiency': 0.8,
            'Shaping Method': 'Price',
            'Charging Availability': 1,
            'Generating Availability': 1,
            'Charging Resource': np.nan
        }])
    ], ignore_index=True)
    storage_table = Storage_Table
    log_df_info(f"{step} storage table", storage_table)


def run_constraints():
    step = "constraints"
    log_step(step, "read fuel constraints from SQL start")
    ts_annual_fuelmax, FuelMax = get_sql_fuel_max_to_aid(src_sql='vAPAC_Plant_Fuel_MinMax_Annual_LIVE', country=country)
    log_df_info(f"{step} fuelmax", FuelMax)
    log_df_info(f"{step} ts_annual", ts_annual_fuelmax)
    if should_skip_read(step, [ts_annual_fuelmax, FuelMax], ts_annual_module_key="constraints"):
        if FuelMax is not None and ts_annual_fuelmax is not None:
            log_step(step, "read hash unchanged; reuse cached constraints")
            return
        log_step(step, "read hash unchanged but no cached constraints; continue")
    log_step(step, "write tbl_AID_Constraint start")
    reload_tbl(engine=engine_dest, dest_tbl='tbl_AID_Constraint', dest_df=FuelMax)
    log_step(step, f"write tbl_AID_Constraint done rows={len(FuelMax)}")

    TS_Annual_Max = pd.DataFrame(ts_annual_fuelmax)
    TS_Annual_Max.rename(columns={'Set ID': 'ID',
                                  'Constraint Type': 'Use'}, inplace=True)
    TS_Annual_Max = TS_Annual_Max.drop(['Level', 'zREM Comment', 'Description', 'Fuel Type',
                                        'Limit Units', 'Limit Type', 'Chronological Method', 'Limit'], axis=1)
    log_df_info(f"{step} ts_annual_max", TS_Annual_Max)
    log_step(step, "update tbl_AID_Time_Series_Annual start")
    update_aid_id(dest_tbl='tbl_AID_Time_Series_Annual', dest_df=TS_Annual_Max, module_key="constraints")
    log_step(step, f"update tbl_AID_Time_Series_Annual done rows={len(TS_Annual_Max)}")

    Fuel_local = Fuel.copy() if Fuel is not None else pd.DataFrame()
    if not Fuel_local.empty:
        cols = ['Fuel ID', 'Fuel Name', 'Fuel Type', 'Units', 'Price', 'zREM Topology']
        Fuel_local = Fuel_local[[c for c in cols if c in Fuel_local.columns]]
    log_df_info(f"{step} fuel local (in-memory)", Fuel_local)

    FuelMax.rename(columns={'zREM Comment': 'Fuel ID'}, inplace=True)
    FuelMax = FuelMax[['Fuel ID', 'Set ID', 'Constraint Type']].reset_index(drop=True)
    FuelLimits = FuelMax.pivot_table(index=['Fuel ID'], columns='Constraint Type', values='Set ID', aggfunc='first')
    FuelLimits = FuelLimits.reset_index()
    if 'Fuel Min' not in FuelLimits.columns:
        FuelLimits['Fuel Min'] = np.nan
    if 'Fuel Max' not in FuelLimits.columns:
        FuelLimits['Fuel Max'] = np.nan
    FuelLimits['Set ID'] = np.where((FuelLimits['Fuel Min'].notnull() & FuelLimits['Fuel Max'].notnull()),
                                       FuelLimits['Fuel Min'] + ', ' + FuelLimits['Fuel Max'],
                                       np.where(FuelLimits['Fuel Min'].isnull(), FuelLimits['Fuel Max'], FuelLimits['Fuel Min']))
    FuelLimits = FuelLimits[['Fuel ID', 'Set ID']].reset_index(drop=True)
    Fuel_local = pd.merge(Fuel_local, FuelLimits, how='left', on=['Fuel ID'])
    Fuel_local.rename(columns={'Set ID': 'Fuel Constraint ID'}, inplace=True)
    Fuel_local = pd.concat([
        Fuel_local,
        pd.DataFrame([{
            'Fuel ID': 'Others',
            'Fuel Name': 'Others',
            'Fuel Type': 'Others',
            'Units': 'US$/mmbtu',
            'Price': 0.1,
            'zREM Topology': 'All'
        }])
    ], ignore_index=True)
    Fuel_local = Fuel_local.reset_index(drop=True)

    log_step(step, "write tbl_AID_Fuel start")
    reload_tbl(engine=engine_dest, dest_tbl='tbl_AID_Fuel', dest_df=Fuel_local)
    log_step(step, f"write tbl_AID_Fuel done rows={len(Fuel_local)}")


    return


def run_clone_operating_rules():
    step = "clone_operating_rules"
    src_sql = 'tbl_AID_Operating_Rules'
    sql_qry = """SELECT * FROM """ + src_sql
    log_step(step, "read tbl_AID_Operating_Rules start")
    df_op = pd.read_sql_query(sql_qry, engine_dest)
    df_op = df_op.drop(['Primary Key'], axis=1)
    log_df_info(f"{step} rules", df_op)

    str_id = """('"""
    for index, row in df_op.iterrows():
        str_id += row['Rule Value'][3:] + """','"""
    str_id = str_id[:-2] + ')'

    src_sql = 'tbl_AID_Time_Series_Annual'
    sql_qry = """SELECT * FROM """ + src_sql + """ WHERE [ID] IN """ + str_id
    log_step(step, "read tbl_AID_Time_Series_Annual for rules start")
    df_op_values = pd.read_sql_query(sql_qry, engine_dest)
    df_op_values = df_op_values.drop(['Primary Key'], axis=1)
    log_df_info(f"{step} rule values", df_op_values)

    log_step(step, "append operating rules start")
    upload_sql(engine=engine_dest, df=df_op, dest_sql='tbl_AID_Operating_Rules', existMethod='append')
    log_step(step, f"append operating rules done rows={len(df_op)}")
    log_step(step, "append operating rule values start")
    upload_sql(
        engine=engine_dest,
        df=df_op_values,
        dest_sql='tbl_AID_Time_Series_Annual',
        existMethod='append',
        module_key="operating_rules",
    )
    log_step(step, f"append operating rule values done rows={len(df_op_values)}")


def run_all_modules():
    run_excel_imports()
    run_topology()
    run_demand()
    run_transmission()
    run_hydro()
    run_shapes()
    run_plants_existing()
    run_plants_newbuild()
    run_emission()
    run_resources_load()
    run_fuel()
    run_constraints()
    run_postprocess_resource()
    run_storage()
    if df_resourceGroup is None or df_resource is None or storage_table is None:
        raise RuntimeError("final tables missing; ensure resources/postprocess/storage ran")
    log_step("final_tables", "write final resource tables start")
    reload_tbl(engine=engine_dest, dest_tbl='tbl_AID_Resource_Groups', dest_df=df_resourceGroup)
    reload_tbl(engine=engine_dest, dest_tbl='tbl_AID_Resources', dest_df=df_resource)
    reload_tbl(engine=engine_dest, dest_tbl='tbl_AID_Storage', dest_df=storage_table)
    log_step("final_tables", "write final resource tables done")
    finalize_ts_annual_ids_cleanup()


def print_banner():
    border = "###############################################"
    width = len(border)
    inner = width - 2
    def line(text):
        return f"#{text:^{inner}}#"
    lines = [
        border,
        line("WM Aurora data IO for China"),
        line("v4.00"),
        border,
    ]
    print("\n".join(lines))

def interactive_loop():
    global demand_year_start, demand_year_end, demand_base_year, debug_mode, force_update, debug_run_ts
    while True:
        print_banner()
        display_start = demand_year_start if demand_year_start is not None else "default"
        display_end = demand_year_end if demand_year_end is not None else "default"
        display_base = demand_base_year if demand_base_year is not None else "default"
        display_debug = "true" if debug_mode else "false"
        display_force = "true" if force_update else "false"
        print("\nOptions:")
        print(f"  1. set years (start={display_start}, end={display_end}, base={display_base})")
        print(f"  2. set debug mode (current={display_debug})")
        print(f"  3. set force update (current={display_force})")
        print("  r. run all modules")
        print("  q. quit")
        choice = input("Select option: ").strip().lower()
        if choice in ("q", "quit", "exit"):
            break
        if choice in ("r", "run"):
            init_environment()
            run_all_modules()
            continue
        if choice == "1":
            ys = input("Enter year start (blank to keep current): ").strip()
            ye = input("Enter year end (blank to keep current): ").strip()
            yb = input("Enter base year for hourly (blank to keep current): ").strip()
            if ys:
                demand_year_start = int(ys)
            if ye:
                demand_year_end = int(ye)
            if yb:
                demand_base_year = int(yb)
            reset_runtime_state()
            continue
        if choice == "2":
            val = input("Enable debug mode? (true/false, blank to keep current): ").strip().lower()
            if val in ("true", "false"):
                debug_mode = val == "true"
                if debug_mode:
                    debug_run_ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
                else:
                    debug_run_ts = None
                reset_runtime_state()
            continue
        if choice == "3":
            val = input("Force update (ignore hashes)? (true/false, blank to keep current): ").strip().lower()
            if val in ("true", "false"):
                force_update = val == "true"
            continue
        log(f"Invalid selection: {choice}")


def main():
    parser = argparse.ArgumentParser(
        description="China-only Aurora IO CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    args = parser.parse_args()

    global demand_year_start, demand_year_end, demand_base_year, debug_mode
    demand_year_start = None
    demand_year_end = None
    demand_base_year = None
    global debug_run_ts
    if debug_mode:
        debug_run_ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")

    interactive_loop()


if __name__ == "__main__":
    main()
