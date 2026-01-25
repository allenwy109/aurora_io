# -*- coding: utf-8 -*-
"""
Created on Mon Jul 9 14:22:57 2019

@author: kwadav


Version 4.0
=============
- expand time range to 2060

Version 3.1:
=============
- added function to check for leap year
- ref LDC would have an additional 24 rows if it is leap year, and Annual GWh would be distributed across an additional day.

Version 3.0:
=============
- ref LDC is now made to start on Mon and an additional week is added at the end
- when using the ref LDC's shape for forecast years, an offset is taken to ensure start day of week is same.
- 8760 rows are then take from that offset for the shape
- additional day in leap years are not accounted for (missing 29 feb)

Version 2.2:
=============
- When multiple Historical LDCs for any Area are available in the DB, it will now use the latest year's shape

Version 2.1:
=============
- big fix: different Area's Shape/Loadfactor can now be used to generate LDCs.

Version 2.0:
=============
- changed user input/assumption to Annual Load Factors

"""

print("\nWood Mackenzie LDC generator v4.0")
print("\nCopyright © 2019, Wood Mackenzie Limited.\n")
print(f"Loading Python libraries and functions...")

import warnings
import getpass
import os
from datetime import datetime
import time
import urllib
import pandas as pd
import numpy as np
import xlwings as xw
from sqlalchemy import create_engine
import sqlalchemy
from scipy.stats import norm
from scipy.optimize import fsolve


# <editor-fold desc="GLOBAL Variables">
params = urllib.parse.quote_plus("DRIVER={ODBC Driver 17 for SQL Server};"
                                 "SERVER=ANVDEVSQLVPM01;"
                                 "DATABASE=WM_POWER_RENEWABLES;"
                                 "Trusted_Connection=Yes")
engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params, fast_executemany=True)

StartYr = 2000
user = getpass.getuser()
timestamp = datetime.now()
#src_asmpt = r'C:\Users\kwadav\OneDrive - Verisk Analytics\LDC_generator.xlsx'
# </editor-fold>


# <editor-fold desc="DEFINE Functions">

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

def get8760map():
    """

    :return:
    """
    df = pd.DataFrame({'dtm': pd.date_range('2011-01-01', '2012-01-01', freq='1H', closed='left')})
    df['Hr_Yr'] = df.index + 1
    df['Mth'] = df['dtm'].dt.month
    df['Day'] = df['dtm'].dt.day
    df['Hr_Day'] = df['dtm'].dt.hour
    df = df.drop(['dtm'], 1)

    return df

def get8784map():
    """

    :return:
    """
    df = pd.DataFrame({'dtm': pd.date_range('2012-01-01', '2013-01-01', freq='1H', closed='left')})
    df['Hr_Yr'] = df.index + 1
    df['Mth'] = df['dtm'].dt.month
    df['Day'] = df['dtm'].dt.day
    df['Hr_Day'] = df['dtm'].dt.hour
    df = df.drop(['dtm'], 1)

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

def getUserVar():
    """

    :param      :
    :return     : user input - update cycle

    """

    wb = xw.Book.caller()

    sht = wb.sheets['Assumptions']
    strupdcycle = sht.range('B1').value
    strupload = sht.range('B2').value

    sht = wb.sheets['ref']
    strcsvpath = sht.range('rngCSVpath').value

    return strupdcycle, strupload, strcsvpath

def getCombo():
    """

    :param      :
    :return     : df of combination to use to generate LDC.
                  3 columns in df - 'refLDC' is Area Name of Historical LDC to use as a shaping reference
                                    'asmptArea' is the Area Name of the Analyst's adjusted annual LDC mean & Std Dev
                                    'gwhArea' is the  Target Annual GWh Generation of Model Zone

    """
    #wb = xw.Book(src_asmpt)
    wb = xw.Book.caller()
    sht = wb.sheets['Assumptions']
    df_combo = pd.DataFrame(sht.range('ComboSelection').value)
    df_combo.columns = ['refLDC', 'asmptArea', 'gwhArea', 'runLDC']
    df_combo = df_combo[df_combo['runLDC'] == 'Y'].reset_index(drop=True)

    return df_combo

def get_analyst_assumption(combo):
    # Input 3: Analysts' Annual mean & stdev adjustment (From Excel)
    #wb = xw.Book(src_asmpt)
    wb = xw.Book.caller()
    sht = wb.sheets['Assumptions']
    df_tgt = pd.DataFrame(sht.range('H2').expand().value)
    df_tgt.columns = df_tgt.iloc[0,:] + '_' + df_tgt.iloc[1,:]
    df_tgt = df_tgt.iloc[2:, :].reset_index(drop=True)
    df_tgt.rename(columns={'Area_Year':'Yr'}, inplace=True)
    df_tgt = df_tgt.dropna(axis=0, how='all')
    for col in df_tgt.columns:
        df_tgt[col] = pd.to_numeric(df_tgt[col], errors='coerce')
    df_tgt['Yr'] = df_tgt['Yr'].astype(int)
    df_tgt = df_tgt[df_tgt['Yr'] >= StartYr].reset_index(drop=True)
    df_tgt = pd.melt(df_tgt, id_vars=['Yr'], var_name='Area', value_name='Qty')
    df_tgt[['Area', 'Metric']] = df_tgt['Area'].str.split('_', expand=True)
    df_tgt = pd.pivot_table(df_tgt, index=['Yr', 'Area'], columns='Metric', values='Qty',fill_value=0).reset_index()
    df_tgt = df_tgt.sort_values(by = ['Area', 'Yr']).reset_index(drop=True)
    df_tgt = df_tgt[df_tgt['Area'] == combo['asmptArea']].reset_index(drop=True)

    return df_tgt

def get_latest_ldc_gwh(combo, engine):
    """

    :param combo    : dataframe of selected combination of variables to use.
                      dataframe requires 2 columns: 'gwhArea' & 'refLDC'
    :param engine   : sqlalchemy connection engine to connect to SQL Server to retrieve latest data
    :return         : 2 dataframes
                      - df_gwh: df of Annual GWh Grid Generation for Area in 'gwhArea' column of combo df
                      - df_ldc: df of latest Reference LDC (with Normalisayion and Cumulative Probability
                      Distribution calculated) for Area in 'refLDC' column of combo df
    """

    # Source SQL table/view name
    sql_LDC_hist = 'vAPAC_LoadDurationCurve_HIST_Latest'
    sql_DemandAnnual = 'vAPAC_PowerDemand_GWh_LIVE'

    # Input 1: process reference LDC (from SQL Server) df - normalise Reference LDC against Max & calculate
    # Cumulative Prob Distribution)
    sql_qry = """SELECT * FROM """ + sql_LDC_hist + """ WHERE Area = '""" + combo['refLDC'] + """'"""
    # !!! IMPORTANT: Change above sql statement when DB table start to contain newer published LDC for each Area.
    # Currently it only has 1 LDC per area. !!!
    df_ldc = pd.read_sql_query(sql_qry, engine)
    df_ldc = df_ldc.sort_values(by=['HourOfYear']).reset_index(drop=True)
    df_ldc['Normalised'] = df_ldc['Demand_MW'] / max(df_ldc['Demand_MW'])
    df_ldc['CPD'] = norm.cdf(df_ldc['Normalised'], np.mean(df_ldc['Normalised']), np.std(df_ldc['Normalised']))

    # Input 2: Analysts' Annual GWh forecast from SQL Server
    sql_qry = """SELECT * FROM """ + sql_DemandAnnual + """ WHERE Area = '""" + combo['gwhArea'] + """'"""
    df_gwh = pd.read_sql_query(sql_qry, engine)
    df_gwh = df_gwh[df_gwh['Year'] >= StartYr].reset_index(drop=True)
    # Only use Generation GWh (losses included, excludes single 'Captive' sector, includes 'Grid_Captive')
    # Managed by SQL Server via SQL View aggregation instead of df manipulation below:
    #df_gwh = df_gwh[df_gwh['Metric'] == 'Generation'].reset_index(drop=True)
    #df_gwh = df_gwh[df_gwh['Sector_Detailed'] == 'Grid'].reset_index(drop=True)
    df_gwh.rename(columns={'Year':'Yr', 'Qty':'GWh'}, inplace=True)

    return df_ldc, df_gwh

def generateLDC(df_tgt, df_gwh, df_ldc):
    """

    Function that accepts user inputs/assumptions on LDC distribution mean & stdev annually and Goal Seeks the Peak
    demand that gives the desired Annual GWh

    :param df_tgt:  user assumption on annual mean & stdev
    :param df_gwh:  annual GWh demand data from SL Server by Zones
    :param df_ldc:  reference year 8760 ldc shape
    :return:
    """

    # Derived 1: apply Input 1 to Input 3 to calculate (for each Year) the Inverse of the Cumulative Distribution function
    # using Analysts' adjusted mean & stdev
    df_ppf = pd.DataFrame()
    for index, row in df_tgt.iterrows():
        df_ppf[int(row['Yr'])] = norm.ppf(df_ldc['CPD'], loc=row['Mean'] , scale=row['Stdev'])


    # Output: apply Input 2 to Derived 1 to generate 8760 Load Duration Curves that matches the Annual Demand
    df_LDC = pd.DataFrame()
    lst_peak = []
    for index, row in df_gwh.iterrows():
        # Non-linear optimisation to meet Demand forecast (aka Excel's GoalSeek)
        yr = int(row['Yr'])
        annual_gwh_tgt = row['GWh']*1000
        area = str(row['Area'])
        initial_estimate = annual_gwh_tgt/8760

        def seek_gwh(x, tgt, year):
            return int(sum(df_ppf[year]/max(df_ppf[year])*x)) - tgt

        with warnings.catch_warnings():
            warnings.filterwarnings(action='ignore', category=RuntimeWarning)
            y = fsolve(func=seek_gwh, x0=initial_estimate, args=(annual_gwh_tgt, yr))

        # calculate LDC for forecast year that matches Annual GWh
        df_LDC[yr] = df_ppf[yr]/max(df_ppf[yr])*y
        # find Peak demand for that Year
        lst_peak += [[yr, area, max(df_LDC[yr])]]

    # OUTPUT: LDC & Annual Peak Demand for Area
    df_LDC['Area'] = area
    df_LDC['Hr_Yr'] = df_LDC.index + 1
    df_PEAK = pd.DataFrame(lst_peak)
    df_PEAK.columns = ['Yr', 'Area', 'Peak']

    return df_PEAK, df_LDC

def generateLDC_v2(df_tgt, df_gwh, df_ldc):
    """

    Function that accepts user inputs/assumptions on annual load Factors (and derive the Peak MW) and Goal Seeks the LDC stdev
    (while maintaining the reference LDC's mean) that gives the desired Annual GWh

    :param df_tgt:  user assumption on annual load factor
    :param df_gwh:  annual GWh demand data from SL Server by Zones
    :param df_ldc:  reference year 8760 ldc shape
    :return:
    """

    # Calculate mean of normalised ref_ldc's mean
    ref_mean = df_ldc['Normalised'].mean()

    # Derive Annual Peak from GWh & Load Factor
    df_peak = pd.merge(df_tgt, df_gwh, how='inner', on=['Yr'])
    df_peak = df_peak.drop(['Area_x'], 1)
    df_peak.rename(columns={'Area_y': 'Area'}, inplace=True)
    df_peak['Peak_MW'] = df_peak['GWh']/8.76/df_peak['LoadFactor']

     # Output: Generate 8760 Load Duration Curves that matches the Annual Demand
    df_LDC = pd.DataFrame()
    lst_peak = []
    #subset to selected Years
    #df_peak = df_peak[df_peak['Yr'] >= 2020 ].reset_index(drop=True)
    for index, row in df_peak.iterrows():
        # Non-linear optimisation to meet Demand forecast (aka Excel's GoalSeek)
        yr = int(row['Yr'])
        annual_gwh_tgt = row['GWh']*1000
        area = str(row['Area'])
        peak = row['Peak_MW']
        initial_estimate = 0.1

        df_ppf = pd.DataFrame()
        def seek_gwh(x, tgt, year):
            df_ppf[year] = norm.ppf(df_ldc['CPD'], loc=ref_mean, scale=x)
            return (sum(df_ppf[year]/max(df_ppf[year])*peak)) - tgt

        with warnings.catch_warnings():
            warnings.filterwarnings(action='ignore', category=RuntimeWarning)
            y = fsolve(func=seek_gwh, x0=initial_estimate, args=(annual_gwh_tgt, yr))

        # calculate LDC for forecast year that matches Annual GWh
        df_ppf[yr] = norm.ppf(df_ldc['CPD'], loc=ref_mean, scale=y)
        df_LDC[yr] = df_ppf[yr]/max(df_ppf[yr])*peak
        # find Peak demand for that Year
        lst_peak += [[yr, area, max(df_LDC[yr])]]

    # OUTPUT: LDC & Annual Peak Demand for Area
    df_LDC['Area'] = area
    df_LDC['Hr_Yr'] = df_LDC.index + 1
    df_PEAK = pd.DataFrame(lst_peak)
    df_PEAK.columns = ['Yr', 'Area', 'Peak']

    return df_PEAK, df_LDC

def generateLDC_v3(df_tgt, df_gwh, df_ldc):
    """

    Function that accepts user inputs/assumptions on annual load Factors (and derive the Peak MW) and Goal Seeks the LDC stdev
    (while maintaining the reference LDC's mean) that gives the desired Annual GWh

    :param df_tgt:  user assumption on annual load factor
    :param df_gwh:  annual GWh demand data from SL Server by Zones
    :param df_ldc:  reference year 8760 ldc shape
    :return:
    """

    # Calculate mean of normalised ref_ldc's mean
    ref_mean = df_ldc['Normalised'].mean()

    # find ref LDC year's starting day
    ref_day1 = df_ldc[df_ldc['HourOfYear'] == 1].DayOfWeek[0]
    if ref_day1 > 1:
        # take 1st week's shape
        ldc_topup = df_ldc.iloc[0:168, :].copy()
        # find the days required to make it start on Mon
        ldc_topup = ldc_topup[ldc_topup['DayOfWeek'] < ref_day1]
        # make ref LDC start on Mon
        df_ldc = pd.concat([ldc_topup, df_ldc]).reset_index(drop=True)
    # add additional 1 week at bottom to make sure there is enough
    ldc_topup = df_ldc.iloc[-168:, :].copy()
    df_ldc = pd.concat([df_ldc, ldc_topup]).reset_index(drop=True)

    # Derive Annual Peak from GWh & Load Factor
    df_peak = pd.merge(df_tgt, df_gwh, how='inner', on=['Yr'])
    df_peak = df_peak.drop(['Area_x'], 1)
    df_peak.rename(columns={'Area_y': 'Area'}, inplace=True)
    df_peak['Peak_MW'] = df_peak['GWh']/8.76/df_peak['LoadFactor']

     # Output: Generate 8760 Load Duration Curves that matches the Annual Demand
    df_LDC = pd.DataFrame()
    df_LDC_leapyr = pd.DataFrame()
    lst_peak = []
    #subset to selected Years
    #df_peak = df_peak[df_peak['Yr'] == 2020 ].reset_index(drop=True)
    for index, row in df_peak.iterrows():
        # Non-linear optimisation to meet Demand forecast (aka Excel's GoalSeek)
        yr = int(row['Yr'])
        annual_gwh_tgt = row['GWh']*1000
        area = str(row['Area'])
        peak = row['Peak_MW']
        initial_estimate = 0.1

        # find current year's Jan 1st's DayOfWeek
        yr_dayofweek = pd.Timestamp(str(yr) + '-01-01').dayofweek + 1
        # take an offset so shape starts on same day
        df_ldc_yr = df_ldc.iloc[((yr_dayofweek-1)*24):, :].reset_index(drop=True)

        # check for leap year to account for additional day in reference shape
        leap_yr = check_leap_yr(year=yr)
        if leap_yr == True:
            # take 8760 + 24 rows from offset
            df_ldc_yr = df_ldc_yr.iloc[:8784, :].reset_index(drop=True)
        else:
            # take only 8760 rows from offset
            df_ldc_yr = df_ldc_yr.iloc[:8760, :].reset_index(drop=True)
        # reset HourOfYear
        df_ldc_yr['HourOfYear'] = df_ldc_yr.index + 1

        # goal seek to meet annual energy & annual peak
        df_ppf = pd.DataFrame()
        def seek_gwh(x, tgt, year):
            df_ppf[year] = norm.ppf(df_ldc_yr['CPD'], loc=ref_mean, scale=x)
            return (sum(df_ppf[year]/max(df_ppf[year])*peak)) - tgt

        with warnings.catch_warnings():
            warnings.filterwarnings(action='ignore', category=RuntimeWarning)
            y = fsolve(func=seek_gwh, x0=initial_estimate, args=(annual_gwh_tgt, yr))

        # calculate LDC for forecast year that matches Annual GWh
        df_ppf[yr] = norm.ppf(df_ldc_yr['CPD'], loc=ref_mean, scale=y)
        if leap_yr == True:
            df_LDC_leapyr[yr] = df_ppf[yr]/max(df_ppf[yr])*peak
            # find Peak demand for that Year
            lst_peak += [[yr, area, max(df_LDC_leapyr[yr])]]
        else:
            df_LDC[yr] = df_ppf[yr]/max(df_ppf[yr])*peak
            # find Peak demand for that Year
            lst_peak += [[yr, area, max(df_LDC[yr])]]

    # OUTPUT: LDC & Annual Peak Demand for Area
    df_LDC['Area'] = area
    df_LDC['Hr_Yr'] = df_LDC.index + 1
    df_LDC_leapyr['Area'] = area
    df_LDC_leapyr['Hr_Yr'] = df_LDC_leapyr.index + 1
    df_PEAK = pd.DataFrame(lst_peak)
    df_PEAK.columns = ['Yr', 'Area', 'Peak']

    return df_PEAK, df_LDC, df_LDC_leapyr

def writeformat_excel(sheet, offset, pivottable, title):
    """

    Function that formats the excel output table.

    Args:
        sheet (xlwings sheets object): thw sheet object to be formatted
        offset (int): integer indicating the row number on the sheet to write to
        pivottable (dataframe): dataframe of output that has been pivoted
        title (string): table title

    Returns: nothing
    """
    sheet.range('A' + str(offset)).value = title
    sheet.range('A' + str(offset)).api.Font.Bold = True
    sheet.range('A' + str(offset)).api.Font.Size = 12
    sheet.range('A' + str(offset + 1)).options(index=False, header=True).value = pivottable
    sheet.range('A' + str(offset + 1)).expand('right').api.Font.Bold = True
    sheet.range('A' + str(offset + 1)).expand('right').api.Font.Size = 10
    sheet.range('A' + str(offset + 1)).expand().color = (226, 240, 250)
    sheet.range('A' + str(offset + 2)).expand().api.Font.Size = 8

    return

def click_GenerateLDC():
    """

    :return:
    """

    df_8760 = get8760map()
    df_8784 = get8784map()
    df_combo = getCombo()
    # df_combo = df_combo.iloc[:2, :].reset_index(drop=True) #uncomment this line to debug only a few loops
    OP_Peak = pd.DataFrame()
    OP_LDC = pd.DataFrame()
    OP_LDC_LeapYr = pd.DataFrame()
    # loop through every combination defined to generate & optimise a LDC for every Year
    for index, combo in df_combo.iterrows():
        #combo = df_combo.iloc[0,:] #uncomment this line to debug only 1 combo
        print(f" Optimising LDC to match Annual GWh forecast for : {combo['gwhArea']}")
        start = time.time()
        df_tgt = get_analyst_assumption(combo)
        df_ldc, df_gwh = get_latest_ldc_gwh(combo, engine)
        df_PEAK, df_LDC, df_LDC_leapyr = generateLDC_v3(df_tgt, df_gwh, df_ldc)
        end = time.time()
        print(f"  {combo['gwhArea']} took {end - start} seconds to optimise.")

        # build OUTPUT df by appending df of each combo
        OP_Peak = OP_Peak.append(df_PEAK, ignore_index=True)
        OP_LDC = OP_LDC.append(df_LDC, ignore_index=True)
        OP_LDC_LeapYr = OP_LDC_LeapYr.append(df_LDC_leapyr, ignore_index=True)


    #re-order columns by bringing Area & Hr_Yr to the front
    col = OP_LDC.columns.tolist()
    col = col[-2:] + col[:-2]
    OP_LDC = OP_LDC.loc[:, col]
    OP_LDC = pd.merge(OP_LDC, df_8760, how='left', on='Hr_Yr')
    OP_LDC_LeapYr = pd.merge(OP_LDC_LeapYr, df_8784, how='left', on='Hr_Yr')

    # Reformat Outputs to Requirements
    OP_Peak = pd.pivot_table(OP_Peak, index=['Area'],
                                      columns='Yr',
                                      values='Peak',
                                      fill_value=0).reset_index()

    return OP_Peak, OP_LDC, OP_LDC_LeapYr

# </editor-fold>


if __name__ == '__main__':

    active_excel = xw.books.active.name
    xw.Book(active_excel).set_mock_caller()


    updatecycle, upload, csvpath = getUserVar()

    # <editor-fold desc="Generate LDC">
    start = time.time()
    Peak, LDC, LDC_LeapYr = click_GenerateLDC()
    LDCflat_leapyr = pd.melt(LDC_LeapYr, id_vars=['Area', 'Hr_Yr', 'Mth', 'Day', 'Hr_Day'], var_name='Year', value_name='Demand_MW')
    LDCflat = pd.melt(LDC, id_vars=['Area', 'Hr_Yr', 'Mth', 'Day', 'Hr_Day'], var_name='Year', value_name='Demand_MW')
    LDCflat = pd.concat([LDCflat, LDCflat_leapyr]).reset_index(drop=True)
    LDCflat['User'] = user
    LDCflat['TimeStamp'] = timestamp
    LDCflat['TimeStamp'] = LDCflat['TimeStamp'].dt.round('1s')
    LDCflat['Year'] = LDCflat['Year'].astype(int)
    end = time.time()
    print(f"LDC generator took {end - start} seconds in total.")
    # </editor-fold>

    # LDCflat = LDCflat[(LDCflat['Year'] >= 2015) & (LDCflat['Year'] <= 2040)]

    # <editor-fold desc="Write to Excel">
    print(f"\nWriting Annual results for all Areas to excel....")
    wb = xw.Book.caller()
    sht = wb.sheets['Outputs']
    sht.clear()

    op_offset = 1
    writeformat_excel(sht, op_offset, Peak, 'Peak Demand (MW)')
    op_offset += len(Peak) + 3

    # </editor-fold>

    if upload == 'Yes':
        # <editor-fold desc="Upload to LIVE table">
        sqlstart = time.time()
        print(f" Uploading LDCs ({len(LDCflat)} records) to SQL Server...")
        update_db_tbl(engine=engine, dest_df=LDCflat, dest_tbl='APAC_LoadDurationCurves_Forecast_LIVE', by_col='Area')
        sqlend = time.time()
        print(f"   Upload took {sqlend - sqlstart} seconds.")
        # </editor-fold>

        # <editor-fold desc="Upload to Archive">
        sqlstart = time.time()
        print(f" Archiving LDCs ({len(LDCflat)} records) in SQL Server...")
        LDCflat['UpdateCycle'] = updatecycle
        LDCflat['Dataset_Name'] = 'Zone Archive'
        upload_to_db(engine=engine, dest_df=LDCflat, dest_tbl='APAC_LoadDurationCurves_Forecast_Datasets', existMethod='append')
        sqlend = time.time()
        print(f"   Archiving took {sqlend - sqlstart} seconds.")
        # </editor-fold>
    else:
        csvstart = time.time()
        print(f" Writing LDCs ({len(LDCflat)} records) in CSV locally...")
        csvfile = 'LDC.csv'
        csvdir = os.path.join(csvpath, csvfile)
        LDCflat.to_csv(csvdir, sep=',', index=False)
        csvend = time.time()
        print(f"   Writing csv locally took {csvend - csvstart} seconds.")


print(f"\nLDC generation completed.")
print(f'\nCopyright © 2019, Wood Mackenzie Limited. All rights reserved.\n')
