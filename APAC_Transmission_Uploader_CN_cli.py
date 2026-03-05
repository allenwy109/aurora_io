"""
APAC Transmission Uploader CLI — Standalone (China only)
Reads transmission Excel, transforms data, writes to SQL.
Interactive menu with debug/force-update/dry-run controls.
"""
import time
import urllib.parse
import datetime as dt
import getpass
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy

pd.set_option('future.no_silent_downcasting', True)

# ── defaults ──────────────────────────────────────────────────────────────────
COUNTRY = 'China'
YR_END = 2060
EXCEL_PATH = r'L:\Power_Renewables\Inputs\APAC_Transmission_2026H1 for upload.xlsm'
SHEET_LINE = 'LiveUpdate'
SHEET_PRICE = 'T&D Tariffs'

SERVER = 'ANVDEVSQLVPM01'
DATABASE = 'WM_POWER_RENEWABLES'

DEST_TBL_LIVE  = 'APAC_PowerTransmission_LIVE'
DEST_TBL_TARIFF = 'APAC_PowerTransmission_Tariff_LIVE'
DEST_TBL_INFRA  = 'APAC_PowerTransmission_InfrastructureProject_LIVE'
DEST_TBL_TS     = 'APAC_PowerInfrastructure_AnnualTimeSeries'

# ── runtime state ─────────────────────────────────────────────────────────────
engine_src = None
user = getpass.getuser()
timestamp = None


# ══════════════════════════════════════════════════════════════════════════════
#  SQL helpers
# ══════════════════════════════════════════════════════════════════════════════

def _build_engine():
    params = urllib.parse.quote_plus(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={SERVER};"
        f"DATABASE={DATABASE};"
        f"Trusted_Connection=Yes"
    )
    return create_engine("mssql+pyodbc:///?odbc_connect=%s" % params, fast_executemany=True)


def sqlcoldict_dt(df):
    dtypedict = {}
    for col, dtype in zip(df.columns, df.dtypes):
        s = str(dtype)
        if "object" in s:
            dtypedict[col] = sqlalchemy.types.NVARCHAR(length=255)
        elif "datetime" in s:
            dtypedict[col] = sqlalchemy.types.DATETIME()
        elif "float" in s:
            dtypedict[col] = sqlalchemy.types.Float(precision=6, asdecimal=True)
        elif "int" in s:
            dtypedict[col] = sqlalchemy.types.INT()
    return dtypedict


def upload_sql(engine, df, dest_sql, existMethod):
    df_dict = sqlcoldict_dt(df)
    df.to_sql(dest_sql, con=engine, if_exists=existMethod, index=False,
              chunksize=10000, dtype=df_dict)


def execute_sqlcur(engine, sql):
    connx = engine.raw_connection()
    cursor = connx.cursor()
    cursor.execute(sql)
    connx.commit()
    cursor.close()


# ══════════════════════════════════════════════════════════════════════════════
#  Column normalisation helpers
# ══════════════════════════════════════════════════════════════════════════════

def _norm_col(name):
    return " ".join(str(name).strip().lower().split())


def _apply_alias_columns(df, alias_map):
    if not alias_map:
        return df
    existing = {_norm_col(c) for c in df.columns}
    for target, aliases in alias_map.items():
        if _norm_col(target) in existing:
            continue
        for alias in aliases:
            if _norm_col(alias) in existing:
                real = next(c for c in df.columns if _norm_col(c) == _norm_col(alias))
                df = df.rename(columns={real: target})
                existing.discard(_norm_col(alias))
                existing.add(_norm_col(target))
                break
    return df


def _rename_by_normalized(df, required_cols, name):
    norm_map = {_norm_col(c): c for c in df.columns}
    rename_map = {}
    for req in required_cols:
        if req in df.columns:
            continue
        nk = _norm_col(req)
        if nk in norm_map:
            rename_map[norm_map[nk]] = req
        else:
            raise ValueError(f"{name}: missing column '{req}' (available: {list(df.columns)})")
    return df.rename(columns=rename_map)


def _rename_by_normalized_optional(df, cols):
    norm_map = {_norm_col(c): c for c in df.columns}
    rename_map = {}
    for req in cols:
        if req in df.columns:
            continue
        nk = _norm_col(req)
        if nk in norm_map:
            rename_map[norm_map[nk]] = req
    return df.rename(columns=rename_map)


# ══════════════════════════════════════════════════════════════════════════════
#  Logging
# ══════════════════════════════════════════════════════════════════════════════

def log(msg):
    print(f"[{dt.datetime.now().strftime('%H:%M:%S')}] {msg}")


def log_step(step, detail):
    log(f"[{step}] {detail}")


# ══════════════════════════════════════════════════════════════════════════════
#  Price sheet parser
# ══════════════════════════════════════════════════════════════════════════════

def _parse_price_sheet(xls_path, sheet):
    try:
        raw = pd.read_excel(xls_path, sheet_name=sheet, usecols="BU:DX")
    except ValueError:
        raw = pd.read_excel(xls_path, sheet_name=sheet)
    raw = raw.dropna(axis=0, how="all").dropna(axis=1, how="all")
    header_idx = None
    max_scan = min(15, len(raw))
    for i in range(max_scan):
        row = raw.iloc[i].astype(str).str.strip().str.lower()
        if row.str.contains(
            "interconnection|power link name|source province|destination province"
            "|from province|to province"
        ).any():
            header_idx = i
            break
    if header_idx is not None:
        raw.columns = raw.iloc[header_idx]
        raw = raw.iloc[header_idx + 1:].reset_index(drop=True)
    raw = raw.dropna(axis=0, how="all").dropna(axis=1, how="all").reset_index(drop=True)
    if len(raw) > 0 and any(pd.isna(c) or str(c).strip() == "" for c in raw.columns):
        first_row = raw.iloc[0]
        new_cols = []
        for c, v in zip(raw.columns, first_row):
            if pd.isna(c) or str(c).strip() == "":
                new_cols.append(v)
            else:
                new_cols.append(c)
        raw.columns = new_cols
        raw = raw.iloc[1:].reset_index(drop=True)
    return raw


# ══════════════════════════════════════════════════════════════════════════════
#  Core pipeline
# ══════════════════════════════════════════════════════════════════════════════

def read_transmission_excel():
    """Step 1: Read LiveUpdate sheet and normalise columns."""
    log_step("read", "reading LiveUpdate sheet")
    transmission_data = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_LINE, header=1)
    transmission_data = _apply_alias_columns(transmission_data, {
        "From Province": ["Source province", "From Regio"],
        "To Province": ["Destination province", "To Regio"],
        "From Region": ["Source regional grid"],
        "To Region": ["Destination regional grid"],
    })
    transmission_data = _rename_by_normalized(
        transmission_data,
        ['Model Start Year', 'Special label for profiling', 'Adjusted Cap',
         'From Province', 'To Province'],
        f"{EXCEL_PATH}:{SHEET_LINE}",
    )

    if pd.api.types.is_datetime64_any_dtype(transmission_data['Model Start Year']):
        transmission_data['Model Start Year'] = transmission_data['Model Start Year'].dt.year
    else:
        transmission_data['Model Start Year'] = pd.to_numeric(
            transmission_data['Model Start Year'], errors="coerce")
    transmission_data['Model Start Year'] = transmission_data['Model Start Year'].astype("Int64")

    # Strip whitespace (including \xa0) from all string columns at source
    for col in transmission_data.select_dtypes(include='object').columns:
        mask = transmission_data[col].apply(type) == str
        transmission_data.loc[mask, col] = (
            transmission_data.loc[mask, col]
            .str.replace('\xa0', ' ', regex=False)
            .str.strip()
        )

    log_step("read", f"LiveUpdate rows={len(transmission_data)}")
    return transmission_data


def expand_to_yearly(transmission_data):
    """Step 2: Expand transmission lines to yearly time series."""
    log_step("expand", "expanding to yearly time series")
    date_range = pd.DataFrame(range(2000, YR_END + 1), columns=['Year'])
    date_range['Year'] = date_range['Year'].astype('Int64')
    df_rows = []
    for _, row_data in transmission_data.iterrows():
        data_row = row_data.to_frame().T
        if 'Model Start Year' in data_row.columns:
            data_row['Model Start Year'] = pd.to_numeric(
                data_row['Model Start Year'], errors='coerce').astype('Int64')
        df_row = data_row.merge(date_range, how='outer',
                                left_on='Model Start Year', right_on='Year')
        df_row = df_row.ffill().infer_objects(copy=False)
        df_row['Year'] = pd.to_numeric(df_row['Year'], errors='coerce')
        df_row['Model Start Year'] = pd.to_numeric(df_row['Model Start Year'], errors='coerce')
        df_row['Year'] = df_row['Year'].fillna(df_row['Model Start Year'])
        df_row['Model Start Year'] = df_row['Model Start Year'].fillna(df_row['Year'])
        df_row['Year'] = df_row['Year'].astype(int)
        df_row['Model Start Year'] = df_row['Model Start Year'].astype(int)
        df_row['Value'] = np.where(
            (df_row['Year'] == df_row['Model Start Year']) & (df_row['Special label for profiling'] == 'F'),
            df_row['Adjusted Cap'] * 0.7,
            np.where(
                (df_row['Year'] == df_row['Model Start Year'] + 1) & (df_row['Special label for profiling'] == 'F'),
                df_row['Adjusted Cap'] * 0.9,
                np.where(
                    (df_row['Year'] == df_row['Model Start Year']) & (df_row['Special label for profiling'] == 'L'),
                    df_row['Adjusted Cap'] * 0.3,
                    np.where(
                        (df_row['Year'] == df_row['Model Start Year'] + 1) & (df_row['Special label for profiling'] == 'L'),
                        df_row['Adjusted Cap'] * 0.6,
                        np.where(
                            (df_row['Year'] == df_row['Model Start Year'] + 2) & (df_row['Special label for profiling'] == 'L'),
                            df_row['Adjusted Cap'] * 0.8,
                            np.where(
                                df_row['Year'] < df_row['Model Start Year'], 0,
                                np.where(
                                    df_row['Special label for profiling'] == 'NA',
                                    df_row['Adjusted Cap'] * 1,
                                    df_row['Adjusted Cap'])))))))
        df_row = df_row.dropna(axis=1, how='all')
        if not df_row.empty and not df_row.isna().all().all():
            df_rows.append(df_row)

    df_rows = [d for d in df_rows if d is not None and not d.empty and d.notna().any().any()]
    df = pd.concat(df_rows, ignore_index=True) if df_rows else pd.DataFrame()
    df['Type'] = 'Max'
    df['Unit'] = 'MW'
    df['Metric'] = 'Capacity'

    df_min = df.copy()
    df_min['Type'] = 'Min'
    df_min['Value'] = df_min['Value'] * 0.5

    log_step("expand", f"Capacity Max={len(df)}, Min={len(df_min)}")
    return df, df_min


def read_price_data():
    """Step 3: Read and melt price/tariff sheet."""
    log_step("price", "reading T&D Tariffs sheet")
    price_data = _parse_price_sheet(EXCEL_PATH, SHEET_PRICE)
    price_data = _apply_alias_columns(price_data, {
        "Power link name": ["Power link", "PowerLink name", "Link name", "Interconnection name"],
        "Source province": ["Source Province", "From Province", "From Region"],
        "Destination province": ["Destination Province", "To Province", "To Region"],
        "Source regional grid": ["Source region grid", "Source region",
                                  "From regional grid", "From region grid", "Source grid"],
        "Destination regional grid": ["Destination region grid", "Destination region",
                                       "To regional grid", "To region grid", "Destination grid"],
    })
    price_data = _rename_by_normalized_optional(price_data, [
        "Power link name", "Source province", "Source regional grid",
        "Destination province", "Destination regional grid",
    ])
    if ("Interconnection" not in price_data.columns
            and "Source province" in price_data.columns
            and "Destination province" in price_data.columns):
        price_data['Interconnection'] = (
            price_data['Source province'] + '-' + price_data['Destination province'])
    id_vars = [c for c in [
        "Power link name", "Source province", "Source regional grid",
        "Destination province", "Destination regional grid", "Interconnection"
    ] if c in price_data.columns]
    price_data = pd.melt(price_data, id_vars=id_vars, var_name='Year', value_name='Value')
    price_data['Year'] = pd.to_numeric(price_data['Year'], errors='coerce')
    price_data = price_data[price_data['Year'].notna()].reset_index(drop=True)
    price_data['Year'] = price_data['Year'].astype(int)
    # Strip whitespace (including \xa0) from string columns
    for col in price_data.select_dtypes(include='object').columns:
        mask = price_data[col].apply(type) == str
        price_data.loc[mask, col] = (
            price_data.loc[mask, col]
            .str.replace('\xa0', ' ', regex=False)
            .str.strip()
        )
    log_step("price", f"melted rows={len(price_data)}")
    return price_data


def merge_and_build(df_cap_max, df_cap_min, price_data, transmission_data):
    """Step 4: Merge capacity + wheeling, build all output DataFrames."""
    log_step("merge", "building wheeling + output frames")
    price_line = df_cap_max.merge(price_data, how='left', on=['Interconnection', 'Year'])
    price_line['Type'] = 'Flat'
    price_line['Unit'] = 'US$/MWh'
    price_line['Metric'] = 'Wheeling'
    wheeling_cols = [
        'ID', 'Transmission Line Name', 'Chinese Name', 'Status',
        'Number of Loops', 'Interconnection', 'From Province', 'From Region',
        'To Province', 'To Region', 'Primary Fuel',
        'Special label for profiling', 'Length (km)', 'DC/AC', 'Voltage Grade',
        'Capacity (MW)', 'Deduction coefficient', 'Adjusted Cap',
        'Construction commencement', 'COD', 'COD year', 'Model Start Year',
        'Capex (Mn RMB)', 'Commentaries', 'Year', 'Value_y', 'Type', 'Unit', 'Metric'
    ]
    wheeling = price_line[[c for c in wheeling_cols if c in price_line.columns]].copy()
    if 'Value_y' in wheeling.columns:
        wheeling.rename(columns={'Value_y': 'Value'}, inplace=True)

    # df_txn_all  (→ APAC_PowerTransmission_LIVE)
    df_txn_all = pd.concat([df_cap_max, df_cap_min, wheeling], ignore_index=True)
    df_txn_all['Year'] = df_txn_all['Year'].astype(int)
    df_txn_all['Country'] = COUNTRY
    df_all_cols = [
        'ID', 'Country', 'Transmission Line Name', 'Chinese Name', 'Status',
        'Number of Loops', 'Interconnection', 'From Province', 'From Region',
        'To Province', 'To Region', 'Primary Fuel',
        'Special label for profiling', 'Length (km)', 'DC/AC', 'Voltage Grade',
        'Capacity (MW)', 'Deduction coefficient', 'Adjusted Cap',
        'Construction commencement', 'COD', 'COD year', 'Model Start Year',
        'Capex (Mn RMB)', 'Year', 'Metric', 'Value', 'Type', 'Unit', 'Commentaries'
    ]
    df_txn_all = df_txn_all[[c for c in df_all_cols if c in df_txn_all.columns]]
    df_txn_all['User'] = user
    df_txn_all['TimeStamp'] = timestamp

    # df_txn_price  (→ APAC_PowerTransmission_Tariff_LIVE)
    df_txn_price = price_data.copy()
    df_txn_price['TimeStamp'] = timestamp
    df_txn_price['Country'] = COUNTRY

    # df_txn_line  (→ APAC_PowerTransmission_InfrastructureProject_LIVE)
    df_txn_line = transmission_data.copy()
    df_txn_line['TimeStamp'] = timestamp
    df_txn_line['Country'] = COUNTRY
    # SQL table requires non-null ID (PK) — generate unique IDs for blanks
    if 'ID' in df_txn_line.columns:
        mask = df_txn_line['ID'].isna() | (df_txn_line['ID'].astype(str).str.strip() == '')
        if mask.any():
            for idx in df_txn_line.index[mask]:
                df_txn_line.at[idx, 'ID'] = f"AUTO_{idx}"
        # Deduplicate on ID (PK constraint) — keep first occurrence
        dup_mask = df_txn_line.duplicated(subset=['ID'], keep='first')
        if dup_mask.any():
            dup_ids = df_txn_line.loc[dup_mask, 'ID'].unique().tolist()
            log_step("merge", f"  dedup: dropping {dup_mask.sum()} duplicate ID(s): {dup_ids[:10]}")
            df_txn_line = df_txn_line[~dup_mask].reset_index(drop=True)
        # Debug: show IDs that look like Interconnection values
        if 'Interconnection' in df_txn_line.columns:
            inter_as_id = df_txn_line[df_txn_line['ID'] == df_txn_line['Interconnection']]
            if len(inter_as_id) > 0:
                log_step("merge", f"  WARNING: {len(inter_as_id)} rows have ID == Interconnection")
                log_step("merge", f"  examples: {inter_as_id['ID'].head(5).tolist()}")

    log_step("merge", f"LIVE={len(df_txn_all)}, Tariff={len(df_txn_price)}, Infra={len(df_txn_line)}")
    return df_txn_all, df_txn_price, df_txn_line


def build_annual_timeseries(df_txn_all):
    """Step 5: Build AnnualTimeSeries from capacity + wheeling."""
    log_step("ts", "building AnnualTimeSeries")
    # Strip trailing/leading whitespace (including \xa0 non-breaking space)
    # from string columns — SQL Server ignores trailing spaces in PK
    # comparisons but pandas groupby does not, causing duplicates.
    for col in df_txn_all.select_dtypes(include='object').columns:
        mask = df_txn_all[col].apply(type) == str
        df_txn_all.loc[mask, col] = (
            df_txn_all.loc[mask, col]
            .str.replace('\xa0', ' ', regex=False)
            .str.strip()
        )
    cap_mask = df_txn_all['Metric'] == 'Capacity'
    whl_mask = df_txn_all['Metric'] == 'Wheeling'
    grp_cols_full = ['Country', 'From Province', 'From Region', 'To Province',
                     'To Region', 'Interconnection', 'Year']
    grp_cols = [c for c in grp_cols_full if c in df_txn_all.columns]
    df_cap = df_txn_all.loc[cap_mask, grp_cols + ['Value', 'Metric', 'Type']].copy()
    df_cap = df_cap.groupby(grp_cols + ['Metric', 'Type'], as_index=False)['Value'].sum()
    df_cap.rename(columns={'Value': 'Capacity_MW', 'Metric': 'Metric_line',
                           'Type': 'Type_line'}, inplace=True)
    df_whl = df_txn_all.loc[whl_mask, grp_cols + ['Value']].copy()
    df_whl = df_whl.groupby(grp_cols, as_index=False)['Value'].mean()
    df_whl.rename(columns={'Value': 'Tariff_($/MWh)'}, inplace=True)
    df_whl['Metric_tariff'] = 'Wheeling'
    df_whl['Type_tariff'] = 'Flat'
    df_ts = df_cap.merge(df_whl, on=grp_cols, how='left')
    df_ts['TimeStamp'] = timestamp
    ts_cols_full = ['Country', 'From Province', 'From Region', 'To Province', 'To Region',
                    'Interconnection', 'Year', 'Capacity_MW', 'Metric_line', 'Type_line',
                    'Tariff_($/MWh)', 'Metric_tariff', 'Type_tariff', 'TimeStamp']
    df_ts = df_ts[[c for c in ts_cols_full if c in df_ts.columns]]
    log_step("ts", f"AnnualTimeSeries rows={len(df_ts)}")
    return df_ts


def _get_sql_columns(engine, table_name):
    """Query SQL Server for the column names of a table."""
    sql = f"SELECT TOP 0 * FROM [{table_name}]"
    df_empty = pd.read_sql(sql, engine)
    return set(df_empty.columns)


def write_to_sql(df_txn_all, df_txn_price, df_txn_line, df_ts):
    """Step 6: Delete-by-country then append for all 4 tables.
    Automatically drops DataFrame columns that don't exist in the SQL table."""
    tables = [
        (DEST_TBL_INFRA,  df_txn_line),
        (DEST_TBL_TARIFF, df_txn_price),
        (DEST_TBL_TS,     df_ts),
        (DEST_TBL_LIVE,   df_txn_all),
    ]
    print("\n" + "=" * 60)
    print("WRITING TO SQL...")
    for tbl, df_w in tables:
        tw = time.time()
        # Filter to only columns that exist in the SQL table
        sql_cols = _get_sql_columns(engine_src, tbl)
        extra = set(df_w.columns) - sql_cols
        if extra:
            log_step("sql", f"  dropping columns not in {tbl}: {sorted(extra)}")
            df_w = df_w[[c for c in df_w.columns if c in sql_cols]]
        del_sql = f"DELETE FROM {tbl} WHERE [Country] = '{COUNTRY}'"
        log_step("sql", f"DELETE {tbl} for {COUNTRY}")
        execute_sqlcur(engine=engine_src, sql=del_sql)
        log_step("sql", f"WRITE {tbl} ({len(df_w)} rows)")
        upload_sql(engine=engine_src, df=df_w, dest_sql=tbl, existMethod='append')
        log_step("sql", f"  done ({time.time()-tw:.1f}s)")
    total = sum(len(d) for _, d in tables)
    print(f"\nTOTAL written: {total} rows")


# ══════════════════════════════════════════════════════════════════════════════
#  Run pipeline
# ══════════════════════════════════════════════════════════════════════════════

def run_pipeline():
    """Execute the full transmission upload pipeline."""
    global engine_src, timestamp
    t0 = time.time()
    timestamp = dt.datetime.now().replace(microsecond=0)

    log("Initialising SQL engine")
    engine_src = _build_engine()

    transmission_data = read_transmission_excel()
    df_cap_max, df_cap_min = expand_to_yearly(transmission_data)
    price_data = read_price_data()
    df_txn_all, df_txn_price, df_txn_line = merge_and_build(
        df_cap_max, df_cap_min, price_data, transmission_data)
    df_ts = build_annual_timeseries(df_txn_all)
    write_to_sql(df_txn_all, df_txn_price, df_txn_line, df_ts)

    elapsed = time.time() - t0
    log(f"Pipeline complete — {elapsed:.1f}s total")


if __name__ == "__main__":
    run_pipeline()
