# -*- coding: utf-8 -*-
"""
Japan IO precheck:
- Test DB connectivity (source & destination)
- Test required Excel files are accessible and readable
"""

import argparse
import datetime as dt
from pathlib import Path
import urllib

import pandas as pd

try:
    pd.set_option("future.no_silent_downcasting", True)
except Exception:
    pass
from sqlalchemy import create_engine, text


# ---- defaults (match jp_cli) ----
COUNTRY = "Japan"
AURORA_SQLDB = "ANVDEVSQLVPM01"
AURORA_DBNAME = "Aurora_APAC_DEV_Japan_test"
SRC_DBNAME = "WM_POWER_RENEWABLES"
INPUT_DIR = r"L:\Power_Renewables\Inputs"

ASSUMPTIONS_XLS = INPUT_DIR + r"\APAC_Assumptions.xlsx"
HYDRO_XLS = INPUT_DIR + r"\APAC_Hydro.xlsx"
FUEL_XLS = INPUT_DIR + r"\APAC_Fuels.xlsx"


# ---- helpers ----

def log(level, msg):
    ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {level}: {msg}")


def check_db(name, server, dbname):
    params = urllib.parse.quote_plus(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={server};"
        f"DATABASE={dbname};"
        "Trusted_Connection=Yes"
    )
    try:
        engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params, fast_executemany=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        log("OK", f"{name} connection OK ({server}/{dbname})")
        return engine
    except Exception as exc:
        log("FAIL", f"{name} connection failed ({server}/{dbname}): {exc}")
        return None


def try_query(engine, sql, label):
    if engine is None:
        log("SKIP", f"{label} (no connection)")
        return
    try:
        with engine.connect() as conn:
            conn.execute(text(sql))
        log("OK", f"{label}")
    except Exception as exc:
        log("FAIL", f"{label}: {exc}")


def norm_col(name):
    return " ".join(str(name).strip().lower().split())


def apply_alias_columns(df, alias_map):
    if not alias_map:
        return df
    norm_map = {norm_col(c): c for c in df.columns}
    for target, aliases in alias_map.items():
        norm_target = norm_col(target)
        if norm_target in norm_map:
            continue
        for alias in aliases:
            norm_alias = norm_col(alias)
            if norm_alias in norm_map:
                df[target] = df[norm_map[norm_alias]]
                norm_map[norm_target] = target
                break
    return df


def rename_by_normalized(df, required_cols, label):
    norm_map = {norm_col(c): c for c in df.columns}
    rename_map = {}
    missing = []
    for req in required_cols:
        norm_req = norm_col(req)
        if norm_req in norm_map:
            rename_map[norm_map[norm_req]] = req
        else:
            missing.append(req)
    if missing:
        raise ValueError(f"{label} missing columns (normalized): {missing}")
    return df.rename(columns=rename_map)


def _read_excel_checked(path, sheet, label, header=0, usecols=None, required=None, alias_map=None):
    try:
        try:
            df = pd.read_excel(path, sheet_name=sheet, header=header, usecols=usecols, nrows=5)
        except ValueError:
            df = pd.read_excel(path, sheet_name=sheet, header=header, nrows=5)
        if df.empty:
            raise ValueError("sheet has no data")
        df = apply_alias_columns(df, alias_map)
        if required:
            df = rename_by_normalized(df, required, label)
        return True, None
    except Exception as exc:
        return False, exc


def check_excel(path, sheet, label, header=0, usecols=None, required=None, alias_map=None):
    p = Path(path)
    if not p.exists():
        log("FAIL", f"{label} file not found: {path}")
        return False
    sheets = sheet if isinstance(sheet, (list, tuple)) else [sheet]
    last_exc = None
    for sht in sheets:
        ok, exc = _read_excel_checked(path, sht, label, header=header, usecols=usecols, required=required, alias_map=alias_map)
        if ok:
            log("OK", f"{label} readable (sheet={sht})")
            return True
        last_exc = exc
    log("FAIL", f"{label} read failed: {last_exc}")
    return False


def main():
    parser = argparse.ArgumentParser(description="Japan IO precheck (DB + Excel)")
    parser.add_argument("--aurora-db", default=AURORA_DBNAME)
    parser.add_argument("--aurora-server", default=AURORA_SQLDB)
    parser.add_argument("--src-db", default=SRC_DBNAME)
    parser.add_argument("--src-server", default=AURORA_SQLDB)
    args = parser.parse_args()

    # DB checks
    src_engine = check_db("Source", args.src_server, args.src_db)
    dest_engine = check_db("Destination", args.aurora_server, args.aurora_db)

    # lightweight permission checks
    try_query(src_engine, "SELECT TOP 1 * FROM vAID_Topology_Zones", "Source view vAID_Topology_Zones")
    try_query(src_engine, "SELECT TOP 1 * FROM vAPAC_Plant_Attributes_Annual_LIVE", "Source view vAPAC_Plant_Attributes_Annual_LIVE")
    try_query(dest_engine, "SELECT TOP 1 * FROM tbl_AID_Resources", "Dest table tbl_AID_Resources")

    # Excel checks
    check_excel(
        HYDRO_XLS,
        "Monthly",
        "Hydro Monthly",
        header=0,
        required=["LevelName", "Level", "ShapeName", "Year"],
        alias_map={"ShapeName": ["Shape Name"]},
    )
    check_excel(HYDRO_XLS, "Vector", "Hydro Vector", header=0, required=["Area"])

    for sheet in [
        "PlantLife", "VOM", "HeatRate", "CapacityFactor", "FixedCost",
        "EmissionRate", "EmissionPrice", "StorageDuration",
    ]:
        req = ["LevelName", "Level", "PlantType", "PlantTech"]
        if sheet in ("EmissionRate", "EmissionPrice"):
            req.append("Pollutant")
        check_excel(
            ASSUMPTIONS_XLS,
            sheet,
            f"Assumptions {sheet}",
            header=1,
            required=req,
            alias_map={
                "PlantType": ["Plant Type"],
                "PlantTech": ["Plant Tech"],
            },
        )

    check_excel(
        FUEL_XLS,
        "Fuel",
        "Fuel",
        header=0,
        usecols="B:BQ",
        required=["LevelName", "Level", "FuelName", "Description", "FuelType", "Metric", "Units"],
        alias_map={
            "FuelName": ["Fuel Name"],
            "FuelType": ["Fuel Type"],
        },
    )


if __name__ == "__main__":
    main()
