import pandas as pd
import openpyxl


def read_veh_counts(filepath, sheet_name):
    return pd.read_excel(
        filepath,
        sheet_name,
        header=13,
        nrows=8,
        # usecols=  # "A:T" for VanNess@13th, "A:R" otherwise
        index_col="PERIOD",
    ).dropna(how="all", axis="columns")


def _read_active_counts(filepath, sheet_name, usecols):
    # special case for Van Ness @ 13th (sheet naming for "Van Ness" is
    # inconsistent, hence only pattern matching with "ess")
    if sheet_name.endswith("ess@13th"):
        header = 52
        skiprows = [53]
    else:
        header = 50
        skiprows = [51]
    return pd.read_excel(
        filepath,
        sheet_name,
        header=header,
        skiprows=skiprows,
        nrows=8,
        usecols=usecols,  # Montgomery@Bush has a SCRMBL column too
        # index_col="15 MIN COUNTS",
    ).dropna(how="all", axis="columns")


def read_ped_counts(filepath, sheet_name):
    usecols = "A:G"
    return _read_active_counts(filepath, sheet_name, usecols)


def read_bike_counts(filepath, sheet_name):
    usecols = "I:P"
    df = _read_active_counts(filepath, sheet_name, usecols)
    df.columns = df.columns.str.rstrip(".1")
    return df


def calc_total(counts_df):
    return counts_df["TOTAL"].sum()


def fix_sheet_name(sheet_name):
    sheet_name = sheet_name.split("-")[1]
    if sheet_name == "LeavenWorth@Eddy":
        return "Leavenworth@Eddy"
    elif sheet_name == "Van ness@13th":
        return "VanNess@13th"
    elif sheet_name == "3rd@PalouAve":
        return "3rd@Palou"
    else:
        return sheet_name


def read_halfday_totals(filepath):
    sheet_names = (
        x
        for x in openpyxl.load_workbook(filepath, read_only=True).sheetnames
        if "Photo" not in x
    )
    return pd.DataFrame.from_dict(
        {
            fix_sheet_name(sheet_name): (
                calc_total(read_veh_counts(filepath, sheet_name)),
                calc_total(read_ped_counts(filepath, sheet_name)),
                calc_total(read_bike_counts(filepath, sheet_name)),
            )
            for sheet_name in sheet_names
        },
        orient="index",
        columns=["veh", "ped", "bike"],
    ).astype(int)


def read_raw_year_totals(am_filepath, pm_filepath):
    am_df = read_halfday_totals(am_filepath).add_suffix("_am")
    pm_df = read_halfday_totals(pm_filepath).add_suffix("_pm")
    return pd.concat((am_df, pm_df), axis=1, join="outer")
