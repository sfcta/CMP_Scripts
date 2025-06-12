import argparse
from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl
from openpyxl import load_workbook


def read_day_dir_dayofweek(workbook, sheet_name, day_num):
    row = 8 + day_num * 48
    date = workbook[sheet_name][f"D{row}"].value
    if date is None:
        raise EOFError(
            "No data here; day_num is probably beyond "
            "the number of days collected at this location."
        )
    else:
        return date.split(" ")[0]


def is_weekday(dayofweek: str):
    if dayofweek in {"MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY"}:
        return True
    elif dayofweek in {"SATURDAY", "SUNDAY"}:
        return False
    else:
        raise ValueError(f"{dayofweek} unrecognized.")


def read_day_dir_counts_df(filepath, sheet_name, direction, day_num):
    """Read the counts table from a single day and direction

    Parameters
    ----------
    filepath : _type_
        _description_
    sheet_name : _type_
        _description_
    direction : _type_
        _description_
    day_num : _type_
        the nth day of data collection within this worksheet

    Returns
    -------
    _type_
        _description_

    Raises
    ------
    ValueError
        _description_
    """
    if direction == 0:
        cols = "A:F"
    elif direction == 1:
        cols = "H:M"
    else:
        raise ValueError("direction must be 0 or 1")
    header = 10 + day_num * 48
    try:
        df = pd.read_excel(
            filepath,
            sheet_name,
            usecols=cols,
            header=header,
            skiprows=[header + 1],
            nrows=24,
        )
    except ValueError:
        raise EOFError(
            "Reached end of sheet; day_num is probably beyond "
            "the number of days collected at this location."
        )
    if direction == 1:
        # pandas append ".1" to column names to the right table (direction 1)
        # since they duplicate the headers of the left table (direction 0)
        df.columns = df.columns.str.rstrip(".1")
    return df.rename(columns={"    TIME": "TIME", "HOUR ": "HOUR_TOTALS"}).set_index(
        "TIME"
    )


def get_am_peak_totals(day_dir_counts_df):
    """Get a.m. peak (0700-0900) totals for a single day and direction

    Parameters
    ----------
    day_dir_counts_df : _type_
        the counts table from a single day and direction

    Returns
    -------
    _type_
        _description_
    """
    return day_dir_counts_df.iloc[7:9]["HOUR_TOTALS"].sum()


def get_pm_peak_totals(day_dir_counts_df):
    """Get p.m. peak (1630-1830) totals for a single day and direction

    Parameters
    ----------
    day_dir_counts_df : _type_
        the counts table from a single day and direction

    Returns
    -------
    _type_
        _description_
    """
    return np.sum(
        (
            day_dir_counts_df.iloc[16][["30-45", "45-60"]].sum(),
            day_dir_counts_df.iloc[17]["HOUR_TOTALS"],
            day_dir_counts_df.iloc[18][["00-15", "15-30"]].sum(),
        )
    )


def read_day_total(workbook, sheet_name, direction, day_num):
    if direction == 0:
        col = "F"
    elif direction == 1:
        col = "M"
    else:
        raise ValueError("direction must be 0 or 1")
    row = 37 + day_num * 48
    total = workbook[sheet_name][f"{col}{row}"].value
    if total is None:
        raise EOFError(
            "No data here; day_num is probably beyond "
            "the number of days collected at this location."
        )
    else:
        return total


def read_totals(filepath, workbook, sheet_name, direction, weekday_only=True):
    """_summary_

    Note that filepath and workbook are both needed because pandas is used to
    read the am/pm peak totals, while openpyxl is used to read the daily totals,
    and loading the openpyxl workbook every function run would be slow.

    Parameters
    ----------
    filepath : _type_
        _description_
    workbook : _type_
        _description_
    sheet_name : _type_
        _description_
    direction : _type_
        _description_

    Returns
    -------
    _type_
        _description_
    """
    day_num = -1
    am_peak_totals = []
    pm_peak_totals = []
    daily_totals = []
    while True:
        day_num += 1
        try:
            if weekday_only and not is_weekday(
                read_day_dir_dayofweek(workbook, sheet_name, day_num)
            ):
                continue
            df = read_day_dir_counts_df(filepath, sheet_name, direction, day_num)
            am_peak_totals.append(get_am_peak_totals(df))
            pm_peak_totals.append(get_pm_peak_totals(df))
            daily_totals.append(
                read_day_total(workbook, sheet_name, direction, day_num)
            )
        except EOFError:  # assume we've reached the end of the sheet
            break
    return am_peak_totals, pm_peak_totals, daily_totals


def read_sheet_directions(workbook, sheet_name):
    return (
        workbook[sheet_name]["D10"].value,
        workbook[sheet_name]["K10"].value,
    )


def read_sheet_mean_totals(
    filepath, workbook, sheet_name, raw_values=False, weekday_only=True
):
    """Read count totals (i.e. volumes) from a single worksheet

    This is where we start referring to totals as volumes

    Parameters
    ----------
    filepath : _type_
        _description_
    workbook : _type_
        _description_
    sheet_name : _type_
        _description_

    Returns
    -------
    _type_
        _description_
    """
    # direction of tables on the right
    dir0, dir1 = read_sheet_directions(workbook, sheet_name)
    # avg daily total (in each direction)
    vols_dir0 = map(
        np.mean,
        read_totals(filepath, workbook, sheet_name, 0, weekday_only=weekday_only),
    )
    rows = [[sheet_name, dir0, *vols_dir0]]
    if dir1 != "O":
        vols_dir1 = map(np.mean, read_totals(filepath, workbook, sheet_name, 1))
        rows.append([sheet_name, dir1, *vols_dir1])
    if raw_values:
        return rows
    else:
        column_names = (
            "sheet_name",
            "direction",
            "cmp_am_peak_vol",
            "cmp_pm_peak_vol",
            "daily_vol",
        )
        return pd.DataFrame(
            rows,
            columns=column_names,
        )


def read_xlsxs_volumes(xlsx_filepaths, weekday_only=True):
    return pd.concat(
        read_xlsx_volumes(f, weekday_only=weekday_only) for f in xlsx_filepaths
    )


def read_xlsx_volumes(filepath, weekday_only=True):
    wb = load_workbook(filename=filepath, read_only=True)
    column_names = (
        "sheet_name",
        "direction",
        "cmp_am_peak_vol",
        "cmp_pm_peak_vol",
        "daily_vol",
    )
    count_totals = []
    sheet_names = [sn for sn in wb.sheetnames if not sn.endswith("PHOTO")]
    for sheet_name in sheet_names:
        print("parsing sheet", sheet_name)
        count_totals.extend(
            read_sheet_mean_totals(
                filepath,
                wb,
                sheet_name,
                raw_values=True,
                weekday_only=weekday_only,
            )
        )
    return pd.DataFrame(
        count_totals,
        columns=column_names,
    )


def add_cmp_non_peak_volume(volumes_df):
    volumes_df["cmp_non_peak_vol"] = (
        volumes_df["daily_vol"]
        - volumes_df["cmp_am_peak_vol"]
        - volumes_df["cmp_pm_peak_vol"]
    )
    return volumes_df


def rename_sheet_name_to_location(volumes_df, locations_filepath):
    locations = {
        k: v[0][0]
        for k, v in (
            pl.read_csv(locations_filepath, columns=["2023_location_id", "location"])
            .unique()  # some locations (and location IDs) have both directions
            .rows_by_key("2023_location_id")
        ).items()
    }
    volumes_df["2023_location_id"] = (
        # split on "-" or " "
        volumes_df["sheet_name"].str.split(r"[-\s]", expand=True)[1].astype(int)
    )
    volumes_df["location"] = volumes_df["2023_location_id"].map(locations)
    # reorder columns and drop sheet_name
    return volumes_df[
        [
            "2023_location_id",
            "location",
            "direction",
            "cmp_am_peak_vol",
            "cmp_pm_peak_vol",
            "cmp_non_peak_vol",
            "daily_vol",
        ]
    ]


def parse_wiltec_excel(raw_xlsx_filepaths, locations_filepath, output_csv_filepath):
    print("parsing workbooks", list(raw_xlsx_filepaths))
    rename_sheet_name_to_location(
        add_cmp_non_peak_volume(
            read_xlsxs_volumes(raw_xlsx_filepaths, weekday_only=True)
        ),
        locations_filepath,
    ).to_csv(output_csv_filepath, index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "raw_xlsx_glob_filepaths", help="Wiltec-collected xlsx input (glob) filpath(s)"
    )
    parser.add_argument(
        "locations_filepath",
        help=(
            "Locations xlsx input filepath: "
            "check location IDs and raw_xlsx sheet numbers match"
        ),
    )
    parser.add_argument("output_csv_filepath", help="Output CSV filepath")
    args = parser.parse_args()

    raw_xlsx_glob_filepaths = Path(args.raw_xlsx_glob_filepaths)
    raw_xlsx_filepaths = list(
        raw_xlsx_glob_filepaths.parent.glob(raw_xlsx_glob_filepaths.name)
    )
    parse_wiltec_excel(
        raw_xlsx_filepaths,
        args.locations_filepath,
        args.output_csv_filepath,
    )
