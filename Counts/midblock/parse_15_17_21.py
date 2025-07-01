"""(re)parse 2015, 2017, 2021 mid-block data"""
# %%
from functools import reduce
from pathlib import Path

import polars as pl

# %%
def read_input(filepath, locations: pl.DataFrame, year):
    filepath = Path(filepath)
    if filepath.suffix == ".xlsx":
        df = pl.read_excel(filepath)
    elif filepath.suffix == ".csv":
        df = pl.read_csv(filepath)
    else:
        raise NotImplementedError
    return (
        df
        .drop_nulls("Vol")
        .join(
            locations,
            left_on=["ID", "Direction"],
            right_on=[f"name_{year}", "direction"],
        )
        .select(
            location_id_nodir_2023=pl.col("id_nodir_2023"),
            location_id_withdir_2023=pl.col("id_withdir_2023"),
            location=pl.col("name_2023"),
            direction=pl.col("Direction"),
            date=pl.col("Date").str.strptime(pl.Date, "%Y.%m.%d"),
            time=pl.col("Time")
            .str.split("-")
            .cast(pl.List(pl.Int16))
            .list.to_struct(fields=["start_time", "end_time"]),
            volume=pl.col("Vol"),
        )
        .unnest("time")
        .with_columns(dow=pl.col("date").dt.weekday())  # Mon = 1 .. Sun = 7
    )


def add_bool_cols(df):
    return df.with_columns(
        anyweekday=(pl.col("dow") < 6),
        tuetothu=((1 < pl.col("dow")) & (pl.col("dow") < 5)),
        daily=pl.lit(True),
        cmp_am_peak=((700 <= pl.col("start_time")) & (pl.col("end_time") <= 900)),
        cmp_pm_peak=((1630 <= pl.col("start_time")) & (pl.col("end_time") <= 1830)),
    ).with_columns(cmp_non_peak=~(pl.col("cmp_am_peak") | pl.col("cmp_pm_peak")))


group_by = [
    "location_id_nodir_2023",
    "location_id_withdir_2023",
    "location",
    "direction",
]
sort_by = ["location_id_nodir_2023", "direction"]


def calc_daily_peak_vols(df, dow_mode, peak_period, group_by=group_by, sort_by=sort_by):
    return (
        df.filter(pl.col(dow_mode) & pl.col(peak_period))
        .group_by(group_by)
        .agg((pl.sum("volume") / pl.n_unique("date")).alias(f"{peak_period}_vol"))
        .sort(sort_by)
    )


def join_daily_peak_vols(
    df, dow_mode, peak_periods, group_by=group_by, sort_by=sort_by
):
    return reduce(
        lambda left, right: left.join(right, on=group_by),
        [calc_daily_peak_vols(df, dow_mode, period) for period in peak_periods],
    ).sort(sort_by)

# %%
locations = pl.read_csv(
    r"Q:\Data\Observed\Streets\Counts\CMP\cmp-midblock-locations.csv",
    columns=[
        "id_nodir_2023",
        "id_withdir_2023",
        "name_2015",
        "name_2017",
        "name_2023",
        "direction",
    ],
).with_columns(
    name_2021=pl.col("name_2015").str.extract(r"^\d+_", 0)
)

# %%
input_dir_2015_2017 = Path(r"Q:\CMP\LOS Monitoring 2017\Iteris\Task C5\ADT\v4\Master CSV file")
output_dir = Path(r"Q:\Data\Observed\Streets\Counts\CMP")
years = [2015, 2017, 2021]
input_filepaths = {
    2015: input_dir_2015_2017 / f"ADT2015 V4.xlsx",
    2017: input_dir_2015_2017 / f"ADT2017 V4.xlsx",
    2021: r"Q:\CMP\LOS Monitoring 2021\Counts\ADT\data2021.csv"
    }
dfs = {y: add_bool_cols(read_input(input_filepaths[y], locations, y)) for y in years}

# %%
dow_modes = ["anyweekday", "tuetothu"]
peak_periods = ["cmp_am_peak", "cmp_pm_peak", "cmp_non_peak", "daily"]

# %%
for y, df in dfs.items():
    for dow_mode in dow_modes:
        join_daily_peak_vols(df, dow_mode, peak_periods).write_csv(
            output_dir / f"{y}/midblock/cmp_midblock_adt-{y}-{dow_mode}.csv"
        )
