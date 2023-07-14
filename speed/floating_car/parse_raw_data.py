"""Calculate summary statistics for speed via travel time on segments

the script ignores actual floating car speed in the raw data
"""
import argparse
import re
from pathlib import Path

import geopandas as gpd
import pandas as pd
import polars as pl


def parse_xls(filepath: Path) -> dict:
    col = "Date/Time"
    ts = pd.read_excel(filepath, "Run Data", usecols=[col])[col]  # to Series
    return {
        "t_i": ts.iloc[0],
        "t_f": ts.iloc[-1],
    }


# ignore the run_id at the end
filename_re_pattern = re.compile("Route #(\d+) ([AP]M)-([NSEW]B)-\d{3}.xls")


def parse_filename(filepath: Path) -> dict:
    cmp_segid, period, direction = filename_re_pattern.fullmatch(
        filepath.name
    ).groups()
    return {
        "cmp_segid": int(cmp_segid),
        "period": period,
        "direction": direction,
    }


def parse_run(filepath: Path) -> dict:
    return parse_xls(filepath) | parse_filename(filepath)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "directory", help="floating car run raw data directory"
    )
    args = parser.parse_args()
    dir = Path(args.directory)

    cmp_seg_lengths = gpd.read_file(
        r"Q:\CMP\LOS Monitoring 2021\CMP_plus_shp\old_cmp_plus\cmp_segments_plus.shp"
    )[["cmp_segid", "length"]]

    # floating car runs
    (
        pl.from_records(
            [parse_run(p) for p in dir.rglob("*.xls")]
            # schema_overrides={"cmp_segid": pl.Int}
        )
        .with_columns(
            pl.col("t_i", "t_f").str.to_datetime("%m/%d/%y %H:%M:%S")
        )
        .with_columns(
            (pl.col("t_f") - pl.col("t_i")).dt.seconds().alias("travel_time_s")
        )
        .join(pl.from_pandas(cmp_seg_lengths), on="cmp_segid", how="left")
        .with_columns(
            (3600 * pl.col("length") / pl.col("travel_time_s"))
            .round(3)
            .alias("speed")
        )
        .groupby(["cmp_segid", "period", "direction"])
        .agg(
            pl.mean("speed").prefix("avg_"),
            pl.std("speed").prefix("std_"),
            pl.min("speed").prefix("min_"),
            pl.max("speed").prefix("max_"),
            pl.count("speed").alias("sample_size"),
        )
        .sort("cmp_segid", "period")
        .write_csv(dir / "floating_car-speed-summary_stats.csv")
    )
