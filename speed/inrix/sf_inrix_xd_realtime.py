import argparse
import os
import re
from glob import glob
from pathlib import Path
from pprint import pprint
from zipfile import ZipFile

import geopandas as gpd
import numpy as np
import pandas as pd
import polars as pl
import tomllib


# Define processing functions
# LOS function using 1985 HCM
def los_1985(cls, spd):
    if spd is None:
        return " "
    else:
        if cls == "Fwy":  # Freeway
            if spd >= 60:
                return "A"
            elif spd >= 55:
                return "B"
            elif spd >= 49:
                return "C"
            elif spd >= 41:
                return "D"
            elif spd >= 30:
                return "E"
            elif (spd > 0) and (spd < 30):
                return "F"
        elif cls == "1":  # Arterial Class I
            if spd >= 35:
                return "A"
            elif spd >= 28:
                return "B"
            elif spd >= 22:
                return "C"
            elif spd >= 17:
                return "D"
            elif spd >= 13:
                return "E"
            elif (spd > 0) and (spd < 13):
                return "F"
        elif cls == "2":  # Arterial Class II
            if spd >= 30:
                return "A"
            elif spd >= 24:
                return "B"
            elif spd >= 18:
                return "C"
            elif spd >= 14:
                return "D"
            elif spd >= 10:
                return "E"
            elif (spd > 0) and (spd < 10):
                return "F"
        elif cls == "3":  # Arterial Class III
            if spd >= 25:
                return "A"
            elif spd >= 19:
                return "B"
            elif spd >= 13:
                return "C"
            elif spd >= 9:
                return "D"
            elif spd >= 7:
                return "E"
            elif (spd > 0) and (spd < 7):
                return "F"


# LOS function using 2000 HCM
def los_2000(cls, spd):
    if spd is None:
        return " "
    else:
        if cls == "Fwy":  # Freeway
            return " "
        elif cls == "1":  # Arterial Class I
            if spd > 42:
                return "A"
            elif spd > 34:
                return "B"
            elif spd > 27:
                return "C"
            elif spd > 21:
                return "D"
            elif spd > 16:
                return "E"
            elif (spd > 0) and (spd <= 16):
                return "F"
        if cls == "2":  # Arterial Class II
            if spd > 35:
                return "A"
            elif spd > 28:
                return "B"
            elif spd > 22:
                return "C"
            elif spd > 17:
                return "D"
            elif spd > 13:
                return "E"
            elif (spd > 0) and (spd <= 13):
                return "F"
        elif cls == "3":  # Arterial Class III
            if spd > 30:
                return "A"
            elif spd > 24:
                return "B"
            elif spd > 18:
                return "C"
            elif spd > 14:
                return "D"
            elif spd > 10:
                return "E"
            elif (spd > 0) and (spd <= 10):
                return "F"
        elif cls == "4":  # Arterial Class IV
            if spd > 25:
                return "A"
            elif spd > 19:
                return "B"
            elif spd > 13:
                return "C"
            elif spd > 9:
                return "D"
            elif spd > 7:
                return "E"
            elif (spd > 0) and (spd <= 7):
                return "F"


# Define percentile funtion to get 5th and 20th percentile speed
def percentile(n):
    def percentile_(x):
        return np.percentile(x, n)

    percentile_.__name__ = "percentile_%s" % n
    return percentile_


# Calculate CMP segment level average speeds and LOS
def spatial_coverage(
    df,
    cmp_period_agg,
    group_cols,
    rename_cols,
    ss_threshold,
    coverage_threshold,
):
    # select the segment IDs that are not in cmp_period_agg yet (i.e. not
    # covered by all the previous coverage thresholds yet), but that are covered
    # under the current coverage threshold
    select = (~df["CMP_SegID"].isin(cmp_period_agg["cmp_segid"])) & (
        df["spatial_coverage"] >= coverage_threshold
    )
    cmp_tt_agg = pd.DataFrame()
    if select.sum() > 0:
        cmp_tt_agg = (
            df[select]
            .groupby(group_cols)
            .agg(
                {
                    "TT": ["count", "sum"],
                    "Length_Matched": "sum",
                    "Speed": ["std", percentile(5), percentile(20)],
                }
            )
            .reset_index()
        )
        cmp_tt_agg.columns = rename_cols
        cmp_tt_agg["avg_speed"] = round(
            cmp_tt_agg["Len"] / cmp_tt_agg["TT"], 3
        )
        cmp_tt_agg["cov"] = round(
            100 * cmp_tt_agg["std_speed"] / cmp_tt_agg["avg_speed"], 3
        )
        cmp_tt_agg = cmp_tt_agg[cmp_tt_agg["sample_size"] >= ss_threshold]

    if len(cmp_tt_agg) > 0:
        cmp_tt_agg["comment"] = (
            "Calculation performed on "
            + str(coverage_threshold)
            + "% or greater of length"
        )
        return cmp_tt_agg
    else:
        # might be better to return an empty DataFrame?
        # A maybe monad would've been useful
        return None


# Calculate CMP segment level average speeds, LOS, and reliability metrics
def cmp_seg_level_speed_and_los(
    cmp_segs, conf_len, cmp_period_df, ss_threshold, cur_year, cur_period
):
    cmp_period = (
        cmp_period_df.with_columns(
            (pl.col("Length_Matched") / pl.col("Speed(miles/hour)")).alias(
                "TT"
            )
        )
        # Get total travel time at a particular date_time on a CMP segment
        .groupby(["CMP_SegID", "Date", "Date_Time"])
        .agg(pl.sum("Length_Matched"), pl.sum("TT"))
        .join(conf_len, on="CMP_SegID", how="left")
        .with_columns(
            (pl.col("Length_Matched") / pl.col("TT")).alias("Speed"),
            (100 * pl.col("Length_Matched") / pl.col("CMP_Length")).alias(
                "spatial_coverage"
            ),
        )
    ).to_pandas()  # so I don't have to change the code below

    group_cols = ["CMP_SegID", "Date"]
    rename_cols = [
        "cmp_segid",
        "date",
        "sample_size",
        "TT",
        "Len",
        "std_speed",
        "pcnt5th",
        "pcnt20th",
    ]

    coverage_thresholds = [99, 95, 90, 85, 80, 75, 70]
    cmp_period_agg = pd.DataFrame(columns=rename_cols)
    for coverage_threshold in coverage_thresholds:
        coverage = spatial_coverage(
            cmp_period,
            cmp_period_agg,
            group_cols,
            rename_cols,
            ss_threshold,
            coverage_threshold,
        )
        if coverage is not None:
            cmp_period_agg = pd.concat(
                (cmp_period_agg, coverage), ignore_index=True
            )

    cmp_period_agg["year"] = cur_year
    cmp_period_agg["period"] = cur_period

    cmp_period_agg["cmp_segid"] = cmp_period_agg["cmp_segid"].astype(
        "int64"
    )  # MAX ADDED (Cast to int for merging)

    cmp_period_agg = pd.merge(
        cmp_period_agg,
        cmp_segs[["cmp_segid", "cls_hcm85", "cls_hcm00"]],
        on="cmp_segid",
        how="left",
    )
    cmp_period_agg["los_hcm85"] = cmp_period_agg.apply(
        lambda x: los_1985(x.cls_hcm85, x.avg_speed), axis=1
    )
    cmp_period_agg["los_hcm00"] = cmp_period_agg.apply(
        lambda x: los_2000(x.cls_hcm00, x.avg_speed), axis=1
    )

    # TMP incomplete porting from pandas to polars
    return pl.from_pandas(
        cmp_period_agg, schema_overrides={"period": str}
    ).select(
        "cmp_segid",
        "year",
        "date",
        "period",
        "avg_speed",
        "los_hcm85",
        "los_hcm00",
        "std_speed",
        "pcnt5th",
        "pcnt20th",
        "cov",
        "sample_size",
        "comment",
    )


def _dirs_in_zip(zip_filepath: str) -> set:
    with ZipFile(zip_filepath, "r") as z:
        return set([os.path.dirname(f) for f in z.namelist()])


def _zip_hashdir(zip_filepath: str) -> str:
    dirs = _dirs_in_zip(zip_filepath)
    if len(dirs) > 1:
        raise RuntimeError(
            f"Multiple directories found inside {zip_filepath}. There should "
            "only be one directory inside the zip files downloaded from INRIX."
        )
    return next(iter(dirs))  # return the next/only item in dirs


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("toml_filepath")
    args = parser.parse_args()

    with open(args.toml_filepath, "rb") as f:
        config = tomllib.load(f)

    # Sample size thresholds
    ss_threshold_peaks = (
        10  # Minimum sample size for the AM/PM monitoring period
    )
    ss_threshold_hourly = 10  # Miniumum sample size for hourly

    # Get CMP and INRIX correspondence table
    conflation = pl.read_csv(
        Path(config["geo"]["network_conflation_correspondence_filepath"])
    )
    conf_len = conflation.groupby("CMP_SegID").agg(
        pl.sum("Length_Matched").alias("CMP_Length")
    )

    # Read in the INRIX data using dask to save memory
    zip_filepaths = sorted(
        Path(config["input"]["dir"]).glob(config["input"]["glob"]),
        # natural sort of part numbers
        key=lambda f: [int(x) for x in re.findall(r"\d+", f.stem)],
    )
    print("inputs files:")
    pprint(zip_filepaths)
    input_dfs = []
    for zip_filepath in zip_filepaths:
        print(f"Reading input file {zip_filepath}...")
        # fsspec filepath (e.g. for dask):
        # (
        #     f"zip://{_zip_hashdir(zip_filepath)}/data.csv"
        #     "::file://{zip_filepath}"
        # )
        # seems like polars cannot handle reading zip CSVs via fsspec
        with ZipFile(zip_filepath) as z:
            with z.open(
                f"{_zip_hashdir(zip_filepath)}/data.csv", mode="r"
            ) as f:
                input_df = pl.read_csv(
                    f.read(),
                    dtypes={
                        "Hist Av Speed(miles/hour)": pl.Int64,
                        "CValue": pl.Float64,
                    },
                )
        if len(input_df):  # if > 0
            input_df = input_df.filter(
                input_df["Segment ID"].is_in(conflation["INRIX_SegID"])
            )
            input_dfs.append(input_df)
    cmp_df = pl.concat(input_dfs, how="vertical", rechunk=False)

    # Calculate reference speed for CMP segments
    inrix_link_refspd = cmp_df.unique(
        subset=["Segment ID", "Ref Speed(miles/hour)"]
    )
    cmp_segs_refspd = (
        conflation.join(
            inrix_link_refspd.select(["Segment ID", "Ref Speed(miles/hour)"]),
            left_on="INRIX_SegID",
            right_on="Segment ID",
            how="left",
        )
        .drop_nulls(subset="Ref Speed(miles/hour)")
        .with_columns(
            (pl.col("Length_Matched") / pl.col("Ref Speed(miles/hour)")).alias(
                "RefTT"
            ),
        )
        # at this point, turn from link-based to segment-based
        .groupby("CMP_SegID")
        .agg(pl.sum("Length_Matched"), pl.sum("RefTT"))
        .with_columns(
            (pl.col("Length_Matched") / pl.col("RefTT")).alias("refspd_inrix")
        )
        .rename({"CMP_SegID": "cmp_segid"})
    )

    # Create date and time fields for subsequent filtering
    cmp_df = (
        cmp_df.with_columns(
            pl.col("Date Time")
            .str.slice(0, 16)
            .str.replace("T", " ")
            .alias("Date_Time")
        )
        .with_columns(
            pl.col("Date_Time").str.slice(0, 10).alias("Date"),
            pl.col("Date_Time").str.to_datetime().alias("Day"),
        )
        .with_columns(
            pl.col("Day").dt.weekday().alias("DOW"),  # Tue=2, Wed=3, Thu=4
            pl.col("Day").dt.hour().alias("Hour"),
            pl.col("Day").dt.minute().alias("Minute"),
        )
        # remove weekends & 0 speed
        .filter((pl.col("DOW") <= 5) & (pl.col("Speed(miles/hour)") > 0))
        .join(  # Get inrix_segid : cmp_segid mapping
            conflation,
            left_on="Segment ID",
            right_on="INRIX_SegID",
            how="outer",
        )
    )

    print("Processing hourly & AM/PM periods...")

    cmp_segs = gpd.read_file(
        Path(config["geo"]["cmp_shp"])
    )  # CMP segment shapefile
    cmp_segs_los_dfs = []

    # Hourly
    for hour in range(24):
        print(f"Hour = {hour}")
        cmp_segs_los_dfs.append(
            cmp_seg_level_speed_and_los(
                cmp_segs,
                conf_len,
                cmp_df.filter(pl.col("Hour") == hour),
                ss_threshold_hourly,
                cur_year=config["input"]["year"],
                cur_period=hour,
            )
        )

    # AM (7-9am)
    print("AM Period")
    cmp_segs_los_dfs.append(
        cmp_seg_level_speed_and_los(
            cmp_segs,
            conf_len,
            cmp_df.filter((pl.col("Hour") == 7) | (pl.col("Hour") == 8)),
            ss_threshold_peaks,
            cur_year=config["input"]["year"],
            cur_period="AM",
        )
    )

    # PM (4:30-6:30pm)
    print("PM Period")
    subset = cmp_df.filter(
        ((pl.col("Hour") == 16) & (pl.col("Minute") >= 30))
        | (pl.col("Hour") == 17)
        | ((pl.col("Hour") == 18) & (pl.col("Minute") < 30))
    )
    cmp_segs_los_dfs.append(
        cmp_seg_level_speed_and_los(
            cmp_segs,
            conf_len,
            subset,
            ss_threshold_peaks,
            cur_year=config["input"]["year"],
            cur_period="PM",
        )
    )

    cmp_segs_los = pl.concat(cmp_segs_los_dfs, how="vertical")
    print("Finished processing periods.")

    # Calculate reliability metrics
    cmp_segs_los = cmp_segs_los.join(
        cmp_segs_refspd.select("cmp_segid", "refspd_inrix"),
        on="cmp_segid",
        how="left",
    ).with_columns(
        pl.max(1, (pl.col("refspd_inrix") / pl.col("pcnt5th"))).alias("tti95"),
        pl.max(1, (pl.col("refspd_inrix") / pl.col("pcnt20th"))).alias(
            "tti80"
        ),
        pl.max(0, (pl.col("avg_speed") / pl.col("pcnt5th") - 1)).alias("bi"),
    )

    # Write out df for both hourly & AM/PM peak
    # peaks
    cmp_segs_los.filter(pl.col("period").is_in(["AM", "PM"])).write_csv(
        Path(
            config["output"]["dir"],
            f'{config["output"]["filename_stem"]}_AMPM.csv',
        )
    )
    # hourly
    cmp_segs_los.filter(~pl.col("period").is_in(["AM", "PM"])).rename(
        {"period": "hour"}
    ).write_csv(
        Path(
            config["output"]["dir"],
            f'{config["output"]["filename_stem"]}_Hourly.csv',
        )
    )
