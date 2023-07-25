"""
Assumed to be used before the monitoring period to decide which
segments need floating car runs. Copied as is from legacy/...2021.py.
Revise docstring in 2025 cycle.
"""
from os.path import join

import dask.dataframe as dd
import geopandas as gp
import numpy as np
import pandas as pd

NETCONF_DIR = r"Q:\CMP\LOS Monitoring 2021\Network_Conflation"
CORR_FILE = "CMP_Segment_INRIX_Links_Correspondence_2101_Manual - PLUS.csv"

DATA_DIR = r"Q:\Data\Observed\Streets\INRIX\v2101"
INPUT_PATHS = [["All_SF_2021-02-14_to_2021-03-28_1_min_part_", 8]]

SHP_FILE = r"Q:\GIS\Transportation\Roads\INRIX\XD\21_01\maprelease-shapefiles\SF\Inrix_XD_2101_SF.shp"
OUT_FILE = r"Q:\CMP\LOS Monitoring 2021\Sample_Size_Analysis\SF_INRIX_Sample_Size_SixWeeks.shp"


# Get CMP and INRIX correspondence table
conflation = pd.read_csv(join(NETCONF_DIR, CORR_FILE))
conflation[["CMP_SegID", "INRIX_SegID"]] = conflation[
    ["CMP_SegID", "INRIX_SegID"]
].astype(int)
conf_len = conflation.groupby("CMP_SegID").Length_Matched.sum().reset_index()
conf_len.columns = ["CMP_SegID", "CMP_Length"]

df = pd.DataFrame()
for p in INPUT_PATHS:
    for i in range(1, p[1]):
        df1 = dd.read_csv(
            join(DATA_DIR, "%s%s\data.csv" % (p[0], i)), assume_missing=True
        )
        if len(df1) > 0:
            df1["Segment ID"] = df1["Segment ID"].astype("int")
            df1 = df1[df1["Segment ID"].isin(conflation["INRIX_SegID"])]
            df = dd.concat([df, df1], axis=0, interleave_partitions=True)

print("Combined record #: ", len(df))

# Create date and time fields for subsequent filtering
df["Date_Time"] = df["Date Time"].str[:16]
df["Date_Time"] = df["Date_Time"].str.replace("T", " ")
df["Date"] = df["Date_Time"].str[:10]
df["Day"] = dd.to_datetime(df["Date_Time"])
df["DOW"] = df.Day.dt.dayofweek  # Tue-1, Wed-2, Thu-3
df["Hour"] = df.Day.dt.hour
df["Minute"] = df.Day.dt.minute


# Get AM (7-9am) speeds on Tue, Wed, and Thu
df_am = df[
    ((df["DOW"] >= 1) & (df["DOW"] <= 3))
    & ((df["Hour"] == 7) | (df["Hour"] == 8))
]
print("Number of records in AM: ", len(df_am))

# Get PM (4:30-6:30pm) speeds on Tue, Wed, and Thu
df_pm = df[
    ((df["DOW"] >= 1) & (df["DOW"] <= 3))
    & (
        ((df["Hour"] == 16) & (df["Minute"] >= 30))
        | (df["Hour"] == 17)
        | ((df["Hour"] == 18) & (df["Minute"] < 30))
    )
]
print("Number of records in PM: ", len(df_pm))

# Calculate AM sample size for each XD link
df_am_sum = df_am.groupby("Segment ID").Date_Time.agg(["count"]).compute()
df_am_sum = df_am_sum.reset_index()
df_am_sum.columns = ["Segment ID", "Count_AM"]

# Calculate PM sample size for each XD link
df_pm_sum = df_pm.groupby("Segment ID").Date_Time.agg(["count"]).compute()
df_pm_sum = df_pm_sum.reset_index()
df_pm_sum.columns = ["Segment ID", "Count_PM"]

num_days = 18  # six weeks
df_am_sum["Pcnt_AM"] = round(
    100 * df_am_sum["Count_AM"] / num_days / 2 / 60, 3
)
df_pm_sum["Pcnt_PM"] = round(
    100 * df_pm_sum["Count_PM"] / num_days / 2 / 60, 3
)
print("Number of links with speeds in AM: ", len(df_am_sum))
print("Number of links with speeds in PM: ", len(df_pm_sum))

# Read in INRIX XD network shapefile
sf_shp = gp.read_file(SHP_FILE)
print("Number of links in SF: ", len(sf_shp))

# Attach the sample size measures to the shapefile
df_am_sum["Segment ID"] = df_am_sum["Segment ID"].astype(np.int64)
df_pm_sum["Segment ID"] = df_pm_sum["Segment ID"].astype(np.int64)
sf_shp["XDSegID"] = sf_shp["XDSegID"].astype(np.int64)
df_samplesize = pd.merge(
    sf_shp, df_am_sum, left_on="XDSegID", right_on="Segment ID", how="left"
)  # AM
df_samplesize = pd.merge(
    df_samplesize,
    df_pm_sum,
    left_on="XDSegID",
    right_on="Segment ID",
    how="left",
)  # PM

# Create adequacy indicators for 10%, 30%, and 50% thresholds
df_samplesize["ADQT_AM_10"] = df_samplesize.apply(
    lambda x: "Yes" if x["Pcnt_AM"] > 10 else "No", axis=1
)  # 10% threshold for AM
df_samplesize["ADQT_AM_30"] = df_samplesize.apply(
    lambda x: "Yes" if x["Pcnt_AM"] > 30 else "No", axis=1
)  # 30% threshold for AM
df_samplesize["ADQT_AM_50"] = df_samplesize.apply(
    lambda x: "Yes" if x["Pcnt_AM"] > 50 else "No", axis=1
)  # 50% threshold for AM

df_samplesize["ADQT_PM_10"] = df_samplesize.apply(
    lambda x: "Yes" if x["Pcnt_PM"] > 10 else "No", axis=1
)  # 10% threshold for PM
df_samplesize["ADQT_PM_30"] = df_samplesize.apply(
    lambda x: "Yes" if x["Pcnt_PM"] > 30 else "No", axis=1
)  # 30% threshold for PM
df_samplesize["ADQT_PM_50"] = df_samplesize.apply(
    lambda x: "Yes" if x["Pcnt_PM"] > 50 else "No", axis=1
)  # 50% threshold for PM

# Output the combined shapefile
df_samplesize.to_file(OUT_FILE)
