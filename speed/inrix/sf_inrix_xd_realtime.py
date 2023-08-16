import argparse
import tomllib
from pathlib import Path

from core import (
    glob_inrix_xd_zips_directory,
    read_and_process_speed_to_los_and_reliability,
)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("toml_filepath")
    args = parser.parse_args()
    with open(args.toml_filepath, "rb") as f:
        config = tomllib.load(f)

    # sample size thresholds
    peak_min_sample_size_threshold = 10  # AM/PM peak monitoring period
    hourly_min_sample_size_threshold = 10  # Miniumum sample size for hourly
    spatial_coverage_thresholds = [99, 95, 90, 85, 80, 75, 70]

    zip_filepaths = glob_inrix_xd_zips_directory(
        config["input"]["dir"], config["input"]["glob"]
    )

    read_and_process_speed_to_los_and_reliability(
        zip_filepaths,
        Path(config["output"]["dir"], config["output"]["filename_stem"]),
        Path(config["geo"]["network_conflation_correspondence_filepath"]),
        Path(config["geo"]["cmp_shp"]),
        spatial_coverage_thresholds,
        config["input"]["year"],
        peak_min_sample_size_threshold,
        monthly_update=True,
        hourly_min_sample_size_threshold=hourly_min_sample_size_threshold,
    )
