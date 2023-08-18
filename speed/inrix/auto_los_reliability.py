import argparse
import tomllib

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

    peak_min_sample_size_threshold = 180  # AM/PM peak monitoring period
    spatial_coverage_thresholds = [99, 95, 90, 85, 80, 75, 70]
    # Minimum spatial coverage for reliability measurement
    reliability_spatial_coverage_threshold = 70
    if reliability_spatial_coverage_threshold != min(
        spatial_coverage_thresholds
    ):
        raise Warning(
            "reliability_spatial_coverage_threshold "
            "should be consistent with min(spatial_coverage_thresholds)."
        )

    # Read in the INRIX data
    zip_filepaths = glob_inrix_xd_zips_directory(
        config["input"]["dir"], config["input"]["glob"]
    )

    read_and_process_speed_to_los_and_reliability(
        zip_filepaths,
        config["output"]["filepath_stem"],
        config["geo"]["network_conflation_correspondence_filepath"],
        config["geo"]["cmp_segments_gis_filepath"],
        spatial_coverage_thresholds,
        config["input"]["year"],
        peak_min_sample_size_threshold,
        monthly_update=False,
        floating_car_run_speeds_filepath=config["input"][
            "floating_car_run_speeds_filepath"
        ],
        hourly_min_sample_size_threshold=None,
        reliability_spatial_coverage_threshold=reliability_spatial_coverage_threshold,
    )
