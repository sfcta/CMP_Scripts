import argparse
import tomllib

from core import (
    glob_inrix_xd_zips_directory,
    read_and_process_speed_to_los_and_reliability,
)

if __name__ == "__main__":
    # parser = argparse.ArgumentParser()
    # parser.add_argument("toml_filepath")
    # args = parser.parse_args()
    # with open(args.toml_filepath, "rb") as f:
    #     config = tomllib.load(f)

    network_conflation_correspondence_filepath = r"Q:\CMP\LOS Monitoring 2021\Network_Conflation\CMP_Segment_INRIX_Links_Correspondence_2101_Manual_PLUS_Updated.csv"
    DATA_DIR = r"Q:\Data\Observed\Streets\INRIX\v2101"
    out_filepath_stem = r"Q:\CMP\LOS Monitoring 2023\Auto_LOS_and_Reliability\CMP2021_Auto_LOS_and_Reliability"
    INPUT_GLOB = "SF_county_network_for_CMP2021_2021-04-05_to_2021-05-22_1_min_part_*.zip"
    cmp_segments_gis_filepath = r"Q:\CMP\LOS Monitoring 2021\CMP_plus_shp\old_cmp_plus\cmp_segments_plus.shp"
    floating_car_run_speeds_filepath = r"Q:\Data\Observed\Streets\Speed\CMP-floating_car_run\2021\floating_car-speed-summary_stats.csv"
    year = 2021

    peak_min_sample_size_threshold = (
        180  # for the AM/PM peak monitoring period
    )

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
    zip_filepaths = glob_inrix_xd_zips_directory(DATA_DIR, INPUT_GLOB)

    read_and_process_speed_to_los_and_reliability(
        zip_filepaths,
        out_filepath_stem,
        network_conflation_correspondence_filepath,
        cmp_segments_gis_filepath,
        spatial_coverage_thresholds,
        year,
        peak_min_sample_size_threshold,
        monthly_update=False,
        floating_car_run_speeds_filepath=floating_car_run_speeds_filepath,
        hourly_min_sample_size_threshold=None,
        reliability_spatial_coverage_threshold=reliability_spatial_coverage_threshold,
    )
