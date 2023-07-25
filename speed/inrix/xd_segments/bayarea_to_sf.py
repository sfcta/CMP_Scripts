"""
Filter the INRIX XD segments Bay Area shapefile for SF road segments, then save
"""
import argparse
from base import read_bayarea_xd_segments, sf_gis_filepath


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Filter the INRIX XD segments Bay Area shapefile "
            "for SF road segments, then save."
        )
    )
    parser.add_argument("map_release_version", help='e.g. "2301')
    args = parser.parse_args()
    map_release_version = args.map_release_version
    gdf = read_bayarea_xd_segments(map_release_version, sf_only=True)
    gdf.to_file(sf_gis_filepath(map_release_version))
