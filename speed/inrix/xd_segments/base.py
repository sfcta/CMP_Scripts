"""Tools to manipulate INRIX XD road network segments"""
from pathlib import Path

import geopandas as gpd


def _map_dir(map_release_version: str) -> Path:
    return Path("Q:\GIS\Transportation\Roads\INRIX\XD", map_release_version)


def _sf_gis_filename(map_release_version: str) -> str:
    """created by bayarea_to_sf.py"""
    return f"INRIX_XD-SF-{map_release_version}.gpkg"


def sf_gis_filepath(map_release_version: str) -> Path:
    return Path(
        _map_dir(map_release_version), _sf_gis_filename(map_release_version)
    )


def read_bayarea_xd_segments(
    map_release_version: str, sf_only: bool = True
) -> gpd.GeoDataFrame:
    """
    Parameters
    ----------
    map_release_version
    sf_filter
        whether to filter for SF segments only
    """
    map_dir = _map_dir(map_release_version)
    # as downloaded from INRIX, without .shp or .zip:
    bayarea_gis_filename_noext = "USA_CA_BayArea_shapefile"
    filepath = Path(
        map_dir,
        "maprelease-shapefiles",
        f"{bayarea_gis_filename_noext}.zip",
    )
    if map_release_version == "2301":
        gdf = gpd.read_file(f"zip://{filepath}!{bayarea_gis_filename_noext}")
    else:
        gdf = gpd.read_file(f"zip://{filepath}")
    gdf["XDSegID"] = gdf["XDSegID"].astype(int)
    if sf_only:
        # filter for segments in SF only
        gdf = gdf[gdf.County == "San Francisco"]
    return gdf


def read_sf_xd_segments(map_release_version: str) -> gpd.GeoDataFrame:
    gdf = gpd.read_file(sf_gis_filepath(map_release_version))
    gdf["XDSegID"] = gdf["XDSegID"].astype(int)
    return gdf
