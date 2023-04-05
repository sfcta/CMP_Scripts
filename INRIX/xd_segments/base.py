"""Tools to manipulate INRIX XD road network segments"""
import geopandas as gpd
from os.path import join


# downloaded from INRIX
bayarea_gis_filename_noext = "USA_CA_BayArea_shapefile"  # without .shp or .zip


def _map_dir(map_release_version: str) -> str:
    return join("Q:\GIS\Transportation\Roads\INRIX\XD", map_release_version)


def _sf_gis_filename(map_release_version: str) -> str:
    """created by bayarea_to_sf.py"""
    return f"INRIX_XD-SF-{map_release_version}.gpkg"


def sf_gis_filepath(map_release_version: str) -> str:
    return join(
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
    filepath = join(
        map_dir,
        "maprelease-shapefiles",
        f"{bayarea_gis_filename_noext}.zip!{bayarea_gis_filename_noext}",
    )
    gdf = gpd.read_file(f"zip://{filepath}")
    if sf_only:
        # filter for segments in SF only
        gdf = gdf[gdf.County == "SAN FRANCISCO"]
    return gdf
