import pandas as pd
from os.path import join
from base import _map_dir


def _read_xddiff_csv(map_release_version: str, xddiff_dir: str):
    """
    Parameters1
    ----------
    map_release_version
        e.g. "2301"
    xddiff_dir : {"maprelease-xdadded",
                  "maprelease-xdremoved",
                  "maprelease-xdreplaced"}
    """
    if xddiff_dir not in {
        "maprelease-xdadded",
        "maprelease-xdremoved",
        "maprelease-xdreplaced",
    }:
        raise KeyError
    map_dir = _map_dir(map_release_version)
    return pd.read_csv(join(map_dir, xddiff_dir, "USA_CALIFORNIA.csv"))


def read_replace_csv(map_release_version: str) -> pd.DataFrame:
    return _read_xddiff_csv(map_release_version, "maprelease-xdreplaced")


def _xd_segments_diffed(xd_segments_gdf, xddiff_df):
    """Filter for XD Segments that are added/removed/replaced"""
    return xd_segments_gdf[
        xd_segments_gdf["XDSegID"].isin(xddiff_df["SegId"].tolist())
    ]


def xd_segments_added(xd_segments_gdf, map_release_version: str):
    """
    Parameters
    ----------
    xd_segments_gdf
    map_release_version
        make sure this is the same map release version as xd_segments_gdf
    """
    return _xd_segments_diffed(
        xd_segments_gdf,
        _read_xddiff_csv(map_release_version, "maprelease-xdadded"),
    )


def xd_segments_removed(
    previous_xd_segments_gdf, current_map_release_version: str
):
    """
    Parameters
    ----------
    previous_xd_segments_gdf
        xd_segments_gdf from the previous map release version
    current_map_release_version
        the current map release version, one after that of old_xd_segments_gdf
    """
    return _xd_segments_diffed(
        previous_xd_segments_gdf,
        _read_xddiff_csv(current_map_release_version, "maprelease-xdremoved"),
    )
