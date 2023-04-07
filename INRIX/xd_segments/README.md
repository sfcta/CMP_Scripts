# INRIX XD road network segments
Manage INRIX XD road network segments

## Directory structure
Scripts related INRIX XD road network segments are located in `INRIX/xd_segments/` and `Conflation` (because there is a script conflating the CMP and DTA networks in that directory so not sure how best to organize the directory tree)

## Location of data
### CMP network (see README.md in the root directory of this repo)
### INRIX XD shapefiles
- traffic data are in: `Q:\Data\Observed\Streets\INRIX`
  - the downloaded data are filed by map release version (the `vxx0x` directories (e.g. v2301 for map release 23.1))
- map/shapefiles are in: `Q:\GIS\Transportation\Roads\INRIX\XD`
  - starting from map version 2202, the map version directory name format is changed from ##_0# to ##0# 

## Determining the map version for downloaded INRIX data
- the map version for the downloaded INRIX data can be determined in `reportContents.json` in the `mapVersion` field

## INRIX Map Version updates
- INRIX updates their map version twice a year, in March and September
- map data can be downloaded at https://map-data-downloader.inrix.com/

### Network conflation process
The INRIX XD road network segments are matched to CMP road segments via a network conflation process. The initial network conflation process is recorded in `Q:\CMP\LOS Monitoring 2019\Network_Conflation\Network Conflation Process Memo.docx`

### Updating network conflation after each INRIX map version update
Also reference "XD - Map Update - Best Practices for Map Matching.docx" on the Documents section of https://map-data-downloader.inrix.com/

The network conflation correspondences have to be updated after each INRIX map version update. The updating process involves regenerating the network conflation correspondences file with the new INRIX map data. The correspondences file should stay mostly the same as the previous version's file (unless INRIX did a major update to its road segments).

The inputs/outputs for the conflation process are located at `Q:\CMP\LOS Monitoring yyyy\Network_Conflation\vxx0x\` (except for the data downloaded directly from INRIX, the storage location of which is specified in the "Location of data" section above). The results are stored in a correspondences CSV file in this directory.

In the following, `xx0x` refers to the map release version (e.g. 2301 for map release 23.1).

1. Every March and September, download the latest map release at https://map-data-downloader.inrix.com/
    1. Create a `xx0x` directory under `Q:\GIS\Transportation\Roads\INRIX\XD` to place the downloaded data.
    2. Download the following files to `Q:\GIS\Transportation\Roads\INRIX\XD\xx0x\`:
        1. Shapefiles: USA CA Bay Area  
        2. XDAdded: USA CA
        3. XDRemoved: USA CA
        4. XDReplaced: USA CA
2. Filter for only SF data in the Bay Area shapefile downloaded in step 1.2.1: run `python bayarea_to_sf.py xx0x`.
3. Create `Q:\CMP\LOS Monitoring YYYY\Network_Conflation\vxx0x\`, where `YYYY` is that the map version was released, e.g. `Q:\CMP\LOS Monitoring 2022\Network_Conflation\v2202` for map release version 22.2.
4. Copy `Q:\GIS\Transportation\Roads\INRIX\XD\xx0x\INRIX_XD-SF-xx0x.gpkg` (created in step 2) into `Q:\CMP\LOS Monitoring YYYY\Network_Conflation\vxx0x\` (created in step 3).
5. `SF_CMP_INRIX_Network_Conflation.py`.
    1. Copy `CMP_Scripts/Conflation/SF_CMP_INRIX_Network_Conflation.py` to `Q:\CMP\LOS Monitoring YYYY\Network_Conflation\vxx0x\` (created in step 3).
    2. Change the `MAP_VER` (to `xx0x`, i.e. the current INRIX map release version) and `cmp` (to `Q:\GIS\Transportation\Roads\CMP\cmp_roadway_segments` if conflating onto the CMP network (as in the biennial reports), or `Q:\CMP\LOS Monitoring 2022\CMP_exp_shp\cmp_segments_exp_v2201.shp` if conflating onto the online COVID monthly congestion tracker network) variables as appropriate.
    3. Leave the `MANUAL_UPDATE` variable at the end of the script as `True`. Start by using `manual_add.csv` and `manual_remove.csv` from the previous update.
    4. `cd` into `Q:\CMP\LOS Monitoring YYYY\Network_Conflation\vxx0x\` (created in step 3), then run `python SF_CMP_INRIX_Network_Conflation.py`.
    5. Probably: make changes to `manual_add.csv` and `manual_remove.csv` as needed and rerun step 4.
6. `xd_diff.ipynb` can then be used to explore the differences between the current and the last map version of the INRIX XD road network. (And probably potentially loop back to step 5.4 and 5.5 if necessary).
7. If desired, use GIS software to visualize the results for further verification.

## TODO
- Why are we not using OSM to help do the network conflation between CHAMP/CMP networks and INRIX's?
- Update the network conflation process to be more robust and simplify
