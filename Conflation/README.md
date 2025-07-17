# network conflation

## CMP x DTA network conflation
SF_CMP_DTA_Network_Conflation.py (last touched in 2022/12/9) was used to automatically establish the corresponding relationship between CMP segments and the DTA (dynamic traffic assignment model) network links. It finds DTA links that are near each CMP segment and decides the matching links based on street name, angle and distance attributes.

CH removed this script in 2025/7 since it's basically a copy of SF_CMP_INRIX_Network_Conflation.py (which CH is updating and so the code will start diverging) with the following changes:
- MAP_VER = 2022
- the variable `inrix_sf` is used to hold the DTA network, with `inrix_sf = "links"` and `segid_col = "ID"`
- `inrix_net_prj = inrix_net_prj.rename(columns={"PreviousXD": "PreviousSe", "NextXDSegI": "NextSegID"})` is commented out
- when reading in the dta projected shapefile, instead of `SegID` and `RoadName` (for INRIX), `ID` and `Start` are used
- rem_df is from `manual_remove.csv`, add_df is from `manual_add.csv`

TODO: put all of these into a TOML config file instead