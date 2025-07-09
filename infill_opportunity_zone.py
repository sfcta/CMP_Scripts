# %%
import geopandas as gpd

# %%
sf_crs = 7131
segments = gpd.read_parquet(
    r"Q:\GIS\Transportation\Roads\CMP\cmp_roadway_segments.parquet"
).to_crs(sf_crs)
ioz = gpd.read_file(
    r"Q:\GIS\Policy\San_Francisco\Infill_Opportunity_Zone\sf-infill_opportunity_zone-2024.gpkg"
).to_crs(sf_crs)

# %%
segments["exempt-ioz-2024"] = (
    segments.geometry.intersection(ioz.union_all()).length / segments.length
) > 0.5
segments["exempt-AM-2024"] = (
    segments["exempt-AM-inaugural_cycle_LOS_F"] | segments["exempt-ioz-2024"]
)
segments["exempt-PM-2024"] = (
    segments["exempt-PM-inaugural_cycle_LOS_F"] | segments["exempt-ioz-2024"]
)

# %%
segments.to_parquet(r"Q:\GIS\Transportation\Roads\CMP\cmp_roadway_segments.parquet")
