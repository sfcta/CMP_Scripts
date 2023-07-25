# CMP_Scripts
Data preparation scripts for the Congestion Management Program (CMP)

## Definitions
- peak periods (in use since (at least) CMP 2007)
  - AM: 7-9am
  - PM: 4:30-6:30pm
  - the earliest instance I could find for this definition was in the LOS Monitoring appendix to CMP 2007, where it also said that the 2004 and 2006 studies used 4-6pm as the PM peak (Carter Burgess 2007 CMP Spring 2007 Level of Service Monitoring, p2; p172 in 2007_CMP_ALL.pdf)

## Map / GIS
- The CMP road network data (for the biennial report) is at Q:\GIS\Transportation\Roads\CMP\cmp_roadway_segments.*
- The COVID monthly congestion tracker road network is at Q:\CMP\LOS Monitoring 2022\CMP_exp_shp\cmp_segments_exp_v2201.*
- Note that CMP and CHAMP segments are different

## Sections/Metrics
### Transit
- GTFS data: can be downloaded from https://database.mobilitydata.org/
### Screenline volumes (Bay Bridge and San Mateo County Line)
From PeMS data, use PeMS/.....py