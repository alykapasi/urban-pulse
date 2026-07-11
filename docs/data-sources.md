# Data Sources (NYC)

Candidate sources. None ingested yet — each gets a full entry (URL, format,
update cadence, schema notes, known quirks) when adopted.

| Source | What | Format | Notes |
|---|---|---|---|
| NYC TLC Trip Records | Yellow/green taxi + FHV/HVFHV trips | Monthly Parquet | Zone IDs only (no lat/lon); ~2 month publication lag; schema drift across years |
| NYC Taxi Zones | 263 zone polygons | Shapefile / GeoJSON | The geospatial backbone; check CRS (published in EPSG:2263, NY State Plane ft) |
| Citi Bike | Trip histories | Monthly CSV (zipped) | Real lat/lon + station IDs; schema changed ~2021; station churn |
| MTA GTFS | Static transit schedules | GTFS zip | Route/stop/trip reference data |
| MTA GTFS-RT | Real-time subway/bus positions & alerts | Protobuf feeds | Streaming candidate |
| NOAA / Open-Meteo | Weather observations | API / CSV | Central Park station (USW00094728) is the canonical NYC series |
| NYC 311 | Service requests | Socrata API / CSV | Large; useful for disruption/anomaly context |
| NYC Open Data events | Permitted events | Socrata API | Event/demand correlation |

## Known cross-cutting quirks

- TLC timestamps are local NY time, no timezone marker — DST transitions produce duplicate/missing hours.
- TLC data contains negative fares, 0-distance trips, trips ending before they start.
- Taxi zone shapefile uses EPSG:2263 (feet), not WGS84 — must reproject before joining lat/lon data.
- Citi Bike station IDs changed format over time; some stations move coordinates.
