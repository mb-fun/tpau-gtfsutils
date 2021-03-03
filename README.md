# TPAU GTFS Utilities

## How To Use

### Requirements

- Anaconda

### Installation/Setup (Windows)

- Download repository by running `git clone git@github.com:anniekfifer/tpau-gtfsutils.git` in the CMD tool
- In the Anaconda Prompt application, run `initial-setup.bat` from the project root directory 

### Configuring and running a utility

- Edit the input parameters in the appropriate config yaml file in the `config/` folder. Follow formatting guidelines in the file's comments. 
- Copy or move any input GTFS is in the `data/` folder (GTFS in this folder will be read from, but not be altered in any way)
- To run a utility:
  - Open Anaconda Prompt application
  -  In project root directory, run `tpau-utils.bat` with the appropriate utility name (`average_headway`, `one_day`, `interpolate_stoptimes`, `stop_visits`, or `cluster_stops`) and optionally a path to the config file (if omitted this will look for a `yaml` file with the utility name, i.e `average_headway.yaml`). 
    -  Example: `tpau-utils.bat average_headway myconfig.yaml`

### Configuring and running example

- Edit `config/average_headway.yaml` input parameters to use example values in comments
- Make sure that `good_feed.zip` (included in repo for testing) is in `data/`
- In the Anaconda Prompt application, run `tpau-utils.bat average_headway`
  
### Output

- Application output will go to `output/` directory

## Behavior

### Average Headways

Outputs csv reports with average headway minutes for each distinct Route/Direction pair within the date and time ranges provided.

Report csv headers:
`route_id,direction_id,agency_id,route_long_name,agency_name,date,start_time,end_time,average_headway_minutes,trip_start_times`
  
### Interpolate Stoptimes

Outputs GTFS feeds with missing stop arrival/departure times filled in with estimations based on shapes/shape_dist_traveled.

Requirements:
- shapes.txt is present 
- trips.txt includes shape_dist_traveled
- Feed does not use flex areas

### Cluster Stops

Outputs GTFS feeds with stops clustered by radius (prioritizing rail/lift/tram stops, then prioritizing by stop visits) to new stops, as well as a Stop Visits report for each new feed.

New stops used across feeds will share stop_id, location and other stop information.

### One Day

Outputs GTFS feed with service reduced to input date and timeranges and with exceptions removed. Entities made unused by the date/time filtering (i.e. routes, trips, stops, calendars) are removed from the feed.

### Stop Visits

Outputs csv report of stop visits within provided date and time range, for each Agency/Stop pair in feed. Stops can be filtered by shapefile or geojson if provided. 

Report csv headers:
`agency_id,agency_name,stop_id,stop_name,stop_lat,stop_lon,visit_counts,boardings,alightings`

### GTFS Output Notes

GTFS output may include some minor unintentional changes to the data, such as:

- Decimal truncation -- Decimals are rounded to the nearest 12 decimal places. This would most commonly occur in lat/lon coordinates, but 12 decimal places is sufficiently for most purposes. Trailing zeros are also stripped from decimals over one place.
- Column reordering -- Columns that serve as IDs for a file (i.e. trip_id in trips.txt) may be brought to the front of the columns.
- Quotation removal -- The utilities remove wrapping quotes for fields that do not otherwise contain quotations or commas. 
