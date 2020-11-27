## Testing

The testing branch contains test configs and a script `run_tests.sh` to run utilities in bulk.

Test feeds used in the configs can be downloaded at [http://www.oregon-gtfs.com/](http://www.oregon-gtfs.com/) (with the exception of archive AAMPO and LTD feeds).

## Stop Visits Testing Results

Tested against TNeXT Stop Visit Report for AAMPO 2019:
    1. Generate report with utilities using `stop_visits.aampo-2019-full.yaml` (AAMPO 2019 feed provided by ODOT):
        `python main.py -u stop_visits -c testing/configs/stop_visits.aampo-2019-full.yaml -i testing/data/ -o testing/output/`
    2. Test results against AAMPO-2019-full-Stops Summary Report (`aampo-2019-full-stops.csv`):
        `. run_validate_stop_visits_report.sh output/stop_visits/stop_visit_report.csv aampo-2019-full-stops.csv`
    
    
Output shows a handful of inconsistencies between the two reports, the utilties report includes stop and visits that TNeXT does not:
        ```
        Stops with different visit counts: 
        Agency ID Stop ID  GTFS Utils Visits  TNeXT Visits
                1  802038                3.0           NaN
                1  802254                3.0           NaN
                1  802221                3.0           NaN
                1  802010                3.0           NaN
                1  802054               15.0           NaN
                1  802039               10.0           NaN
                1  802040               12.0           NaN
                1  802551               12.0           NaN
                1  802032                5.0           NaN
                1  802005                8.0           NaN
                1  802475               11.0           NaN
                1  802033                3.0           NaN
                1  802008                3.0           NaN
                1  802009                3.0           NaN
                1  802022                3.0           NaN
                1  802034                6.0           NaN
                1  802549                3.0           NaN
        ```

However, unless I used incorrect parameters I believe the utilities report is correct and the TNeXT report should include these stops:
    - The reports are both for Wednesday 7/24/2019, with no time range bounds
    - In the AAMPO 2019 feed:
        - service_id 1 is the only calendar that is active that day, and there are no service exceptions
        - trip_ids 1-11 and 21-45 use service_id 1
        - There are several instances of stops above being served on these trips:
            - For example, trip_ids 21, 24, and 25 serve several of the stops not included in the TNeXT report:
            ```
            trip_id,block_id,route_id,service_id,direction_id,shape_id
            24,801640,1,07:35:00,07:35:00,1
            24,2440150,2,07:38:00,07:38:00,1
            24,802035,3,07:41:00,07:41:00,1
            24,802038,4,07:52:00,07:52:00,0
            24,802034,5,07:56:00,07:56:00,1
            24,802254,6,07:58:00,07:58:00,0
            24,802221,7,07:58:00,07:58:00,0
            24,802010,8,08:01:00,08:01:00,1
            24,802475,9,08:10:00,08:10:00,1
            24,802054,10,08:15:00,08:15:00,1
            24,802039,11,08:18:00,08:18:00,0
            24,802052,12,08:31:00,08:31:00,0
            24,801798,13,08:35:00,08:35:00,1
            24,801827,14,08:39:00,08:39:00,0
            24,801826,15,08:41:00,08:41:00,0
            24,801825,16,08:42:00,08:42:00,0
            24,801640,17,08:45:00,08:45:00,1
            ```