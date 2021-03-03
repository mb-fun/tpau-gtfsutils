@echo off
REM Runs the utility specified by user.
REM First argument is the utility (required)
REM Second argument is the config file (optional, program defaults to configs/[utility].yaml)

IF "%1" == "" (
    ECHO Utility argument required, please use one of the following:
    ECHO   average_headway
    ECHO   one_day
    ECHO   stop_visits
    ECHO   interpolate_stoptimes
    ECHO   cluster_stops
    EXIT /b
)

CALL conda activate gtfsutils

IF "%2" == "" (python main.py -u %1) ELSE (python main.py -u %1 -c %2)

CALL conda deactivate
