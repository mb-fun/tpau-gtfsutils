@echo off

IF "%1" == "" (
    ECHO Utility argument required, please use one of the following:
    ECHO   average_headways
    EXIT
)

IF "%1" != "average_headway" (
    ECHO Utility argument not recognized, please use one of the following:
    ECHO   average_headways
    EXIT
)

CALL activate gtfsutils
python main.py %1
CALL conda deactivate