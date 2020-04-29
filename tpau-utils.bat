@echo off

IF "%1" == "" (
    ECHO Utility argument required, please use one of the following:
    ECHO   average_headway
    EXIT /b
)

IF NOT "%1" == "average_headway" (
    ECHO Utility argument not recognized, please use one of the following:
    ECHO   average_headway
    EXIT /b
)

CALL activate gtfsutils
python main.py -u %1
CALL conda deactivate