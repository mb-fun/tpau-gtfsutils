@echo off

IF "%1" == "" (
    ECHO Utility argument required, please use one of the following:
    ECHO   average_headway
    ECHO   one_day
    EXIT /b
)

CALL conda activate gtfsutils
python main.py -u %1
CALL conda deactivate
