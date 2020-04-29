echo off

CALL activate gtfsutils

IF "%1" == ""^
 ECHO Utility argument required, please use one of the following:^ 
    average_headways^

ELSE IF "%1" != "average_headway" THEN^
 ECHO Utility argument not recognized, please use one of the following:^
    average_headways^

ELSE python main.py %1

CALL conda deactivate