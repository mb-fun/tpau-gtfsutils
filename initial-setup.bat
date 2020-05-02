rem # If you change this name be sure to remove any conda existing sessions
call conda create -n gtfsutils python=3.7

call conda activate gtfsutils

rem #new install
pip install activitysim

CALL conda deactivate

rem # update to a new release
rem # pip install -U activitysim

python setup.py develop
