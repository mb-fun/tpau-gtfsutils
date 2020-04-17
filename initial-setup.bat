rem # If you change this name be sure to remove any conda existing sessions
conda create -n gtfsutils python=3.7

activate gtfsutils

rem #new install
pip install activitysim

rem # update to a new release
rem # pip install -U activitysim

python setup.py develop
