rem # If you change this name be sure to remove any conda existing sessions
call conda create -n gtfsutils python=3.7

call conda activate gtfsutils

rem #new install
call conda install pandas
call conda install geopandas
call conda install pyyaml
call conda install shapely

python setup.py develop

CALL conda deactivate
