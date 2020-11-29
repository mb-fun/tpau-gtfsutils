rem # If you change this name be sure to remove any conda existing sessions
call conda create -n gtfsutils python=3.7

call conda activate gtfsutils

rem #new install
conda install pandas
conda install geopandas
conda install pyyaml
conda install shapely

python setup.py develop

CALL conda deactivate
