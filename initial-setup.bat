rem # If you change this name be sure to remove any conda existing sessions
call conda create -n gtfsutils python=3.7

call conda activate gtfsutils

rem #new install
pip install pandas
pip install pyyaml

CALL conda deactivate

python setup.py develop
