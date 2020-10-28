conda create -n gtfsutils python=3.7

conda activate gtfsutils
pip install pyyaml
pip install pandas
pip install geopandas
pip install shapely

python setup.py develop

conda deactivate
