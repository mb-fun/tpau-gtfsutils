conda create -n gtfsutils python=3.7

conda activate gtfsutils
conda install pyyaml
conda install pandas
conda install geopandas
conda install shapely

python setup.py develop

conda deactivate
