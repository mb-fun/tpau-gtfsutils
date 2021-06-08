call conda env create -f environment.yml

call conda activate gtfsutils
python setup.py develop
CALL conda deactivate
