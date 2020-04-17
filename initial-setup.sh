# If you change this name be sure to remove any conda existing sessions
conda create -n gtfsutils python=3.7

if [[ "$*" == *-m* ]]
then
    conda activate gtfsutils
else
    activate gtfsutils
fi

#new install
pip install activitysim

#update to a new release
# pip install -U activitysim

python setup.py develop
