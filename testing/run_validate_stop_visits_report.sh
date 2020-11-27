conda activate gtfsutils
python validate_stop_visits_report.py -u $1 -t $2
conda deactivate
