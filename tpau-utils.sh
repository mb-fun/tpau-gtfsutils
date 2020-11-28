conda activate gtfsutils

echo $2
if [[ $2 =~ "yaml" ]]
  then python main.py -u $1 -c $2
  else python main.py -u $1
fi

conda deactivate
