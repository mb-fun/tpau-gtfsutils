#/bin/bash

# run all configs in testing/configs that match provided sting (probably a utility name)

conda activate gtfsutils

util=$1

for f in testing/configs/*.yaml; do
    if [[ $f =~ $util ]];
        then python main.py -u $util -c $f -i testing/data/ -o testing/output/;
    fi;
done

conda deactivate