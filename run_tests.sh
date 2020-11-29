#/bin/bash

# run all configs in testing/configs that match provided sting (probably a utility name)

conda activate gtfsutils

util=$1

for f in testing/configs/*.yaml; do
    if [[ $f =~ $util ]] && [[ $f != *template* ]]
    then 
        filename=$(basename -- "$f")
        filename="${filename%.*}"
        # output_file=testing/testing_output/$filename.output.txt
        echo "Running $util with config $f..."
        python main.py -e -u $util -c $f -i testing/data/ -o testing/output/
    fi;
done

conda deactivate
