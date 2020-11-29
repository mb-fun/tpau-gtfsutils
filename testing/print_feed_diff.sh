feed_1=$1
feed_2=$2

output=$3

mkdir tmp_1
mkdir tmp_2

unzip $feed_1 -d tmp_1
unzip $feed_2 -d tmp_2

> $output

for fpath in tmp_1/*.txt; do
    filename=$(basename -- "$fpath")

    # lat and lon is truncated a negligable amount in output, we don't need to see that diff
    if [[ $fpath != *shapes* ]];
    then
        if [ -z "$output" ]
        then 
            echo "---------diff for $feed_1 $feed_2 $filename------------"
            diff tmp_1/$filename tmp_2/$filename
        else 
            echo "---------diff for $feed_1 $feed_2 $filename------------" >> $output
            head -n 1 $fpath >> $output
            diff tmp_1/$filename tmp_2/$filename >> $output
            echo -e "\n" >> $output
        fi;
    fi;
done

rm tmp_1/*.txt
rm tmp_2/*.txt
rm -r tmp_1
rm -r tmp_2
