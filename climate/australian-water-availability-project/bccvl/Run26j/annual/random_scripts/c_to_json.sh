#!/bin/bash


function echo_entry()
{
    echo "{"
    echo "        \"title\" : \"$1\","
    echo "        \"file_pattern\" : \"ann_$2_\","
    echo "        \"unit\" : \"$3\""
    echo "},"
    echo "{"
    echo "        \"title\" : \"$1 (Percentile Rank Data)\","
    echo "        \"file_pattern\" : \"pcr_ann_$2_\","
    echo "        \"unit\" : \"\""
    echo "},"

}

name=
tag=
unit=

while read line
do
	if [ -z "$name" ]; then name=$line;
	elif [ -z "$tag" ]; then tag=$line;
	else
		echo_entry "$name" "$tag" "$line"
		name=
		tag=
	fi
done < $1
