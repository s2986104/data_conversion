#!/bin/bash

scipt_dir=$(dirname $0)

remote_directory="ftp://ftp.bom.gov.au/anon/home/geofabric"

tmp_curl_file=$(mktemp .tmp_curl.XXXX)
curl $remote_directory/ 2>/dev/null 1>$tmp_curl_file # grab remote file name and file size out of this
source_files=($(sed 's/.* //' $tmp_curl_file))
source_file_size=($(cat $tmp_curl_file | awk '{print $5}'))
total_files=${#source_files[@]}
for (( j=0; j<$total_files; j++ ));
do
	f=${source_files[$j]}
	remote_file=$remote_directory/$f
	local_file=$f
	# todo should probably be md5sum based, or use some local metadata file
	if [ -s $local_file ]; then
		remote_size=${source_file_size[$j]}
		local_size=$(ls -l $local_file | awk '{print $5}')
		if [ $remote_size -eq $local_size ]; then
			echo Already exists: $local_file;
			continue;
		else 
			echo Size mismatch $local_file=$local_size $remote_file=$remote_size
		fi
	fi
	tmp_file=$(mktemp .tmp_ftp_retrieve.XXXX)
	cmd="curl $remote_file -o $tmp_file"
	eval $cmd || (echo Failed: $cmd && exit 1)
	echo $cmd succeeded	
	cmd="mv $tmp_file $local_file"
	eval $cmd || (echo Failed: $cmd && exit 1)
	echo $cmd succeeded	
done
# todo trap perhaps
rm -f $tmp_curl_file

