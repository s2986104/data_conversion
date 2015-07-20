#!/bin/bash


scipt_dir=$(dirname $0)

#tmp_dir=$(mktemp -d /tmp/tmp.XXXXX)
#
#pushd $tmp_dir 2>/dev/null

remote_root="ftp://ftp.eoc.csiro.au/pub/awap/Australia_historical/Run26j"

source_dirs=($(curl $remote_root/ 2>/dev/null| sed 's/.* //'))

total_dirs=${#source_dirs[@]}
for (( i=0; i<$total_dirs; i++ ));
do
	remote_directory=$remote_root/${source_dirs[$i]}/        
	source_files=($(curl $remote_directory 2>/dev/null| sed 's/.* //'))
	total_files=${#source_files[@]}
	for (( j=0; j<$total_files; j++ ));
	do
		f=${source_files[$j]}

                # not sure if this will byte me
		if [ ! ${f##*.} = "zip" ]; then continue; fi

		remote_file=$remote_root/${source_dirs[$i]}/$f
		cmd="curl $remote_file -o $f"
		eval $cmd || (echo Failed: $cmd && exit 1)
		echo $cmd succeeded	

		unzip_log="unzip_log"
		cmd="unzip $f | grep -v Archive > $unzip_log"
		echo $cmd
		eval $cmd || (echo Failed: $cmd && exit 1)
		
		rm -f $f

		files_to_convert=($(grep ann $unzip_log | awk '{print $2}' | grep \.flt))
		total_conversions=${#files_to_convert[@]}
		for (( k = 0; k < $total_conversions; k++ ))
		do
			echo ${files_to_convert[$k]}
			local_file=${files_to_convert[$k]}
			local_file=${local_file##*/}
			local_file=$(echo $local_file | sed 's/\.flt$/.tif/') # do this better some day
			cmd="gdal_translate ${files_to_convert[$k]} $local_file -a_srs EPSG:4326"
			echo $cmd
			eval $cmd || (echo Failed: $cmd && exit 1)
		done
		cmd=$(cat $unzip_log | xargs rm -f)
		eval $cmd || (echo Failed: $cmd && exit 1)
		rm -f $unzip_log
		
	done
done

#popd 2>/dev/null

#rm -rf $tmp_dir
