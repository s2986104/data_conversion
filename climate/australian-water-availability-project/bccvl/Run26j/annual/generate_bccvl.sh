#!/bin/bash

# this is the command I used to generate the tiffs grouped by year

# note this is just a hacky script (i.e. it was just a command line that I "logged" as a script for posterity)

export source_dir=$PWD;
output_dir=/home/dave/dispatch/awap_ann
ls *ann*.tif | 
    #grep 1952 |  #Only do 1952 - remove this line to do all
    sed 's/.*_//; s/\..*//' | 
    sort -u  |
    while read date; 
        do 
            dir=$( mktemp -d /tmp/tmp.XXXX); 
            pushd $dir; 
            name_dir=awap_ann_$date; 
            mkdir $name_dir; 
            pushd $name_dir; 
            mkdir bccvl; 
            mkdir data; 
            year_only=$(echo $date | sed -n 's/\(^....\).*/\1/p'); 
            sed "s/START/$year_only/; s/END/$year_only/" $source_dir/metadata_template.json > bccvl/metadata.json; 
            cmd="cp $source_dir/*ann*$date*tif data"; 
            echo $cmd; 
            eval $cmd; 
            popd; 
            zip -r $output_dir/awap_ann_$date.zip .; 
            popd; 
            rm -rf $dir; 
        done
   
