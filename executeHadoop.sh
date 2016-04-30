#!/bin/bash


#$1 = class to run
#$2 = input file 1
#$3 = input file 2(optional) 
#$4 = join type (optional) 

jarName='./target/mapreduce_project-0.0.1.jar'

if [ "$#" -eq 2 ] ||  [ "$#" -eq 4 ]; then

    strPath="/user/output/$1$(date +%d%m%Y%H%M%S)"

    if [ "$#" -eq 2 ]; then
        echo "hadoop jar $jarName main.$1 $2 $strPath"
	    hadoop jar $jarName main.$1 $2 $strPath        
    else 
        echo "hadoop jar $jarName main.$1 $2 $3 $4 $strPath"
	    hadoop jar $jarName main.$1 $2 $3 $4 $strPath
        
    fi
    echo "File Output Path: $strPath"
else
    echo 'Invalid number of parameters.'
    echo 'Expected: ./executeHadoop.sh <ClassName> <input_file1> <input_file2(optional)>'
fi

