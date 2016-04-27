#!/bin/bash


#$1 = class to run
#$2 = input file
#$3 = input file(optional) 

jarName='mapreduce_project-0.0.1.jar'

if [ "$#" -eq 2 ] ||  [ "$#" -eq 3 ]; then

    if [ "$#" -eq 2 ]; then
	    #output=$2
        echo "inside 2"
    else 
	    #output=$3
        echo "inside 3"
    fi
    strPath="/user/output/$1$(date +%d%m%Y%H%M%S)"
    echo $strPath
    #hadoop fs -mkdir -p $strPath
    
    #hadoop jar mapreduce_project-0.0.1.jar main.BrazilWordCounter /datasets/Users-reduced /output/count
    #hadoop jar $1 $2 $3 $4
    if [ "$#" -eq 2 ]; then
	    #hadoop jar $jarName main.$1 $2 $strPath
        echo "hadoop jar $jarName main.$1 $2 $strPath"
    else 
	    #hadoop jar $jarName main.$1 $2 $3 $strPath
        echo "hadoop jar $jarName main.$1 $2 $3 $strPath"
    fi
else
    echo 'Invalid number of parameters.'
    echo 'Expected: ./executeHadoop.sh <ClassName> <input_file1> <input_file2(optional)>'
fi

