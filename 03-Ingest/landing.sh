#!/usr/bin/env bash

#define download path
download_path="https://github.com/fpineyro/homework-0/blob/master/starwars.csv"
filename=${download_path##*/}
#define landing prefix
landing_prefix="/home/hadoop/landing"

#wget file from path
wget -P $landing_prefix $download_path

#put file into hadoop fs
hadoop fs -put $landing_prefix/$filename /ingest

#delete file from landing
rm $landing_prefix/$filename
