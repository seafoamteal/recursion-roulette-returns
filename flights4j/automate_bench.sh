#!/bin/bash

cat sample_routes.csv | while read LINE
    do
        for pair in $LINE; do
    
            IFS=',' read -r origin dest <<< "$(echo "$pair" | tr -d '"')"

            sbt "run sparkData/parquet_data/fd.parquet $origin $dest"

            echo "$origin -> $dest complete"
        done
    done