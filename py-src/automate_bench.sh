#!/bin/bash

echo "" > 'data/output/best_flights_time_taken'

cat sample_routes.csv | while read LINE
    do
        for pair in $LINE; do
            #read input into origin and dest variables
            IFS=',' read -r origin dest <<< "$(echo "$pair" | tr -d '"')"

            #running the query and measuring time taken and memory usage
            python3.13 -m mprof run --include-children --interval 0.5 best_airport.py $origin $dest

            #appending graph image to output (ASSUMING WE ARE RUNNING THIS SCRIPT FROM py-src FOLDER)
            python3.13 -m mprof plot -o "data/output/${origin}_${dest}_graph.png"

            #removing .dat files to make the folder cleaner
            rm mprofile_*.dat

            echo "$origin -> $dest complete"
        done
    done

    