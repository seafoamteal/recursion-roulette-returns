#!/bin/bash

echo "" > 'data/output/best_flights_time_taken'

cat sample_routes.csv | while read LINE
    do
        for pair in $LINE; do
            IFS=',' read -r origin dest <<< "$(echo "$pair" | tr -d '"')"
            python best_airport.py $origin $dest
            echo "$origin -> $dest complete"
        done
    done

    