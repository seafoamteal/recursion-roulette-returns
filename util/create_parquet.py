import duckdb

duckdb.sql("""
    COPY (
        SELECT
            FlightDate as Date,
            Reporting_Airline as AirlineCode,
            Flight_Number_Reporting_Airline as FlightNumber,
            OriginAirportID,
            Origin,
            OriginCityMarketID,
            DestAirportID,
            Dest,
            DestCityMarketID,
            CRSDepTime,
            DepTime,
            DepDelay,
            DepDelayMinutes,
            DepDel15,
            DepartureDelayGroups,
            CRSArrTime,
            ArrTime,
            ArrDelay,
            ArrDelayMinutes,
            ArrDel15,
            ArrivalDelayGroups,
            Cancelled,
            Diverted,
            CRSElapsedTime,
            ActualElapsedTime,
            AirTime,
            CarrierDelay
            WeatherDelay,
            NASDelay,
            SecurityDelay,
            LateAircraftDelay
        FROM read_csv("raw_data/*.csv", union_by_name=true)
    )
    TO "flight_data.parquet" (FORMAT PARQUET);
""")
