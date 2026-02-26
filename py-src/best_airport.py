from polars.functions import date
import time
import polars as pl

pl.Config.set_tbl_rows(20)

origin = "LAX"
dest = "DFW"
route = "-".join(sorted([origin, dest]))

start = time.monotonic()
latest_routes = (
    pl.scan_parquet("data/fd.parquet")
    .with_columns(
        route=(
            pl.when(pl.col("Origin") < pl.col("Dest"))
            .then(pl.concat_str(pl.col("Origin"), pl.col("Dest"), separator="-"))
            .otherwise(pl.concat_str(pl.col("Dest"), pl.col("Origin"), separator="-"))
        ).alias("route"),
        flight_code=(
            pl.concat_str(pl.col("AirlineCode"), pl.col("FlightNumber"))
        ).alias("flight_code"),
    )
    .group_by(pl.col("flight_code"))
    .agg(
        pl.col("Date").max().alias("latest_flight"),
        pl.col("route").sort_by(pl.col("Date"), descending=True).first(),
    )
    .filter((pl.col("route") == route) & (pl.col("latest_flight").ge(date(2025, 1, 1))))
    .select(pl.col("flight_code"))
)

lf = (
    pl.scan_parquet("data/fd.parquet")
    .with_columns(
        flight_code=(
            pl.concat_str(pl.col("AirlineCode"), pl.col("FlightNumber"))
        ).alias("flight_code"),
    )
    .filter(
        (pl.col("Cancelled") != 1)
        & (pl.col("Diverted") != 1)
        & (pl.col("ArrDelay").is_not_null())
        & (pl.col("Origin") == origin)
        & (pl.col("Dest") == dest)
    )
    .join(latest_routes, on="flight_code", how="semi")
    .group_by(pl.col("flight_code"))
    .agg(
        pl.col("ArrDelay").mean().alias("avg_delay"),
        pl.len().alias("number_of_flights"),
        pl.col("Date").max().alias("latest_flight"),
    )
    .filter(
        (pl.col("latest_flight").ge(date(2025, 1, 1)))
        & (pl.col("number_of_flights").ge(10))
    )
    .sort(pl.col("avg_delay"))
    .limit(20)
    .collect()
)

end = time.monotonic()

print(lf)
print(end - start)
