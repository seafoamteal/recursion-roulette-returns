from polars.functions import date
import time
import polars as pl
import argparse


def main():
    pl.Config.set_tbl_rows(20)

    parser = argparse.ArgumentParser(
        prog="Flight Finder",
        description="""
        Finds the best flights that currently serve the given route.
        Best = Least average arrival delay.
        Currently = This is
            1. the last route it has flown and
            2. it has done so sometime between the start of the last calendar
            year up to now.
        """,
        epilog="Runs the query using Polars.",
    )

    parser.add_argument("origin")
    parser.add_argument("dest")

    args = parser.parse_args()
    run_query(args.origin, args.dest)


def run_query(origin: str, dest: str):
    start = time.monotonic()
    base = pl.scan_parquet("data/fd.parquet").with_columns(
        route=(
            pl.when(
                ((pl.col("Origin") == origin) & (pl.col("Dest") == dest))
                | ((pl.col("Dest") == origin) & (pl.col("Origin") == dest))
            )
            .then(True)
            .otherwise(False)
        ).alias("route"),
        flight_code=(
            pl.concat_str(pl.col("AirlineCode"), pl.col("FlightNumber"))
        ).alias("flight_code"),
    )

    latest_routes = (
        base.group_by(pl.col("flight_code"))
        .agg(
            pl.col("Date").max().alias("latest_flight"),
            pl.col("route").sort_by(pl.col("Date"), descending=True).first(),
        )
        .filter((pl.col("route")) & (pl.col("latest_flight").ge(date(2025, 1, 1))))
        .select(pl.col("flight_code"))
    )

    lf = (
        base.filter(
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


if __name__ == "__main__":
    main()
