from polars.lazyframe.in_process import InProcessQuery
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

    # A given flight number is not guaranteed to have flown only one route --
    # airlines often reuse flight numbers between different routes. This means
    # we can't simply check whether a flight has ever flown the route we want.
    # We need to check if it's the _last route it has served_. Of course, there's
    # no way to know for sure without looking up current filght schedules, but
    # this is a good enough heuristic.
    base = pl.scan_parquet("data/fd.parquet").with_columns(
        # A route is bidirectional. It does not suffice to check if the last trip
        # a flight has taken is ORIG => DEST because it might also have been
        # DEST => ORIG, so we check for both.
        route=(
            pl.when(
                ((pl.col("Origin") == origin) & (pl.col("Dest") == dest))
                | ((pl.col("Dest") == origin) & (pl.col("Origin") == dest))
            )
            .then(True)
            .otherwise(False)
        ).alias("route"),
        # A flight is identified to passengers through a combination of the
        # airline's IATA code and its flight number, e.g. EI46.
        flight_code=(
            pl.concat_str(pl.col("AirlineCode"), pl.col("FlightNumber"))
        ).alias("flight_code"),
    )

    active_routes = (
        base.group_by(pl.col("flight_code"))
        .agg(
            pl.col("Date").max().alias("latest_flight"),
            pl.col("route").sort_by(pl.col("Date"), descending=True).first(),
        )
        # A given flight number might have serviced the route at some point in
        # the past before being discontinued, so we also check that it's latest
        # flight was in the last calendar year. N.B. Data is till Nov 2025.
        .filter((pl.col("route")) & (pl.col("latest_flight").ge(date(2025, 1, 1))))
    )

    lf = (
        # For simplicity's sake, we don't want to deal with cancelled and
        # diverted flights, so we ignore both.
        base.filter(
            (pl.col("Cancelled") != 1)
            & (pl.col("Diverted") != 1)
            & (pl.col("ArrDelay").is_not_null())
            & (pl.col("Origin") == origin)
            & (pl.col("Dest") == dest)
        )
        # Make sure we only consider the active routes we have already
        # found
        .join(active_routes, on="flight_code", how="semi")
        .group_by(pl.col("flight_code"))
        .agg(
            pl.col("ArrDelay").mean().alias("avg_delay"),
            pl.len().alias("number_of_flights"),
            # We know this flight number has flown this route recently,
            # but sometimes flights only serve one half of a bidirectional route.
            # e.g. DL827 used to fly both LAX => ATL and ATL => LAX, but now
            # it only does ATL => LAX. We don't want to include it in our final
            # results, so we also make sure that it has recently flown the exact
            # route + direction we are looking for.
            pl.col("Date").max().alias("latest_flight"),
        )
        .filter(
            (pl.col("latest_flight").ge(date(2025, 1, 1)))
            # The dataset also includes private jets and other non-commercial
            # planes. We don't want them in our results, so a good heuristic
            # to filter them is by filtering out flights that have flown this
            # route quite infrequently.
            & (pl.col("number_of_flights").ge(10))
        )
        .sort(pl.col("avg_delay"))
        .limit(20)
        .collect()
    )

    if isinstance(lf, InProcessQuery):
        lf = lf.fetch_blocking()

    end = time.monotonic()

    print(lf)
    print(f"Time taken: {end - start}s")


if __name__ == "__main__":
    main()
