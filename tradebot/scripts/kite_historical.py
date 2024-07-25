import concurrent.futures
import os
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import polars as pl
from ratelimit import limits, sleep_and_retry

from tradebot.brokers.connection.KiteConnection import KiteConnection
from tradebot.conf.kite_conf import kite_histroical_rate_limit


def generate_dataframe(
    candles: List[List[str, float, float, float, float, Optional[float]]], symbol: str
) -> pl.DataFrame:
    """
    Returns the Dataframe for the given candles provided

    parameters:
        candles: List of List containg OHLCV data
    symbol:
        tradingsymbol of the data
    """
    return (
        pl.DataFrame(
            candles,
            schema=[
                ("timestamp", pl.Utf8),
                ("open", pl.Float64),
                ("high", pl.Float64),
                ("low", pl.Float64),
                ("close", pl.Float64),
                ("volume", pl.Int64),
            ],
            orient="row",
        )
        .lazy()
        .with_columns(
            pl.lit("EQ").alias("instrument_type"),
            pl.lit(symbol).alias("trading_symbol"),
            pl.col("timestamp").str.to_date("%Y-%m-%dT%H:%M:%S%z").alias("date"),
            pl.col("timestamp").str.to_time("%Y-%m-%dT%H:%M:%S%z").alias("time"),
            pl.col("timestamp")
            .str.to_datetime("%Y-%m-%dT%H:%M:%S%z")
            .dt.convert_time_zone(time_zone="Asia/Calcutta")
            .alias("datetime"),
        )
        .drop("timestamp")
        .select(
            "instrument_type",
            "trading_symbol",
            "datetime",
            "date",
            "time",
            "open",
            "high",
            "low",
            "close",
            "volume",
        )
        .sort("datetime", descending=True)
    ).collect()


kite = KiteConnection(
    credentials_yaml_file="E:\TradeBot\tradebot\tradebot\credentials\kite_credentials.yaml"
)()

instruments_list = kite.instruments()


def get_date_ranges(
    from_date: datetime, to_date: datetime, interval: str, rate_limit: Dict[str, int]
) -> List[List[datetime, datetime]]:
    """
    Divides start date and end date into appropriate chunks to satify
    the interval rate limit

    datetime: "%Y-%m-%d %H:%M:%S"
    """
    if interval not in rate_limit:
        raise ValueError(
            "Invalid interval. Must be one of: " + ", ".join(rate_limit.keys())
        )

    max_days = rate_limit[interval]

    current_start = from_date
    date_ranges = []

    while current_start < to_date:
        current_end = min(current_start + timedelta(days=max_days), to_date)
        date_ranges.append((current_start, current_end))
        current_start = current_end + timedelta(seconds=1)

    return date_ranges


@limits(calls=3, period=1)
@sleep_and_retry
def get_historical_data(
    instrument_token: str,
    from_date: datetime,
    to_date: datetime,
    interval: str,
    continuous=False,
    oi=False,
):
    """
    datetime: "%Y-%m-%d %H:%M:%S"
    """
    data = kite.historical_data(
        instrument_token=instrument_token,
        from_date=from_date,
        to_date=to_date,
        interval=interval,
        continuous=continuous,
        oi=oi,
    )


symbols_file = "file"

all_data = []
with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
    futures = []
    for start_date, end_date in date_ranges:
        future = executor.submit(
            get_historical_data,
            instruments_token,
            start_date,
            end_date,
            interval,
            continuous,
            oi,
        )
        futures.append(future)
        param_map[future] = (instruments_token, start_date, end_date)

    for future in concurrent.futures.as_completed(futures):
        try:
            response = future.result()
            if response["status"] == "success":
                all_data.extend(response["data"]["candles"])
            else:
                print(f"Faile: {param_map[future]}")

        except Exception as e:
            raise e
