import polars as pl
from ratelimit import sleep_and_retry, limits

from tradebot.brokers.connection.KiteConnection import KiteConnection
from tradebot.conf.kite.kite_conf import kite_histroical_rate_limit
from tradebot.utils.db_utils import DBConnection, create_connection_string

from datetime import datetime, timedelta

from typing import List, Dict, Tuple, Optional

import concurrent.futures
from tqdm import tqdm
from datetime import datetime
from tradebot.utils.logging_utils import set_logger

logger = set_logger("KiteHistorical")

db_cred_path = (
    "/home/parthgandhi/Projects/tradebot/tradebot/credentials/TSDB_credentials.yaml"
)

db = DBConnection(credentials_yaml_file=db_cred_path)

kite_cred_path = (
    "/home/parthgandhi/Projects/tradebot/tradebot/credentials/kite_credentials.yaml"
)

instruments = pl.read_csv("home/parthgandhi/Downloads/instruments.csv")


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
    ).collect()


def get_date_ranges(
    from_date: datetime, to_date: datetime, interval: str, rate_limit: Dict[str, int]
) -> List[Tuple[datetime, datetime]]:
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
    kite,
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

    logger.info(
        f"""
Data received for {instrument_token} from {start_date} to {end_date}
"""
    )
    return data


if __name__ == "__main__":

    listing_df = db.execute_db("""select * from "LISTING_DATES";""").select(
        "tradingsymbol", "listing_date"
    )

    listing_df_names = listing_df.get_column("name").to_list()

    instruments_df = (
        pl.read_csv("home/parthgandhi/Downloads/instruments.csv")
        .filter(
            (pl.col("name").is_in(listing_df_names)) & (pl.col("exchange") == "BSE")
        )
        .select("instrument_token", "name")
    )

    input_df = listing_df.join(instruments_df, on="name", how="inner").select(
        "instrument_token", "listing_date", "tradingsymbol", "name"
    )

    token, listing_date, symbol = (
        input_df.get_column("instrument_token").to_list(),
        input_df.get_column("listing_date").to_list(),
        input_df.get_column("tradingsymbol").to_list(),
    )

    input_list = list(zip(token, listing_date, symbol))

    to_date = "1-august-2024"
    date_obj = datetime.strptime(to_date, "%d-%B-%Y")
    to_date = date_obj.strftime("%Y-%m-%d %H:%M:%S")

    interval = "minute"

    conn_str = create_connection_string(db_cred_path)

    kite = KiteConnection(kite_cred_path)()

    for token, listing_date, symbol in tqdm(input_list):
        date_ranges = get_date_ranges(
            from_date=listing_date,
            to_date=to_date,
            interval=interval,
            rate_limit=kite_histroical_rate_limit,
        )

        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
            futures = []
            param_map = {}
            for start_date, end_date in date_ranges:
                future = executor.submit(
                    get_historical_data, token, start_date, end_date, interval
                )
                futures.append(future)
                param_map[future] = (start_date, end_date)

            for future in concurrent.futures.as_completed(futures):
                try:
                    response = future.result()
                    if response["status"] == "success":
                        candles = response["data"]["candles"]
                        if len(candles) > 0:
                            df = generate_dataframe(candles=candles, symbol=symbol)
                            df.write_database(
                                table_name=f"KITE_EQ_{interval}_HISTORICAL_DATA",
                                connection=conn_str,
                                if_table_exists="append",
                            )
                            logger.info(
                                f"Data Inserted for {symbol} : {param_map[future]}"
                            )
                        else:
                            logger.info(
                                f"No data received for {symbol} : {param_map[future]}"
                            )
                    else:
                        logger.error(f"Failed for {symbol}: {param_map[future]}")

                except Exception as e:
                    logger.error(e)
                    raise e
