import polars as pl
from ratelimit import sleep_and_retry, limits
from tradebot.brokers.connection.KiteConnection import KiteConnection
from tradebot.conf.kite.kite_conf import kite_histroical_rate_limit
from tradebot.utils.db_utils import DBConnection, create_connection_string
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional
import concurrent.futures
from tqdm import tqdm
from tradebot.utils.logging_utils import set_logger
import os

logger = set_logger("KiteHistorical")

# Use environment variables or a config file for paths
DB_CRED_PATH = os.environ.get("DB_CRED_PATH", "/path/to/TSDB_credentials.yaml")
KITE_CRED_PATH = os.environ.get("KITE_CRED_PATH", "/path/to/kite_credentials.yaml")
INSTRUMENTS_CSV_PATH = os.environ.get(
    "INSTRUMENTS_CSV_PATH", "/path/to/instruments.csv"
)

db = DBConnection(credentials_yaml_file=DB_CRED_PATH)


def generate_dataframe(
    candles: List[List[str, float, float, float, float, Optional[float]]], symbol: str
) -> pl.DataFrame:
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
            pl.col("timestamp")
            .str.to_datetime("%Y-%m-%dT%H:%M:%S%z")
            .dt.convert_time_zone("Asia/Calcutta")
            .alias("datetime"),
            pl.col("datetime").dt.date().alias("date"),
            pl.col("datetime").dt.time().alias("time"),
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
    if interval not in rate_limit:
        raise ValueError(
            f"Invalid interval. Must be one of: {', '.join(rate_limit.keys())}"
        )

    max_days = rate_limit[interval]
    date_ranges = []
    current_start = from_date

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
    data = kite.historical_data(
        instrument_token=instrument_token,
        from_date=from_date,
        to_date=to_date,
        interval=interval,
        continuous=continuous,
        oi=oi,
    )

    logger.info(f"Data received for {instrument_token} from {from_date} to {to_date}")
    return data


def process_historical_data(
    kite,
    input_list: List[Tuple[str, datetime, str]],
    to_date: str,
    interval: str,
    conn_str: str,
):
    date_obj = datetime.strptime(to_date, "%d-%B-%Y")
    to_date = date_obj.strftime("%Y-%m-%d %H:%M:%S")

    for token, listing_date, symbol in tqdm(input_list):
        date_ranges = get_date_ranges(
            from_date=listing_date,
            to_date=to_date,
            interval=interval,
            rate_limit=kite_histroical_rate_limit,
        )

        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
            futures = {
                executor.submit(
                    get_historical_data, kite, token, start_date, end_date, interval
                ): (start_date, end_date)
                for start_date, end_date in date_ranges
            }

            for future in concurrent.futures.as_completed(futures):
                start_date, end_date = futures[future]
                try:
                    response = future.result()
                    if response["status"] == "success":
                        candles = response["data"]["candles"]
                        if candles:
                            df = generate_dataframe(candles=candles, symbol=symbol)
                            df.write_database(
                                table_name=f"KITE_EQ_{interval}_HISTORICAL_DATA",
                                connection=conn_str,
                                if_table_exists="append",
                            )
                            logger.info(
                                f"Data Inserted for {symbol}: {start_date} to {end_date}"
                            )
                        else:
                            logger.info(
                                f"No data received for {symbol}: {start_date} to {end_date}"
                            )
                    else:
                        logger.error(f"Failed for {symbol}: {start_date} to {end_date}")
                except Exception as e:
                    logger.error(
                        f"Error processing {symbol}: {start_date} to {end_date}: {str(e)}"
                    )


def main():
    listing_df = db.execute_db("""SELECT * FROM "LISTING_DATES";""").select(
        "tradingsymbol", "listing_date"
    )

    listing_df_names = listing_df.get_column("name").to_list()

    instruments_df = (
        pl.read_csv(INSTRUMENTS_CSV_PATH)
        .filter(
            (pl.col("name").is_in(listing_df_names)) & (pl.col("exchange") == "BSE")
        )
        .select("instrument_token", "name")
    )

    input_df = listing_df.join(instruments_df, on="name", how="inner").select(
        "instrument_token", "listing_date", "tradingsymbol", "name"
    )

    input_list = list(
        zip(
            input_df.get_column("instrument_token").to_list(),
            input_df.get_column("listing_date").to_list(),
            input_df.get_column("tradingsymbol").to_list(),
        )
    )

    to_date = "1-August-2024"
    interval = "minute"

    conn_str = create_connection_string(DB_CRED_PATH)
    kite = KiteConnection(KITE_CRED_PATH)()

    process_historical_data(kite, input_list, to_date, interval, conn_str)


if __name__ == "__main__":
    main()
