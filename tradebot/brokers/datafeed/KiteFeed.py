from tradebot.brokers.datafeed.DataFeed import DataFeed
from tradebot.brokers.datafeed.exceptions import ListEnumError, SubscibeListError
import os
from typing import List
import polars as pl
from datetime import date

import logging

logger = logging.getLogger(__name__)


class KiteInstrumentCheck:
    """
    This class is responsible for santiy chcecks for instrument list received
    from Kite End and saving if evrything goes well
    """

    def __init__(self, instrument_list: pl.DataFrame):
        """
        instrument_list:
            isntrument list received from ktie in polars dataframe
        """
        self.instrument_list = instrument_list

    def check_instrument_list(self):
        main_df = (
            self.instrument_list.lazy()
            .select(pl.exclude("last_price"))
            .with_columns(
                pl.col("instrument_type").cast(pl.Enum(["EQ", "PE", "CE", "FUT"])),
                pl.col("segment").cast(
                    pl.Enum(
                        [
                            "NCO-OPT",
                            "BFO-OPT",
                            "NFO-FUT",
                            "BSE",
                            "BCD-FUT",
                            "BCD-OPT",
                            "MCX-FUT",
                            "MCX-OPT",
                            "CDS-OPT",
                            "NFO-OPT",
                            "NCO",
                            "NCO-FUT",
                            "NSE",
                            "BFO-FUT",
                            "INDICES",
                            "CDS-FUT",
                        ]
                    )
                ),
                pl.col("exchange").cast(
                    pl.Enum(
                        [
                            "NFO",
                            "CDS",
                            "NCO",
                            "MCX",
                            "NSE",
                            "BCD",
                            "NSEIX",
                            "BFO",
                            "BSE",
                        ]
                    )
                ),
                pl.col("instrument_token")
                .n_unique()
                .over("exchange", "tradingsymbol")
                .alias("unique_ins_token_count"),
            )
        )

        try:
            self.instrument_list = main_df.collect()
            logger.info("Kite Instrument List Enum Check Successful !!")
        except pl.exceptions.InvalidOperationError as e:
            raise ListEnumError(list_type="Instrument List", value=e)
        except Exception as e:
            raise e

        invalid_df = self.get_invalid_instruments()

        if invalid_df.shape[0] != 0:
            logger.warning(
                f"""
                The following symbols {invalid_df.select("tradeingsymbol").get_column[0].to_list()}
                can not be traded on  {invalid_df.select("exchange").get_column[0].to_list()}
                due to multiple instrument tokens and have been removed from instrument list.
            """
            )
        self.get_valid_instruments()

    def get_valid_instruments(self):
        """
        Filter the instrument list for tradeable instrument token
        """
        self.instrument_list = (
            self.instrument_list.lazy()
            .filter(pl.col("unique_ins_token_count") == 1)
            .select(
                pl.lit(date.today()).alias("date"),
                pl.exclude("unique_ins_token_count", "date"),
            )
        ).collect()

    def get_invalid_instruments(self) -> pl.DataFrame:
        """
        Returns Dataframe of Symbols that will be invalid to trade
        due to duplicate instrument token
        """
        invalid_df = (
            self.instrument_list.lazy()
            .filter(pl.col("unique_ins_token_count") != 1)
            .select("exchange", "tradingsymbol", "instrument_token")
        ).collect()

        return invalid_df

    def save_instrument_list(self, connection_object: object):
        self.instrument_list.write_database(
            table_name="instrument_list_kite",
            connection=connection_object,
            if_table_exists="append",
        )
        logger.info(f"Kite Instrument List Succesfully written to DataBase")

    def __call__(self, connection_object):
        self.check_instrument_list()
        self.save_instrument_list(connection_object=connection_object)
        return self.instrument_list


class KiteSubscribeCheck:
    """
    Responsible for sanity checks of symbols we will subscribe
    """

    def __init__(self, file_name: str, instrument_list: pl.DataFrame):
        self.file_name = file_name
        self.instrument_list = instrument_list

    def symbol_tradeable(self):
        raise NotImplementedError

    def duplication_check(self):
        raise NotImplementedError

    def fields_check(self):
        try:
            self.subscribe_list = (
                pl.scan_csv(self.file_name).with_columns(
                    pl.col("Exchange").cast(
                        pl.Enum(
                            [
                                "NFO",
                                "CDS",
                                "NCO",
                                "MCX",
                                "NSE",
                                "BCD",
                                "NSEIX",
                                "BFO",
                                "BSE",
                            ]
                        )
                    ),
                    pl.col("MODE").cast(pl.Enum(["full", "ltp", "quote"])),
                )
            ).collect()
        except pl.exceptions.InvalidOperationError as e:
            raise ListEnumError(list_type="Subscribe List", value=e)
        except Exception as e:
            raise e

    def length_check(self):
        list_len = self.subscribe_list.shape[0]
        if list_len > 3000:
            raise SubscibeListError(
                f"""Max 3000 tokens can be subscribed but {list_len} tokens are gievn"""
            )
        else:
            logger.info("Subscribe List Length Check Successful")


def create_subscribe_list(instrument_list, subscribe_list):
    raise NotImplementedError


class KiteFeed(DataFeed):
    """
    Gets tick data from Kite Broker
    """

    def __init__(self, connection_object: KiteConnect, subscribe_list: dict):
        self.kite = connection_object
        self.subscribe_list

    def on_ticks(ws, ticks):
        raise NotImplementedError

    def on_connect(ws, response):
        raise NotImplementedError

    def on_close(ws, code, reason):
        raise NotImplementedError

    def on_error(ws, code, reason):
        raise NotImplementedError

    def on_order_update(ws, data):
        raise NotImplementedError

    def on_noreconnect(ws):
        raise NotImplementedError

    def on_reconnect(ws, attempts_count):
        raise NotImplementedError
