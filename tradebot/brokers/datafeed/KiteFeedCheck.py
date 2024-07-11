from tradebot.brokers.datafeed.DataFeed import DataFeed
from tradebot.brokers.datafeed.exceptions import ListEnumError, SubscibeListError
import os
from typing import List
import polars as pl
from datetime import date
from collections import Counter

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

    def duplication_check(self):
        duplicate_symbols = (
            (
                self.subscribe_list.lazy()
                .select("Symbol")
                .with_columns(
                    pl.col("Symbol").n_unique().over("Symbol").alias("symbol_count")
                )
                .filter(pl.col("symbol_count") > 1)
            )
            .get_columns[0]
            .to_list()
        )
        if len(duplicate_symbols) > 0:
            logger.warning(
                f"""
                The folliwng duplicate symbols were found in {str(Counter(duplicate_symbols))}
                Selecting only the first occurence for each symbol
            """
            )
            self.instrument_list = self.instrument_list.group_by("Symbol").agg(
                pl.col("*").first()
            )
        else:
            logger.info("Subscribe List passed Duplication Check")

    def symbol_tradeable(self):
        not_tradeable_symbols = (
            (
                self.subscribe_list.join(
                    self.instrument_list,
                    left_on="Symbol",
                    right_on="tradingsymbol",
                    how="left",
                )
                .lazy()
                .select("Symbol", "tradingsymbol")
                .filter(pl.col("tradingsymbol").is_null())
            )
            .get_columns[0]
            .to_list()
        ).collect()

        if len(not_tradeable_symbols) > 0:
            logger.warning(
                f"""
                The following symbols are not tradeable : {not_tradeable_symbols}
                and are filtered out from subscribe_list
            """
            )

            self.instrument_list = self.instrument_list.filter(
                ~pl.col("Symbol").is_in(not_tradeable_symbols)
            )
        else:
            logger.info("all Symbols are Tradeabe in Subscribe List")

    def check_subscribe_list(self) -> pl.DataFrame:
        self.fields_check()
        self.length_check()
        self.duplication_check()
        self.symbol_tradeable()
        return self.subscribe_list

    def __call__(self):
        return self.check_subscribe_list()


def create_subscribe_list(instrument_list, subscribe_list):
    return instrument_list.join(
        subscribe_list,
        left_on=["Symbol", "Exchange"],
        right_on=["tradingsymbol", "exchange"],
    ).select("tradingsymbol", "instrument_token", "MODE")
