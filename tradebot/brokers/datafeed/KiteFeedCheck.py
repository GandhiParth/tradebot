import logging
from collections import Counter
from datetime import date

import polars as pl

from tradebot.brokers.datafeed.exceptions import ListEnumError, SubscibeListError

logger = logging.getLogger(__name__)


class KiteInstrumentCheck:
    """
    This class is responsible for santiy chcecks for instrument list received
    from Kite End and saving if evrything goes well

    methods:

    check_instrument_list:
        check the given instrument list and returns it

    ():
        runs check_instrument_list
    """

    def __init__(self, instrument_list: pl.DataFrame) -> None:
        """
        instrument_list:
            instrument list received from ktie in polars dataframe
        """
        self.instrument_list = instrument_list

    def check_instrument_list(self) -> pl.DataFrame:
        """
        checks instrument list wtih appropriate checks
        """
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
        except Exception as e:
            raise ListEnumError(list_type="Instrument List", value=e) from e

        invalid_df = self._get_invalid_instruments()

        if invalid_df.shape[0] != 0:
            logger.warning(
                f"""
                The following symbols {invalid_df.select("tradeingsymbol").get_column[0].to_list()}
                can not be traded on  {invalid_df.select("exchange").get_column[0].to_list()}
                due to multiple instrument tokens and have been removed from instrument list.
            """
            )
        self._get_valid_instruments()

        return self.instrument_list

    def _get_valid_instruments(self) -> None:
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

    def _get_invalid_instruments(self) -> pl.DataFrame:
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

    def __call__(self) -> pl.DataFrame:
        return self.check_instrument_list()


class KiteSubscribeCheck:
    """
    Responsible for sanity checks of symbols we will subscribe

    methods:

    check_subscibe_list:
        runs check on the subscribe list and returns correct version of it

    ():
        runs check_subscribe_list
    """

    def __init__(self, file_name: str, instrument_list: pl.DataFrame) -> None:
        """
        file_name:
            csv file for the subscribe list
        """
        self.file_name = file_name
        self.instrument_list = instrument_list

    def _fields_check(self) -> None:
        """
        checks for the exchnage and mode column in subscribe list
        """
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
        except Exception as e:
            raise ListEnumError(list_type="Subscribe List", value=e) from e

    def _length_check(self) -> None:
        """
        checks if tokens to subscribe adhere to the limit
        """
        list_len = self.subscribe_list.shape[0]
        if list_len > 3000:
            raise SubscibeListError(
                f"""Max 3000 tokens can be subscribed but {list_len} tokens are gievn"""
            )
        else:
            logger.info("Subscribe List Length Check Successful")

    def _duplication_check(self) -> None:
        """
        checks if duplicate symbols were passed and takes the first occurence
        if they have been passed
        """
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

    def _symbol_tradeable_check(self) -> None:
        """
        checks if the symbols are tradeable in the list and if not
        filters them out of the list
        """
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
        """
        performs various check on the subscribe list and returns
        correct version of subscribe list
        """
        self._fields_check()
        self._duplication_check()
        self._length_check()
        self._symbol_tradeable_check()
        return self.subscribe_list

    def __call__(self) -> pl.DataFrame:
        return self.check_subscribe_list()


def create_kws_list(
    instrument_list: pl.DataFrame, subscribe_list: pl.DataFrame
) -> pl.DataFrame:
    """
    Returns dataframe merged on isntrument list & subscribe list to be used for Kite Web Socket
    """
    return +instrument_list.join(
        subscribe_list,
        left_on=["Symbol", "Exchange"],
        right_on=["tradingsymbol", "exchange"],
    ).select("tradingsymbol", "instrument_token", "MODE")
