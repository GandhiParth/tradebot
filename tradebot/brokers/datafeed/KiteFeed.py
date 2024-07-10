from tradebot.brokers.datafeed.DataFeed import DataFeed
import os
from typing import List
import polars as pl


class KiteInstrumentCheck:
    """
    This class is responsible for santiy chcecks for instrument list received
    from Kite End and saving if evrything goes well
    """

    def __init__(self, connection_object: KiteConnect, instrument_list):
        self.kite = connection_object

    def check_instrument_list(self):
        raise NotImplementedError

    def save_instrument_list(self, connection_object: object):
        raise NotImplementedError


class KiteSubscribeCheck:
    """
    Responsible for sanity checks of symbols we will subscribe
    """

    def symbol_tradeable(self):
        raise NotImplementedError

    def duplication_check(self):
        raise NotImplementedError

    def fields_check(self):
        raise NotImplementedError

    def length_check(self):
        raise NotImplementedError


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
