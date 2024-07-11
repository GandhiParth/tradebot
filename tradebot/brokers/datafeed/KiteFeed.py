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
