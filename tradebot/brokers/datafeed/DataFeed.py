from abc import ABC, abstractmethod


class DataFeed(ABC):
    """
    Base Class for getting DataFeed from Broker
    """

    def __init__(self, connection_object: object) -> None:
        """
        connection_object:
            auto_login object returned from BrokerConnection
        """

    @abstractmethod
    def stream_data(self):
        """
        gets the tick data for the subscribed symbols
        """
