from abc import ABC, abstractmethod
from tradebot.brokers.connection.credentials import Credentials

import logging

logger = logging.getLogger(__name__)


class BrokerConnection(ABC):
    """
    Base Class to handle connection to various brokers.
    """

    def __init__(self, credentials: Credentials):
        self.credentials = credentials

    @abstractmethod
    def auto_login(self) -> object:
        """
        automatically logins to the broker
        """
        pass

    def __call__(self) -> object:
        return self.auto_login()
