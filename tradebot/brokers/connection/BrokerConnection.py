from abc import ABC, abstractmethod

from tradebot.utils.utils import read_yaml_file


class BrokerConnection(ABC):
    """
    Base Class to handle connection to various brokers.
    """

    def __init__(self, credentials_yaml_file: str) -> None:
        """
        credentials_file: YAML file of credentials
        """
        self.credentials = read_yaml_file(credentials_yaml_file)

    @abstractmethod
    def auto_login(self) -> object:
        """
        automatically logins to the broker and returns the connection object
        """

    def __call__(self) -> object:
        return self.auto_login()
