from abc import ABC, abstractmethod
from typing import Any, Dict


class BrokerConnection(ABC):
    """
    Base Class to handle connection to various brokers.
    """

    def __init__(self, credentials_yaml_file: str) -> None:
        """
        credentials_file: YAML file of credentials
        """
        self.credentials = self._load_credentials(credentials_yaml_file)

    def _load_credentials(self, yaml_file: str) -> Dict[str, Any]:
        """
        Loads the YAML file and returns it
        """
        with open(yaml_file, "r") as file:
            return yaml.safe_load(file)

    @abstractmethod
    def auto_login(self) -> object:
        """
        automatically logins to the broker and returns the connection object
        """

    def __call__(self) -> object:
        return self.auto_login()
