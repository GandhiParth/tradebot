import logging

import psycopg2

from tradebot.brokers.connection.BrokerConnection import BrokerConnection
from tradebot.brokers.connection.exceptions import BrokerConnectionError

logger = logging.getLogger(__name__)


class DBConnection(BrokerConnection):
    """
    Handles connection to different PostgreSQL DataBase
    """

    def __init__(self, credentials_yaml_file: str) -> None:
        super().__init__(credentials_yaml_file)
        self.dbname = dbname

    def auto_login(self) -> object:
        try:
            conn = psycopg2.connect(
                dbname=self.credentials["dbname"],
                user=self.credentials["user"],
                password=self.credentials["password"],
                host=self.credentials["host"],
                port=self.credentials["port"],
            )
            logger.info(
                f"""Connected to database `{self.credentials["dbname"]}` successfully!"""
            )
            return conn
        except Exception as e:
            logger.error(
                f"""Unable to connect to database `{self.credentials["dbname"]}`"""
            )
            logger.error(e)
            raise BrokerConnectionError(e) from e
