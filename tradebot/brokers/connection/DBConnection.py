from tradebot.brokers.connection.BrokerConnection import BrokerConnection
from tradebot.brokers.connection.credentials import DBCredentials
import psycopg2

import logging

logger = logging.getLogger(__name__)


class DBConnection(BrokerConnection):
    """
    Handles Connection to Different DataBase
    """

    def __init__(self, credentials: DBCredentials) -> None:
        supre.__init__(credentials)

    def auto_login(self):
        try:
            conn = psycopg2.connect(
                dbname=self.credentials.dbname.value,
                user=self.credentials.user.value,
                password=self.credentials.password.value,
                host=self.credentials.host.value,
                port=self.credentials.port.value,
            )
            logger.info(f"Connected to database '{dbname}' successfully!")
            return conn
        except psycopg2.Error as e:
            logger.info(f"Unable to connect to database '{dbname}'")
            logger.error(e)
            return None
