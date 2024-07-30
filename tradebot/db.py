import logging

import polars as pl
import psycopg2
from psycopg2 import DatabaseError, InterfaceError, OperationalError, pool

from tradebot.brokers.connection.BrokerConnection import BrokerConnection
from tradebot.brokers.connection.exceptions import BrokerConnectionError

logger = logging.getLogger(__name__)


class DBConnection(BrokerConnection):
    """
    Handles connection to different PostgreSQL databases with a connection pool.

    Attributes:
        dbname (str): The name of the database to connect to.
        connection_pool (psycopg2.pool.SimpleConnectionPool): The connection pool for database connections.
    """

    def __init__(
        self,
        credentials_yaml_file: str,
        dbname: str,
        minconn: int = 1,
        maxconn: int = 10,
    ) -> None:
        """
        Initialize the DBConnection instance with a connection pool.

        Args:
            credentials_yaml_file (str): Path to the YAML file containing database credentials.
            dbname (str): The name of the database to connect to.
            minconn (int): Minimum number of connections in the pool. Defaults to 1.
            maxconn (int): Maximum number of connections in the pool. Defaults to 10.
        """
        super().__init__(credentials_yaml_file)
        self.dbname = dbname
        self.connection_pool = self._create_pool(minconn, maxconn)

    def _create_pool(self, minconn: int, maxconn: int) -> pool.SimpleConnectionPool:
        """
        Create a connection pool.

        Args:
            minconn (int): Minimum number of connections in the pool.
            maxconn (int): Maximum number of connections in the pool.

        Returns:
            pool.SimpleConnectionPool: The connection pool.
        """
        try:
            return pool.SimpleConnectionPool(
                minconn,
                maxconn,
                dbname=self.dbname,
                user=self.credentials["user"],
                password=self.credentials["password"],
                host=self.credentials["host"],
                port=self.credentials["port"],
            )
        except (OperationalError, InterfaceError, DatabaseError) as e:
            logger.error(
                f"Unable to create connection pool for database '{self.dbname}'"
            )
            logger.error(e)
            raise BrokerConnectionError(e) from e

    def auto_login(self) -> object:
        """
        Get a connection from the connection pool.

        Returns:
            conn: A connection object to the database.

        Raises:
            BrokerConnectionError: If unable to get a connection from the pool.
        """
        try:
            conn = self.connection_pool.getconn()
            if conn:
                logger.info(f"Connected to database '{self.dbname}' successfully!")
                return conn
            else:
                raise BrokerConnectionError("Unable to get a connection from the pool")
        except (OperationalError, InterfaceError, DatabaseError) as e:
            logger.error(
                f"Unable to get a connection from the pool for database '{self.dbname}'"
            )
            logger.error(e)
            raise BrokerConnectionError(e) from e

    def execute_db(self, query: str) -> pl.DataFrame:
        """
        Execute a query on the database and return the results as a Polars DataFrame.

        Args:
            query (str): The SQL query to execute.

        Returns:
            pl.DataFrame: The query results as a Polars DataFrame.
        """
        conn = None
        cursor = None
        try:
            conn = self.auto_login()
            cursor = conn.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]
            df = pl.DataFrame(results, schema=column_names)
            logger.info("Query executed and results fetched successfully!")
            return df
        except (OperationalError, InterfaceError, DatabaseError) as e:
            logger.error("Error executing query")
            logger.error(e)
            raise BrokerConnectionError(e) from e
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.connection_pool.putconn(conn)
                logger.info("Database connection returned to pool.")

    def write_to_db(self, table: str, data: pl.DataFrame, mode: str = "append") -> None:
        """
        Write data to the database table with specified mode.

        Args:
            table (str): The name of the table to write data to.
            data (pl.DataFrame): The data to write as a Polars DataFrame.
            mode (str): The mode of writing, either 'append' or 'replace'. Defaults to 'append'.
        """
        if mode not in ["append", "replace"]:
            raise ValueError("Mode should be either 'append' or 'replace'")

        conn = None
        cursor = None
        try:
            conn = self.auto_login()
            cursor = conn.cursor()

            if mode == "replace":
                cursor.execute(f"TRUNCATE TABLE {table}")

            insert_query = f"INSERT INTO {table} VALUES %s"
            execute_values(cursor, insert_query, data.to_numpy().tolist())
            conn.commit()
            logger.info("Data written to database successfully!")
        except (OperationalError, InterfaceError, DatabaseError) as e:
            logger.error("Error writing data to database")
            logger.error(e)
            raise BrokerConnectionError(e) from e
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.connection_pool.putconn(conn)
                logger.info("Database connection returned to pool.")
