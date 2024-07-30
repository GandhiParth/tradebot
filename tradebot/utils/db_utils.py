import logging
from typing import List, Dict, Any, Optional
import polars as pl
import psycopg2
from psycopg2 import sql, pool
from psycopg2.extras import execute_values
from psycopg2.extensions import connection
from tradebot.brokers.connection.exceptions import BrokerConnectionError

from tradebot.utils.utils import read_yaml_file

logger = logging.getLogger(__name__)


def create_connection_string(credentials_yam_file: str) -> str:
    """
    Reads credentials from a yam file and returns a connection string
    """

    config = read_yaml_file(credentials_yam_file)
    user = config["user"]
    host = config["host"]
    port = config["port"]
    dbname = config["dbname"]
    password = config["password"]

    connection_string = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"

    return connection_string


class DBConnection:
    """
    provides various method to database
    """

    def __init__(
        self, credentials_yaml_file: str, minconn: int = 1, maxconn: int = 20
    ) -> None:
        """
        Initialize the DBConnection instance with a threaded connection pool.
        """
        self.credentials = read_yaml_file(credentials_yaml_file)
        self.connection_pool = self._create_pool(minconn, maxconn)
        self.dbname = self.credentials["dbname"]

    def _create_pool(self, minconn: int, maxconn: int) -> pool.ThreadedConnectionPool:
        """
        Creates a Threaded Connection Pool
        """

        try:
            return pool.ThreadedConnectionPool(
                minconn=minconn,
                maxconn=maxconn,
                dbname=self.credentials["dbname"],
                user=self.credentials["user"],
                password=self.credentials["password"],
                host=self.credentials["host"],
                port=self.credentials["port"],
            )
        except Exception as e:
            logger.error(
                f"""Unable to create connection pool for database '{self.credentials['dbname']}'"""
            )
            logger.error(e)
            raise BrokerConnectionError(e) from e

    def auto_login(self) -> connection:
        """
        Get a connection object from the connection pool
        """
        try:
            conn = self.connection_pool.getconn()
            return conn
        except Exception as e:
            logger.error(
                f"""Unable to get a connection from pool for the database {self.credentials["dbname"]}"""
            )
            logger.error(e)
            raise BrokerConnectionError from e

    def release_connection(self, conn: connection) -> None:
        """
        Release a connection back to pool
        """

        self.connection_pool.putconn(conn)
        logger.info("Database Connection Returned to Pool")

    def execute_query(self, query: str, fetch: bool = True) -> Optional[List[tuple]]:
        """
        Execute a query on the DataBase
        """

        conn = None
        try:
            conn = self.auto_login()
            with conn.cursor as cursor:
                cursor.execute(query)
                if fetch:
                    result = cursor.fetchall()
                    logger.info(
                        f"Query executed successfully. Fetched {len(result)} rows."
                    )
                    return result
                else:
                    cursor.commit()
                    logger.info("Query Executed Successfully")
                    return None

        except Exception as e:
            logger.error(e)
            raise e

        finally:
            if conn:
                self.release_connection(conn)

    def execute_db(self, query: str) -> pl.DataFrame:
        """
        Execute query against the database and returns result as Polars Dataframe
        """
        conn = None
        try:
            conn = self.auto_login()
            with conn.cursor() as cursor:
                cursor.execute(query)
                results = cursor.fetchall()
                col_names = [desc[0] for desc in cursor.description]

            df = pl.Dataframe(results, schema=col_names)
            logger.info(
                f"Query executed successfully. Converted {len(results)} rows to Polars DataFrame."
            )
            return df
        except Exception as e:
            logger.error(e)
            raise e
        finally:
            if conn:
                self.release_connection(conn)

    def write_data_to_table(self, table_name: str, data: List[Dict[str, Any]]) -> None:
        """
        write data to a specified table
        """
        if not data:
            return

        columns = data[0].keys()
        values = [[row[col] for col in columns] for row in data]

        placeholders = ", ".join(["%s"] * len(columns))
        insert_query = (
            f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        )

        conn = None
        try:
            conn = self.auto_login()
            with conn.cursor() as cursor:
                execute_values(cursor, insert_query, values)
            conn.commit()
            logger.info(f"Data written to table '{table_name}' successfully.")
        except Exception as e:
            logger.error(f"Error writing data to table '{table_name}'")
            logger.error(e)
            if conn:
                conn.rollback()
            raise BrokerConnectionError(e) from e
        finally:
            if conn:
                self.release_connection(conn)

    def create_table_from_yaml(self, schema_yaml_file: str) -> None:
        """
        Create tables and indexes from a YAML schema file.
        """
        try:
            conn = None
            schema = read_yaml_file(schema_yaml_file)
            conn = self.auto_login()
            with conn.cursor() as cursor:
                table_name = schema["table_name"]
                table_schema = schema["SCHEMA"]
                columns = []
                for col_name, col_info in table_schema.items():
                    if col_name not in ["primary_key", "indexes", "timescale"]:
                        col_type = col_info["type"]
                        nullable = (
                            "NOT NULL" if col_info.get("nullable") is False else ""
                        )
                        columns.append(f"{col_name} {col_type} {nullable}".strip())

                if "primary_key" in table_schema:
                    primary_key = ", ".join(table_schema["primary_key"])
                    columns.append(f"PRIMARY KEY ({primary_key})")

                create_table_query = (
                    f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)})"
                )
                cursor.execute(create_table_query)
                logger.info(f"Table '{table_name}' created successfully.")

                if "indexes" in table_schema:
                    for index_name, index_columns in table_schema["indexes"].items():
                        if isinstance(index_columns, list):
                            index_columns_str = ", ".join(index_columns)
                            create_index_query = f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} ({index_columns_str})"
                        else:
                            create_index_query = f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} ({index_columns})"
                        cursor.execute(create_index_query)
                        logger.info(
                            f"Index '{index_name}' created for table '{table_name}'."
                        )

                if "timescale" in table_schema:
                    for time_column in table_schema["timescale"]:
                        create_hypertable_query = f"SELECT create_hypertable('{table_name}', '{time_column}', if_not_exists => TRUE)"
                        cursor.execute(create_hypertable_query)
                        logger.info(
                            f"Hypertable created for table '{table_name}' on column '{time_column}'."
                        )

                        # Connection to Broker Not Successful: Details cannot create a unique index without the column "listing_date"
                        #  (used in partitioning) getting this error for above yaml

            conn.commit()
            logger.info(
                "All tables, indexes, and hypertables created successfully from YAML schema."
            )

        except Exception as e:
            logger.error("Error in creating tables from YAML schema.")
            logger.error(e)
            raise BrokerConnectionError(e) from e
        finally:
            if conn:
                self.release_connection(conn)
