# config.py
import os
import psycopg2
from psycopg2 import sql
from contextlib import contextmanager
from dotenv import load_dotenv

# Load environment variables from a .env file
load_dotenv()

def get_db_config():
    """
    Retrieves database configuration from environment variables.

    Returns:
        dict: A dictionary containing database connection parameters.
    """
    config = {
        'user': os.getenv("user"),
        'password': os.getenv("password"),
        'host': os.getenv("host"),
        'port': os.getenv("port"),
        'dbname': os.getenv("dbname")
    }

    # Validate that all required configurations are present
    missing = [key for key, value in config.items() if not value]
    if missing:
        raise ValueError(f"Missing database configuration for: {', '.join(missing)}")

    return config


class DatabaseSession:
    """
    A singleton class to manage PostgreSQL database sessions using psycopg2.
    """
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DatabaseSession, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self.config = get_db_config()
        self.connection = None
        self._initialized = True

    def connect(self):
        if self.connection is None or self.connection.closed:
            try:
                self.connection = psycopg2.connect(**self.config)
                print("Database connection established.")
            except psycopg2.Error as e:
                print(f"Error connecting to the database: {e}")
                raise
        else:
            print("Using existing database connection.")

    def close(self):
        """
        Closes the database connection.
        """
        if self.connection and not self.connection.closed:
            self.connection.close()
            print("Database connection closed.")

    @contextmanager
    def get_cursor(self):
        """
        Provides a context manager for database cursor operations.

        Usage:
            with db_session.get_cursor() as cursor:
                cursor.execute("YOUR QUERY")
        """
        self.connect()
        cursor = self.connection.cursor()
        try:
            yield cursor
            self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            print(f"Transaction failed: {e}")
            raise
        finally:
            cursor.close()

    def execute_query(self, query, params=None):
        with self.get_cursor() as cursor:
            cursor.execute(query, params)
            try:
                results = cursor.fetchall()
                return results
            except psycopg2.ProgrammingError:
                # No results to fetch
                return []

    def execute_query_one(self, query, params=None):
        with self.get_cursor() as cursor:
            cursor.execute(query, params)
            try:
                result = cursor.fetchone()
                return result
            except psycopg2.ProgrammingError:
                # No result to fetch
                return None

    def execute_select(self, query, params=None):
        return self.execute_query(query, params)

    def execute_insert(self, query, params=None):
        with self.get_cursor() as cursor:
            cursor.execute(query, params)
            result = cursor.fetchone()
            return result[0] if result else None

    def execute_update(self, query, params=None):
        with self.get_cursor() as cursor:
            cursor.execute(query, params)
            return cursor.rowcount

    def execute_delete(self, query, params=None):
        with self.get_cursor() as cursor:
            cursor.execute(query, params)
            return cursor.rowcount

    def execute_many(self, query, params_list):
        """
        Executes a single query against all parameter tuples provided.

        Args:
            query (str): The SQL query to execute.
            params_list (list of tuples): A list where each tuple contains parameters for the query.

        Returns:
            None
        """
        with self.get_cursor() as cursor:
            try:
                cursor.executemany(query, params_list)
                print(f"Executed {len(params_list)} records successfully.")
            except psycopg2.Error as e:
                print(f"Error executing many records: {e}")
                raise

    def execute_bulk_insert(self, query, params_list, page_size=100):
        """
        Executes a bulk insert using psycopg2.extras.execute_values for enhanced performance.

        Args:
            query (str): The SQL query with a placeholder for values.
            params_list (list of tuples): A list where each tuple contains parameters for the query.
            page_size (int): Number of records to insert per batch.

        Returns:
            list: A list of inserted record IDs if RETURNING clause is used; otherwise, an empty list.
        """
        from psycopg2.extras import execute_values

        with self.get_cursor() as cursor:
            try:
                execute_values(cursor, query, params_list, page_size=page_size)
                if "RETURNING" in query.upper():
                    returned_ids = [row[0] for row in cursor.fetchall()]
                    print(f"Inserted records with IDs: {returned_ids}")
                    return returned_ids
                else:
                    print(f"Executed bulk insert of {len(params_list)} records successfully.")
                    return []
            except psycopg2.Error as e:
                print(f"Error during bulk insert: {e}")
                raise
