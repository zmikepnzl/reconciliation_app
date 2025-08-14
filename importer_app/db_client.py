import os
import psycopg2
import psycopg2.extras # Import for RealDictCursor
from urllib.parse import urlparse
import logging
from contextlib import contextmanager

# Configure logging for the DBClient
logger = logging.getLogger(__name__)

class DBClient:
    """
    A client for interacting with the PostgreSQL database.
    This class supports transactional blocks using the 'with' statement.
    """
    def __init__(self):
        self.database_url = os.environ.get("DATABASE_URL")
        if not self.database_url:
            raise ValueError("DATABASE_URL environment variable is not set.")
        self.conn = None
        self.cursor = None

    @contextmanager
    def _get_connection(self):
        """Internal helper to establish and yield a connection, ensuring it's closed."""
        try:
            conn = psycopg2.connect(self.database_url)
            conn.autocommit = False # Ensure manual transaction control
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            logging.info("Database connection established.")
            yield conn, cursor
        except psycopg2.OperationalError as e:
            logging.error(f"Error connecting to database: {e}")
            raise # Re-raise to propagate connection errors
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
                logging.info("Database connection closed.")

    def __enter__(self):
        """Establishes a database connection and returns the client instance for a 'with' block."""
        # When entering the context, we establish the connection and cursor
        # and store them on the instance. The actual commit/rollback is
        # handled by __exit__.
        try:
            self.conn = psycopg2.connect(self.database_url)
            self.conn.autocommit = False # Disable autocommit for manual transaction control
            self.cursor = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            logging.info("Database connection established for context.")
        except psycopg2.OperationalError as e:
            logging.error(f"Error connecting to database in __enter__: {e}")
            self.conn = None
            self.cursor = None
            raise # Re-raise the exception to propagate the connection error
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Commits or rolls back the transaction and closes the connection."""
        if self.conn:
            if exc_type is None:
                self.conn.commit()
                logging.info("Transaction committed successfully.")
            else:
                self.conn.rollback()
                logging.error(f"Transaction rolled back due to exception: {exc_val}")
            
            if self.cursor:
                self.cursor.close()
            self.conn.close()
            logging.info("Database connection closed from context.")
            self.cursor = None
            self.conn = None
    
    def _execute_query(self, query, params=None, fetch='none'):
        """
        Executes a SQL query.
        :param query: The SQL query string.
        :param params: A tuple of parameters for the query.
        :param fetch: 'one' for single row, 'all' for all rows, 'none' for no fetch.
        :return: Fetched data or None.
        """
        if not self.cursor:
            raise Exception("No active database cursor. Ensure DBClient is used within a 'with' block.")
            
        try:
            self.cursor.execute(query, params)
            if fetch == 'one':
                return self.cursor.fetchone()
            elif fetch == 'all':
                return self.cursor.fetchall()
            return None
        except psycopg2.Error as e:
            logging.error(f"Query failed: {e}\nQuery: {query}\nParams: {params}")
            raise # Re-raise the exception to trigger rollback in __exit__

    def _execute_raw_sql(self, sql_content):
        """
        Executes raw SQL content directly against the database.
        This method is intended for executing full scripts or multiple statements.
        It relies on the context manager for transaction management.
        """
        if not self.cursor:
            raise Exception("No active database cursor. Ensure DBClient is used within a 'with' block.")
            
        try:
            self.cursor.execute(sql_content)
            logging.info("Raw SQL script executed successfully.")
        except psycopg2.Error as e:
            logging.error(f"Raw SQL script execution failed: {e}")
            raise # Re-raise the exception to trigger rollback in __exit__

    def get_rows(self, table_name, columns="*", where_clause=None, params=None, order_by=None, limit=None):
        """Fetches multiple rows from a table."""
        query_parts = [f"SELECT {columns} FROM \"{table_name}\""]
        
        if where_clause:
            query_parts.append(f" WHERE {where_clause}")
        if order_by:
            query_parts.append(f" ORDER BY {order_by}")
        if limit is not None: # Added limit parameter
            query_parts.append(f" LIMIT {limit}")
        query_parts.append(";")
        
        query = " ".join(query_parts)
        return self._execute_query(query, params, fetch='all')

    def get_row(self, table_name, columns="*", where_clause=None, params=None):
        """Fetches a single row from a table based on a WHERE clause."""
        query_parts = [f"SELECT {columns} FROM \"{table_name}\""]
        
        if where_clause:
            query_parts.append(f" WHERE {where_clause}")
        
        query_parts.append(" LIMIT 1;") # Ensure only one row is returned
        
        query = " ".join(query_parts)
        return self._execute_query(query, params, fetch='one')

    def get_row_by_id(self, table_name, row_id, columns="*"):
        """Fetches a single row by its primary key (id)."""
        query = f"SELECT {columns} FROM \"{table_name}\" WHERE id = %s;"
        return self._execute_query(query, (row_id,), fetch='one')

    def get_row_id(self, table_name, id_field, id_value):
        """Retrieves the ID of a row based on a unique field."""
        query = f"SELECT id FROM \"{table_name}\" WHERE \"{id_field}\" = %s LIMIT 1;"
        result = self._execute_query(query, (id_value,), fetch='one')
        return result['id'] if result else None

    def create_row(self, table_name, data, returning_id=True):
        """Inserts a new row into a table and optionally returns its new ID."""
        columns = ', '.join([f'"{k}"' for k in data.keys()])
        placeholders = ', '.join(['%s' for _ in data.values()])
        query = f"INSERT INTO \"{table_name}\" ({columns}) VALUES ({placeholders})"
        
        if returning_id:
            query += " RETURNING id;"
        else:
            query += ";" # Just execute the insert without returning anything

        values = list(data.values())
        
        if returning_id:
            result = self._execute_query(query, values, fetch='one')
            return {'id': result['id']} if result else None
        else:
            self._execute_query(query, values)
            return None # No ID to return

    def update_row(self, table_name, row_id, data):
        """Updates an existing row in a table."""
        set_clause = ', '.join([f'"{k}" = %s' for k in data.keys()])
        query = f"UPDATE \"{table_name}\" SET {set_clause} WHERE id = %s;"
        values = list(data.values()) # Convert to list for psycopg2
        values.append(row_id)
        self._execute_query(query, values)

    def delete_row(self, table_name, row_id):
        """Deletes a row from a table by its ID."""
        query = f"DELETE FROM \"{table_name}\" WHERE id = %s;"
        self._execute_query(query, (row_id,))

    def delete_row_where(self, table_name, where_clause, params):
        """
        Deletes rows from a table based on a WHERE clause.
        :param table_name: The name of the table.
        :param where_clause: The WHERE clause (e.g., "column = %s AND another_column = %s").
        :param params: A tuple or list of parameters for the WHERE clause.
        """
        query = f"DELETE FROM \"{table_name}\" WHERE {where_clause};"
        self._execute_query(query, params)