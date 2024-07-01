import uuid

from crewai_tools import tool
import mysql.connector
from dotenv import load_dotenv
import os

_ = load_dotenv()
DB_USER = os.getenv('DB_USER')
DB_KEY = os.getenv('DB_KEY')


def connect(table):
    # Establish a connection to the database
    return mysql.connector.connect(
        user=DB_USER,
        password=DB_KEY,
        host='localhost',
        database=table,
        port=3306
    )


@tool("SearchDB")
def search(table_name: str, search_string: str):
    """
    This tool is useful for retrieving data entries from a database.
    """
    with connect(table_name) as cnx:
        with cnx.cursor() as cursor:
            # Create the SELECT query
            query = f"SELECT * FROM {table_name} WHERE ID = %s"

            # Execute the query
            cursor.execute(query, (search_string,))

            # Fetch the result
            result = cursor.fetchone()

    return result


@tool("InsertDB")
def insert(table_name: str, entries: str):
    """
    This tool is useful for storing data entries in a database.
    """
    full, summ = entries.split('>>>')
    full = full.strip()
    summ = summ.strip()

    identifier = uuid.uuid4().hex

    values = (identifier, full, summ)
    keys = ('ID', 'FULL_DESCRIPTION', 'SUMMARY')

    with connect(table_name) as cnx:
        with cnx.cursor() as cursor:
            query = f"INSERT INTO {table_name} ({', '.join(keys)}) VALUES ({', '.join(['%s'] * len(values))})"

            # Execute the query
            cursor.execute(query, values)

            # Commit the changes
            cnx.commit()


@tool("InsertDB")
def delete_where_id(table_name: str, id_to_delete: str):
    """
    This tool is useful for deleting data entries in a database.
    """
    with connect(table_name) as cnx:
        with cnx.cursor() as cursor:
            query = f"DELETE FROM {table_name} WHERE ID = %s"

            # Execute the query
            cursor.execute(query, (id_to_delete,))

            # Commit the changes
            cnx.commit()

