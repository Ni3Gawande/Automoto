import pandas as pd
import pytest
import logging
from sqlalchemy import create_engine
import os

logging.basicConfig(
    filename=r'C:\Users\Anshu\Desktop\folder\ETL\ETLFramework\Logs\etlprocess.log',  # Name of the log file
    filemode='a',  # 'a' to append, 'w' to overwrite
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log format
    level=logging.INFO  # Set the logging level
)
logger = logging.getLogger(__name__)


# Test logging setup
def file_to_db_verify(file_path, file_type, table_name, db_engine):
    """
    Verify data between a file and a database table.
    :param file_path: Path to the source file
    :param file_type: Type of the file (csv, xml, json)
    :param table_name: Name of the target database table
    :param db_engine: SQLAlchemy engine connected to the database
    """

    logger.info('Starting data verification between file and database table')

    # Read the source file
    if file_type == 'csv':
        logger.info(f"Source file is {file_type}")
        df_expected = pd.read_csv(file_path)
    elif file_type == 'xml':
        logger.info(f"Source file is {file_type}")
        df_expected = pd.read_xml(file_path, xpath='.//item')
    elif file_type == 'json':
        logger.info(f"Source file is {file_type}")
        df_expected = pd.read_json(file_path)
    else:
        logger.error(f"Unsupported file type: {file_type}")
        raise ValueError(f"Unsupported file type: {file_type}")

    # Read data from the database table
    logger.info(f"Fetching data from the database table: {table_name}")
    query = f"SELECT * FROM {table_name}"
    df_actual = pd.read_sql(query, db_engine)

    # Validate the data
    assert df_actual.equals(df_expected), f"Data mismatch between file '{file_path}' and table '{table_name}'"
    logger.info(f"Data validation passed: File '{file_path}' matches table '{table_name}'.")



def db_to_db_verify(source_table_name, source_engine, target_table_name, target_table_engine):
    """
    Verify data between two database tables.
    :param source_table_name: Name of the source database table
    :param source_engine: SQLAlchemy engine connected to the source database
    :param target_table_name: Name of the target database table
    :param target_table_engine: SQLAlchemy engine connected to the target database
    """


    logger.info('Starting data verification between source and target database tables')

    # Read data from the source table
    source_table_query = f"SELECT * FROM {source_table_name}"
    logger.info(f"Fetching data from source table: {source_table_name}")
    df_expected = pd.read_sql(source_table_query, source_engine)

    # Read data from the target table
    target_table_query = f"SELECT * FROM {target_table_name}"
    logger.info(f"Fetching data from target table: {target_table_name}")
    df_actual = pd.read_sql(target_table_query, target_table_engine)

    # Validate the data
    assert df_actual.equals(df_expected), (
        f"Data mismatch between source table '{source_table_name}' and target table '{target_table_name}'"
    )
    logger.info(
        f"Data validation passed: Source table '{source_table_name}' matches target table '{target_table_name}'.")



