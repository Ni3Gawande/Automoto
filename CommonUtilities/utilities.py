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


def file_to_db_verify(file_path, file_type, table_name, db_engine):
    logger.info('Verify data between file  as source and target as database')
    if file_type == 'csv':
        logger.info("Source file is csv")
        df_expected = pd.read_csv(file_path)
    elif file_type == 'xml':
        logger.info("Source file is xml")
        df_expected = pd.read_xml(file_path, xpath='.//item')
    elif file_type == 'json':
        logger.info('Source file is json')
        df_expected = pd.read_json(file_path)
    else:
        logger.info(f"Source file is {file_type}")
        raise ValueError(f"Unsupported file type passed {file_type}")

    logger.info('Execute the database table querry')
    query = f"select * from {table_name}"
    logger.info(f"executing the query {query}")
    df_actual = pd.read_sql(query, db_engine)
    assert df_actual.equals(df_expected), f"Data extraction failed to load in {table_name}"
    logger.info(f"Data validation passed. The data in file '{file_path}' matches table '{table_name}'.")


def db_to_db_verify(source_table_name, source_engine, target_table_name, target_table_engine):
    logger.info('Verify data between file  as source database and target database')
    source_table_query = f"Select * from {source_table_name}"
    target_table_query = f"Select * from {target_table_name}"
    logger.info("Execute the source database query")
    df_expected = pd.read_sql(source_table_query, source_engine)
    logger.info("Execute the target database query")
    logger.info('Validating data between source and target database')
    df_actual = pd.read_sql(target_table_query, target_table_engine)
    assert df_actual.equals(df_expected), f"Data extraction failed to load {source_table_name}"
    logger.info(
        f"Data validation passed. The data in source database table {source_table_name} matches target database table{target_table_name}")
