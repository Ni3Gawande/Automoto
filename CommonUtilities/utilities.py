import pandas as pd
import pytest
import logging
from sqlalchemy import create_engine
# from Config.logging_config import logger
import os



logging.basicConfig(
    filename=r'C:\Users\Anshu\Desktop\folder\ETL\ETLFramework\Logs\etlprocess.log',  # Name of the log file
    filemode='a',  # 'a' to append, 'w' to overwrite
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log format
    level=logging.INFO  # Set the logging level
)
logger = logging.getLogger(__name__)


# Save the mismatched records to a file
def save_defect_data(df_actual, df_expected, file_path):
    defect_file = df_actual.merge(df_expected, how='outer', indicator=True).query("_merge != 'both'")
    defect_file['_merge'] = defect_file['_merge'].replace({'left_only': 'df_actual', 'right_only': 'df_expected'})
    defect_file.rename(columns={'_merge': 'side'}, inplace=True)
    location = fr"C:\Users\Anshu\Desktop\folder\ETL\ETLFramework\defect\{file_path}"
    if not defect_file.empty:
        defect_file.to_csv(location, index=False)
        logger.error(f"Mismatch records are  stored at location: {location}")
    else:
        logger.info("No Mismatch records found. Defect file was not created.")
    return defect_file

# Test logging setup
def file_to_db_verify(file_path, file_type, table_name, db_engine, defect_file_path):
    # Read the source file
    if file_type == 'csv':
        logger.info(f"Fetching the data from {file_path}")
        df_expected = pd.read_csv(file_path)
    elif file_type == 'xml':
        logger.info(f"Fetching the data from {file_path}")
        df_expected = pd.read_xml(file_path, xpath='.//item')
    elif file_type == 'json':
        logger.info(f"Fetching the data from {file_path}")
        df_expected = pd.read_json(file_path)
    else:
        logger.error(f"Unsupported file type: {file_type}")
        raise ValueError(f"Unsupported file type: {file_type}")

    # Read data from the database table
    logger.info(f"Fetching data from the database table: {table_name}")
    query = f"SELECT * FROM {table_name}"
    # comparing the data between df_actual and df_expected
    logger.info(f"Compairing the data between {file_path} and  {table_name}")
    df_actual = pd.read_sql(query, db_engine)

    # Validate the data
    if not df_actual.equals(df_expected):
        save_defect_data(df_actual, df_expected, defect_file_path)  # Save mismatched data to the file
        raise AssertionError(f"Data mismatch found between file '{file_path}' and table '{table_name}'")
        # logger.error(f"Data mismatch found between file '{file_path}' and table '{table_name}'")
        # pytest.fail(f"Data mismatch found between file '{file_path}' and table '{table_name}'")
    else:
        logger.info(f"Data validation passed: File '{file_path}' matches table '{table_name}'")


def db_to_db_verify(source_table_query, source_engine, target_table_query, target_table_engine,defect_file_path):
    # Read data from the source table
    query_expected = pd.read_sql(source_table_query, source_engine).astype(str)
    # Read data from the target table
    query_actual = pd.read_sql(target_table_query, target_table_engine).astype(str)
    # Validate the data
    defect_file_path=save_defect_data(query_actual, query_expected, defect_file_path)  # Save mismatched data to the file
    if not defect_file_path.empty:
        raise AssertionError(f"Data mismatch found between file '{source_table_query}' and table '{target_table_query}'")
    else:
        logger.info(f"Data validation passed")

# def db_to_db_verify(query1,db_engine1,query2,db_engine2):
#     df_expected  = pd.read_sql(query1,db_engine1)
#     logger.info(f"expected data is :{df_expected}")
#     df_actual = pd.read_sql(query2, db_engine2)
#     logger.info(f"actual data is :{df_actual}")
#     # implement the logic to write the differential data between source and target
#     assert df_actual.astype(str).equals(df_expected.astype(str)), f"Data comparision failed to load"