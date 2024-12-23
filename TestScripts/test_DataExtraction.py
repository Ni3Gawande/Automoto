import pandas as pd
from sqlalchemy import create_engine
import pytest
import logging
from CommonUtilities.utilities import *
from Config.config import *
# from Config.logging_config import logger
import os

mysql_engine = create_engine(
    f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}')
sqlserver_engine = create_engine(
    'mssql+pyodbc://LAPTOP-J82A4UMN/fun?driver=SQL+Server+Native+Client+11.0&trusted_connection=yes')


# Ensure logging is configured only once
logging.basicConfig(
    filename=r'C:\Users\Anshu\Desktop\folder\ETL\ETLFramework\Logs\etlprocess.log',
    filemode='a',  # 'a' to append, 'w' to overwrite
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log format
    level=logging.INFO  # Log level set to INFO
)

logger = logging.getLogger(__name__)


def test_extraction_from_sales_data_CSV_to_sales_staging_MySQL():
    logger.info("Data extraction from sales_data.csv to staging_sales has started")
    try:
        file_to_db_verify('Testdata/sales_data.csv', 'csv', 'staging_sales', mysql_engine, 'defect_sales.csv')
        logger.info("Data extraction from sales_data.csv to sales_staging has completed")
    except Exception as e:
        logger.error(f"Error occurred during data extraction: {e}")
        pytest.fail(f"Test failed due to an error: {e}")


def test_extraction_from_product_data_csv_to_product_staging_MySQL():
    logger.info('Data extraction from product_data.csv to staging_product has started')
    try:
        file_to_db_verify('Testdata/product_data.csv', 'csv', 'staging_product', mysql_engine, 'defect_product.csv')
        logger.info("Data extraction from product_data.csv to staging_product has completed")
    except Exception as e:
        logger.error(f"Error occured during data extraction {e}")
        pytest.fail(f"Test failed due to {e}")


def test_extraction_from_supplier_data_json_to_supplier_staging_MySQL():
    logger.info('Data extraction from supplier_data.json to staging_supplier has started')
    try:
        file_to_db_verify('Testdata/supplier_data.json', 'json', 'staging_supplier', mysql_engine, 'defect_supplier')
        logger.info('Data extraction from supplier_data.json to staging_supplier has completed')
    except Exception as e:
        logger.error(f"Error occured during data extraction {e}")
        pytest.fail(f"Test failed due to {e}")


@pytest.mark.smoke
def test_extraction_from_inventory_data_xml_to_inventory_staging_MySQL():
    logger.info('Data extraction from inventory_data to staging_inventory has started')
    try:
        file_to_db_verify('Testdata/inventory_data.xml', 'xml', 'staging_inventory', mysql_engine, 'defect_inventory')
        logger.info('Data extraction from inventory_data to staging_inventory has completed')
    except Exception as e:
        logger.error(f"Error occured due during data extraction {e}")
        pytest.fail(f"Test failed due to {e}")


@pytest.mark.regration
def test_extraction_from_stores_oracle_to_stores_staging_MySQL():
    logger.info(
        "Data validation started between stores table from sqlserver database to staging_stores table from mysql database")
    try:
        logger.info('query_expected:Running SQL query to read the data from stores table from sqlserver database')
        queue_expected = """Select * from stores"""
        logger.info('query_actual:Running SQL query to read the data from staging_stores table from mysql database')
        query_actual = """select * from staging_stores"""
        db_to_db_verify(queue_expected, sqlserver_engine, query_actual, mysql_engine, 'defect_store.csv')
        logger.info(
            "Data validation completed between stores table of sqlserver database to staging_stores table of mysql database")
    except Exception as e:
        logger.error(f"Error occure during data extraction {e}")
        pytest.fail(f"Test failed due to {e}")
