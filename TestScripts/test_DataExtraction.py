import pandas as pd
from sqlalchemy import create_engine
import pytest
import logging
from CommonUtilities.utilities import *
from Config.config import *
import os

mysql_engine = create_engine(f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}')
sqlserver_engine = create_engine('mssql+pyodbc://LAPTOP-J82A4UMN/fun?driver=SQL+Server+Native+Client+11.0&trusted_connection=yes')


logging.basicConfig(
    filename=r'C:\Users\Anshu\Desktop\folder\ETL\ETLFramework\Logs\etlprocess.log',  # Name of the log file
    filemode='a',  # 'a' to append, 'w' to overwrite
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log format
    level=logging.INFO  # Set the logging level
)
logger = logging.getLogger(__name__)
logger.debug("Logging setup initialized")
logger.info("Source file is csv")

def test_extraction_from_sales_data_CSV_to_sales_staging_MySQL():
    file_to_db_verify('Testdata/sales_data.csv','csv','staging_sales',mysql_engine)

def test_extraction_from_product_data_csv_to_product_staging_MySQL():
    file_to_db_verify('Testdata/product_data.csv', 'csv', 'staging_product', mysql_engine)

def test_extraction_from_supplier_data_json_to_supplier_staging_MySQL():
    file_to_db_verify('Testdata/supplier_data.json', 'json', 'staging_supplier', mysql_engine)

def test_extraction_from_inventory_data_xml_to_inventory_staging_MySQL():
    file_to_db_verify('Testdata/inventory_data.xml', 'xml', 'staging_inventory', mysql_engine)

def test_extraction_from_stores_data_sql_to_stores_staging_MySQL():
    db_to_db_verify('stores',sqlserver_engine,'staging_stores',mysql_engine)
