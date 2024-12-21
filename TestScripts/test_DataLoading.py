import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle

from CommonUtilities.utilities import *
from Config.config import *
import pytest
import logging

mysql_engine = create_engine(f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}')

# Logging mechanism
logging.basicConfig(
    filename='Logs/etlprocess.log',  # Name of the log file
    filemode='a',  # 'a' to append, 'w' to overwrite
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log format
    level=logging.INFO  # Set the logging level
)
logger = logging.getLogger(__name__)

def test_fact_sales_table_load():
    logger.info("test_fact_sales_table_load test has started .......")
    try:
        query_expected = """select sales_id,product_id,store_id,quantity,total_amount as total_sales ,sale_date from sales_with_deatils;"""
        query_actual = """select sales_id,product_id,store_id,quantity,total_sales,sale_date from fact_sales ;"""
        # db_to_db_verify(query_expected,mysql_engine,query_actual,mysql_engine)
        db_to_db_verify(query_expected,mysql_engine,query_actual,mysql_engine,'loading_facts_sales.csv')
        logger.info("test_fact_sales_table_load test has completed .......")
    except Exception as e:
        logger.error(f"Error occured during data transformation: {e}")
        pytest.fail(f"Test failed due to an error {e}")

def test_monthly_sales_summary_table_load():
    logger.info("test_monthly_sales_summary_table_load test has started .......")
    try:
        query_expected = """select * from  monthly_sales_summary_source ;"""
        query_actual = """select * from  monthly_sales_summary ;"""
        db_to_db_verify(query_expected,mysql_engine,query_actual,mysql_engine,'loading_monthly_sales.csv')
        # db_to_db_verify(query_expected,mysql_engine,query_actual,mysql_engine)
        logger.info("test_monthly_sales_summary_table_load test has completed .......")
    except Exception as e:
        logger.error(f"Error occured during data transformation: {e}")
        pytest.fail(f"Test failed due to an error {e}")


def test_fact_inventory_level_by_store_table_load():
    logger.info("test_monthly_sales_summary_table_load test has started .......")
    try:
        query_expected='select product_id,store_id,quantity_on_hand ,last_updated from staging_inventory'
        query_actual='select product_id,store_id,quantity_on_hand ,last_updated from fact_inventory '
        db_to_db_verify(query_expected,mysql_engine,query_actual,mysql_engine,'fact_inventory_levels_by_store.csv')
        # db_to_db_verify(query_expected,mysql_engine,query_actual,mysql_engine)
        logger.info("test_monthly_sales_summary_table_load test has completed .......")
    except Exception as e:
        logger.error(f"Error occured during data transformation: {e}")
        pytest.fail(f"Test failed due to an error {e}")

#
def test_load_inventory_fact():
    logger.info("test_load_inventory_fact_table_load test has started .......")
    try:
        query_expected = """ select store_id,total_inventory from aggregated_inventory_levels; """
        query_actual = """ select store_id,cast(total_inventory as float) from inventory_levels_by_store; """
        db_to_db_verify(query_expected, mysql_engine, query_actual, mysql_engine,'fact_inventory_fact.csv')
        # db_to_db_verify(query_expected, mysql_engine, query_actual, mysql_engine)
        logger.info("test_load_inventory_fact_table_load has completed .......")
    except Exception as e:
        logger.error(f"Error occured during data transformation: {e}")
        pytest.fail(f"Test failed due to an error {e}")
