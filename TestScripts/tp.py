# import pytest
# import pandas as pd
# import logging
# from sqlalchemy import create_engine
# from Config.config import *
# mysql_engine = create_engine(
#     f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}')
#
#
# def defect_file_path(df_actual,df_expected,path):
#     combined=df_actual.merge(df_expected,how='outer',indicator=True).query("_merge!='both'")
#     combined['_merge']=combined['_merge'].replace({'left_only':'df_actual','right_only':'df_expected'})
#     combined.rename(columns={'_merge':'side'},inplace=True)
#     defect_file_path=fr"C:\Users\Anshu\Desktop\folder\ETL\ETLFramework\defect\{path}"
#     combined.to_csv(defect_file_path,index=False)
#
#
# def file_to_db_verify(file_path, file_type, table_name, db_engine,path):
#     if file_type == 'csv':
#         df_expected = pd.read_csv(file_path)
#     elif file_type == 'xml':
#         df_expected = pd.read_xml(file_path, xpath='.//item')
#     elif file_type == 'json':
#         df_expected = pd.read_json(file_path)
#     else:
#         raise ValueError(f"Unsupported file type: {file_type}")
#
#     query = f"SELECT * FROM {table_name}"
#     df_actual = pd.read_sql(query, db_engine)
#     if not df_actual.equals(df_expected):
#         defect_file_path(df_actual,df_expected,path)
#         pytest.fail(f"Data mismatch fail")
#
#     else:
#         logging.info('Passed')
#
#
# def test_somethig():
#     try:
#         file_to_db_verify('Testdata/sales_data.csv', 'csv', 'staging_sales', mysql_engine,'sales_data_defect.csv')
#     except Exception as e:
#         print('error due to {e}')
#
#
# import pytest
# import pandas as pd
# import logging
# from sqlalchemy import create_engine
# from Config.config import *
#
# mysql_engine = create_engine(
#     f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}')
#
# # Save the mismatched records to a file
# def save_defect_data(df_actual, df_expected, file_path):
#     combined = df_actual.merge(df_expected, how='outer', indicator=True).query("_merge != 'both'")
#     combined['_merge'] = combined['_merge'].replace({'left_only': 'df_actual', 'right_only': 'df_expected'})
#     combined.rename(columns={'_merge': 'side'}, inplace=True)
#     defect_file = fr"C:\Users\Anshu\Desktop\folder\ETL\ETLFramework\defect\{file_path}"
#     combined.to_csv(defect_file, index=False)
#     logging.info(f"Mismatched data saved to {defect_file}")
#
# # Validate data between file and DB
# def file_to_db_verify(file_path, file_type, table_name, db_engine, defect_file_path):
#     try:
#         if file_type == 'csv':
#             df_expected = pd.read_csv(file_path)
#         elif file_type == 'xml':
#             df_expected = pd.read_xml(file_path, xpath='.//item')
#         elif file_type == 'json':
#             df_expected = pd.read_json(file_path)
#         else:
#             raise ValueError(f"Unsupported file type: {file_type}")
#
#         query = f"SELECT * FROM {table_name}"
#         df_actual = pd.read_sql(query, db_engine)
#
#         # Compare actual and expected data
#         if not df_actual.equals(df_expected):
#             save_defect_data(df_actual, df_expected, defect_file_path)  # Save mismatched data to the file
#             pytest.fail(f"Data mismatch found between file '{file_path}' and table '{table_name}'")
#         else:
#             logging.info(f"Data validation passed: File '{file_path}' matches table '{table_name}'")
#
#     except Exception as e:
#         logging.error(f"Error during data validation: {e}")
#         pytest.fail(f"Test failed due to error: {e}")
#
# # Sample test case
# def test_something():
#     try:
#         file_to_db_verify('Testdata/sales_data.csv', 'csv', 'staging_sales', mysql_engine, 'sales_data_defect.csv')
#     except Exception as e:
#         logging.error(f"Error during the test: {e}")
