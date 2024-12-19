import pandas as pd
from sqlalchemy import create_engine

sqlserver_engine = create_engine(
    'mssql+pyodbc://LAPTOP-J82A4UMN/fun?driver=SQL+Server+Native+Client+11.0&trusted_connection=yes')

# query='select * from information_schema.tables'
# df_results=pd.read_sql(query,sqlserver_engine)
# print(df_results.head(15))

expected_tables=['class','k14','dhoom']
#
# def check_the_table_available_in_database(tables,sqlserver_engine):
#
#     result=pd.read_sql(query,sqlserver_engine)
#     for table in tables:
#         query = f"select * from information_schema.{table}"
#         if not result.empty:
#             print(f'{table} is present in databse')
#         else:
#             raise ValueError(f"{table} is not present in database")
#
# check_the_table_available_in_database(expected_tables,sqlserver_engine)


def check_the_table_available_in_database(tables, sqlserver_engine):
    """
    Checks if the specified tables are available in the database.

    :param tables: List of table names to check.
    :param sqlserver_engine: SQLAlchemy engine to connect to the database.
    """
    for table in tables:
        query = f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table}'"

        # Execute query and fetch result into a DataFrame
        result = pd.read_sql(query, sqlserver_engine)
        # try:
        #     pass
        # except Exception as e:
        #     raise RuntimeError(f"Error executing query: {query}. Error: {str(e)}")

        # Check if the result is empty
        if not result.empty:
            print(f"'{table}' is present in the database.")
        else:
            raise ValueError(f"'{table}' is not present in the database.")

# Example usage
# Ensure `sqlserver_engine` is a valid SQLAlchemy engine connected to your database
check_the_table_available_in_database(expected_tables, sqlserver_engine)
