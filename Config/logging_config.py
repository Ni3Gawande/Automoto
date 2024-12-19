# import logging
# import os
#
# # Define the log directory and file
# LOG_DIR = r'C:\Users\Anshu\Desktop\folder\ETL\ETLFramework\Logs'
# os.makedirs(LOG_DIR, exist_ok=True)  # Ensure log directory exists
# LOG_FILE = os.path.join(LOG_DIR, 'etlprocess.log')
#
# # Create a logger
# logger = logging.getLogger(__name__)
# logger.setLevel(logging.INFO)  # Set the logging level
#
# # File handler for logging to a file
# file_handler = logging.FileHandler(LOG_FILE)
# file_handler.setLevel(logging.INFO)  # File log level
# file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
# file_handler.setFormatter(file_formatter)
#
# # Stream handler for logging to console
# console_handler = logging.StreamHandler()
# console_handler.setLevel(logging.INFO)  # Console log level
# console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
# console_handler.setFormatter(console_formatter)
#
# # Add handlers to the logger
# if not logger.handlers:  # Avoid adding handlers multiple times
#     logger.addHandler(file_handler)
#     logger.addHandler(console_handler)
#
# # Log file path info (for debugging purposes)
# print(f"Logging to file: {LOG_FILE}")

#-------------------------------------------------------------------------------------

# import logging
# import os
#
# # Define the log directory and file
# LOG_DIR = r'C:\Users\Anshu\Desktop\folder\ETL\ETLFramework\Logs'
# os.makedirs(LOG_DIR, exist_ok=True)  # Ensure log directory exists
# LOG_FILE = os.path.join(LOG_DIR, 'etlprocess.log')
#
# # Create a logger
# logger = logging.getLogger(__name__)
# logger.setLevel(logging.INFO)  # Set the logging level
#
# # File handler for logging to a file
# file_handler = logging.FileHandler(LOG_FILE)
# file_handler.setLevel(logging.INFO)  # File log level
# file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
# file_handler.setFormatter(file_formatter)
#
# # Add the file handler to the logger
# if not logger.handlers:  # Avoid adding handlers multiple times
#     logger.addHandler(file_handler)
#
# # Log file path info (for debugging purposes)
# print(f"Logging to file: {LOG_FILE}")

#-------------------------------------------------------------------------
# import logging
# import os
#
# # Define the log directory and file
# LOG_DIR = r'C:\Users\Anshu\Desktop\folder\ETL\ETLFramework\Logs'
# os.makedirs(LOG_DIR, exist_ok=True)  # Ensure log directory exists
# LOG_FILE = os.path.join(LOG_DIR, 'etlprocess4.log')
#
# # Configure logging to file
# logging.basicConfig(
#     filename=LOG_FILE,
#     filemode='a',  # Append mode
#     format='%(asctime)s - %(levelname)s - %(message)s',  # Log format
#     level=logging.INFO  # Log level
# )
#
# logger = logging.getLogger()  # Root logger
#
# # Log file path info (for debugging purposes)
# print(f"Logging to file: {LOG_FILE}")

#-------------------------------------------------------------------
import logging
import os

# Define the log directory and file
LOG_DIR = r'C:\Users\Anshu\Desktop\folder\ETL\ETLFramework\Logs'
os.makedirs(LOG_DIR, exist_ok=True)  # Ensure log directory exists
LOG_FILE = os.path.join(LOG_DIR, 'etlprocess.log')

# Create a logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)  # Set the logging level

# File handler for logging to a file
file_handler = logging.FileHandler(LOG_FILE)
file_handler.setLevel(logging.INFO)  # File log level
file_formatter = logging.Formatter('%(asctime)s - %(filename)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)

# Add the file handler to the logger
if not logger.handlers:  # Avoid adding handlers multiple times
    logger.addHandler(file_handler)

# Log file path info (for debugging purposes)
print(f"Logging to file: {LOG_FILE}")
