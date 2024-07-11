Project Description

This project is designed to extract, transform, and load (ETL) data on the largest banks by market capitalization from a web page. The project also includes functionalities for currency conversion, saving the data to a CSV file and a SQLite database, and executing SQL queries to analyze the data.

Functions:

extract(url, table_attribs)
Extracts the required information from the website and saves it to a dataframe.

transform(df, csv_path)
Transforms the dataframe by adding columns for market capitalization in different currencies based on exchange rates provided in a CSV file.

load_to_csv(df, output_path)
Saves the final dataframe as a CSV file.

load_to_db(df, sql_connection, table_name)
Saves the final dataframe to a database table.

run_queries(query_statements, sql_connection)
Runs SQL queries on the database table and prints the output.

log_progress(message)
Logs the mentioned message at a given stage of code execution to a log file.

Database Schema

Table Name: Largest_banks
Columns:
Name (TEXT)
MC_USD_Billion (REAL)
MC_GBP_Billion (REAL)
MC_EUR_Billion (REAL)
MC_INR_Billion (REAL)
Queries

The script runs the following queries:
SELECT * FROM Largest_banks
SELECT AVG(MC_GBP_Billion) FROM Largest_banks
SELECT Name FROM Largest_banks LIMIT 5
Logging

The script logs the progress of the ETL process in a log file named code_log.txt with timestamps.
