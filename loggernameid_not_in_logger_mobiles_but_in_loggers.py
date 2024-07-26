# -*- coding: utf-8 -*-
"""
Created on Fri Jul 26 19:21:49 2024

@author: nichm
"""

import mysql.connector
import pandas as pd

# Establish the connection
conn = mysql.connector.connect(
            host="localhost",
            database="test_schema",
            user="root",
            password="admin123"
)

# Define SQL queries to select all columns
query_loggers = "SELECT * FROM loggers"
query_logger_mobile = "SELECT * FROM logger_mobile"

# Load the data into DataFrames
df_loggers = pd.read_sql(query_loggers, conn)
df_logger_mobile = pd.read_sql(query_logger_mobile, conn)

# Check column names to confirm they are correct
print("Columns in df_loggers:", df_loggers.columns)
print("Columns in df_logger_mobile:", df_logger_mobile.columns)

merged_df = pd.merge(df_loggers[['logger_id', 'logger_name']], 
                     df_logger_mobile[['logger_id']], 
                     on='logger_id', 
                     how='left', 
                     indicator=True)

# Filter to keep only rows where logger_id is present in df_loggers but not in df_logger_mobile
left_only_df = merged_df[merged_df['_merge'] == 'left_only']

# Ensure that logger_name in left_only_df is not in df_loggers with corresponding logger_id in df_logger_mobile
filtered_df = left_only_df[~left_only_df['logger_name'].isin(
    df_loggers[df_loggers['logger_id'].isin(df_logger_mobile['logger_id'])]['logger_name']
)]

# Display the filtered result
print("Logger details where logger_id is present in df_loggers but not in df_logger_mobile and logger_name is not in logger_mobile:")
print(filtered_df[['logger_id', 'logger_name']])

# Close the connection
conn.close()
