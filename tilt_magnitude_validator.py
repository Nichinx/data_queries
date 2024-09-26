# -*- coding: utf-8 -*-
"""
Created on Thu Sep 26 14:19:01 2024

@author: nichm
"""

import mysql.connector
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

#######PROMPT LOGGER NAME

# Prompt the user for logger_name
logger_name = input("Enter the logger_name: ")

# Database connection setup
db_config = {
    'host': '192.168.150.112',
    'user': 'pysys_local',
    'password': 'NaCAhztBgYZ3HwTkvHwwGVtJn5sVMFgg',
    'database': 'analysis_db'
}

conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()

# Check the number_of_segments in tsm_sensors by matching logger_name (tsm_name)
tsm_query = f"""
    SELECT tsm_name, number_of_segments, date_deactivated
    FROM analysis_db.tsm_sensors
    WHERE tsm_name = '{logger_name}'
"""

cursor.execute(tsm_query)
tsm_results = cursor.fetchall()

if len(tsm_results) == 0:
    print(f"No entry found for tsm_name '{logger_name}' in tsm_sensors.")
    cursor.close()
    conn.close()
    exit()

# Filter for active entries where date_deactivated is NULL
active_tsm = [result for result in tsm_results if result[2] is None]

if len(active_tsm) == 0:
    print(f"No active entry found for tsm_name '{logger_name}' in tsm_sensors.")
    cursor.close()
    conn.close()
    exit()

if len(active_tsm) > 1:
    print(f"Multiple active entries found for tsm_name '{logger_name}'. Using the first match.")

# Get the number_of_segments from the first active entry
number_of_segments = active_tsm[0][1]

print(f"Number of segments for '{logger_name}': {number_of_segments}")

# Get the current date and calculate the timedelta of 3 months (90 days)
now = datetime.now()
time_delta = timedelta(days=90)

# SQL query to fetch data from tilt_xxxx where node_id <= number_of_segments
tilt_query = f"""
    SELECT * FROM analysis_db.tilt_{logger_name}
    WHERE node_id <= {number_of_segments}
    ORDER BY ts DESC
"""

cursor.execute(tilt_query)
rows = cursor.fetchall()

# Close the cursor and connection
cursor.close()
conn.close()

# Load the data into a DataFrame
# Get column names from cursor description
columns = [column[0] for column in cursor.description]
df = pd.DataFrame(rows, columns=columns)

# Check if 'is_live' is in the columns and handle accordingly
if 'is_live' in df.columns:
    if len(df.columns) == 10:
        df.columns = ['data_id', 'ts_written', 'ts', 'node_id', 'type_num', 'xval', 'yval', 'zval', 'batt', 'is_live']
else:
    if len(df.columns) == 9:
        df.columns = ['data_id', 'ts_written', 'ts', 'node_id', 'type_num', 'xval', 'yval', 'zval', 'batt']
    else:
        print("Unexpected column length. Please check the database structure.")
        cursor.close()
        conn.close()
        exit()

# Set type_num to 1 if logger_name is a 4-letter word
if len(logger_name) == 4:
    df['type_num'] = 1

# Convert the 'ts' column to datetime
df['ts'] = pd.to_datetime(df['ts'])

# Get the last ts for each node_id and type_num
last_ts_per_node = df.groupby(['node_id', 'type_num'])['ts'].max().reset_index(name='last_ts')

# Check if the last ts is older than 3 months
last_ts_per_node['time_diff'] = now - last_ts_per_node['last_ts']
no_data_nodes = last_ts_per_node[last_ts_per_node['time_diff'] > time_delta]

# Report node_id and type_num with no data in the last 3 months
if not no_data_nodes.empty:
    print("No data since the following timestamps (more than 3 months old):")
    for index, row in no_data_nodes.iterrows():
        print(f"node_id: {row['node_id']}, type_num: {row['type_num']}, last ts: {row['last_ts']}")
        
# Filter rows to only include data from the last 3 months
df = df[df['ts'] >= (now - time_delta)]

# Compute the magnitude: mag = sqrt(xval^2 + yval^2 + zval^2) / 1024 -> (version1-4)
df['magnitude'] = np.sqrt(df['xval']**2 + df['yval']**2 + df['zval']**2) / 1024

# Filter rows where magnitude is not within 1 ± 0.08
lower_bound = 1 - 0.08
upper_bound = 1 + 0.08
filtered_df = df[(df['magnitude'] < lower_bound) | (df['magnitude'] > upper_bound)]

# Group by node_id and type_num, and calculate percentage of occurrences
total_counts = df.groupby(['node_id', 'type_num']).size()
filtered_counts = filtered_df.groupby(['node_id', 'type_num']).size()

# Calculate percentage of occurrences for each node_id and type_num
percentages = (filtered_counts / total_counts) * 100
percentages = percentages.reset_index(name='percentage')

# Merge percentages with the filtered data
result_df = filtered_df.merge(percentages, on=['node_id', 'type_num'], how='left')

# Select and return node_id, type_num, and percentage of total occurrence
final_result = result_df[['node_id', 'type_num', 'percentage']].drop_duplicates().reset_index(drop=True)

# Output the result
print("\nFiltered data with magnitude not within 1 ± 0.08:")
print(final_result)



