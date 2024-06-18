# -*- coding: utf-8 -*-
"""
Created on Wed Jun 19 01:04:31 2024

@author: nichm
"""

import mysql.connector
from datetime import datetime, timedelta

# Replace with your actual database configuration
db_config = {
    'user': 'pysys_local',
    'password': 'NaCAhztBgYZ3HwTkvHwwGVtJn5sVMFgg',
    'host': '192.168.150.112',
    'database': 'analysis_db'
}

# Function to prompt user for datetime input and handle errors
def input_datetime(prompt):
    while True:
        dt_str = input(prompt)
        try:
            dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
            return dt
        except ValueError:
            try:
                dt = datetime.strptime(dt_str, "%Y-%m-%d")
                return dt
            except ValueError:
                print("Invalid datetime format. Please use YYYY-MM-DD HH:MM:SS or YYYY-MM-DD.")
                # Continue the loop to prompt for input again

# Function to prompt user for logger name and validate its existence
def input_logger_name(prompt):
    while True:
        logger_suffix = input(prompt).strip()
        logger_name = f"tilt_{logger_suffix}"

        # Check if the logger table exists in the database
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        
        # Using SQL to check if the table exists
        cursor.execute(f"SHOW TABLES LIKE '{logger_name}'")
        exists = cursor.fetchone()
        
        cursor.close()
        conn.close()

        if exists:
            return logger_name
        else:
            print(f"Logger '{logger_suffix}' not found. Please enter a valid logger name.")

# Function to prompt user for time gap in days
def input_time_gap(prompt):
    while True:
        try:
            gap = int(input(prompt).strip())
            if gap > 0:
                return gap
            else:
                print("Time gap must be a positive integer.")
        except ValueError:
            print("Invalid input. Please enter a positive integer.")

# Prompt the user to enter the identifier for the logger
logger_name = input_logger_name("Enter logger name: ")

# Prompt the user if they want to specify start_ts
query_start_ts = input("Do you want to specify start_ts? (Y/N): ").strip().lower()

start_ts = None
if query_start_ts == 'y':
    start_ts = input_datetime("Enter start_ts (YYYY-MM-DD HH:MM:SS or YYYY-MM-DD): ")

# Prompt the user if they want to specify end_ts
query_end_ts = input("Do you want to specify end_ts? (Y/N): ").strip().lower()

end_ts = None
if query_end_ts == 'y':
    end_ts = input_datetime("Enter end_ts (YYYY-MM-DD HH:MM:SS or YYYY-MM-DD): ")

# Prompt the user for the time gap in days
time_gap = input_time_gap("Enter the time gap in days to check for no data: ")

# Establish the database connection
conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()

# Construct the query to fetch timestamps from the specified table
if start_ts and end_ts:
    query = f"SELECT ts FROM {logger_name} WHERE ts BETWEEN '{start_ts}' AND '{end_ts}' ORDER BY ts ASC"
elif start_ts:
    query = f"SELECT ts FROM {logger_name} WHERE ts >= '{start_ts}' ORDER BY ts ASC"
elif end_ts:
    query = f"SELECT ts FROM {logger_name} WHERE ts <= '{end_ts}' ORDER BY ts ASC"
else:
    query = f"SELECT ts FROM {logger_name} ORDER BY ts ASC"

cursor.execute(query)

# Fetch all timestamps
timestamps = cursor.fetchall()

# Close the connection
cursor.close()
conn.close()

# Process the timestamps to find ranges without data
def find_ts_gaps(timestamps, time_gap):
    if not timestamps:
        return []

    ts_without_data = []

    # Extract timestamps from fetched data
    timestamps = [ts[0] for ts in timestamps if ts[0] is not None]

    if not timestamps:
        return []

    # Initialize the start and end timestamp
    current_ts = timestamps[0]
    end_ts = timestamps[-1]

    # Iterate through the timestamps to find gaps
    for i in range(1, len(timestamps)):
        prev_ts = timestamps[i - 1]
        current_ts = timestamps[i]

        # Calculate the difference in days between consecutive timestamps
        diff = (current_ts - prev_ts).days

        # If the difference is greater than or equal to the time gap, record the gap
        if diff >= time_gap:
            gap_start = prev_ts + timedelta(hours=1)
            gap_end = current_ts - timedelta(hours=1)
            ts_without_data.append((gap_start, gap_end))

    return ts_without_data

# Function to format timestamp ranges
def format_ts_ranges(ts_without_data):
    ranges = []
    for gap_start, gap_end in ts_without_data:
        if gap_start == gap_end:
            ranges.append(gap_start.strftime("%Y-%m-%d %H:%M:%S"))
        else:
            ranges.append(f"{gap_start.strftime('%Y-%m-%d %H:%M:%S')} to {gap_end.strftime('%Y-%m-%d %H:%M:%S')}")
    return ranges

# Find the timestamps without data
ts_without_data = find_ts_gaps(timestamps, time_gap)

# Format the timestamp ranges
ts_without_data_ranges = format_ts_ranges(ts_without_data)

# Print the results
print("\nTimestamps without data:")
for ts_range in ts_without_data_ranges:
    print(ts_range)
