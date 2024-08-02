# -*- coding: utf-8 -*-
"""
Created on Tue Jun 18 18:18:08 2024

@author: nichm
"""

import mysql.connector
from datetime import datetime, timedelta

db_config = {
    'user': 'pysys_local',
    'password': 'NaCAhztBgYZ3HwTkvHwwGVtJn5sVMFgg',
    'host': '192.168.150.112',
    'database': 'analysis_db'
}

# Function to prompt user for datetime input and handle errors
def input_datetime(prompt):
    while True:
        try:
            dt_str = input(prompt)
            dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
            return dt
        except ValueError:
            print("Invalid datetime format. Please use YYYY-MM-DD HH:MM:SS.")
            # Continue the loop to prompt for input again

# Function to prompt user for logger name and validate its existence
def input_logger_name(prompt):
    while True:
        logger_suffix = input(prompt).strip()
        logger_name = f"tilt_{logger_suffix}"

        # Check if the logger table exists in the database
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute(f"SHOW TABLES LIKE '{logger_name}'")
        exists = cursor.fetchone()
        cursor.close()
        conn.close()

        if exists:
            return logger_name
        else:
            print(f"Logger '{logger_suffix}' not found. Please enter a valid logger identifier.")
            
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
    while True:
        try:
            start_ts_str = input("Enter start_ts (YYYY-MM-DD HH:MM:SS): ")
            start_ts = datetime.strptime(start_ts_str, "%Y-%m-%d %H:%M:%S")
            break  # Break out of the loop if input is valid
        except ValueError:
            print("Invalid datetime format. Please use YYYY-MM-DD HH:MM:SS.")
            # Continue the loop to prompt for input again

# Prompt the user if they want to specify end_ts
query_end_ts = input("Do you want to specify end_ts? (Y/N): ").strip().lower()

end_ts = None
if query_end_ts == 'y':
    while True:
        try:
            end_ts_str = input("Enter end_ts (YYYY-MM-DD HH:MM:SS): ")
            end_ts = datetime.strptime(end_ts_str, "%Y-%m-%d %H:%M:%S")
            break  # Break out of the loop if input is valid
        except ValueError:
            print("Invalid datetime format. Please use YYYY-MM-DD HH:MM:SS.")
            # Continue the loop to prompt for input again

# Prompt the user for the time gap in days
time_gap = input_time_gap("Enter the time gap in days to check for data or no data: ")

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

# # Process the timestamps to find dates with and without data
# def find_date_gaps(timestamps):
#     if not timestamps:
#         return [], []

#     dates_with_data = []
#     dates_without_data = []

#     # Extract dates from timestamps, ensuring no None values
#     dates = [ts[0].date() for ts in timestamps if ts[0] is not None]

#     if not dates:
#         return [], []

#     # Initialize the start and end date
#     current_date = dates[0]
#     end_date = dates[-1]

#     # Iterate through each date from start to end
#     while current_date <= end_date:
#         if current_date in dates:
#             dates_with_data.append(current_date)
#         else:
#             dates_without_data.append(current_date)
#         current_date += timedelta(days=7)

#     return dates_with_data, dates_without_data

# # Function to format date ranges
# def format_date_ranges(dates):
#     if not dates:
#         return []

#     ranges = []
#     start_date = dates[0]
#     prev_date = dates[0]

#     for current_date in dates[1:]:
#         if current_date == prev_date + timedelta(days=7):
#             prev_date = current_date
#         else:
#             if start_date == prev_date:
#                 ranges.append(str(start_date))
#             else:
#                 ranges.append(f"{start_date} to {prev_date}")
#             start_date = current_date
#             prev_date = current_date

#     # Add the last range
#     if start_date == prev_date:
#         ranges.append(str(start_date))
#     else:
#         ranges.append(f"{start_date} to {prev_date}")

#     return ranges

# # Find the dates with and without data
# dates_with_data, dates_without_data = find_date_gaps(timestamps)

# # Format the date ranges
# dates_with_data_ranges = format_date_ranges(dates_with_data)
# dates_without_data_ranges = format_date_ranges(dates_without_data)

# # Print the results
# print("Dates with data:")
# for date_range in dates_with_data_ranges:
#     print(date_range)

# print("\nDates without data:")
# for date_range in dates_without_data_ranges:
#     print(date_range)


##############################################################################
###timestamp printout in range

# # Process the timestamps to find ranges with and without data
# def find_ts_gaps(timestamps, time_gap):
#     if not timestamps:
#         return [], []

#     ts_with_data = []
#     ts_without_data = []

#     # Extract timestamps from fetched data
#     timestamps = [ts[0] for ts in timestamps if ts[0] is not None]

#     if not timestamps:
#         return [], []

#     # Initialize the start and end timestamp
#     current_ts = timestamps[0]
#     end_ts = timestamps[-1]

#     # Create a set for fast lookup
#     timestamps_set = set(timestamps)

#     # Iterate through each timestamp from start to end
#     while current_ts <= end_ts:
#         if current_ts in timestamps_set:
#             ts_with_data.append(current_ts)
#         else:
#             ts_without_data.append(current_ts)
#         current_ts += timedelta(days=time_gap)

#     return ts_with_data, ts_without_data

# # Function to format timestamp ranges
# def format_ts_ranges(timestamps):
#     if not timestamps:
#         return []

#     ranges = []
#     start_ts = timestamps[0]
#     prev_ts = timestamps[0]

#     for current_ts in timestamps[1:]:
#         if current_ts == prev_ts + timedelta(days=time_gap):
#             prev_ts = current_ts
#         else:
#             if start_ts == prev_ts:
#                 ranges.append(start_ts.strftime("%Y-%m-%d %H:%M:%S"))
#             else:
#                 ranges.append(f"{start_ts.strftime('%Y-%m-%d %H:%M:%S')} to {prev_ts.strftime('%Y-%m-%d %H:%M:%S')}")
#             start_ts = current_ts
#             prev_ts = current_ts

#     # Add the last range
#     if start_ts == prev_ts:
#         ranges.append(start_ts.strftime("%Y-%m-%d %H:%M:%S"))
#     else:
#         ranges.append(f"{start_ts.strftime('%Y-%m-%d %H:%M:%S')} to {prev_ts.strftime('%Y-%m-%d %H:%M:%S')}")

#     return ranges

# # Find the timestamps with and without data
# ts_with_data, ts_without_data = find_ts_gaps(timestamps, time_gap)

# # Format the timestamp ranges
# ts_with_data_ranges = format_ts_ranges(ts_with_data)
# ts_without_data_ranges = format_ts_ranges(ts_without_data)

# # Print the results
# print("Timestamps with data:")
# for ts_range in ts_with_data_ranges:
#     print(ts_range)

# print("\nTimestamps without data:")
# for ts_range in ts_without_data_ranges:
#     print(ts_range)

########################################################################
#### no data print only

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
            gap_start = prev_ts + timedelta(days=1)
            gap_end = current_ts - timedelta(days=1)
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