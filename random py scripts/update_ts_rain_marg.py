# -*- coding: utf-8 -*-
"""
Created on Thu Jun 20 23:45:54 2024

@author: nichm
"""

import mysql.connector as connector
from datetime import datetime, timedelta
import time

def update_latest_data():
    # Connect to the database
    db = connector.connect(
        host="192.168.150.112",
        database="analysis_db",
        user="pysys_local",
        password="NaCAhztBgYZ3HwTkvHwwGVtJn5sVMFgg"
    )

    cursor = db.cursor()

    # Fetch rows where ts_written > '2024-06-20' and ts is not '2024'
    select_query = """
                    SELECT data_id, ts FROM rain_marg
                    WHERE ts_written > '2024-06-20' AND YEAR(ts) != 2024
                    """
    cursor.execute(select_query)
    rows = cursor.fetchall()

    # Get current timestamp for logging
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for row in rows:
        data_id, ts = row
        # Ensure ts is a string in the expected format '%Y-%m-%d %H:%M:%S'
        if isinstance(ts, datetime):
            ts = ts.strftime('%Y-%m-%d %H:%M:%S')  # Convert datetime to string

        # Convert ts to a datetime object
        ts_datetime = datetime.strptime(ts, '%Y-%m-%d %H:%M:%S')

        # Replace the year with 2024
        ts_new = ts_datetime.replace(year=2024)
        # Convert back to string
        ts_new_str = ts_new.strftime('%Y-%m-%d %H:%M:%S')
        
        # Prepare the update query
        update_query = """
                       UPDATE rain_marg SET ts = %s WHERE data_id = %s
                       """
        
        # Print the timestamp of execution, update query, and new ts value
        print(f"{current_time}: Executing update query: {update_query} with ts = {ts_new_str} for data_id = {data_id}")

        # Execute the update query
        cursor.execute(update_query, (ts_new_str, data_id))
        
        # Print out the updated data
        print(f"{current_time}: Updated data_id {data_id}: ts updated from {ts} to {ts_new_str}")

    # Commit the changes outside the loop
    db.commit()

    # Close the cursor and connection
    cursor.close()
    db.close()

# Run the script every minute
while True:
    update_latest_data()
    # Wait for 1 minute (60 seconds)
    time.sleep(60)
