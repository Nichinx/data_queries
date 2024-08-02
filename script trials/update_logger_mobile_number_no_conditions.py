# -*- coding: utf-8 -*-
"""
Created on Fri Jul 26 17:45:07 2024

@author: nichm
"""

import mysql.connector
from mysql.connector import Error

def get_valid_logger_name(cursor):
    while True:
        # Prompt for logger name
        logger_name = input("Enter the logger name: ")

        # Query to check if logger exists
        query = """
        SELECT 1
        FROM test_schema.loggers
        WHERE logger_name = %s
        """
        cursor.execute(query, (logger_name,))
        result = cursor.fetchone()
        
        if result:
            return logger_name
        else:
            print(f"Error: Logger with name '{logger_name}' not found. Please try again.")

def update_logger_mobile_number(connection):
    cursor = connection.cursor()

    # Ensure a valid logger name is entered
    logger_name = get_valid_logger_name(cursor)

    # Ask for the case
    print("Select the case:")
    print("1. Logger has existing GSM -> update sim_num and gsm_id")
    print("2. Logger with no GSM before: router to ARQ mode")
    print("3. Remove logger GSM: ARQ mode to router or decommission")
    case = int(input("Enter the case number (1, 2, or 3): "))

    if case not in [1, 2, 3]:
        print("Invalid case number. Only cases 1, 2, and 3 are supported.")
        return

    # Query to get current details
    query = """
    SELECT l.logger_id, l.site_id, l.logger_name, lm.mobile_id, lm.sim_num, lm.gsm_id
    FROM test_schema.loggers AS l
    INNER JOIN test_schema.logger_mobile AS lm
    ON l.logger_id = lm.logger_id
    WHERE l.logger_name = %s
    """
    cursor.execute(query, (logger_name,))
    result = cursor.fetchone()
    
    if result:
        # Print details in a readable format
        print("- - - - - - - - - - - - - ")
        print("Current details:")
        print(f"logger_id: {result[0]}")
        print(f"site_id: {result[1]}")
        print(f"logger_name: {result[2]}")
        print(f"mobile_id: {result[3]}")
        print(f"sim_num: {result[4]}")
        print(f"gsm_id: {result[5]}")
        print("- - - - - - - - - - - - - ")
        
        if case in [1, 2]:
            # Prompt for new SIM number
            new_sim_num = input("Enter the new SIM number: ")

            # Prompt for GSM ID
            print("Select GSM ID:")
            print("4 - Globe1")
            print("6 - Globe2")
            print("5 - Smart1")
            print("7 - Smart2")
            gsm_id = int(input("Enter the GSM ID: "))

            # Update SIM number and GSM ID
            update_query = """
            UPDATE test_schema.logger_mobile
            SET sim_num = %s, gsm_id = %s
            WHERE mobile_id = %s
            """
            cursor.execute(update_query, (new_sim_num, gsm_id, result[3]))

        elif case == 3:
            # Update SIM number and GSM ID to NULL
            update_query = """
            UPDATE test_schema.logger_mobile
            SET sim_num = NULL, gsm_id = NULL
            WHERE mobile_id = %s
            """
            cursor.execute(update_query, (result[3],))

        connection.commit()
        print("Update successful.")
    
    cursor.close()

def main():
    try:
        # Connect to the database
        connection = mysql.connector.connect(
            host="localhost",
            database="new_schema",
            user="root",
            password="admin123"
        )
        if connection.is_connected():
            print("Connected to the database.")
            update_logger_mobile_number(connection)
    
    except Error as e:
        print("Error while connecting to MySQL", e)
    
    finally:
        if connection.is_connected():
            connection.close()
            print("MySQL connection is closed.")

if __name__ == "__main__":
    main()
