# -*- coding: utf-8 -*-
"""
Created on Wed Jul 31 21:25:53 2024

@author: nichm
"""

import mysql.connector
from mysql.connector import Error
from datetime import datetime

def get_valid_logger_name(cursor):
    while True:
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

    logger_name = get_valid_logger_name(cursor)

    print("Select the case:")
    print("1. Logger has existing GSM -> update sim_num and gsm_id")
    print("2. Logger with no GSM before: router to ARQ mode")
    print("3. Remove logger GSM: ARQ mode to router or decommission")
    print("")
    case = int(input("Enter the case number (1, 2, or 3): "))

    if case not in [1, 2, 3]:
        print("Invalid case number. Only cases 1, 2, and 3 are supported.")
        return

    query = """
    SELECT l.logger_id, l.site_id, l.logger_name, lm.mobile_id, lm.date_activated, lm.date_deactivated, lm.sim_num, lm.gsm_id
    FROM test_schema.loggers AS l
    INNER JOIN test_schema.logger_mobile AS lm
    ON l.logger_id = lm.logger_id
    WHERE l.logger_name = %s
    """
    cursor.execute(query, (logger_name,))
    result = cursor.fetchone()

    if case in [1, 3] and not result:
        print("Error: No existing GSM entry found for the selected logger.")
        return

    if case == 2:
        if not result or result[3] is None:
            new_sim_num = input("Enter the new SIM number: ")
            if len(new_sim_num) > 12:
                print("Error: The SIM number exceeds the allowed length.")
                return

            print("Select GSM ID:")
            print("4 - Globe1")
            print("6 - Globe2")
            print("5 - Smart1")
            print("7 - Smart2")
            gsm_id = int(input("Enter the GSM ID: "))
            print(" ")

            cursor.execute("SELECT logger_id FROM test_schema.loggers WHERE logger_name = %s", (logger_name,))
            logger_id = cursor.fetchone()[0]

            # Get the last mobile_id from the logger_mobile table
            cursor.execute("SELECT COALESCE(MAX(mobile_id), 0) FROM test_schema.logger_mobile")
            last_mobile_id = cursor.fetchone()[0]
            new_mobile_id = last_mobile_id + 1

            insert_query = """
            INSERT INTO test_schema.logger_mobile (mobile_id, logger_id, sim_num, gsm_id, date_activated)
            VALUES (%s, %s, %s, %s, %s)
            """
            cursor.execute(insert_query, (new_mobile_id, logger_id, new_sim_num, gsm_id, datetime.now().strftime('%Y-%m-%d')))
            connection.commit()
            print("New entry created successfully in logger_mobile.")
            
            cursor.execute(query, (logger_name,))
            result = cursor.fetchone()
            print("Updated details:")
            print(f"logger_id: {result[0]}")
            print(f"site_id: {result[1]}")
            print(f"logger_name: {result[2]}")
            print(f"mobile_id: {result[3]}")
            print(f"sim_num: {result[6]}")
            print(f"gsm_id: {result[7]}")
            print("- - - - - - - - - - -")

        else:
            print("Logger already has a GSM entry. Please use case 1 to update it.")

    elif case == 1:
        print("- - - - - - - - - - -")
        print("Current details:")
        print(f"logger_id: {result[0]}")
        print(f"site_id: {result[1]}")
        print(f"logger_name: {result[2]}")
        print(f"mobile_id: {result[3]}")
        print(f"sim_num: {result[6]}")
        print(f"gsm_id: {result[7]}")
        print(" ")
        
        new_sim_num = input("Enter the new SIM number: ")
        if len(new_sim_num) > 12:
            print("Error: The SIM number exceeds the allowed length.")
            return

        print("Select GSM ID:")
        print("4 - Globe1")
        print("6 - Globe2")
        print("5 - Smart1")
        print("7 - Smart2")
        gsm_id = int(input("Enter the GSM ID: "))
        print(" ")

        update_query = """
        UPDATE test_schema.logger_mobile
        SET sim_num = %s, gsm_id = %s
        WHERE mobile_id = %s
        """
        cursor.execute(update_query, (new_sim_num, gsm_id, result[3]))
        connection.commit()
        print(f"SIM number and GSM ID successfully updated for {logger_name}.")
        
        cursor.execute(query, (logger_name,))
        result = cursor.fetchone()
        print("Updated details:")
        print(f"logger_id: {result[0]}")
        print(f"site_id: {result[1]}")
        print(f"logger_name: {result[2]}")
        print(f"mobile_id: {result[3]}")
        print(f"sim_num: {result[6]}")
        print(f"gsm_id: {result[7]}")
        print("- - - - - - - - - - -")

    elif case == 3:
        print("- - - - - - - - - - -")
        print("Current details:")
        print(f"logger_id: {result[0]}")
        print(f"site_id: {result[1]}")
        print(f"logger_name: {result[2]}")
        print(f"mobile_id: {result[3]}")
        print(f"sim_num: {result[6]}")
        print(f"gsm_id: {result[7]}")
        print(" ")
        
        update_query = """
        UPDATE test_schema.logger_mobile
        SET sim_num = NULL, gsm_id = NULL
        WHERE mobile_id = %s
        """
        cursor.execute(update_query, (result[3],))
        connection.commit()
        print(f"SIM number and GSM ID deleted for {logger_name}.")
        
        cursor.execute(query, (logger_name,))
        result = cursor.fetchone()
        print("Updated details:")
        print(f"logger_id: {result[0]}")
        print(f"site_id: {result[1]}")
        print(f"logger_name: {result[2]}")
        print(f"mobile_id: {result[3]}")
        print(f"sim_num: {result[6]}")
        print(f"gsm_id: {result[7]}")
        print("- - - - - - - - - - -")

    cursor.close()

def main():
    try:
        #LOCAL
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
