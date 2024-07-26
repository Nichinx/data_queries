# -*- coding: utf-8 -*-
"""
Created on Fri Jul 26 18:31:55 2024

@author: nichm
"""

import mysql.connector
from mysql.connector import Error
from datetime import datetime

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

def create_mobile_entry(cursor):
    print("Creating new entry in mobile_numbers table.")

    # Prompt for new details
    new_sim_num = input("Enter the new SIM number: ")

    # Validate length of the new SIM number
    if len(new_sim_num) > 20:  # Adjust length as needed
        print("Error: The SIM number exceeds the allowed length.")
        return None

    print("Select GSM ID:")
    print("4 - Globe1")
    print("6 - Globe2")
    print("5 - Smart1")
    print("7 - Smart2")
    gsm_id = int(input("Enter the GSM ID: "))

    # Insert new entry into mobile_numbers
    insert_query = """
    INSERT INTO test_schema.mobile_numbers (sim_num, gsm_id)
    VALUES (%s, %s)
    """
    cursor.execute(insert_query, (new_sim_num, gsm_id))

    # Get the newly created mobile_id
    cursor.execute("SELECT LAST_INSERT_ID()")
    mobile_id = cursor.fetchone()[0]
    
    return mobile_id, new_sim_num, gsm_id

def update_logger_mobile_number(connection):
    cursor = connection.cursor()

    # Ensure a valid logger name is entered
    logger_name = get_valid_logger_name(cursor)

    # Ask for the case
    print("Select the case:")
    print("1. Logger has existing GSM -> update sim_num and gsm_id")
    print("2. Logger with no GSM before: router to ARQ mode")
    print("3. Remove logger GSM: ARQ mode to router or decommission")
    print("4. Delete logger/mobile entry")
    print("")
    case = int(input("Enter the case number (1, 2, 3, or 4): "))

    if case not in [1, 2, 3, 4]:
        print("Invalid case number. Only cases 1, 2, 3, and 4 are supported.")
        return

    # Query to get current details
    query = """
    SELECT l.logger_id, l.site_id, l.logger_name, lm.mobile_id, lm.date_activated, lm.date_deactivated, lm.sim_num, lm.gsm_id
    FROM test_schema.loggers AS l
    LEFT JOIN test_schema.logger_mobile AS lm
    ON l.logger_id = lm.logger_id
    WHERE l.logger_name = %s
    """
    cursor.execute(query, (logger_name,))
    result = cursor.fetchone()

    if case in [1, 3, 4]:
        if not result:
            print("Error: No existing GSM entry found for the selected logger.")
            return

    if case == 2:
        if not result or result[3] is None:
            # Logger exists but no entry in logger_mobile, so create one
            mobile_entry = create_mobile_entry(cursor)
            if mobile_entry:
                mobile_id, new_sim_num, gsm_id = mobile_entry
                
                # Get logger_id from loggers table
                cursor.execute("SELECT logger_id FROM test_schema.loggers WHERE logger_name = %s", (logger_name,))
                logger_id = cursor.fetchone()[0]

                # Insert into logger_mobile
                insert_query = """
                INSERT INTO test_schema.logger_mobile (logger_id, mobile_id, sim_num, gsm_id, date_activated)
                VALUES (%s, %s, %s, %s, %s)
                """
                cursor.execute(insert_query, (logger_id, mobile_id, new_sim_num, gsm_id, datetime.now().strftime('%Y-%m-%d')))

                connection.commit()
                print("New entry created successfully in logger_mobile.")

        else:
            # Handle existing logger_mobile entry
            # Print details in a readable format
            print("- - - - - - - - - - -")
            print("Current details:")
            print(f"logger_id: {result[0]}")
            print(f"site_id: {result[1]}")
            print(f"logger_name: {result[2]}")
            print(f"mobile_id: {result[3]}")
            print(f"sim_num: {result[4]}")
            print(f"gsm_id: {result[5]}")
            print("- - - - - - - - - - -")

            # Prompt for new SIM number
            new_sim_num = input("Enter the new SIM number: ")

            # Validate length of the new SIM number
            if len(new_sim_num) > 12:  # Adjust length as needed
                print("Error: The SIM number exceeds the allowed length.")
                return

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
            cursor.execute(update_query, (new_sim_num, gsm_id if case == 1 else None, result[3]))
            print(f"SIM number successfully updated for {logger_name}.")

    elif case == 1:
        if result:
            # Print details in a readable format
            print("- - - - - - - - - - -")
            print("Current details:")
            print(f"logger_id: {result[0]}")
            print(f"site_id: {result[1]}")
            print(f"logger_name: {result[2]}")
            print(f"mobile_id: {result[3]}")
            print(f"sim_num: {result[4]}")
            print(f"gsm_id: {result[5]}")
            print("- - - - - - - - - - -")
            
            # Prompt for new SIM number
            new_sim_num = input("Enter the new SIM number: ")

            # Validate length of the new SIM number
            if len(new_sim_num) > 12:  # Adjust length as needed
                print("Error: The SIM number exceeds the allowed length.")
                return

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
            print(f"SIM number successfully updated for {logger_name}.")

    elif case == 3:
        if result:
            print("- - - - - - - - - - -")
            print("Current details:")
            print(f"logger_id: {result[0]}")
            print(f"site_id: {result[1]}")
            print(f"logger_name: {result[2]}")
            print(f"mobile_id: {result[3]}")
            print(f"sim_num: {result[4]}")
            print(f"gsm_id: {result[5]}")
            print("- - - - - - - - - - -")
            
            # Update SIM number and GSM ID to NULL
            update_query = """
            UPDATE test_schema.logger_mobile
            SET sim_num = NULL, gsm_id = NULL
            WHERE mobile_id = %s
            """
            cursor.execute(update_query, (result[3],))
            print(f"SIM num deleted for {logger_name}.")

    elif case == 4:
        if result:
            print("- - - - - - - - - - -")
            print("Current details:")
            print(f"logger_id: {result[0]}")
            print(f"site_id: {result[1]}")
            print(f"logger_name: {result[2]}")
            print(f"mobile_id: {result[3]}")
            print(f"sim_num: {result[4]}")
            print(f"gsm_id: {result[5]}")
            print("- - - - - - - - - - -")
            
            # Delete from logger_mobile
            delete_logger_mobile_query = """
            DELETE FROM test_schema.logger_mobile
            WHERE mobile_id = %s
            """
            cursor.execute(delete_logger_mobile_query, (result[3],))

            # Delete from mobile_numbers
            delete_mobile_numbers_query = """
            DELETE FROM test_schema.mobile_numbers
            WHERE mobile_id = %s
            """
            # Delete from mobile_numbers
            delete_mobile_numbers_query = """
            DELETE FROM test_schema.mobile_numbers
            WHERE mobile_id = %s
            """
            cursor.execute(delete_mobile_numbers_query, (result[3],))

            # Commit changes
            connection.commit()
            print(f"Logger and corresponding mobile entry successfully deleted for {logger_name}.")
        else:
            print("Error: No existing GSM entry found for the selected logger.")

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

