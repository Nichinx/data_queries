# -*- coding: utf-8 -*-
"""
Created on Fri Aug  2 18:30:02 2024

@author: nichm
"""

import mysql.connector
from mysql.connector import Error
from datetime import datetime

def get_valid_logger_name(connection, cursor):
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
            cursor.execute("SELECT logger_name, site_id FROM test_schema.loggers WHERE logger_name LIKE %s", (logger_name[:3] + '%',))
            matches = cursor.fetchall()

            if matches:
                print(f"Error: Logger with name '{logger_name}' not found. However, we found loggers with similar names:")
                
                site_ids = set()
                for match in matches:
                    print(f" - {match[0]}")
                    site_ids.add(match[1])
                
                if len(site_ids) != 1:
                    print("Error: Multiple or no unique site_id found for similar logger names.")
                    exit()

                # site_id = site_ids.pop()
                create_entry = input("Do you want to create a new entry for this logger? (1 for Yes, 0 for No): ").strip()
                if create_entry == '1':
                    create_new_logger_entry(connection, cursor, logger_name)
                elif create_entry == '0':
                    print("No entry created. Exiting.")
                return None    
        
            else:
                print(f"Error: Logger with name '{logger_name}' not found. Please try again.")

def validate_int(value_str, valid_values):
    return value_str.isdigit() and int(value_str) in valid_values

def create_new_logger_entry(connection, cursor, logger_name):
    while True:
        cursor.execute("SELECT logger_name, site_id FROM test_schema.loggers WHERE logger_name LIKE %s", (logger_name[:3] + '%',))
        matches = cursor.fetchall()
        
        site_ids = set()
        for match in matches:
            site_ids.add(match[1])     
        site_id = site_ids.pop()
        
        date_activated = input("Enter the date activated (YYYY-MM-DD): ")
        try:
            datetime.strptime(date_activated, '%Y-%m-%d')
        except ValueError:
            print("Error: Invalid date format.")
            continue
        
        latitude = input("Enter the latitude: ")
        longitude = input("Enter the longitude: ")
        try:
            latitude = float(latitude)
            longitude = float(longitude)
        except ValueError:
            print("Error: Latitude and Longitude must be numbers.")
            continue

        # has_tilt = input("Has tilt? (1/0): ")
        # has_rain = input("Has rain? (1/0): ")
        # has_piezo = input("Has piezo? (1/0): ")
        # has_soms = input("Has soms? (1/0): ")
        # has_gnss = input("Has gnss? (1/0): ")
        # has_stilt = input("Has stilt? (1/0): ")

        # if not all(x in ['0', '1'] for x in [has_tilt, has_rain, has_piezo, has_soms, has_gnss, has_stilt]):
        #     print("Error: Features must be either 0 or 1.")
        #     continue
    
        has_tilt = input("Has tilt? (0 or 1): ").strip()
        while not validate_int(has_tilt, {0, 1}):
            print("Invalid input. Please enter 0 or 1.")
            has_tilt = input("Has tilt? (0 or 1): ").strip()
        has_tilt = int(has_tilt)
        
        has_rain = input("Has rain? (0 or 1): ").strip()
        while not validate_int(has_rain, {0, 1}):
            print("Invalid input. Please enter 0 or 1.")
            has_rain = input("Has rain? (0 or 1): ").strip()
        has_rain = int(has_rain)
        
        has_piezo = input("Has piezo? (0 or 1): ").strip()
        while not validate_int(has_piezo, {0, 1}):
            print("Invalid input. Please enter 0 or 1.")
            has_piezo = input("Has piezo? (0 or 1): ").strip()
        has_piezo = int(has_piezo)
        
        has_soms = input("Has soms? (0 or 1): ").strip()
        while not validate_int(has_soms, {0, 1}):
            print("Invalid input. Please enter 0 or 1.")
            has_soms = input("Has soms? (0 or 1): ").strip()
        has_soms = int(has_soms)
        
        has_gnss = input("Has gnss? (0 or 1): ").strip()
        while not validate_int(has_gnss, {0, 1}):
            print("Invalid input. Please enter 0 or 1.")
            has_gnss = input("Has gnss? (0 or 1): ").strip()
        has_gnss = int(has_gnss)
        
        has_stilt = input("Has stilt? (0 or 1): ").strip()
        while not validate_int(has_stilt, {0, 1}):
            print("Invalid input. Please enter 0 or 1.")
            has_stilt = input("Has stilt? (0 or 1): ").strip()
        has_stilt = int(has_stilt)


        logger_type = input("Enter logger type (1: masterbox, 2: arq, 3: regular, 4: router, 5: gateway): ").strip()
        if logger_type not in ['1', '2', '3', '4', '5']:
            print("Error: Invalid logger type.")
            continue

        logger_type_map = {'1': 'masterbox', '2': 'arq', '3': 'regular', '4': 'router', '5': 'gateway'}

        model_query = """
        SELECT model_id
        FROM test_schema.logger_models
        WHERE has_tilt = %s AND has_rain = %s AND has_piezo = %s AND has_soms = %s AND has_gnss = %s AND has_stilt = %s AND logger_type = %s
        ORDER BY version DESC
        LIMIT 1
        """
        cursor.execute(model_query, (has_tilt, has_rain, has_piezo, has_soms, has_gnss, has_stilt, logger_type_map[logger_type]))
        model_result = cursor.fetchone()
        if not model_result:
            print("Error: No matching model found.")
            continue

        model_id = model_result[0]

        insert_query = """
        INSERT INTO test_schema.loggers (logger_name, site_id, date_activated, date_deactivated, latitude, longitude, model_id)
        VALUES (%s, %s, %s, NULL, %s, %s, %s)
        """
        cursor.execute(insert_query, (logger_name, site_id, date_activated, latitude, longitude, model_id))        
                
        cursor.execute("SELECT logger_id FROM test_schema.loggers WHERE logger_name = %s", (logger_name,))
        logger_id = cursor.fetchone()[0]

        if logger_type in ['2', '5']:
            # Check for duplicate SIM number
            sim_num = input("Enter the SIM number: ")
            if len(sim_num) > 12:
                print("Error: The SIM number exceeds the allowed length.")
                connection.rollback()  # Rollback changes if SIM number is invalid
                return

            print("Select GSM ID:")
            print("4 - Globe1")
            print("6 - Globe2")
            print("5 - Smart1")
            print("7 - Smart2")
            gsm_id = int(input("Enter the GSM ID: "))
            print(" ")
            
            cursor.execute("SELECT 1 FROM test_schema.logger_mobile WHERE sim_num = %s", (sim_num,))
            if cursor.fetchone():
                print("Error: SIM number already exists in the logger_mobile table.")
                connection.rollback()
                return
    
            # Get the last mobile_id from the logger_mobile table
            cursor.execute("SELECT COALESCE(MAX(mobile_id), 0) FROM test_schema.logger_mobile")
            last_mobile_id = cursor.fetchone()[0]
            new_mobile_id = last_mobile_id + 1
    
            insert_mobile_query = """
            INSERT INTO test_schema.logger_mobile (mobile_id, logger_id, sim_num, date_activated, gsm_id)
            VALUES (%s, %s, %s, %s, %s)
            """
            cursor.execute(insert_mobile_query, (new_mobile_id, logger_id, sim_num, date_activated, gsm_id))
        
        connection.commit()
        print(f"New logger entry created successfully for {logger_name}.")
        return None


def update_logger_mobile_number(connection):
    cursor = connection.cursor()

    logger_name = get_valid_logger_name(connection, cursor)
    
    query = """
    SELECT l.logger_id, l.site_id, l.logger_name, lm.mobile_id, lm.date_activated, lm.date_deactivated, lm.sim_num, lm.gsm_id
    FROM test_schema.loggers AS l
    INNER JOIN test_schema.logger_mobile AS lm
    ON l.logger_id = lm.logger_id
    WHERE l.logger_name = %s
    """
    cursor.execute(query, (logger_name,))
    result = cursor.fetchone()

    if logger_name is not None:

        print("Select the case:")
        print("1. Logger has existing GSM -> update sim_num and gsm_id")
        print("2. Logger with no GSM before: router to ARQ mode")
        print("3. Remove logger GSM: ARQ mode to router or decommission")
        print("")
        case = int(input("Enter the case number (1, 2, or 3): "))
    
        if case not in [1, 2, 3]:
            print("Invalid case number. Only cases 1, 2, and 3 are supported.")
            return
    
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
    
                cursor.execute("SELECT COALESCE(MAX(mobile_id), 0) FROM test_schema.logger_mobile")
                last_mobile_id = cursor.fetchone()[0]
                new_mobile_id = last_mobile_id + 1
    
                sim_check_query = "SELECT 1 FROM test_schema.logger_mobile WHERE sim_num = %s"
                cursor.execute(sim_check_query, (new_sim_num,))
                if cursor.fetchone():
                    print("Error: SIM number already exists.")
                    return
    
                insert_query = """
                INSERT INTO test_schema.logger_mobile (mobile_id, logger_id, sim_num, gsm_id, date_activated)
                VALUES (%s, %s, %s, %s, %s)
                """
                cursor.execute(insert_query, (new_mobile_id, logger_id, new_sim_num, gsm_id, datetime.now().strftime('%Y-%m-%d')))
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
    
            sim_check_query = "SELECT 1 FROM test_schema.logger_mobile WHERE sim_num = %s"
            cursor.execute(sim_check_query, (new_sim_num,))
            if cursor.fetchone():
                print("Error: SIM number already exists.")
                return
    
            update_query = """
            UPDATE test_schema.logger_mobile
            SET sim_num = %s, gsm_id = %s
            WHERE mobile_id = %s
            """
            cursor.execute(update_query, (new_sim_num, gsm_id, result[3]))
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
    
    connection.commit()
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
