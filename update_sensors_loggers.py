# -*- coding: utf-8 -*-
"""
Created on Fri Aug  2 18:30:02 2024

@author: nichm
"""


import configparser
import mysql.connector
from mysql.connector import Error
from datetime import datetime
import time
import sys
import os


def get_valid_logger_name(connection, cursor):
    while True:
        logger_name = input("Enter the logger name: ")

        query = """
        SELECT 1
        FROM commons_db.loggers
        WHERE logger_name = %s
        """
        cursor.execute(query, (logger_name,))
        result = cursor.fetchall()
        
        if result:
            return logger_name
        
        else:
            cursor.execute("SELECT logger_name, site_id FROM commons_db.loggers WHERE logger_name LIKE %s", (logger_name[:3] + '%',))
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
                    print("ay wow bago")
                    print(" ")
                    create_new_logger_entry(connection, cursor, logger_name)
                elif create_entry == '0':
                    print("No entry created. Exiting.")
                return None    
        
            else:
                print(f"Error: Logger with name '{logger_name}' not found. Please try again.")

def create_table_if_not_exists(table_name, cursor, create_table_query):
    # try:
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        if cursor.fetchone() is None:
            cursor.execute(create_table_query)
            print(f"Table {table_name} created successfully.")
    # finally:
    #     cursor.close()

def create_tilt_table(table_name, cursor):
    query = """
    CREATE TABLE `{}` (
      `data_id` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,
      `ts_written` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
      `ts` TIMESTAMP NULL,
      `node_id` TINYINT(3) UNSIGNED NULL,
      `type_num` TINYINT(3) UNSIGNED NULL,
      `xval` SMALLINT(6) NULL,
      `yval` SMALLINT(6) NULL,
      `zval` SMALLINT(6) NULL,
      `batt` FLOAT NULL,
      `is_live` TINYINT(4) NULL DEFAULT 1,
      PRIMARY KEY (`data_id`),
      UNIQUE INDEX `uq_tilt` (`ts` ASC, `node_id` ASC, `type_num` ASC)
    )
    """.format(table_name)
    create_table_if_not_exists(table_name, cursor, query)

def create_temp_table(table_name, cursor):
    query = """
    CREATE TABLE `{}` (
      `data_id` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,
      `ts` TIMESTAMP NULL,
      `node_id` TINYINT(3) UNSIGNED NULL,
      `type_num` TINYINT(3) UNSIGNED NULL,
      `temp_val` SMALLINT(6) NULL,
      PRIMARY KEY (`data_id`)
    )
    """.format(table_name)
    create_table_if_not_exists(table_name, cursor, query)

def create_volt_table(table_name, cursor):
    query = """
    CREATE TABLE `{}` (
      `data_id` INT(11) NOT NULL AUTO_INCREMENT,
      `ts` TIMESTAMP NULL,
      `stat` INT(2) NULL,
      `curr_draw` FLOAT NULL,
      `batt_volt` FLOAT NULL,
      PRIMARY KEY (`data_id`)
    )
    """.format(table_name)
    create_table_if_not_exists(table_name, cursor, query)

def create_rain_table(table_name, cursor):
    query = """
    CREATE TABLE `{}` (
      `data_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
      `ts_written` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
      `ts` timestamp NULL DEFAULT NULL,
      `rain` float DEFAULT NULL,
      `temperature` float DEFAULT NULL,
      `humidity` float DEFAULT NULL,
      `battery1` float DEFAULT NULL,
      `battery2` float DEFAULT NULL,
      `csq` tinyint(3) DEFAULT NULL,
      PRIMARY KEY (`data_id`),
      UNIQUE KEY `unique1` (`ts`)
    )
    """.format(table_name)
    create_table_if_not_exists(table_name, cursor, query)

def insert_into_rainfall_gauges(cursor, logger_name, date_activated, latitude, longitude):
    resolution = input("Enter the resolution for the rain gauge: ")
    insert_rain_gauge_query = """
    INSERT INTO analysis_db.rainfall_gauges (gauge_name, data_source, date_activated, latitude, longitude, resolution) 
    VALUES (%s, 'senslope', %s, %s, %s, %s)
    """
    cursor.execute(insert_rain_gauge_query, (logger_name, date_activated, latitude, longitude, resolution))
    print(f"Rain gauge entry created successfully for {logger_name}.")

def create_piezo_table(table_name, cursor):
    query = """
    CREATE TABLE `{}` (
      `data_id` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,
      `ts` TIMESTAMP NULL,
      `frequency_shift` DECIMAL(6,2) UNSIGNED NULL,
      `temperature` FLOAT NULL,
      PRIMARY KEY (`data_id`),
      UNIQUE INDEX `uq_piezo` (`ts`)
    )
    """.format(table_name)
    create_table_if_not_exists(table_name, cursor, query)

def create_soms_table(table_name, cursor):
    query = """
    CREATE TABLE `{}` (
      `data_id` INT(11) NOT NULL AUTO_INCREMENT,
      `ts` TIMESTAMP NULL,
      `node_id` INT(11) NULL,
      `type_num` INT(11) NULL,
      `mval1` INT(11) NULL,
      `mval2` INT(11) NULL,
      PRIMARY KEY (`data_id`),
      UNIQUE INDEX `uq_soms` (`ts` ASC, `node_id` ASC, `type_num` ASC)
    )
    """.format(table_name)
    create_table_if_not_exists(table_name, cursor, query)

def create_gnss_table(table_name, cursor):
    query = """
    CREATE TABLE `{}` (
      `data_id` INT(10) NOT NULL AUTO_INCREMENT,
      `ts` TIMESTAMP NULL,
      `fix_type` TINYINT(4) NULL,
      `latitude` DOUBLE NULL,
      `longitude` DOUBLE NULL,
      `altitude` DOUBLE NULL,
      `hdop` FLOAT NULL,
      `vdop` FLOAT NULL,
      `pdop` FLOAT NULL,
      PRIMARY KEY (`data_id`)
    )
    """.format(table_name)
    create_table_if_not_exists(table_name, cursor, query)

def validate_int(value_str, valid_values):
    return value_str.isdigit() and int(value_str) in valid_values

def create_new_logger_entry(connection, cursor, logger_name, schema="analysis_db"):
    while True:      
        cursor.execute("SELECT logger_name, site_id FROM commons_db.loggers WHERE logger_name LIKE %s", (logger_name[:3] + '%',))
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
        
        latitude = input("Enter the latitude (in decimal degrees): ")
        longitude = input("Enter the longitude (in decimal degrees): ")
        try:
            latitude = float(latitude)
            longitude = float(longitude)
        except ValueError:
            print("Error: Latitude and Longitude must be numbers.")
            continue
    
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
        
        has_piezo = 0
        # has_piezo = input("Has piezo? (0 or 1): ").strip()
        # while not validate_int(has_piezo, {0, 1}):
        #     print("Invalid input. Please enter 0 or 1.")
        #     has_piezo = input("Has piezo? (0 or 1): ").strip()
        # has_piezo = int(has_piezo)
        
        has_soms = 0
        # has_soms = input("Has soms? (0 or 1): ").strip()
        # while not validate_int(has_soms, {0, 1}):
        #     print("Invalid input. Please enter 0 or 1.")
        #     has_soms = input("Has soms? (0 or 1): ").strip()
        # has_soms = int(has_soms)
        
        has_gnss = input("Has gnss? (0 or 1): ").strip()
        while not validate_int(has_gnss, {0, 1}):
            print("Invalid input. Please enter 0 or 1.")
            has_gnss = input("Has gnss? (0 or 1): ").strip()
        has_gnss = int(has_gnss)
        
        has_stilt = 0
        # has_stilt = input("Has stilt? (0 or 1): ").strip()
        # while not validate_int(has_stilt, {0, 1}):
        #     print("Invalid input. Please enter 0 or 1.")
        #     has_stilt = input("Has stilt? (0 or 1): ").strip()
        # has_stilt = int(has_stilt)


        # logger_type = input("Enter logger type (1: masterbox, 2: arq, 3: regular, 4: router, 5: gateway): ").strip()
        logger_type = input("Enter logger type (1: router, 2: gateway): ").strip()
        if logger_type not in ['1', '2']:
            print("Error: Invalid logger type.")
            continue
        logger_type_map = {'1': 'router', '2': 'gateway'}

        model_query = """
        SELECT model_id
        FROM commons_db.logger_models
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
        INSERT INTO commons_db.loggers (logger_name, site_id, date_activated, date_deactivated, latitude, longitude, model_id)
        VALUES (%s, %s, %s, NULL, %s, %s, %s)
        """
        cursor.execute(insert_query, (logger_name, site_id, date_activated, latitude, longitude, model_id))        
                
        cursor.execute("SELECT logger_id FROM commons_db.loggers WHERE logger_name = %s", (logger_name,))
        logger_id = cursor.fetchone()[0]
        if logger_type in ['2']:
            sim_num = input("Enter the SIM number: ")
            if len(sim_num) != 12:
                print("Error: The SIM number exceeds the allowed length.")
                connection.rollback()  
                return

            gsm_id = 7      
            cursor.execute("SELECT 1 FROM comms_db.logger_mobile WHERE sim_num = %s", (sim_num,))
            if cursor.fetchone():
                print("Error: SIM number already exists in the logger_mobile table.")
                connection.rollback()
                return
    
            cursor.execute("SELECT COALESCE(MAX(mobile_id), 0) FROM comms_db.logger_mobile")
            last_mobile_id = cursor.fetchone()[0]
            new_mobile_id = last_mobile_id + 1
    
            insert_mobile_query = """
            INSERT INTO comms_db.logger_mobile (mobile_id, logger_id, sim_num, date_activated, gsm_id)
            VALUES (%s, %s, %s, %s, %s)
            """
            cursor.execute(insert_mobile_query, (new_mobile_id, logger_id, sim_num, date_activated, gsm_id))
        
        # Create tables based on sensor types
        if has_tilt:
            create_tilt_table(f"tilt_{logger_name}", cursor)
            create_temp_table(f"temp_{logger_name}", cursor)
            create_volt_table(f"volt_{logger_name}", cursor)
        if has_rain:
            create_rain_table(f"rain_{logger_name}", cursor)
            insert_into_rainfall_gauges(cursor, logger_name, date_activated, latitude, longitude)          

        # if has_piezo:
        #     create_piezo_table(f"piezo_{logger_name}", cursor)
        # if has_soms:
        #     create_soms_table(f"soms_{logger_name}", cursor)
        # if has_stilt:
        #     create_stilt_table(f"stilt_{logger_name}", cursor)
        if has_gnss:
            create_gnss_table(f"gnss_{logger_name}", cursor)
        # break
        
        connection.commit()
        print(f"New logger entry created successfully for {logger_name}.")
        return None


def update_logger_mobile_number(connection):
    cursor = connection.cursor()

    logger_name = get_valid_logger_name(connection, cursor)
    if not logger_name:
        return
    
    query = """
    SELECT l.logger_id, l.site_id, l.logger_name, lm.mobile_id, lm.date_activated, lm.date_deactivated, lm.sim_num, lm.gsm_id
    FROM commons_db.loggers AS l
    INNER JOIN comms_db.logger_mobile AS lm
    ON l.logger_id = lm.logger_id
    WHERE l.logger_name = %s
    """
    cursor.execute(query, (logger_name,))
    result = cursor.fetchone()

    if result:
        print("")
        print("Current details:")
        print(f"logger_id: {result[0]}")
        print(f"site_id: {result[1]}")
        print(f"logger_name: {result[2]}")
        print(f"mobile_id: {result[3]}")
        print(f"sim_num: {result[6]}")
        print("Recommended to use Case 1 or 3.")
        print("")

    else:
        print("Current details: No existing GSM entry found for the selected logger.")
        print("Recommended to use Case 2.")
        print("")

    print("Select the case:")
    print("1. Logger has existing GSM -> update sim_num and gsm_id")
    print("2. Logger with no GSM before: router to ARQ mode")
    print("3. Remove logger GSM: ARQ mode to router or decommission")
    print("")
    case = input("Enter the case number (1, 2, or 3). Enter x to exit: ")
    print("")

    try:
        if case == 1:
            if not result:
                # print("Error: No existing GSM entry found for the selected logger.")
                for _ in range(3):
                    print(".")
                    time.sleep(1)
                print("kakasabi lang eh")
                return

            sim_num = input("Enter the new SIM number: ")
            if len(sim_num) != 12:
                print("Error: SIM number must be exactly 12 digits.")
                return
            gsm_id = 7

            sim_check_query = "SELECT 1 FROM comms_db.logger_mobile WHERE sim_num = %s"
            cursor.execute(sim_check_query, (sim_num,))
            if cursor.fetchone():
                print("Error: SIM number already exists in the logger_mobile table.")
                return

            update_query = """
            UPDATE comms_db.logger_mobile
            SET sim_num = %s, gsm_id = %s
            WHERE mobile_id = %s
            """
            cursor.execute(update_query, (sim_num, gsm_id, result[3]))
            print(f"SIM number and GSM ID successfully updated for {logger_name}.")
            
            cursor.execute(query, (logger_name,))
            result = cursor.fetchone()
            print("Updated details:")
            print(f"logger_id: {result[0]}")
            print(f"site_id: {result[1]}")
            print(f"logger_name: {result[2]}")
            print(f"mobile_id: {result[3]}")
            print(f"sim_num: {result[6]}")
            

        elif case == 2:
            if not result or result[3] is None:               
                sim_num = input("Enter the SIM number (63XXXXXXXXXX): ")
                if len(sim_num) != 12:
                    print("Error: SIM number must be exactly 12 digits.")
                    return
                gsm_id = 7  # Hardcoded GSM ID value
                
                cursor.execute("SELECT logger_id FROM commons_db.loggers WHERE logger_name = %s LIMIT 1", (logger_name,))
                logger_id = cursor.fetchone()[0]
                
                cursor.execute("SELECT 1 FROM comms_db.logger_mobile WHERE sim_num = %s", (sim_num,))
                if cursor.fetchone():
                    print("Error: SIM number already exists in the logger_mobile table.")
                    return
                cursor.execute("SELECT COALESCE(MAX(mobile_id), 0) FROM comms_db.logger_mobile")
                last_mobile_id = cursor.fetchone()[0]
                new_mobile_id = last_mobile_id + 1
                insert_mobile_query = """
                INSERT INTO comms_db.logger_mobile (mobile_id, logger_id, sim_num, date_activated, gsm_id)
                VALUES (%s, %s, %s, %s, %s)
                """
                cursor.execute(insert_mobile_query, (new_mobile_id, logger_id, sim_num, datetime.now().strftime('%Y-%m-%d'), gsm_id))
                print("New entry created successfully in logger_mobile.")
                
                cursor.execute(query, (logger_name,))
                result = cursor.fetchone()
                print("Updated details:")
                print(f"logger_id: {result[0]}")
                print(f"site_id: {result[1]}")
                print(f"logger_name: {result[2]}")
                print(f"mobile_id: {result[3]}")
                print(f"sim_num: {result[6]}")
                
            else:
                # print("Logger already has a GSM entry. Please use case 1 to update it.")
                for _ in range(3):
                    print(".")
                    time.sleep(0.4)            
                print("magtatanong lang sana")        
                for _ in range(3):
                    time.sleep(0.4)  
                    print(".")     
                print("nasan ang 2 sa 1 at 3????")
                time.sleep(0.5) 


        elif case == 3:
            if not result:
                # print("Error: No existing GSM entry found for the selected logger.")
                for _ in range(3):
                    print(".")
                    time.sleep(.5)
                print("kulit neto eh")               
                return

            confirm_deactivation = input(f"Do you want to proceed with deactivating or decommissioning logger {logger_name}? Note that this will also delete the GSM details. (1 for Yes, 0 for No): ").strip()
            if confirm_deactivation == '1':
                for _ in range(3):
                    print(".")
                    time.sleep(0.4)
                r_u_sure = input("nagpaalam ka na ba kay kuya kennex? (1 for Yes, 0 for No): ").strip()
                if r_u_sure == '1':
                    enter_OTP = input("Enter OTP: ")
                    for _ in range(3):
                        sys.stdout.write(".")
                        sys.stdout.flush()
                        time.sleep(0.5)
                    print(" char lang >.<")           
                    deactivate_logger = input(f"Do you want to decommission logger {logger_name}? (1 for Yes, 0 for No): ").strip()
                    if deactivate_logger == '1':
                        date_deactivated = input("Enter the date deactivated (YYYY-MM-DD): ")
                        try:
                            datetime.strptime(date_deactivated, '%Y-%m-%d')
                        except ValueError:
                            print("Error: Invalid date format.")
                            return
                        update_query = """
                        UPDATE comms_db.logger_mobile
                        SET date_deactivated = %s, sim_num = NULL, gsm_id = NULL
                        WHERE mobile_id = %s
                        """
                        cursor.execute(update_query, (date_deactivated, result[3]))
                        connection.commit()  # Commit the changes
                        print(f"{logger_name} deactivated. GSM details deleted.")
                        
                        cursor.execute(query, (logger_name,))
                        result = cursor.fetchone()
                        print("Updated details:")
                        print(f"logger_id: {result[0]}")
                        print(f"site_id: {result[1]}")
                        print(f"logger_name: {result[2]}")
                        print(f"mobile_id: {result[3]}")
                        print(f"sim_num: {result[6]}")
                        
                    elif deactivate_logger == '0':
                        update_query = """
                        UPDATE comms_db.logger_mobile
                        SET sim_num = NULL, gsm_id = NULL
                        WHERE mobile_id = %s
                        """
                        cursor.execute(update_query, (result[3],))
                        connection.commit()  # Commit the changes
                        print(f"SIM number deleted for {logger_name}.")
                        
                        cursor.execute(query, (logger_name,))
                        result = cursor.fetchone()
                        print("Updated details:")
                        print(f"logger_id: {result[0]}")
                        print(f"site_id: {result[1]}")
                        print(f"logger_name: {result[2]}")
                        print(f"mobile_id: {result[3]}")
                        print(f"sim_num: {result[6]}")
                        
                    else:
                        print("Invalid input. No changes made.")
                        return
                    
                else:
                    for _ in range(3):
                       print(".")
                       time.sleep(0.5)
                    print("paalam ka muna")
                    return
                
            else:
                print("No changes made.")
                return
            
        elif case == "x":
            print("Exiting.")
            cursor.close()

        else:
            print("Error: Invalid case number selected.")
            return

        connection.commit()
        cursor.close()
        
    except ValueError:
        print("Error: Please enter a valid integer for the case number.")
        return

    except Exception as e:
        print(f"An error occurred: {e}")
        connection.rollback()
        

def get_db_config(config_file):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_file = os.path.join(script_dir, "config.cnf")
    
    config = configparser.ConfigParser()
    config.read(config_file)
    db_config = {
        "host": config["db"]["host"],
        "database": config["db"]["database"],
        "user": config["db"]["user"],
        "password": config["db"]["password"],
    }
    return db_config


def main():
    try:
        db_config = get_db_config("config.cnf")
        connection = mysql.connector.connect(**db_config)
        
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
