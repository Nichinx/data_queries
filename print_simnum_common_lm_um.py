# -*- coding: utf-8 -*-
"""
Created on Wed Jul 31 21:13:57 2024

@author: nichm
"""

import mysql.connector
import pandas as pd

# Database connection
connection = mysql.connector.connect(
    host='192.168.150.112',
    user='pysys_local',
    password='NaCAhztBgYZ3HwTkvHwwGVtJn5sVMFgg',
    database='analysis_db'
)

# Create a cursor object
cursor = connection.cursor(dictionary=True)

# SQL query
query = '''
select u.user_id, u.first_name, u.last_name, um.mobile_id, mn.sim_num, lm.logger_id, lm.mobile_id as lm_mobile_id, lm.sim_num as lm_sim_num, l.logger_name
from commons_db.users as u
inner join comms_db.user_mobiles as um
on u.user_id = um.user_id
inner join comms_db.mobile_numbers as mn
on um.mobile_id = mn.mobile_id
inner join comms_db.logger_mobile as lm
on mn.mobile_id = lm.mobile_id
inner join commons_db.loggers as l
on lm.logger_id = l.logger_id
'''

# Execute the query
cursor.execute(query)

# Fetch all the rows
rows = cursor.fetchall()

# Convert the rows to a DataFrame
df = pd.DataFrame(rows)

# Filter the DataFrame where sim_num matches lm_sim_num
matching_df = df[df['sim_num'] == df['lm_sim_num']]

# Print the resulting DataFrame
print(matching_df)

# Close the cursor and connection
cursor.close()
connection.close()
