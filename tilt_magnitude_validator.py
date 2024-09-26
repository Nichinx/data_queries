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

# Establish the database connection
conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()

# Check the number_of_segments and version in tsm_sensors by matching logger_name (tsm_name)
tsm_query = f"""
    SELECT tsm_name, number_of_segments, date_deactivated, version
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

# Get the number_of_segments and version from the first active entry
number_of_segments = active_tsm[0][1]
version = active_tsm[0][3]
print(f"Number of segments for '{logger_name}': {number_of_segments}, Version: {version}")

# Determine valid type_nums based on version
if version == 2:
    valid_type_nums = [32, 33]
elif version == 3:
    valid_type_nums = [11, 12]
elif version == 4:
    valid_type_nums = [41, 42]
elif version == 5:
    valid_type_nums = [51, 52]

# Get the current date and calculate the timedelta of 6 months (182.5 days)
now = datetime.now()
time_delta = timedelta(days=180)

# SQL query to fetch data from tilt_xxxx where node_id <= number_of_segments and type_num matches valid_type_nums
tilt_query = f"""
    SELECT * FROM analysis_db.tilt_{logger_name}
    WHERE node_id <= {number_of_segments} 
    AND type_num IN ({', '.join(map(str, valid_type_nums))}) 
    AND ts > '2022-01-01'
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

# Check if the last ts is older than ___ months
last_ts_per_node['time_diff'] = now - last_ts_per_node['last_ts']
no_data_nodes = last_ts_per_node[last_ts_per_node['time_diff'] > time_delta]

# Report node_id and type_num with no data in the last ___ months
if not no_data_nodes.empty:
    print("\nNo data since the following timestamps (more than 6 months old):")
    for index, row in no_data_nodes.iterrows():
        print(f"node_id: {row['node_id']}, type_num: {row['type_num']}, last ts: {row['last_ts']}")

# Filter rows to only include data from the last ___ months
df = df[df['ts'] >= (now - time_delta)]

# Adjust accelerometer values based on type_num conditions
def adjust_accelerometer_values(df):
    # Ensure type_num is treated as a string
    df['type_num'] = df['type_num'].astype(str)
    overshoot_condition = (df.xval < -2970) & (df.xval > -3072)
    
    if df.type_num.str.contains('32').any() or df.type_num.str.contains('33').any():
        df.loc[overshoot_condition, 'xval'] += 4096
        df.loc[overshoot_condition, 'yval'] += 4096
        df.loc[overshoot_condition, 'zval'] += 4096

    if df.type_num.str.contains('11').any() or df.type_num.str.contains('12').any():
        df.loc[overshoot_condition, 'xval'] += 4096
        df.loc[overshoot_condition, 'yval'] += 4096
        df.loc[overshoot_condition, 'zval'] += 4096

    if df.type_num.str.contains('41').any() or df.type_num.str.contains('42').any():
        df.loc[overshoot_condition, 'xval'] += 4096
        df.loc[overshoot_condition, 'yval'] += 4096
        df.loc[overshoot_condition, 'zval'] += 4096

    if df.type_num.str.contains('51').any() or df.type_num.str.contains('52').any():
        df.loc[df.xval < -32768, 'xval'] += 65536
        df.loc[df.xval < -32768, 'yval'] += 65536
        df.loc[df.xval < -32768, 'zval'] += 65536

    # Set values out of range to NaN
    xval_out_of_range = df.loc[abs(df.xval) > 13158]
    yval_out_of_range = df.loc[abs(df.yval) > 13158]
    zval_out_of_range = df.loc[abs(df.zval) > 13158]

    df.loc[abs(df.xval) > 1126, 'xval'] = np.nan
    df.loc[abs(df.yval) > 1126, 'yval'] = np.nan
    df.loc[abs(df.zval) > 1126, 'zval'] = np.nan

    return xval_out_of_range, yval_out_of_range, zval_out_of_range

# Adjust values and capture out of range data
xval_out_of_range, yval_out_of_range, zval_out_of_range = adjust_accelerometer_values(df)

# Print node_id and type_num for out-of-range values
out_of_range_data = pd.concat([xval_out_of_range[['node_id', 'type_num']],
                                yval_out_of_range[['node_id', 'type_num']],
                                zval_out_of_range[['node_id', 'type_num']]]).drop_duplicates()

if not out_of_range_data.empty:
    print("\nThe following node_ids and type_nums have values outside the specified ranges:")
    for index, row in out_of_range_data.iterrows():
        print(f"node_id: {row['node_id']}, type_num: {row['type_num']}")


# Compute the magnitude: mag = sqrt(xval^2 + yval^2 + zval^2) / divisor
def compute_magnitude(row):
    if row['type_num'] in [51, 52]:
        divisor = 13158
    else:
        divisor = 1024
    return np.sqrt(row['xval']**2 + row['yval']**2 + row['zval']**2) / divisor

# Apply the magnitude computation
df['magnitude'] = df.apply(compute_magnitude, axis=1)

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


# def detect_fluctuations(df, fluctuation_threshold=2):
#     # Convert the timestamp to datetime if not already
#     df['ts'] = pd.to_datetime(df['ts'])

#     # Sort by timestamp and group by node_id and type_num
#     df = df.sort_values(by='ts')
#     grouped = df.groupby(['node_id', 'type_num'])

#     fluctuation_detected = set()  # To store unique node_id, type_num pairs with fluctuations
#     for (node_id, type_num), group in grouped:
#         group = group.set_index('ts')

#         # print(f"\nChecking for fluctuations for node_id: {node_id}, type_num: {type_num}")

#         # Calculate rolling mean and rolling std (daily, weekly, monthly)
#         # group['xval_rolling_mean_1D'] = group['xval'].rolling(window='1D').mean()  # Daily
#         # group['xval_rolling_std_1D'] = group['xval'].rolling(window='1D').std()

#         group['xval_rolling_mean_7D'] = group['xval'].rolling(window='7D').mean()  # Weekly
#         group['xval_rolling_std_7D'] = group['xval'].rolling(window='7D').std()

#         group['xval_rolling_mean_30D'] = group['xval'].rolling(window='30D').mean()  # Monthly
#         group['xval_rolling_std_30D'] = group['xval'].rolling(window='30D').std()

#         # Repeat the same for yval and zval
#         # group['yval_rolling_mean_1D'] = group['yval'].rolling(window='1D').mean()  # Daily
#         # group['yval_rolling_std_1D'] = group['yval'].rolling(window='1D').std()

#         group['yval_rolling_mean_7D'] = group['yval'].rolling(window='7D').mean()  # Weekly
#         group['yval_rolling_std_7D'] = group['yval'].rolling(window='7D').std()

#         group['yval_rolling_mean_30D'] = group['yval'].rolling(window='30D').mean()  # Monthly
#         group['yval_rolling_std_30D'] = group['yval'].rolling(window='30D').std()

#         # group['zval_rolling_mean_1D'] = group['zval'].rolling(window='1D').mean()  # Daily
#         # group['zval_rolling_std_1D'] = group['zval'].rolling(window='1D').std()

#         group['zval_rolling_mean_7D'] = group['zval'].rolling(window='7D').mean()  # Weekly
#         group['zval_rolling_std_7D'] = group['zval'].rolling(window='7D').std()

#         group['zval_rolling_mean_30D'] = group['zval'].rolling(window='30D').mean()  # Monthly
#         group['zval_rolling_std_30D'] = group['zval'].rolling(window='30D').std()

#         # Calculate Z-scores for xval, yval, zval (daily, weekly, monthly)
#         # group['xval_zscore_1D'] = (group['xval'] - group['xval_rolling_mean_1D']) / group['xval_rolling_std_1D']
#         group['xval_zscore_7D'] = (group['xval'] - group['xval_rolling_mean_7D']) / group['xval_rolling_std_7D']
#         group['xval_zscore_30D'] = (group['xval'] - group['xval_rolling_mean_30D']) / group['xval_rolling_std_30D']

#         # group['yval_zscore_1D'] = (group['yval'] - group['yval_rolling_mean_1D']) / group['yval_rolling_std_1D']
#         group['yval_zscore_7D'] = (group['yval'] - group['yval_rolling_mean_7D']) / group['yval_rolling_std_7D']
#         group['yval_zscore_30D'] = (group['yval'] - group['yval_rolling_mean_30D']) / group['yval_rolling_std_30D']

#         # group['zval_zscore_1D'] = (group['zval'] - group['zval_rolling_mean_1D']) / group['zval_rolling_std_1D']
#         group['zval_zscore_7D'] = (group['zval'] - group['zval_rolling_mean_7D']) / group['zval_rolling_std_7D']
#         group['zval_zscore_30D'] = (group['zval'] - group['zval_rolling_mean_30D']) / group['zval_rolling_std_30D']

#         # Detect fluctuations (set a Z-score threshold for significant fluctuation)
#         group['xval_fluctuation'] = (group['xval_zscore_7D'].abs() > fluctuation_threshold) | \
#                                     (group['xval_zscore_30D'].abs() > fluctuation_threshold)

#         group['yval_fluctuation'] = (group['yval_zscore_7D'].abs() > fluctuation_threshold) | \
#                                     (group['yval_zscore_30D'].abs() > fluctuation_threshold)

#         group['zval_fluctuation'] = (group['zval_zscore_7D'].abs() > fluctuation_threshold) | \
#                                     (group['zval_zscore_30D'].abs() > fluctuation_threshold)

#         # # Filter rows where fluctuations occurred for xval, yval, zval
#         # fluctuation_rows = group[(group['xval_fluctuation']) | 
#         #                           (group['yval_fluctuation']) | 
#         #                           (group['zval_fluctuation'])]
            
#         # # Print out node_id and type_num where fluctuations occur
#         # if not fluctuation_rows.empty:
#         #     for idx, row in fluctuation_rows.iterrows():
#         #         print(f"Fluctuation detected at node_id: {node_id}, type_num: {type_num} at {idx}")
        
#         # Check if any fluctuations occurred
#         if group[['xval_fluctuation', 'yval_fluctuation', 'zval_fluctuation']].any().any():
#             fluctuation_detected.add((node_id, type_num))  # Add to the set to ensure uniqueness
            
#     # Print the unique node_id and type_num where fluctuations were detected
#     for node_id, type_num in fluctuation_detected:
#         print(f"Fluctuation detected for node_id: {node_id}, type_num: {type_num}")


def detect_fluctuations(df, fluctuation_threshold=4):
    # Convert the timestamp to datetime if not already
    df['ts'] = pd.to_datetime(df['ts'])

    # Sort by timestamp and group by node_id and type_num
    df = df.sort_values(by='ts')
    grouped = df.groupby(['node_id', 'type_num'])

    fluctuation_detected = set()  # To store unique node_id, type_num pairs with fluctuations
    for (node_id, type_num), group in grouped:
        group = group.set_index('ts')

        # Calculate rolling std for monthly (30D) intervals
        group['xval_rolling_std_30D'] = group['xval'].rolling(window='30D').std()
        group['yval_rolling_std_30D'] = group['yval'].rolling(window='30D').std()
        group['zval_rolling_std_30D'] = group['zval'].rolling(window='30D').std()

        # Define fluctuations as std exceeding the threshold
        x_fluctuation = group['xval_rolling_std_30D'] > fluctuation_threshold
        y_fluctuation = group['yval_rolling_std_30D'] > fluctuation_threshold
        z_fluctuation = group['zval_rolling_std_30D'] > fluctuation_threshold

        if x_fluctuation.any() or y_fluctuation.any() or z_fluctuation.any():
            fluctuation_detected.add((node_id, type_num))

    return pd.DataFrame(list(fluctuation_detected), columns=['node_id', 'type_num'])



########applying filters
def outlier_filter(df):
        dff = df.copy()
        
        dfmean = dff[['xval','yval','zval']].rolling(min_periods=1,window=48,center=False).mean()
        dfsd = dff[['xval','yval','zval']].rolling(min_periods=1,window=48,center=False).std()

        dfulimits = dfmean + (3*dfsd)
        dfllimits = dfmean - (3*dfsd)
    
        dff.xval[(dff.xval > dfulimits.xval) | (dff.xval < dfllimits.xval)] = np.nan
        dff.yval[(dff.yval > dfulimits.yval) | (dff.yval < dfllimits.yval)] = np.nan
        dff.zval[(dff.zval > dfulimits.zval) | (dff.zval < dfllimits.zval)] = np.nan
        
        dflogic = dff.xval * dff.yval * dff.zval
        
        dff = dff[dflogic.notnull()]
       
        return dff
        
def range_filter_accel(df):
    df.loc[:, 'type_num'] = df.type_num.astype(str)
    
    if df.type_num.str.contains('32').any() | df.type_num.str.contains('33').any():
    ## adjust accelerometer values for valid overshoot ranges
        df.xval[(df.xval<-2970) & (df.xval>-3072)] = df.xval[(df.xval<-2970) & (df.xval>-3072)] + 4096
        df.yval[(df.yval<-2970) & (df.yval>-3072)] = df.yval[(df.yval<-2970) & (df.yval>-3072)] + 4096
        df.zval[(df.zval<-2970) & (df.zval>-3072)] = df.zval[(df.zval<-2970) & (df.zval>-3072)] + 4096
        
        df.xval[abs(df.xval) > 1126] = np.nan
        df.yval[abs(df.yval) > 1126] = np.nan
        df.zval[abs(df.zval) > 1126] = np.nan
        
    if df.type_num.str.contains('11').any() | df.type_num.str.contains('12').any():
    ## adjust accelerometer values for valid overshoot ranges
        df.xval[(df.xval<-2970) & (df.xval>-3072)] = df.xval[(df.xval<-2970) & (df.xval>-3072)] + 4096
        df.yval[(df.yval<-2970) & (df.yval>-3072)] = df.yval[(df.yval<-2970) & (df.yval>-3072)] + 4096
        df.zval[(df.zval<-2970) & (df.zval>-3072)] = df.zval[(df.zval<-2970) & (df.zval>-3072)] + 4096
        
        df.xval[abs(df.xval) > 1126] = np.nan
        df.yval[abs(df.yval) > 1126] = np.nan
        df.zval[abs(df.zval) > 1126] = np.nan
         
    if df.type_num.str.contains('41').any() | df.type_num.str.contains('42').any():
    ## adjust accelerometer values for valid overshoot ranges
        df.xval[(df.xval<-2970) & (df.xval>-3072)] = df.xval[(df.xval<-2970) & (df.xval>-3072)] + 4096
        df.yval[(df.yval<-2970) & (df.yval>-3072)] = df.yval[(df.yval<-2970) & (df.yval>-3072)] + 4096
        df.zval[(df.zval<-2970) & (df.zval>-3072)] = df.zval[(df.zval<-2970) & (df.zval>-3072)] + 4096
        
        df.xval[abs(df.xval) > 1126] = np.nan
        df.yval[abs(df.yval) > 1126] = np.nan
        df.zval[abs(df.zval) > 1126] = np.nan
        
    if df.type_num.str.contains('51').any() | df.type_num.str.contains('52').any():
    ## adjust accelerometer values for valid overshoot ranges
        df.loc[df.xval<-32768, 'xval'] = df.loc[df.xval<-32768, 'xval'] + 65536
        df.loc[df.xval<-32768, 'yval'] = df.loc[df.xval<-32768, 'yval'] + 65536
        df.loc[df.xval<-32768, 'zval'] = df.loc[df.xval<-32768, 'zval'] + 65536
        
        df.loc[abs(df.xval) > 13158, 'xval'] = np.nan
        df.loc[abs(df.xval) > 13158, 'yval'] = np.nan
        df.loc[abs(df.xval) > 13158, 'zval'] = np.nan
    
    return df.loc[df.xval.notnull(), :]

def orthogonal_filter(df):
    lim = .08
    df.type_num = df.type_num.astype(str)
    
    if df.type_num.str.contains('51').any() | df.type_num.str.contains('52').any() :
        div = 13158
    else: 
        div = 1024
        
    dfa = df[['xval','yval','zval']]/div
    mag = (dfa.xval*dfa.xval + dfa.yval*dfa.yval + dfa.zval*dfa.zval).apply(np.sqrt)
    return (df[((mag>(1-lim)) & (mag<(1+lim)))])

def resample_df(df):
    df.ts = pd.to_datetime(df['ts'], unit = 's')
    df = df.set_index('ts')
    df = df.resample('30min').first()
    df = df.reset_index()
    return df
    
def apply_filters(dfl, orthof=True, rangef=True, outlierf=True):

    if dfl.empty:
        return dfl[['ts','node_id','type_num','xval','yval','zval']]  
  
    if rangef:
        dfl = range_filter_accel(dfl)
        if dfl.empty:
            return dfl[['ts','node_id','type_num','xval','yval','zval']]

    if orthof: 
        dfl = orthogonal_filter(dfl)
        if dfl.empty:
            return dfl[['ts','node_id','type_num','xval','yval','zval']]
            
    if outlierf:
        dfl = dfl.groupby(['node_id'])
        dfl = dfl.apply(resample_df)
        dfl = dfl.set_index('ts').groupby('node_id').apply(outlier_filter)
        dfl = dfl.reset_index(level = ['ts'])
        if dfl.empty:
            return dfl[['ts','node_id','type_num','xval','yval','zval']]
    
    dfl = dfl.reset_index(drop=True)     
    try:
        dfl = dfl[['ts','node_id','type_num','xval','yval','zval','batt']]
    except KeyError:
        dfl = dfl[['ts','node_id','type_num','xval','yval','zval']]
    return dfl


filtered_df = apply_filters(df)
# detect_fluctuations(filtered_df, fluctuation_threshold=2)
fluctuation_results = detect_fluctuations(filtered_df)
print("\nNode IDs and Type Numbers where fluctuations were observed:")
print(fluctuation_results)