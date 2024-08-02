# -*- coding: utf-8 -*-
"""
Created on Tue Jul  2 10:06:58 2024

@author: nichm
"""

import mysql.connector
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

def outlier_filter(df):
    dff = df.copy()
    
    dfmean = dff[['x','y','z']].rolling(min_periods=1,window=48,center=False).mean()
    dfsd = dff[['x','y','z']].rolling(min_periods=1,window=48,center=False).std()

    dfulimits = dfmean + (3*dfsd)
    dfllimits = dfmean - (3*dfsd)

    dff.x[(dff.x > dfulimits.x) | (dff.x < dfllimits.x)] = np.nan
    dff.y[(dff.y > dfulimits.y) | (dff.y < dfllimits.y)] = np.nan
    dff.z[(dff.z > dfulimits.z) | (dff.z < dfllimits.z)] = np.nan
    
    dflogic = dff.x * dff.y * dff.z
    
    dff = dff[dflogic.notnull()]
   
    return dff   

def range_filter_accel(df):
    df.loc[:, 'type_num'] = df.type_num.astype(str)
    
    if df.type_num.str.contains('32').any() | df.type_num.str.contains('33').any(): 
        ## adjust accelerometer values for valid overshoot ranges
        df.x[(df.x<-2970) & (df.x>-3072)] = df.x[(df.x<-2970) & (df.x>-3072)] + 4096
        df.y[(df.y<-2970) & (df.y>-3072)] = df.y[(df.y<-2970) & (df.y>-3072)] + 4096
        df.z[(df.z<-2970) & (df.z>-3072)] = df.z[(df.z<-2970) & (df.z>-3072)] + 4096
        
        df.x[abs(df.x) > 1126] = np.nan
        df.y[abs(df.y) > 1126] = np.nan
        df.z[abs(df.z) > 1126] = np.nan
        
    if df.type_num.str.contains('11').any() | df.type_num.str.contains('12').any():
        ## adjust accelerometer values for valid overshoot ranges
        df.x[(df.x<-2970) & (df.x>-3072)] = df.x[(df.x<-2970) & (df.x>-3072)] + 4096
        df.y[(df.y<-2970) & (df.y>-3072)] = df.y[(df.y<-2970) & (df.y>-3072)] + 4096
        df.z[(df.z<-2970) & (df.z>-3072)] = df.z[(df.z<-2970) & (df.z>-3072)] + 4096
        
        df.x[abs(df.x) > 1126] = np.nan
        df.y[abs(df.y) > 1126] = np.nan
        df.z[abs(df.z) > 1126] = np.nan       
        
    if df.type_num.str.contains('41').any() | df.type_num.str.contains('42').any():
        ## adjust accelerometer values for valid overshoot ranges
        df.x[(df.x<-2970) & (df.x>-3072)] = df.x[(df.x<-2970) & (df.x>-3072)] + 4096
        df.y[(df.y<-2970) & (df.y>-3072)] = df.y[(df.y<-2970) & (df.y>-3072)] + 4096
        df.z[(df.z<-2970) & (df.z>-3072)] = df.z[(df.z<-2970) & (df.z>-3072)] + 4096
        
        df.x[abs(df.x) > 1126] = np.nan
        df.y[abs(df.y) > 1126] = np.nan
        df.z[abs(df.z) > 1126] = np.nan
        
    if df.type_num.str.contains('51').any() | df.type_num.str.contains('52').any():
        ## adjust accelerometer values for valid overshoot ranges
        df.loc[df.x<-32768, 'x'] = df.loc[df.x<-32768, 'x'] + 65536
        df.loc[df.x<-32768, 'y'] = df.loc[df.x<-32768, 'y'] + 65536
        df.loc[df.x<-32768, 'z'] = df.loc[df.x<-32768, 'z'] + 65536
        
        df.loc[abs(df.x) > 13158, 'x'] = np.nan
        df.loc[abs(df.x) > 13158, 'y'] = np.nan
        df.loc[abs(df.x) > 13158, 'z'] = np.nan
    
    return df.loc[df.x.notnull(), :]
    
def orthogonal_filter(df):
    lim = .08
    df.type_num = df.type_num.astype(str)
    
    if df.type_num.str.contains('51').any() | df.type_num.str.contains('52').any() :
        div = 13158      
    else: 
        div = 1024      

    dfa = df[['x','y','z']]/div
    mag = (dfa.x*dfa.x + dfa.y*dfa.y + dfa.z*dfa.z).apply(np.sqrt)
    return (df[((mag>(1-lim)) & (mag<(1+lim)))])

def resample_df(df):
    df.ts = pd.to_datetime(df['ts'], unit = 's')
    df = df.set_index('ts')
    df = df.resample('30min').first()
    df = df.reset_index()
    return df
    
def apply_filters(dfl, orthof=True, rangef=True, outlierf=True):
    if dfl.empty:
        return dfl[['ts','node_id','x','y','z']]     
  
    if rangef:
        dfl = range_filter_accel(dfl)
        #dfl = dfl.reset_index(level=['ts'])
        if dfl.empty:
            return dfl[['ts','node_id','x','y','z']]

    if orthof: 
        dfl = orthogonal_filter(dfl)
        if dfl.empty:
            return dfl[['ts','node_id','x','y','z']]
                
    if outlierf:
        dfl = dfl.groupby(['node_id'])
        dfl = dfl.apply(resample_df)
        dfl = dfl.set_index('ts').groupby('node_id').apply(outlier_filter)
        dfl = dfl.reset_index(level = ['ts'])
        if dfl.empty:
            return dfl[['ts','node_id','x','y','z']]

    dfl = dfl.reset_index(drop=True)     
    try:
        dfl = dfl[['ts','node_id','x','y','z','batt']]
    except KeyError:
        dfl = dfl[['ts','node_id','x','y','z']]
    return dfl


# Prompt for user inputs
logger_name = input("Enter the logger name: ")
timedelta_months = int(input("Enter the time delta in months: "))
node_id = int(input("Enter the node_id: "))

# Calculate the start date based on the time delta
end_date = datetime.now()
end_date = end_date + timedelta(days=1)
start_date = end_date - timedelta(days=timedelta_months * 30)
start_date_str = start_date.strftime('%Y-%m-%d')
end_date_str = end_date.strftime('%Y-%m-%d')
window_size = '4H'  # 4-hour rolling window

def apply_filters_rolling_window(data):
    return data.apply(apply_filters)

# Connect to the database
dyna_db = mysql.connector.connect(
    host="192.168.150.112",
    database="analysis_db",
    user="pysys_local",
    password="NaCAhztBgYZ3HwTkvHwwGVtJn5sVMFgg",
)

# Query data
query = f"SELECT * FROM analysis_db.tilt_{logger_name} where ts BETWEEN '{start_date_str}' AND '{end_date_str}' order by ts"
df = pd.read_sql(query, dyna_db)
df.columns = ['data_id','ts_written','ts','node_id','type_num','x','y','z','batt','is_live']
print("df", len(df))

df['ts'] = pd.to_datetime(df['ts'])

# Define the specific node_id
desired_node_id = node_id

# Get unique type_num values from the dataframe
type_nums = df['type_num'].unique()

# Create subplots
fig, axs = plt.subplots(3, 4, figsize=(12, 12), sharex='col')
execution_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
fig.text(0.5, 0.945, f'Execution Time: {execution_time}', ha='center', fontsize=10)

# Add figure title
plt.suptitle(f'{logger_name} node_id {desired_node_id}', fontsize=16)

# Label subplots
node_id_minus_1 = desired_node_id - 1
node_id_plus_1 = desired_node_id + 1

# axs[0, 0].text(-0.2, 0.5, f'node_id {node_id_minus_1}', fontsize=12, ha='center', va='center', rotation=90, transform=axs[0, 0].transAxes)
# axs[1, 0].text(-0.2, 0.5, f'node_id {desired_node_id}', fontsize=12, ha='center', va='center', rotation=90, transform=axs[1, 0].transAxes, fontweight='bold')
# axs[2, 0].text(-0.2, 0.5, f'node_id {node_id_plus_1}', fontsize=12, ha='center', va='center', rotation=90, transform=axs[2, 0].transAxes)

# # Plot type_num 11 and 12 for the node_id - 1 (top row)
# if desired_node_id > 1:
#     df_filtered_minus_1 = df[df['node_id'] == desired_node_id - 1]
#     for type_num in type_nums:
#         df_type_minus_1 = df_filtered_minus_1[df_filtered_minus_1['type_num'] == type_num]
        
#         # Apply filters
#         df_type_minus_1 = apply_filters(df_type_minus_1)
        
#         # Determine row for plotting
#         row = 0
        
#         # Always set y-axis label
#         axs[row, 0].set_ylabel('xval')
#         axs[row, 1].set_ylabel('yval')
#         axs[row, 2].set_ylabel('zval')
#         axs[row, 3].set_ylabel('Battery')
        
#         # Check if dataframe is empty
#         if not df_type_minus_1.empty:
#             if type_num == 11:
#                 axs[row, 0].plot(df_type_minus_1['ts'], df_type_minus_1['x'], 'b-', label='accel 1')
#                 axs[row, 0].legend()

#                 axs[row, 1].plot(df_type_minus_1['ts'], df_type_minus_1['y'], 'b-', label='accel 1')
#                 axs[row, 1].legend()

#                 axs[row, 2].plot(df_type_minus_1['ts'], df_type_minus_1['z'], 'b-', label='accel 1')
#                 axs[row, 2].legend()

#                 axs[row, 3].plot(df_type_minus_1['ts'], df_type_minus_1['batt'], 'b-', label='accel 1')
#                 axs[row, 3].legend()

#             elif type_num == 12:
#                 axs[row, 0].plot(df_type_minus_1['ts'], df_type_minus_1['x'], 'r-', label='accel 2')
#                 axs[row, 0].legend()

#                 axs[row, 1].plot(df_type_minus_1['ts'], df_type_minus_1['y'], 'r-', label='accel 2')
#                 axs[row, 1].legend()

#                 axs[row, 2].plot(df_type_minus_1['ts'], df_type_minus_1['z'], 'r-', label='accel 2')
#                 axs[row, 2].legend()

#                 axs[row, 3].plot(df_type_minus_1['ts'], df_type_minus_1['batt'], 'r-', label='accel 2')
#                 axs[row, 3].legend()
#         else:
#             axs[row, 0].axis('off')
#             axs[row, 1].axis('off')
#             axs[row, 2].axis('off')
#             axs[row, 3].axis('off')

# # Plot type_num 11 and 12 for the desired node_id (middle row)
# df_filtered = df[df['node_id'] == desired_node_id]
# for type_num in type_nums:
#     df_type = df_filtered[df_filtered['type_num'] == type_num]
    
#     # Apply filters
#     df_type = apply_filters(df_type)
    
#     # Determine row for plotting
#     row = 1
    
#     # Always set y-axis label
#     axs[row, 0].set_ylabel('xval')
#     axs[row, 1].set_ylabel('yval')
#     axs[row, 2].set_ylabel('zval')
#     axs[row, 3].set_ylabel('Battery')
    
#     # Check if dataframe is empty
#     if not df_type.empty:
#         if type_num == 11:
#             axs[row, 0].plot(df_type['ts'], df_type['x'], 'b-', label='accel 1')
#             axs[row, 0].legend()

#             axs[row, 1].plot(df_type['ts'], df_type['y'], 'b-', label='accel 1')
#             axs[row, 1].legend()

#             axs[row, 2].plot(df_type['ts'], df_type['z'], 'b-', label='accel 1')
#             axs[row, 2].legend()

#             axs[row, 3].plot(df_type['ts'], df_type['batt'], 'b-', label='accel 1')
#             axs[row, 3].legend()

#         elif type_num == 12:
#             axs[row, 0].plot(df_type['ts'], df_type['x'], 'r-', label='accel 2')
#             axs[row, 0].legend()

#             axs[row, 1].plot(df_type['ts'], df_type['y'], 'r-', label='accel 2')
#             axs[row, 1].legend()

#             axs[row, 2].plot(df_type['ts'], df_type['z'], 'r-', label='accel 2')
#             axs[row, 2].legend()

#             axs[row, 3].plot(df_type['ts'], df_type['batt'], 'r-', label='accel 2')
#             axs[row, 3].legend()
#     else:
#         axs[row, 0].axis('off')
#         axs[row, 1].axis('off')
#         axs[row, 2].axis('off')
#         axs[row, 3].axis('off')

# # Plot type_num 11 and 12 for the node_id + 1 (bottom row)
# df_filtered_plus_1 = df[df['node_id'] == desired_node_id + 1]
# for type_num in type_nums:
#     df_type_plus_1 = df_filtered_plus_1[df_filtered_plus_1['type_num'] == type_num]
    
#     # Apply filters
#     df_type_plus_1 = apply_filters(df_type_plus_1)
    
#     # Determine row for plotting
#     row = 2
    
#     # Always set y-axis label
#     axs[row, 0].set_ylabel('xval')
#     axs[row, 1].set_ylabel('yval')
#     axs[row, 2].set_ylabel('zval')
#     axs[row, 3].set_ylabel('Battery')
    
#     # Check if dataframe is empty
#     if not df_type_plus_1.empty:
#         if type_num == 11:
#             axs[row, 0].plot(df_type_plus_1['ts'], df_type_plus_1['x'], 'b-', label='accel 1')
#             axs[row, 0].legend()

#             axs[row, 1].plot(df_type_plus_1['ts'], df_type_plus_1['y'], 'b-', label='accel 1')
#             axs[row, 1].legend()

#             axs[row, 2].plot(df_type_plus_1['ts'], df_type_plus_1['z'], 'b-', label='accel 1')
#             axs[row, 2].legend()

#             axs[row, 3].plot(df_type_plus_1['ts'], df_type_plus_1['batt'], 'b-', label='accel 1')
#             axs[row, 3].legend()

#         elif type_num == 12:
#             axs[row, 0].plot(df_type_plus_1['ts'], df_type_plus_1['x'], 'r-', label='accel 2')
#             axs[row, 0].legend()

#             axs[row, 1].plot(df_type_plus_1['ts'], df_type_plus_1['y'], 'r-', label='accel 2')
#             axs[row, 1].legend()

#             axs[row, 2].plot(df_type_plus_1['ts'], df_type_plus_1['z'], 'r-', label='accel 2')
#             axs[row, 2].legend()

#             axs[row, 3].plot(df_type_plus_1['ts'], df_type_plus_1['batt'], 'r-', label='accel 2')
#             axs[row, 3].legend()
#     else:
#         axs[row, 0].axis('off')
#         axs[row, 1].axis('off')
#         axs[row, 2].axis('off')
#         axs[row, 3].axis('off')

# # Rotate xticks in the 3rd row subplots
# for col in range(4):
#     axs[2, col].tick_params(axis='x', rotation=45)

# # Set common x label for the entire bottom row
# fig.text(0.5, 0.01, 'Timestamp', ha='center', fontsize=12)

# for row in range(3):
#     axs[row, 3].set_ylim(axs[row, 3].get_ylim()[0] - 0.5, axs[row, 3].get_ylim()[1] + 0.5)

# plt.tight_layout()
# plt.show()








# Plot type_num 11 and 12 for the node_id - 1 (top row)
if desired_node_id > 1:
    df_filtered_minus_1 = df[df['node_id'] == desired_node_id - 1]
    for type_num in type_nums:
        df_type_minus_1 = df_filtered_minus_1[df_filtered_minus_1['type_num'] == type_num]
        
        # Apply filters
        # df_type_minus_1 = apply_filters(df_type_minus_1)
        # df_type_minus_1 = df_type_minus_1.groupby(pd.Grouper(key='ts', freq='4H')).apply(apply_filters)
        df_type_minus_1 = df_type_minus_1.groupby('node_id', group_keys=True).apply(apply_filters)

        # Plot if data exists
        if not df_type_minus_1.empty:
            if type_num == 11:
                axs[0, 0].plot(df_type_minus_1['ts'], df_type_minus_1['x'], 'b-', label='accel 1')
                axs[0, 1].plot(df_type_minus_1['ts'], df_type_minus_1['y'], 'b-', label='accel 1')
                axs[0, 2].plot(df_type_minus_1['ts'], df_type_minus_1['z'], 'b-', label='accel 1')
                axs[0, 3].plot(df_type_minus_1['ts'], df_type_minus_1['batt'], 'b-', label='accel 1')
            elif type_num == 12:
                axs[0, 0].plot(df_type_minus_1['ts'], df_type_minus_1['x'], 'r-', label='accel 2')
                axs[0, 1].plot(df_type_minus_1['ts'], df_type_minus_1['y'], 'r-', label='accel 2')
                axs[0, 2].plot(df_type_minus_1['ts'], df_type_minus_1['z'], 'r-', label='accel 2')
                axs[0, 3].plot(df_type_minus_1['ts'], df_type_minus_1['batt'], 'r-', label='accel 2')
        else:
            for col in range(4):
                axs[0, col].text(0.5, 0.5, 'No data', fontsize=10, ha='center', va='center', transform=axs[0, col].transAxes)

else:
    for col in range(4):
        axs[0, col].text(0.5, 0.5, 'No data', fontsize=10, ha='center', va='center', transform=axs[0, col].transAxes)

# Plot type_num 11 and 12 for the desired node_id (middle row)
df_filtered = df[df['node_id'] == desired_node_id]
for type_num in type_nums:
    df_type = df_filtered[df_filtered['type_num'] == type_num]
    
    # Apply filters
    # df_type = apply_filters(df_type)
    # df_type = df_type.groupby(pd.Grouper(key='ts', freq='4H')).apply(apply_filters)
    df_type = df_type.groupby('node_id', group_keys=True).apply(apply_filters)

    # Plot if data exists
    if not df_type.empty:
        if type_num == 11:
            axs[1, 0].plot(df_type['ts'], df_type['x'], 'b-', label='accel 1')
            axs[1, 1].plot(df_type['ts'], df_type['y'], 'b-', label='accel 1')
            axs[1, 2].plot(df_type['ts'], df_type['z'], 'b-', label='accel 1')
            axs[1, 3].plot(df_type['ts'], df_type['batt'], 'b-', label='accel 1')
        elif type_num == 12:
            axs[1, 0].plot(df_type['ts'], df_type['x'], 'r-', label='accel 2')
            axs[1, 1].plot(df_type['ts'], df_type['y'], 'r-', label='accel 2')
            axs[1, 2].plot(df_type['ts'], df_type['z'], 'r-', label='accel 2')
            axs[1, 3].plot(df_type['ts'], df_type['batt'], 'r-', label='accel 2')
    else:
        for col in range(4):
            axs[1, col].text(0.5, 0.5, 'No data', fontsize=10, ha='center', va='center', transform=axs[1, col].transAxes)

# Plot type_num 11 and 12 for the node_id + 1 (bottom row)
df_filtered_plus_1 = df[df['node_id'] == desired_node_id + 1]
for type_num in type_nums:
    df_type_plus_1 = df_filtered_plus_1[df_filtered_plus_1['type_num'] == type_num]
    
    # Apply filters
    # df_type_plus_1 = apply_filters(df_type_plus_1)
    # df_type_plus_1 = df_type_plus_1.groupby(pd.Grouper(key='ts', freq='4H')).apply(apply_filters)
    df_type_plus_1 = df_type_plus_1.groupby('node_id', group_keys=True).apply(apply_filters)

    # Plot if data exists
    if not df_type_plus_1.empty:
        if type_num == 11:
            axs[2, 0].plot(df_type_plus_1['ts'], df_type_plus_1['x'], 'b-', label='accel 1')
            axs[2, 1].plot(df_type_plus_1['ts'], df_type_plus_1['y'], 'b-', label='accel 1')
            axs[2, 2].plot(df_type_plus_1['ts'], df_type_plus_1['z'], 'b-', label='accel 1')
            axs[2, 3].plot(df_type_plus_1['ts'], df_type_plus_1['batt'], 'b-', label='accel 1')
        elif type_num == 12:
            axs[2, 0].plot(df_type_plus_1['ts'], df_type_plus_1['x'], 'r-', label='accel 2')
            axs[2, 1].plot(df_type_plus_1['ts'], df_type_plus_1['y'], 'r-', label='accel 2')
            axs[2, 2].plot(df_type_plus_1['ts'], df_type_plus_1['z'], 'r-', label='accel 2')
            axs[2, 3].plot(df_type_plus_1['ts'], df_type_plus_1['batt'], 'r-', label='accel 2')
    else:
        for col in range(4):
            axs[2, col].text(0.5, 0.5, 'No data', fontsize=10, ha='center', va='center', transform=axs[2, col].transAxes)

# Label subplots
for row in range(3):
    axs[row, 0].text(-0.2, 0.5, f'node_id {desired_node_id - 1 + row}', fontsize=12, ha='center', va='center', rotation=90, transform=axs[row, 0].transAxes)
    # axs[row, 0].set_ylabel('Node ID', fontsize=10)

for col in range(4):
    axs[0, col].set_title(['xval', 'yval', 'zval', 'Battery'][col], fontsize=10)
    axs[2, col].tick_params(axis='x', rotation=45)

# Set common x label for the entire bottom row
fig.text(0.5, 0.01, 'Timestamp', ha='center', fontsize=12)

# Adjust y limits with tolerance for the 4th column subplots
for row in range(3):
    axs[row, 3].set_ylim(axs[row, 3].get_ylim()[0] - 0.5, axs[row, 3].get_ylim()[1] + 0.5)

plt.tight_layout()
plt.show()

dyna_db.close()