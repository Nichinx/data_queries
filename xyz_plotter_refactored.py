# -*- coding: utf-8 -*-
"""
Created on Tue Jul  2 18:29:40 2024

@author: nichm
"""

import mysql.connector
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from matplotlib.dates import DateFormatter


def outlier_filter(df):
    dff = df.copy()
    dfmean = dff[['x','y','z']].rolling(min_periods=1, window=48, center=False).mean()
    dfsd = dff[['x','y','z']].rolling(min_periods=1, window=48, center=False).std()

    dfulimits = dfmean + (3 * dfsd)
    dfllimits = dfmean - (3 * dfsd)

    dff.x[(dff.x > dfulimits.x) | (dff.x < dfllimits.x)] = np.nan
    dff.y[(dff.y > dfulimits.y) | (dff.y < dfllimits.y)] = np.nan
    dff.z[(dff.z > dfulimits.z) | (dff.z < dfllimits.z)] = np.nan

    dflogic = dff.x * dff.y * dff.z

    dff = dff[dflogic.notnull()]
   
    return dff

# def range_filter_accel(df):s
#     df.loc[:, 'type_num'] = df.type_num.astype(str)

#     if df.type_num.str.contains('32|33').any():
#         df[['x', 'y', 'z']] += np.where((df[['x', 'y', 'z']] < -2970) & (df[['x', 'y', 'z']] > -3072), 4096, 0)
#         df[['x', 'y', 'z']] = df[['x', 'y', 'z']].where(abs(df[['x', 'y', 'z']]) <= 1126)

#     if df.type_num.str.contains('11|12').any():
#         df[['x', 'y', 'z']] += np.where((df[['x', 'y', 'z']] < -2970) & (df[['x', 'y', 'z']] > -3072), 4096, 0)
#         df[['x', 'y', 'z']] = df[['x', 'y', 'z']].where(abs(df[['x', 'y', 'z']]) <= 1126)

#     if df.type_num.str.contains('41|42').any():
#         df[['x', 'y', 'z']] += np.where((df[['x', 'y', 'z']] < -2970) & (df[['x', 'y', 'z']] > -3072), 4096, 0)
#         df[['x', 'y', 'z']] = df[['x', 'y', 'z']].where(abs(df[['x', 'y', 'z']]) <= 1126)

#     if df.type_num.str.contains('51|52').any():
#         df[['x', 'y', 'z']] += np.where(df.x < -32768, 65536, 0)
#         df[['x', 'y', 'z']] = df[['x', 'y', 'z']].where(abs(df[['x', 'y', 'z']]) <= 13158)

#     return df.dropna(subset=['x'])

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
    lim = 0.08
    df['type_num'] = df['type_num'].astype(str)

    if df.type_num.str.contains('51|52').any():
        div = 13158
    else:
        div = 1024

    dfa = df[['x', 'y', 'z']] / div
    mag = (dfa.x**2 + dfa.y**2 + dfa.z**2).apply(np.sqrt)
    return df[((mag > (1 - lim)) & (mag < (1 + lim)))]

def resample_df(df):
    df['ts'] = pd.to_datetime(df['ts'], unit='s')
    df = df.set_index('ts').resample('30min').first().reset_index()
    return df

def apply_filters(dfl, orthof=True, rangef=True, outlierf=True):
    if dfl.empty:
        return dfl

    if rangef:
        dfl = range_filter_accel(dfl)
        if dfl.empty:
            return dfl

    if orthof:
        dfl = orthogonal_filter(dfl)
        if dfl.empty:
            return dfl

    # if outlierf:
    #     dfl = dfl.groupby('node_id').apply(resample_df)
    #     dfl = dfl.set_index('ts').groupby('node_id').apply(outlier_filter)
    #     if dfl.empty:
    #         return dfl
    if outlierf:
        dfl = dfl.groupby(['node_id'])
        dfl = dfl.apply(resample_df)
        dfl = dfl.set_index('ts').groupby('node_id').apply(outlier_filter)
        dfl = dfl.reset_index(level = ['ts'])
        if dfl.empty:
            return dfl[['ts','tsm_name','node_id','x','y','z']]

    dfl = dfl.reset_index(drop=True) 
    return dfl.reset_index()[['ts', 'node_id', 'x', 'y', 'z', 'batt']]

# def main():
#     logger_name = input("Enter the logger name: ")
#     timedelta_months = int(input("Enter the time delta in months: "))
#     node_id = int(input("Enter the node_id: "))

#     end_date = datetime.now() + timedelta(days=1)
#     start_date = end_date - timedelta(days=timedelta_months * 30)
#     start_date_str = start_date.strftime('%Y-%m-%d')
#     end_date_str = end_date.strftime('%Y-%m-%d')

#     dyna_db = mysql.connector.connect(
#         host="192.168.150.112",
#         database="analysis_db",
#         user="pysys_local",
#         password="NaCAhztBgYZ3HwTkvHwwGVtJn5sVMFgg",
#     )

#     query = f"SELECT * FROM analysis_db.tilt_{logger_name} WHERE ts BETWEEN '{start_date_str}' AND '{end_date_str}' ORDER BY ts"
#     df = pd.read_sql(query, dyna_db)
#     df.columns = ['data_id', 'ts_written', 'ts', 'node_id', 'type_num', 'x', 'y', 'z', 'batt', 'is_live']
#     print("Number of rows fetched from database:", len(df))
    
#     if len(logger_name) == 4:
#         df['type_num'] = 1

#     df_filtered = df[df['node_id'] == node_id]
#     type_nums = df['type_num'].unique()

#     fig, axs = plt.subplots(3, 4, figsize=(12, 12), sharex='col')
#     execution_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
#     fig.text(0.5, 0.945, f'Execution Time: {execution_time}', ha='center', fontsize=10)
#     plt.suptitle(f'{logger_name} : node ID {node_id}', fontsize=16)

#     for i, df_group in enumerate([df[df['node_id'] == node_id - 1], df_filtered, df[df['node_id'] == node_id + 1]]):
#         row = i
#         for j, type_num in enumerate(type_nums):
            
#             df_type = df_group[df_group['type_num'] == type_num].copy()
#             df_type = df_type.groupby('node_id', group_keys=True).apply(apply_filters)
            
#             if not df_type.empty:
#                 axs[row, 0].plot(df_type['ts'], df_type['x'], label='accel 1' if type_num == 11 else 'accel 2', color='b' if type_num == 11 else 'r')
#                 axs[row, 1].plot(df_type['ts'], df_type['y'], label='accel 1' if type_num == 11 else 'accel 2', color='b' if type_num == 11 else 'r')
#                 axs[row, 2].plot(df_type['ts'], df_type['z'], label='accel 1' if type_num == 11 else 'accel 2', color='b' if type_num == 11 else 'r')
#                 axs[row, 3].plot(df_type['ts'], df_type['batt'], label='batt')
                
#                 for k in range(4):
#                     axs[row, k].legend()
#             else:
#                 for k in range(4):
#                     axs[row, k].text(0.5, 0.5, 'No data', fontsize=10, ha='center', va='center', transform=axs[row, k].transAxes)  # Centered 'No data'                

#         desired_node_id = node_id
#         if row == 1:
#             axs[row, 0].text(-0.2, 0.5, f'node_id {desired_node_id - 1 + row}', fontsize=12, fontweight='bold', ha='center', va='center', rotation=90, transform=axs[row, 0].transAxes)
#         else:
#             axs[row, 0].text(-0.2, 0.5, f'node_id {desired_node_id - 1 + row}', fontsize=12, ha='center', va='center', rotation=90, transform=axs[row, 0].transAxes)

#     fig.text(0.5, 0.01, 'timestamp', ha='center', fontsize=12)
    
#     for top in range(4):
#         axs[0, top].set_title(['xval', 'yval', 'zval', 'batt'][top], fontsize=10)

#     for col in range(4):
#         axs[2, col].tick_params(axis='x', rotation=45)

#     for row in range(3):
#         axs[row, 3].set_ylim(axs[row, 3].get_ylim()[0] - 0.5, axs[row, 3].get_ylim()[1] + 0.5)

#     plt.show()
#     plt.get_current_fig_manager().window.showMaximized()
#     plt.tight_layout()
#     dyna_db.close()
    
    
def main_v2(): #optional plot with adjacent nodes
    # logger_name = input("Enter the logger name: ")
    # timedelta_months = int(input("Enter the time delta in months: "))
    # node_id = int(input("Enter the node_id: "))
    # plot_adjacent_nodes = input("Do you want to plot with adjacent nodes (Y/N)? ").strip().upper() == 'Y'
  
    while True:
        try: 
            logger_name = input("Enter the logger name: ").strip()
            if not logger_name:
                raise ValueError("Logger name cannot be empty. Please enter a valid logger name.")
        
            timedelta_months = int(input("Enter the time delta in months: "))
            if timedelta_months <= 0:
                raise ValueError("Time delta must be a positive integer. Please enter a valid number of months.")
        
            node_id = int(input("Enter the node_id: "))
            if node_id <= 0:
                raise ValueError("Node ID must be a positive integer. Please enter a valid node ID.")
        
            plot_adjacent_nodes_input = input("Do you want to plot with adjacent nodes (Y/N)? ").strip().upper()
            if plot_adjacent_nodes_input not in ['Y', 'N']:
                raise ValueError("Invalid input. Please enter 'Y' or 'N'.")
            plot_adjacent_nodes = plot_adjacent_nodes_input == 'Y'    
        
            end_date = datetime.now() + timedelta(days=1)
            start_date = end_date - timedelta(days=timedelta_months * 30)
            start_date_str = start_date.strftime('%Y-%m-%d')
            end_date_str = end_date.strftime('%Y-%m-%d')
        
            dyna_db = mysql.connector.connect(
                        host="192.168.150.112",
                        database="analysis_db",
                        user="pysys_local",
                        password="NaCAhztBgYZ3HwTkvHwwGVtJn5sVMFgg",
            )
            query = f"SELECT * FROM analysis_db.tilt_{logger_name} WHERE ts BETWEEN '{start_date_str}' AND '{end_date_str}' ORDER BY ts"
            df = pd.read_sql(query, dyna_db)
        
            if len(logger_name) == 4:
                df['type_num'] = 1
        
            df.columns = ['data_id', 'ts_written', 'ts', 'node_id', 'type_num', 'x', 'y', 'z', 'batt', 'is_live']
            print("Number of rows fetched from database:", len(df))
        
            type_nums = df['type_num'].unique()
            num_rows = 3 if plot_adjacent_nodes else 1
        
            if plot_adjacent_nodes:
                fig, axs = plt.subplots(num_rows, 4, figsize=(12, 12), sharex='col')
            else:
                fig, axs = plt.subplots(4, 1, figsize=(12, 12), sharex='col')
        
            execution_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            fig.text(0.5, 0.945, f'Execution Time: {execution_time}', ha='center', fontsize=10)
            plt.suptitle(f'{logger_name} : node ID {node_id}', fontsize=16)
        
            nodes_to_plot = [node_id - 1, node_id, node_id + 1] if plot_adjacent_nodes else [node_id]
            for i, current_node_id in enumerate(nodes_to_plot):
                row = i if plot_adjacent_nodes else 0
                for j, type_num in enumerate(type_nums):
                    df_group = df[df['node_id'] == current_node_id]
                    df_type = df_group[df_group['type_num'] == type_num].copy()
                    df_type = df_type.groupby('node_id', group_keys=True).apply(apply_filters)
        
                    if not df_type.empty:
                        if plot_adjacent_nodes:
                            axs[row, 0].plot(df_type['ts'], df_type['x'], label='accel 1' if type_num == 11 else 'accel 2', color='b' if type_num == 11 else 'r')
                            axs[row, 1].plot(df_type['ts'], df_type['y'], label='accel 1' if type_num == 11 else 'accel 2', color='b' if type_num == 11 else 'r')
                            axs[row, 2].plot(df_type['ts'], df_type['z'], label='accel 1' if type_num == 11 else 'accel 2', color='b' if type_num == 11 else 'r')
                            axs[row, 3].plot(df_type['ts'], df_type['batt'], label='batt')
                            axs[row, 3].set_ylim(axs[row, 3].get_ylim()[0] - 0.5, axs[row, 3].get_ylim()[1] + 0.5)  # Set ylim for 4th column
                            for k in range(4):
                                axs[row, k].legend()
                        else:
                            axs[0].plot(df_type['ts'], df_type['x'], label='accel 1' if type_num == 11 else 'accel 2', color='b' if type_num == 11 else 'r')
                            axs[1].plot(df_type['ts'], df_type['y'], label='accel 1' if type_num == 11 else 'accel 2', color='b' if type_num == 11 else 'r')
                            axs[2].plot(df_type['ts'], df_type['z'], label='accel 1' if type_num == 11 else 'accel 2', color='b' if type_num == 11 else 'r')
                            axs[3].plot(df_type['ts'], df_type['batt'], label='batt')
                            axs[3].set_ylim(axs[3].get_ylim()[0] - 0.5, axs[3].get_ylim()[1] + 0.5)  # Set ylim for 4th row
                            for k in range(4):
                                axs[k].legend()
                    else:
                        if plot_adjacent_nodes:
                            for k in range(4):
                                axs[row, k].text(0.5, 0.45, 'No data', fontsize=10, ha='center', va='center', transform=axs[row, k].transAxes)
                        else:
                            for k in range(4):
                                axs[k].text(0.5, 0.45, 'No data', fontsize=10, ha='center', va='center', transform=axs[k].transAxes)
        
                if plot_adjacent_nodes:
                    desired_node_id = node_id
                    if row == 1:
                        axs[row, 0].text(-0.2, 0.5, f'node_id {desired_node_id - 1 + row}', fontsize=12, fontweight='bold', ha='center', va='center', rotation=90, transform=axs[row, 0].transAxes)
                    else:
                        axs[row, 0].text(-0.2, 0.5, f'node_id {desired_node_id - 1 + row}', fontsize=12, ha='center', va='center', rotation=90, transform=axs[row, 0].transAxes)
        
            y_labels = ['xval', 'yval', 'zval', 'batt']
            if plot_adjacent_nodes:
                for top in range(4):
                    axs[0, top].set_title(y_labels[top], fontsize=10)
            else:
                for i in range(4):
                    axs[i].set_ylabel(y_labels[i])
        
            fig.text(0.5, 0.01, 'timestamp', ha='center', fontsize=12)
        
            if plot_adjacent_nodes:
                for col in range(4):
                    axs[2, col].tick_params(axis='x', rotation=45)
            else:
                axs[3].tick_params(axis='x', rotation=45)
        
            plt.tight_layout()
            plt.show()
            dyna_db.close()
            
            break
    
        except ValueError as ve:
            print(f"Input error: {ve}. Please try again.\n")
        except Exception as e:
            print(f"An error occurred: {e}. Please try again.\n")
            

def main_v3(): #raw plot
    while True:
        try: 
            logger_name = input("Enter the logger name: ").strip()
            if not logger_name:
                raise ValueError("Logger name cannot be empty. Please enter a valid logger name.")
        
            timedelta_months = int(input("Enter the time delta in months: "))
            if timedelta_months <= 0:
                raise ValueError("Time delta must be a positive integer. Please enter a valid number of months.")
        
            node_id = int(input("Enter the node_id: "))
            if node_id <= 0:
                raise ValueError("Node ID must be a positive integer. Please enter a valid node ID.")
        
            plot_adjacent_nodes_input = input("Do you want to plot with adjacent nodes (Y/N)? ").strip().upper()
            if plot_adjacent_nodes_input not in ['Y', 'N']:
                raise ValueError("Invalid input. Please enter 'Y' or 'N'.")
            plot_adjacent_nodes = plot_adjacent_nodes_input == 'Y'    
        
            show_raw_data_input = input("Do you want to show raw data (Y/N)? ").strip().upper()
            if show_raw_data_input not in ['Y', 'N']:
                raise ValueError("Invalid input. Please enter 'Y' or 'N'.")
            show_raw_data = show_raw_data_input == 'Y'
        
            end_date = datetime.now() + timedelta(days=1)
            start_date = end_date - timedelta(days=timedelta_months * 30)
            start_date_str = start_date.strftime('%Y-%m-%d')
            end_date_str = end_date.strftime('%Y-%m-%d')
        
            dyna_db = mysql.connector.connect(
                        host="192.168.150.112",
                        database="analysis_db",
                        user="pysys_local",
                        password="NaCAhztBgYZ3HwTkvHwwGVtJn5sVMFgg",
            )
            query = f"SELECT * FROM analysis_db.tilt_{logger_name} WHERE ts BETWEEN '{start_date_str}' AND '{end_date_str}' ORDER BY ts"
            df = pd.read_sql(query, dyna_db)
        
            if len(logger_name) == 4:
                df['type_num'] = 1
        
            df.columns = ['data_id', 'ts_written', 'ts', 'node_id', 'type_num', 'x', 'y', 'z', 'batt', 'is_live']
            print("Number of rows fetched from database:", len(df))
        
            type_nums = df['type_num'].unique()
            num_rows = 3 if plot_adjacent_nodes else 1
        
            if plot_adjacent_nodes:
                fig, axs = plt.subplots(num_rows, 4, figsize=(12, 12), sharex='col')
            else:
                fig, axs = plt.subplots(4, 1, figsize=(12, 12), sharex='col')
        
            execution_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            fig.text(0.5, 0.945, f'Execution Time: {execution_time}', ha='center', fontsize=10)
            plt.suptitle(f'{logger_name} : node ID {node_id}', fontsize=16)
        
            nodes_to_plot = [node_id - 1, node_id, node_id + 1] if plot_adjacent_nodes else [node_id]
            for i, current_node_id in enumerate(nodes_to_plot):
                row = i if plot_adjacent_nodes else 0
                for j, type_num in enumerate(type_nums):
                    df_group = df[df['node_id'] == current_node_id]
                    df_type = df_group[df_group['type_num'] == type_num].copy()
                    
                    if show_raw_data:
                        # Plot raw data with 60% transparency
                        axs[row, 0].plot(df_type['ts'], df_type['x'], label='raw accel 1' if type_num == 11 else 'raw accel 2', color='b' if type_num == 11 else 'r', alpha=0.6)
                        axs[row, 1].plot(df_type['ts'], df_type['y'], label='raw accel 1' if type_num == 11 else 'raw accel 2', color='b' if type_num == 11 else 'r', alpha=0.6)
                        axs[row, 2].plot(df_type['ts'], df_type['z'], label='raw accel 1' if type_num == 11 else 'raw accel 2', color='b' if type_num == 11 else 'r', alpha=0.6)
                        axs[row, 3].plot(df_type['ts'], df_type['batt'], label='raw batt', alpha=0.6)
                    else:
                        df_type = df_type.groupby('node_id', group_keys=True).apply(apply_filters)
                        if not df_type.empty:
                            if plot_adjacent_nodes:
                                axs[row, 0].plot(df_type['ts'], df_type['x'], label='accel 1' if type_num == 11 else 'accel 2', color='b' if type_num == 11 else 'r')
                                axs[row, 1].plot(df_type['ts'], df_type['y'], label='accel 1' if type_num == 11 else 'accel 2', color='b' if type_num == 11 else 'r')
                                axs[row, 2].plot(df_type['ts'], df_type['z'], label='accel 1' if type_num == 11 else 'accel 2', color='b' if type_num == 11 else 'r')
                                axs[row, 3].plot(df_type['ts'], df_type['batt'], label='batt')
                                axs[row, 3].set_ylim(axs[row, 3].get_ylim()[0] - 0.5, axs[row, 3].get_ylim()[1] + 0.5)  # Set ylim for 4th column
                                for k in range(4):
                                    axs[row, k].legend()
                            else:
                                axs[0].plot(df_type['ts'], df_type['x'], label='accel 1' if type_num == 11 else 'accel 2', color='b' if type_num == 11 else 'r')
                                axs[1].plot(df_type['ts'], df_type['y'], label='accel 1' if type_num == 11 else 'accel 2', color='b' if type_num == 11 else 'r')
                                axs[2].plot(df_type['ts'], df_type['z'], label='accel 1' if type_num == 11 else 'accel 2', color='b' if type_num == 11 else 'r')
                                axs[3].plot(df_type['ts'], df_type['batt'], label='batt')
                                axs[3].set_ylim(axs[3].get_ylim()[0] - 0.5, axs[3].get_ylim()[1] + 0.5)  # Set ylim for 4th row
                                for k in range(4):
                                    axs[k].legend()
                        else:
                            if plot_adjacent_nodes:
                                for k in range(4):
                                    axs[row, k].text(0.5, 0.45, 'No data', fontsize=10, ha='center', va='center', transform=axs[row, k].transAxes)
                            else:
                                for k in range(4):
                                    axs[k].text(0.5, 0.45, 'No data', fontsize=10, ha='center', va='center', transform=axs[k].transAxes)
        
                if plot_adjacent_nodes:
                    desired_node_id = node_id
                    if row == 1:
                        axs[row, 0].text(-0.2, 0.5, f'node_id {desired_node_id - 1 + row}', fontsize=12, fontweight='bold', ha='center', va='center', rotation=90, transform=axs[row, 0].transAxes)
                    else:
                        axs[row, 0].text(-0.2, 0.5, f'node_id {desired_node_id - 1 + row}', fontsize=12, ha='center', va='center', rotation=90, transform=axs[row, 0].transAxes)
        
            y_labels = ['xval', 'yval', 'zval', 'batt']
            if plot_adjacent_nodes:
                for top in range(4):
                    axs[0, top].set_title(y_labels[top], fontsize=10)
            else:
                for i in range(4):
                    axs[i].set_ylabel(y_labels[i])
        
            fig.text(0.5, 0.01, 'timestamp', ha='center', fontsize=12)
        
            if plot_adjacent_nodes:
                for col in range(4):
                    axs[2, col].tick_params(axis='x', rotation=45)
            else:
                axs[3].tick_params(axis='x', rotation=45)
        
            plt.tight_layout()
            plt.show()
            dyna_db.close()
            
            break
    
        except ValueError as ve:
            print(f"Input error: {ve}. Please try again.\n")
        except Exception as e:
            print(f"An error occurred: {e}. Please try again.\n")
            

def main_v4(): #raw + filtered plot
    while True:
        try: 
            logger_name = input("Enter the logger name: ").strip()
            if not logger_name:
                raise ValueError("Logger name cannot be empty. Please enter a valid logger name.")
        
            timedelta_months = int(input("Enter the time delta in months: "))
            if timedelta_months <= 0:
                raise ValueError("Time delta must be a positive integer. Please enter a valid number of months.")
        
            node_id = int(input("Enter the node_id: "))
            if node_id <= 0:
                raise ValueError("Node ID must be a positive integer. Please enter a valid node ID.")
        
            plot_adjacent_nodes_input = input("Do you want to plot with adjacent nodes (Y/N)? ").strip().upper()
            if plot_adjacent_nodes_input not in ['Y', 'N']:
                raise ValueError("Invalid input. Please enter 'Y' or 'N'.")
            plot_adjacent_nodes = plot_adjacent_nodes_input == 'Y'    
        
            show_raw_data_input = input("Do you want to show raw data (Y/N)? ").strip().upper()
            if show_raw_data_input not in ['Y', 'N']:
                raise ValueError("Invalid input. Please enter 'Y' or 'N'.")
            show_raw_data = show_raw_data_input == 'Y'
        
            end_date = datetime.now() + timedelta(days=1)
            start_date = end_date - timedelta(days=timedelta_months * 30)
            start_date_str = start_date.strftime('%Y-%m-%d')
            end_date_str = end_date.strftime('%Y-%m-%d')
        
            dyna_db = mysql.connector.connect(
                        host="192.168.150.112",
                        database="analysis_db",
                        user="pysys_local",
                        password="NaCAhztBgYZ3HwTkvHwwGVtJn5sVMFgg",
            )
            query = f"SELECT * FROM analysis_db.tilt_{logger_name} WHERE ts BETWEEN '{start_date_str}' AND '{end_date_str}' ORDER BY ts"
            df = pd.read_sql(query, dyna_db)
        
            if len(logger_name) == 4:
                df['type_num'] = 1
        
            df.columns = ['data_id', 'ts_written', 'ts', 'node_id', 'type_num', 'x', 'y', 'z', 'batt', 'is_live']
            print("Number of rows fetched from database:", len(df))
        
            type_nums = df['type_num'].unique()
            num_rows = 3 if plot_adjacent_nodes else 1
        
            if plot_adjacent_nodes:
                fig, axs = plt.subplots(num_rows, 4, figsize=(12, 12), sharex='col')
            else:
                fig, axs = plt.subplots(4, 1, figsize=(12, 12), sharex='col')
        
            execution_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            fig.text(0.5, 0.945, f'Execution Time: {execution_time}', ha='center', fontsize=10)
            plt.suptitle(f'{logger_name} : node ID {node_id}', fontsize=16)
        
            nodes_to_plot = [node_id - 1, node_id, node_id + 1] if plot_adjacent_nodes else [node_id]
            for i, current_node_id in enumerate(nodes_to_plot):
                row = i if plot_adjacent_nodes else 0
                for j, type_num in enumerate(type_nums):
                    df_group = df[df['node_id'] == current_node_id]
                    df_type = df_group[df_group['type_num'] == type_num].copy()
                    
                    if show_raw_data:
                        # Plot raw data with 60% transparency
                        if not df_type.empty:
                            axs[row, 0].plot(df_type['ts'], df_type['x'], label='raw accel 1' if type_num == 11 else 'raw accel 2', color='b' if type_num == 11 else 'r', alpha=0.6)
                            axs[row, 1].plot(df_type['ts'], df_type['y'], label='raw accel 1' if type_num == 11 else 'raw accel 2', color='b' if type_num == 11 else 'r', alpha=0.6)
                            axs[row, 2].plot(df_type['ts'], df_type['z'], label='raw accel 1' if type_num == 11 else 'raw accel 2', color='b' if type_num == 11 else 'r', alpha=0.6)
                            axs[row, 3].plot(df_type['ts'], df_type['batt'], label='raw batt', alpha=0.6)
                    
                    # Apply filters unless showing raw data
                    df_type = df_type.groupby('node_id', group_keys=True).apply(apply_filters)
                    if not df_type.empty:
                        if plot_adjacent_nodes:
                            axs[row, 0].plot(df_type['ts'], df_type['x'], label='accel 1' if type_num == 11 else 'accel 2', color='b' if type_num == 11 else 'r')
                            axs[row, 1].plot(df_type['ts'], df_type['y'], label='accel 1' if type_num == 11 else 'accel 2', color='b' if type_num == 11 else 'r')
                            axs[row, 2].plot(df_type['ts'], df_type['z'], label='accel 1' if type_num == 11 else 'accel 2', color='b' if type_num == 11 else 'r')
                            axs[row, 3].plot(df_type['ts'], df_type['batt'], label='batt')
                            axs[row, 3].set_ylim(axs[row, 3].get_ylim()[0] - 0.5, axs[row, 3].get_ylim()[1] + 0.5)  # Set ylim for 4th column
                            for k in range(4):
                                axs[row, k].legend()
                        else:
                            axs[0].plot(df_type['ts'], df_type['x'], label='accel 1' if type_num == 11 else 'accel 2', color='b' if type_num == 11 else 'r')
                            axs[1].plot(df_type['ts'], df_type['y'], label='accel 1' if type_num == 11 else 'accel 2', color='b' if type_num == 11 else 'r')
                            axs[2].plot(df_type['ts'], df_type['z'], label='accel 1' if type_num == 11 else 'accel 2', color='b' if type_num == 11 else 'r')
                            axs[3].plot(df_type['ts'], df_type['batt'], label='batt')
                            axs[3].set_ylim(axs[3].get_ylim()[0] - 0.5, axs[3].get_ylim()[1] + 0.5)  # Set ylim for 4th row
                            for k in range(4):
                                axs[k].legend()
                    else:
                        if plot_adjacent_nodes:
                            for k in range(4):
                                axs[row, k].text(0.5, 0.45, 'No data', fontsize=10, ha='center', va='center', transform=axs[row, k].transAxes)
                        else:
                            for k in range(4):
                                axs[k].text(0.5, 0.45, 'No data', fontsize=10, ha='center', va='center', transform=axs[k].transAxes)
        
                if plot_adjacent_nodes:
                    desired_node_id = node_id
                    if row == 1:
                        axs[row, 0].text(-0.2, 0.5, f'node_id {desired_node_id - 1 + row}', fontsize=12, fontweight='bold', ha='center', va='center', rotation=90, transform=axs[row, 0].transAxes)
                    else:
                        axs[row, 0].text(-0.2, 0.5, f'node_id {desired_node_id - 1 + row}', fontsize=12, ha='center', va='center', rotation=90, transform=axs[row, 0].transAxes)
        
            y_labels = ['xval', 'yval', 'zval', 'batt']
            if plot_adjacent_nodes:
                for top in range(4):
                    axs[0, top].set_title(y_labels[top], fontsize=10)
            else:
                for i in range(4):
                    axs[i].set_ylabel(y_labels[i])
        
            fig.text(0.5, 0.01, 'timestamp', ha='center', fontsize=12)
        
            if plot_adjacent_nodes:
                for col in range(4):
                    axs[2, col].tick_params(axis='x', rotation=45)
            else:
                axs[3].tick_params(axis='x', rotation=45)
        
            plt.tight_layout()
            plt.show()
            dyna_db.close()
            
            break
    
        except ValueError as ve:
            print(f"Input error: {ve}. Please try again.\n")
        except Exception as e:
            print(f"An error occurred: {e}. Please try again.\n")


def main_v5():  # dynamic label for accel number
    while True:
        try:
            logger_name = input("Enter the logger name: ").strip()
            if not logger_name:
                raise ValueError("Logger name cannot be empty. Please enter a valid logger name.")

            timedelta_months = int(input("Enter the time delta in months: "))
            if timedelta_months <= 0:
                raise ValueError("Time delta must be a positive integer. Please enter a valid number of months.")

            node_id = int(input("Enter the node_id: "))
            if node_id <= 0:
                raise ValueError("Node ID must be a positive integer. Please enter a valid node ID.")

            plot_adjacent_nodes_input = input("Do you want to plot with adjacent nodes (Y/N)? ").strip().upper()
            if plot_adjacent_nodes_input not in ['Y', 'N']:
                raise ValueError("Invalid input. Please enter 'Y' or 'N'.")
            plot_adjacent_nodes = plot_adjacent_nodes_input == 'Y'

            show_raw_data_input = input("Do you want to show raw data (Y/N)? ").strip().upper()
            if show_raw_data_input not in ['Y', 'N']:
                raise ValueError("Invalid input. Please enter 'Y' or 'N'.")
            show_raw_data = show_raw_data_input == 'Y'

            end_date = datetime.now() + timedelta(days=1)
            start_date = end_date - timedelta(days=timedelta_months * 30)
            start_date_str = start_date.strftime('%Y-%m-%d')
            end_date_str = end_date.strftime('%Y-%m-%d')

            dyna_db = mysql.connector.connect(
                host="192.168.150.112",
                database="analysis_db",
                user="pysys_local",
                password="NaCAhztBgYZ3HwTkvHwwGVtJn5sVMFgg",
            )
            query = f"SELECT * FROM analysis_db.tilt_{logger_name} WHERE ts BETWEEN '{start_date_str}' AND '{end_date_str}' ORDER BY ts"
            df = pd.read_sql(query, dyna_db)

            if len(logger_name) == 4:
                df['type_num'] = 1

            df.columns = ['data_id', 'ts_written', 'ts', 'node_id', 'type_num', 'x', 'y', 'z', 'batt', 'is_live']
            print("Number of rows fetched from database:", len(df))

            type_nums = df['type_num'].unique()
            num_rows = 3 if plot_adjacent_nodes else 1

            if plot_adjacent_nodes:
                fig, axs = plt.subplots(num_rows, 4, figsize=(12, 12), sharex='col')
            else:
                fig, axs = plt.subplots(4, 1, figsize=(12, 12), sharex='col')

            execution_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            fig.text(0.5, 0.945, f'Execution Time: {execution_time}', ha='center', fontsize=10)
            plt.suptitle(f'{logger_name} : node ID {node_id}', fontsize=16)

            nodes_to_plot = [node_id - 1, node_id, node_id + 1] if plot_adjacent_nodes else [node_id]
            for i, current_node_id in enumerate(nodes_to_plot):
                row = i if plot_adjacent_nodes else 0
                for j, type_num in enumerate(type_nums):
                    df_group = df[df['node_id'] == current_node_id]
                    df_type = df_group[df_group['type_num'] == type_num].copy()
                    
                    accel_label = f'accel {j+1}'
                    color = 'b' if j == 0 else 'r'
                    
                    if show_raw_data:
                        # Plot raw data with 60% transparency
                        axs[row, 0].plot(df_type['ts'], df_type['x'], label=f'raw {accel_label}', color=color, alpha=0.6)
                        axs[row, 1].plot(df_type['ts'], df_type['y'], label=f'raw {accel_label}', color=color, alpha=0.6)
                        axs[row, 2].plot(df_type['ts'], df_type['z'], label=f'raw {accel_label}', color=color, alpha=0.6)
                        axs[row, 3].plot(df_type['ts'], df_type['batt'], label='raw batt', alpha=0.6)
                    else:
                        df_type = df_type.groupby('node_id', group_keys=True).apply(apply_filters)
                        if not df_type.empty:
                            if plot_adjacent_nodes:
                                axs[row, 0].plot(df_type['ts'], df_type['x'], label=accel_label, color=color)
                                axs[row, 1].plot(df_type['ts'], df_type['y'], label=accel_label, color=color)
                                axs[row, 2].plot(df_type['ts'], df_type['z'], label=accel_label, color=color)
                                axs[row, 3].plot(df_type['ts'], df_type['batt'], label='batt')
                                axs[row, 3].set_ylim(axs[row, 3].get_ylim()[0] - 0.5, axs[row, 3].get_ylim()[1] + 0.5)  # Set ylim for 4th column
                                for k in range(4):
                                    axs[row, k].legend()
                            else:
                                axs[0].plot(df_type['ts'], df_type['x'], label=accel_label, color=color)
                                axs[1].plot(df_type['ts'], df_type['y'], label=accel_label, color=color)
                                axs[2].plot(df_type['ts'], df_type['z'], label=accel_label, color=color)
                                axs[3].plot(df_type['ts'], df_type['batt'], label='batt')
                                axs[3].set_ylim(axs[3].get_ylim()[0] - 0.5, axs[3].get_ylim()[1] + 0.5)  # Set ylim for 4th row
                                for k in range(4):
                                    axs[k].legend()
                        else:
                            if plot_adjacent_nodes:
                                for k in range(4):
                                    axs[row, k].text(0.5, 0.45, 'No data', fontsize=10, ha='center', va='center', transform=axs[row, k].transAxes)
                            else:
                                for k in range(4):
                                    axs[k].text(0.5, 0.45, 'No data', fontsize=10, ha='center', va='center', transform=axs[k].transAxes)

                if plot_adjacent_nodes:
                    desired_node_id = node_id
                    if row == 1:
                        axs[row, 0].text(-0.2, 0.5, f'node_id {desired_node_id - 1 + row}', fontsize=12, fontweight='bold', ha='center', va='center', rotation=90, transform=axs[row, 0].transAxes)
                    else:
                        axs[row, 0].text(-0.2, 0.5, f'node_id {desired_node_id - 1 + row}', fontsize=12, ha='center', va='center', rotation=90, transform=axs[row, 0].transAxes)

            y_labels = ['xval', 'yval', 'zval', 'batt']
            if plot_adjacent_nodes:
                for top in range(4):
                    axs[0, top].set_title(y_labels[top], fontsize=10)
            else:
                for i in range(4):
                    axs[i].set_ylabel(y_labels[i])

            fig.text(0.5, 0.01, 'timestamp', ha='center', fontsize=12)

            if plot_adjacent_nodes:
                for col in range(4):
                    axs[2, col].tick_params(axis='x', rotation=45)
            else:
                axs[3].tick_params(axis='x', rotation=45)

            plt.tight_layout()
            plt.show()
            dyna_db.close()

            break

        except ValueError as ve:
            print(f"Input error: {ve}. Please try again.\n")
        except Exception as e:
            print(f"An error occurred: {e}. Please try again.\n")
            
            
            
def main_v6():  # dynamic type num label
    while True:
        try:
            logger_name = input("Enter the logger name: ").strip()
            if not logger_name:
                raise ValueError("Logger name cannot be empty. Please enter a valid logger name.")

            timedelta_months = int(input("Enter the time delta in months: "))
            if timedelta_months <= 0:
                raise ValueError("Time delta must be a positive integer. Please enter a valid number of months.")

            node_id = int(input("Enter the node_id: "))
            if node_id <= 0:
                raise ValueError("Node ID must be a positive integer. Please enter a valid node ID.")

            plot_adjacent_nodes_input = input("Do you want to plot with adjacent nodes (Y/N)? ").strip().upper()
            if plot_adjacent_nodes_input not in ['Y', 'N']:
                raise ValueError("Invalid input. Please enter 'Y' or 'N'.")
            plot_adjacent_nodes = plot_adjacent_nodes_input == 'Y'

            show_raw_data_input = input("Do you want to show raw data (Y/N)? ").strip().upper()
            if show_raw_data_input not in ['Y', 'N']:
                raise ValueError("Invalid input. Please enter 'Y' or 'N'.")
            show_raw_data = show_raw_data_input == 'Y'

            end_date = datetime.now() + timedelta(days=1)
            start_date = end_date - timedelta(days=timedelta_months * 30)
            start_date_str = start_date.strftime('%Y-%m-%d')
            end_date_str = end_date.strftime('%Y-%m-%d')

            dyna_db = mysql.connector.connect(
                host="192.168.150.112",
                database="analysis_db",
                user="pysys_local",
                password="NaCAhztBgYZ3HwTkvHwwGVtJn5sVMFgg",
            )
            query = f"SELECT * FROM analysis_db.tilt_{logger_name} WHERE ts BETWEEN '{start_date_str}' AND '{end_date_str}' ORDER BY ts"
            df = pd.read_sql(query, dyna_db)

            if len(logger_name) == 4:
                df['type_num'] = 1

            df.columns = ['data_id', 'ts_written', 'ts', 'node_id', 'type_num', 'x', 'y', 'z', 'batt', 'is_live']
            print("Number of rows fetched from database:", len(df))

            type_nums = df['type_num'].unique()
            num_rows = 3 if plot_adjacent_nodes else 1

            if plot_adjacent_nodes:
                fig, axs = plt.subplots(num_rows, 4, figsize=(12, 12), sharex='col')
            else:
                fig, axs = plt.subplots(4, 1, figsize=(12, 12), sharex='col')

            execution_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            fig.text(0.5, 0.945, f'Execution Time: {execution_time}', ha='center', fontsize=10)
            plt.suptitle(f'{logger_name} : node ID {node_id} ({timedelta_months}-month td)', fontsize=16)

            nodes_to_plot = [node_id - 1, node_id, node_id + 1] if plot_adjacent_nodes else [node_id]
            for i, current_node_id in enumerate(nodes_to_plot):
                row = i if plot_adjacent_nodes else 0
                for j, type_num in enumerate(type_nums):
                    df_group = df[df['node_id'] == current_node_id]
                    df_type = df_group[df_group['type_num'] == type_num].copy()

                    accel_label = f'{type_num}'
                    if type_num in [1, 11, 32, 41, 51]:
                        color = 'b'
                    elif type_num in [12, 33, 42, 52]:
                        color = 'r'
                    else:
                        color = 'k'  # default color for unexpected type_num values

                    if show_raw_data:
                        if plot_adjacent_nodes:
                        # Plot raw data with 60% transparency
                            axs[row, 0].plot(df_type['ts'], df_type['x'], label=f'raw {accel_label}', color=color, alpha=0.6)
                            axs[row, 1].plot(df_type['ts'], df_type['y'], label=f'raw {accel_label}', color=color, alpha=0.6)
                            axs[row, 2].plot(df_type['ts'], df_type['z'], label=f'raw {accel_label}', color=color, alpha=0.6)
                            axs[row, 3].plot(df_type['ts'], df_type['batt'], label='raw batt', alpha=0.6)
                            for k in range(4):
                                axs[row, k].legend()
                        else:
                            axs[0].plot(df_type['ts'], df_type['x'], label=f'raw {accel_label}', color=color, alpha=0.6)
                            axs[1].plot(df_type['ts'], df_type['y'], label=f'raw {accel_label}', color=color, alpha=0.6)
                            axs[2].plot(df_type['ts'], df_type['z'], label=f'raw {accel_label}', color=color, alpha=0.6)
                            axs[3].plot(df_type['ts'], df_type['batt'], label='raw batt', alpha=0.6)
                            for k in range(4):
                                axs[k].legend()
                                
                    else:
                        df_type = df_type.groupby('node_id', group_keys=True).apply(apply_filters)
                        if not df_type.empty:
                            if plot_adjacent_nodes:
                                axs[row, 0].plot(df_type['ts'], df_type['x'], label=accel_label, color=color)
                                axs[row, 1].plot(df_type['ts'], df_type['y'], label=accel_label, color=color)
                                axs[row, 2].plot(df_type['ts'], df_type['z'], label=accel_label, color=color)
                                axs[row, 3].plot(df_type['ts'], df_type['batt'], label='batt')
                                axs[row, 3].set_ylim(axs[row, 3].get_ylim()[0] - 0.5, axs[row, 3].get_ylim()[1] + 0.5)  # Set ylim for 4th column
                                for k in range(4):
                                    axs[row, k].legend()
                            else:
                                axs[0].plot(df_type['ts'], df_type['x'], label=accel_label, color=color)
                                axs[1].plot(df_type['ts'], df_type['y'], label=accel_label, color=color)
                                axs[2].plot(df_type['ts'], df_type['z'], label=accel_label, color=color)
                                axs[3].plot(df_type['ts'], df_type['batt'], label='batt')
                                axs[3].set_ylim(axs[3].get_ylim()[0] - 0.5, axs[3].get_ylim()[1] + 0.5)  # Set ylim for 4th row
                                for k in range(4):
                                    axs[k].legend()
                        else:
                            if plot_adjacent_nodes:
                                for k in range(4):
                                    axs[row, k].text(0.5, 0.45, 'No data', fontsize=10, ha='center', va='center', transform=axs[row, k].transAxes)
                            else:
                                for k in range(4):
                                    axs[k].text(0.5, 0.45, 'No data', fontsize=10, ha='center', va='center', transform=axs[k].transAxes)

                if plot_adjacent_nodes:
                    desired_node_id = node_id
                    if row == 1:
                        axs[row, 0].text(-0.2, 0.5, f'node_id {desired_node_id - 1 + row}', fontsize=12, fontweight='bold', ha='center', va='center', rotation=90, transform=axs[row, 0].transAxes)
                    else:
                        axs[row, 0].text(-0.2, 0.5, f'node_id {desired_node_id - 1 + row}', fontsize=12, ha='center', va='center', rotation=90, transform=axs[row, 0].transAxes)

            y_labels = ['xval', 'yval', 'zval', 'batt']
            if plot_adjacent_nodes:
                for top in range(4):
                    axs[0, top].set_title(y_labels[top], fontsize=10)
            else:
                for i in range(4):
                    axs[i].set_ylabel(y_labels[i])

            fig.text(0.5, 0.01, 'timestamp', ha='center', fontsize=12)

            if plot_adjacent_nodes:
                for col in range(4):
                    axs[2, col].tick_params(axis='x', rotation=45)
            else:
                axs[3].tick_params(axis='x', rotation=45)
                
            date_format = DateFormatter('%m-%d %H:%M')  # Define the date format
            if plot_adjacent_nodes:
                for col in range(4):
                    axs[2, col].xaxis.set_major_formatter(date_format)  # Apply format to x-axis
            else:
                axs[3].xaxis.set_major_formatter(date_format)  # Apply format to x-axis


            plt.tight_layout()
            plt.show()
            dyna_db.close()

            break

        except ValueError as ve:
            print(f"Input error: {ve}. Please try again.\n")
        except Exception as e:
            print(f"An error occurred: {e}. Please try again.\n")



def main_v7():  # adjust no data print id theres atleast one type num plot
    while True:
        try:
            logger_name = input("Enter the logger name: ").strip()
            if not logger_name:
                raise ValueError("Logger name cannot be empty. Please enter a valid logger name.")

            timedelta_months = int(input("Enter the time delta in months: "))
            if timedelta_months <= 0:
                raise ValueError("Time delta must be a positive integer. Please enter a valid number of months.")

            node_id = int(input("Enter the node_id: "))
            if node_id <= 0:
                raise ValueError("Node ID must be a positive integer. Please enter a valid node ID.")

            plot_adjacent_nodes_input = input("Do you want to plot with adjacent nodes (Y/N)? ").strip().upper()
            if plot_adjacent_nodes_input not in ['Y', 'N']:
                raise ValueError("Invalid input. Please enter 'Y' or 'N'.")
            plot_adjacent_nodes = plot_adjacent_nodes_input == 'Y'

            show_raw_data_input = input("Do you want to show raw data (Y/N)? ").strip().upper()
            if show_raw_data_input not in ['Y', 'N']:
                raise ValueError("Invalid input. Please enter 'Y' or 'N'.")
            show_raw_data = show_raw_data_input == 'Y'

            end_date = datetime.now() + timedelta(days=1)
            # end_ts ="2020-09-01"                                  ###TEST END TS
            # end_date = datetime.strptime(end_ts, "%Y-%m-%d")      ###TEST END TS
            
            start_date = end_date - timedelta(days=timedelta_months * 30)
            # start_date = end_date - timedelta(days=timedelta_months)
            start_date_str = start_date.strftime('%Y-%m-%d')
            end_date_str = end_date.strftime('%Y-%m-%d')

            dyna_db = mysql.connector.connect(
                host="192.168.150.112",
                database="analysis_db",
                user="hardwareinfra",
                password="veug3r4MTKfsuk5H4rdw4r3",
            )
            query = f"SELECT * FROM analysis_db.tilt_{logger_name} WHERE ts BETWEEN '{start_date_str}' AND '{end_date_str}' ORDER BY ts"
            df = pd.read_sql(query, dyna_db)

            if len(logger_name) == 4:
                df['type_num'] = 1

            #df.columns = ['data_id', 'ts_written', 'ts', 'node_id', 'type_num', 'x', 'y', 'z', 'batt', 'is_live']
            if len(df.columns)==10:
                df.columns = ['data_id', 'ts_written', 'ts', 'node_id', 'type_num', 'x', 'y', 'z', 'batt', 'is_live']
            else:
                df.columns = ['data_id', 'ts_written', 'ts', 'node_id', 'type_num', 'x', 'y', 'z', 'batt']                
            print("Number of rows fetched from database:", len(df))

            type_nums = df['type_num'].unique()
            num_rows = 3 if plot_adjacent_nodes else 1

            if plot_adjacent_nodes:
                fig, axs = plt.subplots(num_rows, 4, figsize=(12, 12), sharex='col')
            else:
                fig, axs = plt.subplots(4, 1, figsize=(12, 12), sharex='col')

            execution_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            fig.text(0.5, 0.945, f'Execution Time: {execution_time}', ha='center', fontsize=10)
            plt.suptitle(f'{logger_name} : node ID {node_id} ({timedelta_months}-month td)', fontsize=16)

            nodes_to_plot = [node_id - 1, node_id, node_id + 1] if plot_adjacent_nodes else [node_id]
            for i, current_node_id in enumerate(nodes_to_plot):
                row = i if plot_adjacent_nodes else 0
                for j, type_num in enumerate(type_nums):
                    df_group = df[df['node_id'] == current_node_id]
                    df_type = df_group[df_group['type_num'] == type_num].copy()
                    
                    df_group = df[df['node_id'] == current_node_id]
                    df_type = df_group[df_group['type_num'] == type_num].copy()
                    
                    last_data_ts = df_type['ts'].max()  # Get the last timestamp
                    last_data_ts_str = last_data_ts.strftime('%Y-%m-%d %H:%M:%S')  # Convert to string
                    
                    # Add the last data timestamp to the figure
                    fig.text(0.98, 0.965, f'Last data ts: {last_data_ts_str}', ha='right', fontsize=10) 

                    accel_label = f'{type_num}'
                    if type_num in [1, 11, 32, 41, 51]:
                        color = 'b'
                    elif type_num in [12, 33, 42, 52]:
                        color = 'r'
                    else:
                        color = 'k'  # default color for unexpected type_num values

                    if show_raw_data:
                        if plot_adjacent_nodes:
                            # Plot raw data with 60% transparency
                            axs[row, 0].plot(df_type['ts'], df_type['x'], label=f'raw {accel_label}', color=color, alpha=0.6)
                            axs[row, 1].plot(df_type['ts'], df_type['y'], label=f'raw {accel_label}', color=color, alpha=0.6)
                            axs[row, 2].plot(df_type['ts'], df_type['z'], label=f'raw {accel_label}', color=color, alpha=0.6)
                            axs[row, 3].plot(df_type['ts'], df_type['batt'], label='raw batt', alpha=0.6)
                            for k in range(4):
                                axs[row, k].legend()
                        else:
                            axs[0].plot(df_type['ts'], df_type['x'], label=f'raw {accel_label}', color=color, alpha=0.6)
                            axs[1].plot(df_type['ts'], df_type['y'], label=f'raw {accel_label}', color=color, alpha=0.6)
                            axs[2].plot(df_type['ts'], df_type['z'], label=f'raw {accel_label}', color=color, alpha=0.6)
                            axs[3].plot(df_type['ts'], df_type['batt'], label='raw batt', alpha=0.6)
                            for k in range(4):
                                axs[k].legend()
                                
                    else:
                        df_type = df_type.groupby('node_id', group_keys=True).apply(apply_filters)
                        if not df_type.empty:
                            if plot_adjacent_nodes:
                                axs[row, 0].plot(df_type['ts'], df_type['x'], label=accel_label, color=color)
                                axs[row, 1].plot(df_type['ts'], df_type['y'], label=accel_label, color=color)
                                axs[row, 2].plot(df_type['ts'], df_type['z'], label=accel_label, color=color)
                                axs[row, 3].plot(df_type['ts'], df_type['batt'], label='batt')
                                axs[row, 3].set_ylim(axs[row, 3].get_ylim()[0] - 0.5, axs[row, 3].get_ylim()[1] + 0.5)  # Set ylim for 4th column
                                for k in range(4):
                                    axs[row, k].legend()
                            else:
                                axs[0].plot(df_type['ts'], df_type['x'], label=accel_label, color=color)
                                axs[1].plot(df_type['ts'], df_type['y'], label=accel_label, color=color)
                                axs[2].plot(df_type['ts'], df_type['z'], label=accel_label, color=color)
                                axs[3].plot(df_type['ts'], df_type['batt'], label='batt')
                                axs[3].set_ylim(axs[3].get_ylim()[0] - 0.5, axs[3].get_ylim()[1] + 0.5)  # Set ylim for 4th row
                                for k in range(4):
                                    axs[k].legend()

                if plot_adjacent_nodes:
                    desired_node_id = node_id
                    if row == 1:
                        axs[row, 0].text(-0.2, 0.5, f'node_id {desired_node_id - 1 + row}', fontsize=12, fontweight='bold', ha='center', va='center', rotation=90, transform=axs[row, 0].transAxes)
                    else:
                        axs[row, 0].text(-0.2, 0.5, f'node_id {desired_node_id - 1 + row}', fontsize=12, ha='center', va='center', rotation=90, transform=axs[row, 0].transAxes)

            y_labels = ['xval', 'yval', 'zval', 'batt']
            if plot_adjacent_nodes:
                for top in range(4):
                    axs[0, top].set_title(y_labels[top], fontsize=10)
            else:
                for i in range(4):
                    axs[i].set_ylabel(y_labels[i])

            fig.text(0.5, 0.01, 'timestamp', ha='center', fontsize=12)
            
            if plot_adjacent_nodes:
                for col in range(4):
                    axs[2, col].tick_params(axis='x', rotation=45)
            else:
                axs[3].tick_params(axis='x', rotation=45)

            date_format = DateFormatter('%m-%d %H:%M')  # Define the date format
            if plot_adjacent_nodes:
                for col in range(4):
                    axs[2, col].xaxis.set_major_formatter(date_format)  # Apply format to x-axis
            else:
                axs[3].xaxis.set_major_formatter(date_format)  # Apply format to x-axis

            plt.tight_layout()
            plt.show()
            dyna_db.close()

            break

        except ValueError as ve:
            print(f"Input error: {ve}. Please try again.\n")
        except Exception as e:
            print(f"An error occurred: {e}. Please try again.\n")



def main_v8():  # Separate plot figure for different type_nums -> save as PNG
    while True:
        try:
            logger_name = input("Enter the logger name: ").strip()
            if not logger_name:
                raise ValueError("Logger name cannot be empty. Please enter a valid logger name.")

            timedelta_months = int(input("Enter the time delta in months: "))
            if timedelta_months <= 0:
                raise ValueError("Time delta must be a positive integer. Please enter a valid number of months.")

            node_id = int(input("Enter the node_id: "))
            if node_id <= 0:
                raise ValueError("Node ID must be a positive integer. Please enter a valid node ID.")

            plot_adjacent_nodes_input = input("Do you want to plot with adjacent nodes (Y/N)? ").strip().upper()
            if plot_adjacent_nodes_input not in ['Y', 'N']:
                raise ValueError("Invalid input. Please enter 'Y' or 'N'.")
            plot_adjacent_nodes = plot_adjacent_nodes_input == 'Y'

            show_raw_data_input = input("Do you want to show raw data (Y/N)? ").strip().upper()
            if show_raw_data_input not in ['Y', 'N']:
                raise ValueError("Invalid input. Please enter 'Y' or 'N'.")
            show_raw_data = show_raw_data_input == 'Y'

            end_date = datetime.now() + timedelta(days=1)
            start_date = end_date - timedelta(days=timedelta_months * 30)
            start_date_str = start_date.strftime('%Y-%m-%d')
            end_date_str = end_date.strftime('%Y-%m-%d')

            dyna_db = mysql.connector.connect(
                host="192.168.150.112",
                database="analysis_db",
                user="hardwareinfra",
                password="veug3r4MTKfsuk5H4rdw4r3",
            )
            query = f"SELECT * FROM analysis_db.tilt_{logger_name} WHERE ts BETWEEN '{start_date_str}' AND '{end_date_str}' ORDER BY ts"
            df = pd.read_sql(query, dyna_db)

            if len(logger_name) == 4:
                df['type_num'] = 1

            if len(df.columns) == 10:
                df.columns = ['data_id', 'ts_written', 'ts', 'node_id', 'type_num', 'x', 'y', 'z', 'batt', 'is_live']
            else:
                df.columns = ['data_id', 'ts_written', 'ts', 'node_id', 'type_num', 'x', 'y', 'z', 'batt']

            print("Number of rows fetched from database:", len(df))

            type_nums = df['type_num'].unique()
            nodes_to_plot = [node_id - 1, node_id, node_id + 1] if plot_adjacent_nodes else [node_id]
            
            for type_num in type_nums:
                for current_node_id in nodes_to_plot:
                    fig, axs = plt.subplots(4, 1, figsize=(24, 12), sharex='col')

                    execution_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    fig.text(0.5, 0.95, f'Execution Time: {execution_time}', ha='center', fontsize=10)
                    plt.suptitle(f'{logger_name} : node ID {current_node_id} (type_num {type_num}) - {timedelta_months}-month td', fontsize=16)
                    
                    df_group = df[df['node_id'] == current_node_id]
                    df_type = df_group[df_group['type_num'] == type_num].copy()
                    
                    last_data_ts = df_type['ts'].max()  # Get the last timestamp
                    last_data_ts_str = last_data_ts.strftime('%Y-%m-%d %H:%M:%S')  # Convert to string
                    
                    # Add the last data timestamp to the figure
                    fig.text(0.98, 0.965, f'Last data ts: {last_data_ts_str}', ha='right', fontsize=10) 

                    accel_label = f'{type_num}'
                    color = 'b' if type_num in [1, 11, 32, 41, 51] else ('r' if type_num in [12, 33, 42, 52] else 'k')

                    # Define marker style
                    marker_style = 'o'  # You can use 'o', 'x', 's', etc. for different marker types
                    marker_size= 2
            
                    if show_raw_data:
                        axs[0].plot(df_type['ts'], df_type['x'], label=f'raw {accel_label}', color=color, alpha=0.5)
                        axs[1].plot(df_type['ts'], df_type['y'], label=f'raw {accel_label}', color=color, alpha=0.5)
                        axs[2].plot(df_type['ts'], df_type['z'], label=f'raw {accel_label}', color=color, alpha=0.5)
                        axs[3].plot(df_type['ts'], df_type['batt'], label='raw batt', alpha=0.5)
                        
                        df_type = df_type.groupby('node_id', group_keys=True).apply(apply_filters)
                        if not df_type.empty:
                            axs[0].plot(df_type['ts'], df_type['x'], label=accel_label, color=color, marker=marker_style, markersize=marker_size)
                            axs[1].plot(df_type['ts'], df_type['y'], label=accel_label, color=color, marker=marker_style, markersize=marker_size)
                            axs[2].plot(df_type['ts'], df_type['z'], label=accel_label, color=color, marker=marker_style, markersize=marker_size)
                            axs[3].plot(df_type['ts'], df_type['batt'], label='batt', marker=marker_style, markersize=marker_size)
                    else:
                        df_type = df_type.groupby('node_id', group_keys=True).apply(apply_filters)
                        if not df_type.empty:
                            axs[0].plot(df_type['ts'], df_type['x'], label=accel_label, color=color, marker=marker_style, markersize=marker_size)
                            axs[1].plot(df_type['ts'], df_type['y'], label=accel_label, color=color, marker=marker_style, markersize=marker_size)
                            axs[2].plot(df_type['ts'], df_type['z'], label=accel_label, color=color, marker=marker_style, markersize=marker_size)
                            axs[3].plot(df_type['ts'], df_type['batt'], label='batt', marker=marker_style, markersize=marker_size)

                    y_labels = ['xval', 'yval', 'zval', 'batt']
                    for i in range(4):
                        axs[i].set_ylabel(y_labels[i])
                        axs[i].legend()

                    axs[3].tick_params(axis='x', rotation=45)
                    date_format = DateFormatter('%m-%d %H:%M')
                    axs[3].xaxis.set_major_formatter(date_format)

                    fig.text(0.5, 0.01, 'timestamp', ha='center', fontsize=12)

                    plt.tight_layout()

                    # Save each figure as a PNG file
                    filename = f"{logger_name}_node_{current_node_id}_type_{type_num}.png"
                    plt.savefig(filename)
                    print(f"Figure saved as {filename}")

                    plt.close(fig)  # Close the figure after saving to avoid memory issues

            dyna_db.close()
            break

        except ValueError as ve:
            print(f"Input error: {ve}. Please try again.\n")
        except Exception as e:
            print(f"An error occurred: {e}. Please try again.\n")



def main_v9():  # Separate plot figure for different type_nums -> save as PNG + LOOP
    while True:
        try:
            logger_name = input("Enter the logger name: ").strip()
            if not logger_name:
                raise ValueError("Logger name cannot be empty. Please enter a valid logger name.")

            # timedelta_months = int(input("Enter the time delta in months: "))
            # if timedelta_months <= 0:
            #     raise ValueError("Time delta must be a positive integer. Please enter a valid number of months.")
            # timedelta_months = 6
            years_to_subtract = 8
            days_in_year = 365
            
            node_id = int(input("Enter the node_id: "))
            if node_id <= 0:
                raise ValueError("Node ID must be a positive integer. Please enter a valid node ID.")

            show_raw_data_input = input("Do you want to show raw data (Y/N)? ").strip().upper()
            if show_raw_data_input not in ['Y', 'N']:
                raise ValueError("Invalid input. Please enter 'Y' or 'N'.")
            show_raw_data = show_raw_data_input == 'Y'
            # show_raw_data = True
            
            end_date = datetime.now() + timedelta(days=1)
            # start_date = end_date - timedelta(days=timedelta_months * 30)
            start_date = end_date - timedelta(days=years_to_subtract * days_in_year)
            start_date_str = start_date.strftime('%Y-%m-%d')
            end_date_str = end_date.strftime('%Y-%m-%d')
            
            # Fetch valid type_nums and number_of_segments for logger_name
            valid_type_nums = fetch_valid_type_nums(logger_name)
           
            dyna_db = mysql.connector.connect(
                host="192.168.150.112",
                database="analysis_db",
                user="hardwareinfra",
                password="veug3r4MTKfsuk5H4rdw4r3"
            )
            
            if valid_type_nums is None:
                query = f"""
                    SELECT * FROM analysis_db.tilt_{logger_name}
                    WHERE ts BETWEEN '{start_date_str}' AND '{end_date_str}'
                    ORDER BY ts
                """
            else:
                query = f"""
                    SELECT * FROM analysis_db.tilt_{logger_name}
                    WHERE ts BETWEEN '{start_date_str}' AND '{end_date_str}'
                    AND type_num IN ({', '.join(map(str, valid_type_nums))})
                    ORDER BY ts
                """
            df = pd.read_sql(query, dyna_db)

            if len(logger_name) == 4:
                df['type_num'] = 1

            if len(df.columns) == 10:
                df.columns = ['data_id', 'ts_written', 'ts', 'node_id', 'type_num', 'x', 'y', 'z', 'batt', 'is_live']
            else:
                df.columns = ['data_id', 'ts_written', 'ts', 'node_id', 'type_num', 'x', 'y', 'z', 'batt']
            print("Number of rows fetched from database:", len(df))

            type_nums = df['type_num'].unique()
            nodes_to_plot = [node_id]       
            for type_num in type_nums:
                for current_node_id in nodes_to_plot:
                    fig, axs = plt.subplots(4, 1, figsize=(24, 12), sharex='col')

                    execution_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    fig.text(0.5, 0.95, f'Execution Time: {execution_time}', ha='center', fontsize=10)
                    plt.suptitle(f'{logger_name} : node ID {current_node_id} (type_num {type_num}) - {years_to_subtract}-years td', fontsize=16)
                    
                    df_group = df[df['node_id'] == current_node_id]
                    df_type = df_group[df_group['type_num'] == type_num].copy()
                    
                    last_data_ts = df_type['ts'].max()  # Get the last timestamp
                    last_data_ts_str = last_data_ts.strftime('%Y-%m-%d %H:%M:%S')  # Convert to string
                    
                    # Add the last data timestamp to the figure
                    fig.text(0.98, 0.965, f'Last data ts: {last_data_ts_str}', ha='right', fontsize=10)            

                    accel_label = f'{type_num}'
                    color = 'b' if type_num in [1, 11, 32, 41, 51] else ('g' if type_num in [12, 33, 42, 52] else 'k')
                    color_raw = 'orange'

                    marker_style = 'o'
                    marker_size= 2
            
                    if show_raw_data:
                        axs[0].plot(df_type['ts'], df_type['x'], label=f'raw {accel_label}', color=color_raw, alpha=0.5)
                        axs[1].plot(df_type['ts'], df_type['y'], label=f'raw {accel_label}', color=color_raw, alpha=0.5)
                        axs[2].plot(df_type['ts'], df_type['z'], label=f'raw {accel_label}', color=color_raw, alpha=0.5)
                        axs[3].plot(df_type['ts'], df_type['batt'], label='raw batt', color=color_raw, alpha=0.5)
                        
                        df_type = df_type.groupby('node_id', group_keys=True).apply(apply_filters)
                        if not df_type.empty:
                            axs[0].plot(df_type['ts'], df_type['x'], label=accel_label, color=color, marker=marker_style, markersize=marker_size)
                            axs[1].plot(df_type['ts'], df_type['y'], label=accel_label, color=color, marker=marker_style, markersize=marker_size)
                            axs[2].plot(df_type['ts'], df_type['z'], label=accel_label, color=color, marker=marker_style, markersize=marker_size)
                            axs[3].plot(df_type['ts'], df_type['batt'], label='batt', marker=marker_style, markersize=marker_size)
                    else:
                        df_type = df_type.groupby('node_id', group_keys=True).apply(apply_filters)
                        if not df_type.empty:
                            axs[0].plot(df_type['ts'], df_type['x'], label=accel_label, color=color, marker=marker_style, markersize=marker_size)
                            axs[1].plot(df_type['ts'], df_type['y'], label=accel_label, color=color, marker=marker_style, markersize=marker_size)
                            axs[2].plot(df_type['ts'], df_type['z'], label=accel_label, color=color, marker=marker_style, markersize=marker_size)
                            axs[3].plot(df_type['ts'], df_type['batt'], label='batt', marker=marker_style, markersize=marker_size)

                    y_labels = ['xval', 'yval', 'zval', 'batt']
                    for i in range(4):
                        axs[i].set_ylabel(y_labels[i])
                        axs[i].legend()

                    axs[3].tick_params(axis='x', rotation=45)
                    date_format = DateFormatter('%Y-%m')
                    axs[3].xaxis.set_major_formatter(date_format)

                    fig.text(0.5, 0.01, 'timestamp', ha='center', fontsize=12)

                    plt.tight_layout()

                    # Save each figure as a PNG file
                    filename = f"{logger_name}_node_{current_node_id}_type_{type_num}.png"
                    plt.savefig(filename)
                    print(f"Figure saved as {filename}")

                    plt.close(fig)  # Close the figure after saving to avoid memory issues

            dyna_db.close()
            
            # Ask if the user wants to save another plot
            save_another = input("Do you want to save another plot (Y/N)? ").strip().upper()
            if save_another != 'Y':
                break 

        except ValueError as ve:
            print(f"Input error: {ve}. Please try again.\n")
        except Exception as e:
            print(f"An error occurred: {e}. Please try again.\n")


def fetch_valid_type_nums(logger_name):
    # Database connection for fetching tsm_sensors info
    dyna_db = mysql.connector.connect(
        host="192.168.150.112",
        database="analysis_db",
        user="hardwareinfra",
        password="veug3r4MTKfsuk5H4rdw4r3",
    )
    cursor = dyna_db.cursor()

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
        dyna_db.close()
        return None, None

    # Filter for active entries where date_deactivated is NULL
    active_tsm = [result for result in tsm_results if result[2] is None]

    if len(active_tsm) == 0:
        print(f"No active entry found for tsm_name '{logger_name}' in tsm_sensors.")
        dyna_db.close()
        return None, None

    version = active_tsm[0][3]
    print(f"'{logger_name}', Version: {version}")

    # Determine valid type_nums based on version
    if version == 2:
        valid_type_nums = [32, 33]
    elif version == 3:
        valid_type_nums = [11, 12]
    elif version == 4:
        valid_type_nums = [41, 42]
    elif version == 5:
        valid_type_nums = [51, 52]
    elif version == 1:
        valid_type_nums = None

    dyna_db.close()
    return valid_type_nums


if __name__ == "__main__":
    main_v7()