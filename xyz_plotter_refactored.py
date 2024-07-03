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

def range_filter_accel(df):
    df.loc[:, 'type_num'] = df.type_num.astype(str)

    if df.type_num.str.contains('32|33').any():
        df[['x', 'y', 'z']] += np.where((df[['x', 'y', 'z']] < -2970) & (df[['x', 'y', 'z']] > -3072), 4096, 0)
        df[['x', 'y', 'z']] = df[['x', 'y', 'z']].where(abs(df[['x', 'y', 'z']]) <= 1126)

    if df.type_num.str.contains('11|12').any():
        df[['x', 'y', 'z']] += np.where((df[['x', 'y', 'z']] < -2970) & (df[['x', 'y', 'z']] > -3072), 4096, 0)
        df[['x', 'y', 'z']] = df[['x', 'y', 'z']].where(abs(df[['x', 'y', 'z']]) <= 1126)

    if df.type_num.str.contains('41|42').any():
        df[['x', 'y', 'z']] += np.where((df[['x', 'y', 'z']] < -2970) & (df[['x', 'y', 'z']] > -3072), 4096, 0)
        df[['x', 'y', 'z']] = df[['x', 'y', 'z']].where(abs(df[['x', 'y', 'z']]) <= 1126)

    if df.type_num.str.contains('51|52').any():
        df[['x', 'y', 'z']] += np.where(df.x < -32768, 65536, 0)
        df[['x', 'y', 'z']] = df[['x', 'y', 'z']].where(abs(df[['x', 'y', 'z']]) <= 13158)

    return df.dropna(subset=['x'])

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

    if outlierf:
        dfl = dfl.groupby('node_id', group_keys=False).apply(resample_df)
        dfl = dfl.set_index('ts').groupby('node_id', group_keys=False).apply(outlier_filter)
        if dfl.empty:
            return dfl

    return dfl.reset_index()[['ts', 'node_id', 'x', 'y', 'z', 'batt']]

def main():
    logger_name = input("Enter the logger name: ")
    timedelta_months = int(input("Enter the time delta in months: "))
    node_id = int(input("Enter the node_id: "))

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
    df.columns = ['data_id', 'ts_written', 'ts', 'node_id', 'type_num', 'x', 'y', 'z', 'batt', 'is_live']
    print("Number of rows fetched from database:", len(df))
    
    if len(logger_name) == 4:
        df['type_num'] = 1

    df_filtered = df[df['node_id'] == node_id]
    type_nums = df['type_num'].unique()

    fig, axs = plt.subplots(3, 4, figsize=(12, 12), sharex='col')
    execution_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    fig.text(0.5, 0.945, f'Execution Time: {execution_time}', ha='center', fontsize=10)
    plt.suptitle(f'{logger_name} : node ID {node_id}', fontsize=16)

    for i, df_group in enumerate([df[df['node_id'] == node_id - 1], df_filtered, df[df['node_id'] == node_id + 1]]):
        row = i
        for j, type_num in enumerate(type_nums):
            
            df_type = df_group[df_group['type_num'] == type_num].copy()
            df_type = df_type.groupby('node_id', group_keys=True).apply(apply_filters)
            
            if not df_type.empty:
                axs[row, 0].plot(df_type['ts'], df_type['x'], label='accel 1' if type_num == 11 else 'accel 2', color='b' if type_num == 11 else 'r')
                axs[row, 1].plot(df_type['ts'], df_type['y'], label='accel 1' if type_num == 11 else 'accel 2', color='b' if type_num == 11 else 'r')
                axs[row, 2].plot(df_type['ts'], df_type['z'], label='accel 1' if type_num == 11 else 'accel 2', color='b' if type_num == 11 else 'r')
                axs[row, 3].plot(df_type['ts'], df_type['batt'], label='batt')
                
                for k in range(4):
                    axs[row, k].legend()
            else:
                for k in range(4):
                    axs[row, k].text(0.5, 0.5, 'No data', fontsize=10, ha='center', va='center', transform=axs[row, k].transAxes)  # Centered 'No data'                

        desired_node_id = node_id
        if row == 1:
            axs[row, 0].text(-0.2, 0.5, f'node_id {desired_node_id - 1 + row}', fontsize=12, fontweight='bold', ha='center', va='center', rotation=90, transform=axs[row, 0].transAxes)
        else:
            axs[row, 0].text(-0.2, 0.5, f'node_id {desired_node_id - 1 + row}', fontsize=12, ha='center', va='center', rotation=90, transform=axs[row, 0].transAxes)

    fig.text(0.5, 0.01, 'timestamp', ha='center', fontsize=12)
    
    for top in range(4):
        axs[0, top].set_title(['xval', 'yval', 'zval', 'batt'][top], fontsize=10)

    for col in range(4):
        axs[2, col].tick_params(axis='x', rotation=45)

    for row in range(3):
        axs[row, 3].set_ylim(axs[row, 3].get_ylim()[0] - 0.5, axs[row, 3].get_ylim()[1] + 0.5)

    plt.show()
    plt.get_current_fig_manager().window.showMaximized()
    plt.tight_layout()
    dyna_db.close()

if __name__ == "__main__":
    main()