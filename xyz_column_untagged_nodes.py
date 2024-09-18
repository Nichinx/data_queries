# -*- coding: utf-8 -*-
"""
Created on Thu Jul  4 19:05:14 2024

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

    if outlierf:
        dfl = dfl.groupby('node_id', group_keys=False).apply(resample_df)
        dfl = dfl.set_index('ts').groupby('node_id', group_keys=False).apply(outlier_filter)
        if dfl.empty:
            return dfl

    return dfl.reset_index()[['ts', 'node_id', 'x', 'y', 'z', 'batt']]

def main():
    logger_name = input("Enter the logger name: ")
    timedelta_months = int(input("Enter the time delta in months: "))
    
    end_date = datetime.now() + timedelta(days=1)
    start_date = end_date - timedelta(days=timedelta_months * 30)
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    execution_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    dyna_db = mysql.connector.connect(
        host="192.168.150.112",
        database="analysis_db",
        user="pysys_local",
        password="NaCAhztBgYZ3HwTkvHwwGVtJn5sVMFgg",
    )
    
    query1 = f"""
            select tsm.site_id, tsm.tsm_id, tsm.logger_id, tsm.tsm_name, tsm.date_activated, tsm.date_deactivated, tsm.version, acc.accel_id, acc.node_id, acc.accel_number, acc.in_use 
            from tsm_sensors as tsm
            inner join accelerometers as acc 
            on tsm.tsm_id = acc.tsm_id
            where date_deactivated is null
            and tsm_name='{logger_name}'
            """
    df = pd.read_sql(query1, dyna_db)
    accel_id_array = df.accel_id.tolist()
    accel_id_list_str = ', '.join(map(str, accel_id_array))
    
    query2 = f"""
            select acc.accel_id, acc.node_id, acc.accel_number, acc.in_use, astat.status, astat.remarks 
            from accelerometers as acc 
            inner join accelerometer_status as astat
            on acc.accel_id = astat.accel_id
            where acc.accel_id in ({accel_id_list_str})
            """
    df_stat = pd.read_sql(query2, dyna_db)
    
    if df_stat.empty:
        query4 = f"""
                select * from tilt_{logger_name}
                where ts between '{start_date_str}' and '{end_date_str}' 
                order by ts
                """
        df_tilt = pd.read_sql(query4, dyna_db)
    else:
        tagged_accel_id_array = df_stat.accel_id.tolist()
        tagged_accel_id_list_str = ', '.join(map(str, tagged_accel_id_array))
    
        query3 = f"""
                select tsm.site_id, tsm.tsm_id, tsm.logger_id, tsm.tsm_name, tsm.date_activated, tsm.date_deactivated, tsm.version, acc.accel_id, acc.node_id, acc.accel_number, acc.in_use 
                from tsm_sensors as tsm
                inner join accelerometers as acc 
                on tsm.tsm_id = acc.tsm_id
                where date_deactivated is null
                and tsm_name='{logger_name}'
                and accel_id not in ({tagged_accel_id_list_str})
                """
        df_tbt = pd.read_sql(query3, dyna_db)
        tbt_node_id_array = list(set(df_tbt.node_id.tolist()))
        tbt_node_id_list_str = ', '.join(map(str, tbt_node_id_array))
    
        query4 = f"""
                select * from tilt_{logger_name}
                where ts between '{start_date_str}' and '{end_date_str}' 
                and node_id in ({tbt_node_id_list_str})
                order by ts
                """
        df_tilt = pd.read_sql(query4, dyna_db)

    dyna_db.close()
    
    
    df = df_tilt.copy()
    # df.columns = ['data_id', 'ts_written', 'ts', 'node_id', 'type_num', 'x', 'y', 'z', 'batt', 'is_live']
    if len(df.columns)==10:
        df.columns = ['data_id', 'ts_written', 'ts', 'node_id', 'type_num', 'x', 'y', 'z', 'batt', 'is_live']
    else:
        df.columns = ['data_id', 'ts_written', 'ts', 'node_id', 'type_num', 'x', 'y', 'z', 'batt'] 
    
    if len(logger_name) == 4:
        df['type_num'] = 1
        
    type_nums = df['type_num'].unique()
    num_rows = 4
    
    node_ids = list(set(df_tilt.node_id.tolist()))
    # for current_node_id in enumerate(node_ids):
    for current_node_id in node_ids:
        fig, axs = plt.subplots(num_rows, 1, figsize=(12, 12), sharex='col')
        # for type_num in enumerate(type_nums):
        for type_num in type_nums:
            df_group = df[df['node_id'] == current_node_id]
            df_type = df_group[df_group['type_num'] == type_num].copy()

            accel_label = f'{type_num}'
            if type_num in [1, 11, 32, 41, 51]:
                color = 'b'
            elif type_num in [12, 33, 42, 52]:
                color = 'r'
            else:
                color = 'k'  # default color for unexpected type_num values
        
            df_type = df_type.groupby('node_id', group_keys=True).apply(apply_filters)
            if not df_type.empty:
                axs[0].plot(df_type['ts'], df_type['x'], label=accel_label, color=color)
                axs[1].plot(df_type['ts'], df_type['y'], label=accel_label, color=color)
                axs[2].plot(df_type['ts'], df_type['z'], label=accel_label, color=color)
                axs[3].plot(df_type['ts'], df_type['batt'], label='batt')
                axs[3].set_ylim(axs[3].get_ylim()[0] - 0.5, axs[3].get_ylim()[1] + 0.5)  # Set ylim for 4th row
                for k in range(4):
                    axs[k].legend()
            else:
                for k in range(4):
                    axs[k].text(0.5, 0.45, 'No data', fontsize=10, ha='center', va='center', transform=axs[k].transAxes)


        y_labels = ['xval', 'yval', 'zval', 'batt']
        for i in range(4):
            axs[i].set_ylabel(y_labels[i])
    
        axs[3].tick_params(axis='x', rotation=45)
            
        date_format = DateFormatter('%m-%d %H:%M')  # Define the date format
        axs[3].xaxis.set_major_formatter(date_format)  # Apply format to x-axis
    
        plt.suptitle(f'{logger_name} : node ID {current_node_id} ({timedelta_months}-month td)', fontsize=16)
        fig.text(0.5, 0.945, f'Execution Time: {execution_time}', ha='center', fontsize=10)
        fig.text(0.5, 0.01, 'timestamp', ha='center', fontsize=12)
    
        plt.tight_layout()
        plt.show()
    
    
    
if __name__ == "__main__":
    main()