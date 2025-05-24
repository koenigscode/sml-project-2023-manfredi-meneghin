import os
import pandas as pd
import numpy as np
import json
import math
from src.other.utils import get_day_of_week, get_date_label, zylaAPI_num_flight_within
from datetime import datetime
from lagerhaus.featuremanagement import FeatureStore, FeatureView, FeatureMetadata
from lagerhaus.datacleaning.preprocessing import drop_columns

def drop_duplicates(df, feature_view):
    df.drop_duplicates(subset = ['depScheduledTime', 'depApIataCode', 'arrApIataCode'], inplace= True)
    df.reset_index(inplace = True)
    df.drop(columns={'index'}, inplace = True)
    return df

def filter_active_flights(df, feature_view):
    row_to_remove = []

    for row in range(df.shape[0]):
        if df.at[row, 'status'] not in {'active'}:
            row_to_remove.append(row)


    # Remove the not wanted rows and reset the index
    df.drop(row_to_remove, inplace=True)
    df.reset_index(inplace = True)
    df.drop(columns={'index'}, inplace = True)
    return df

def create_time_columns(df, feature_view):
    # Create new columns for future df's data
    new_column_names  = ['date','time', 'month', 'trip_time', 'day_of_week']
    new_column_values = []
    # Create data for new columns
    for row in range(df.shape[0]):
        # Get year, month, day and time from the selected row
        dep_ts       = df.at[row, 'depScheduledTime']
        dep_datetime = datetime.strptime(dep_ts, "%Y-%m-%dT%H:%M:%S.%f")
        arr_ts = df.at[row, 'arrScheduledTime']
        arr_datetime = datetime.strptime(arr_ts, "%Y-%m-%dT%H:%M:%S.%f")

        delta_time = arr_datetime - dep_datetime
        trip_time  = math.ceil(delta_time.total_seconds()/60)

        dep_yyyy = dep_datetime.year
        dep_mm   = dep_datetime.month
        dep_dd   = dep_datetime.day
        dep_hh   = dep_datetime.hour

        # Get additional information from the departure datetime
        dep_date_label  = get_date_label( dep_yyyy, dep_mm, dep_dd, 'hyphen')
        day_of_the_week = get_day_of_week(dep_yyyy, dep_mm, dep_dd)
        # Save now: date_label, hour, month, trip_time, day_of_the_week

        new_column_values.append([dep_date_label, dep_hh, dep_mm, trip_time, day_of_the_week])


    # Add the column "flight_within_60min" and calculate these values for each flight
    flight_within, column_name = zylaAPI_num_flight_within(60, df)
    df[column_name] = flight_within
    df[new_column_names] = new_column_values
    return df

# Read flight dataset in .json format and load it on a dataframe
file_name = 'extracted_flight_historical_data.json'
file_path = '/Users/koenig/Desktop/thesis/sml-project-2023-manfredi-meneghin/datasets/flight_historical_data/'
complete_name = file_path + file_name
df = pd.read_json(complete_name)

fs = FeatureStore(df=df, metadata={
    'status': FeatureMetadata(categorical=True),
    'depApIataCode': FeatureMetadata(categorical=True),
    'depDelay': FeatureMetadata(),
    'depApTerminal': FeatureMetadata(),
    'depApGate': FeatureMetadata(),
    'arrApIataCode': FeatureMetadata(categorical=True),
    'airlineIataCode': FeatureMetadata(categorical=True),
    'flightIataNumber': FeatureMetadata(),
    'depScheduledTime': FeatureMetadata(),
    'arrScheduledTime': FeatureMetadata()
})

fv = FeatureView(feature_store=fs, transformers=[drop_duplicates, filter_active_flights, create_time_columns, drop_columns(['depScheduledTime', 'arrScheduledTime'])])


# Save the new dataframe in a new file (.csv)
ts_path = "/Users/koenig/Desktop/thesis/sml-project-2023-manfredi-meneghin/datasets/flight_historical_data/"
ts_name = 'output.csv'
ts_complete_path = os.path.join(ts_path, ts_name)

with open(ts_complete_path, "wb") as df_out:
    fv.get_all().to_csv(df_out, index= False)
df_out.close()

