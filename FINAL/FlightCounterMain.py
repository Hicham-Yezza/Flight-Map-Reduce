#!/usr/bin/env python

#FlightCounter - Main.py

import pandas as pd

import FlightCounterCommon

if __name__ == '__main__':
    
    # Load the input data from a CSV file into a pandas DataFrame
    df = pd.read_csv('AComp_Passenger_data_no_error.csv', header=None, names=['PassengerID', 'FlightID', 'Origin', 'Destination', 'Depart_Time', 'Duration'])
    
    # Preprocess the data     
    df = FlightCounterCommon.clean_data(df)
    
    # call run_map_reduce

    FlightCounterCommon.run_map_reduce(df)
