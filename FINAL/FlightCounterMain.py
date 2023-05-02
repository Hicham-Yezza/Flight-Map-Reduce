#FlightCounter - Main.py

from common import run_map_reduce

if __name__ == '__main__':
    
    # Load the input data from a CSV file into a pandas DataFrame
    df = pd.read_csv('AComp_Passenger_data_no_error.csv', header=None, names=['PassengerID', 'FlightID', 'Origin', 'Destination', 'Depart_Time', 'Duration'])
    
    # Preprocess the data     
    df = clean_data(df)
    
    # call run_map_reduce

    run_map_reduce(df)
