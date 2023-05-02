# Flight Counter - common.py
# Definitions of functions called  by FlightCountMain.py

import pandas as pd
import threading
import os
from functools import reduce
from concurrent.futures import ThreadPoolExecutor

def clean_data(df):
    # Add a header row to the DataFrame
    df.columns = ['PassengerID', 'FlightID', 'Origin', 'Destination', 'Depart_Time', 'Duration']

    # Removing duplicated rows
    df = df.drop_duplicates()

    return df

def mapper(data_chunk):
     """
    Maps a PassengerID to a key-value pair.

    Parameters:
        passenger_id (int): The PassengerID to map.

    Returns:
        tuple: A key-value pair of the PassengerID and a count of 1.
    """
    thread_id = threading.get_ident() % os.cpu_count() + 1
    mapped_data = []
    for passenger_id in data_chunk['PassengerID']:
        mapped_data.append((passenger_id, 1))
    return mapped_data

def combiner(mapped_data):
    """
    Groups the counts for each PassengerID.

    Parameters:
        passenger_id (int): The PassengerID.
        count_list (list): A list of counts (should only contain 1s).

    Returns:
        tuple: A key-value pair of the PassengerID and a list of counts.
    """
    # Use a dictionary to group the data by PassengerID
    grouped_data = {}
    for passenger_id, count in mapped_data:
        if passenger_id not in grouped_data:
            grouped_data[passenger_id] = []
        grouped_data[passenger_id].append(count)

    # Use ThreadPoolExecutor to run the reduce function across all processor cores
    with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        # Reduce the data by calculating the total number of flights for each PassengerID using the reduce function
        reduced_data = list(executor.map(lambda x: (x[0], reduce(lambda a, b: a + b, x[1])), grouped_data.items()))

    return reduced_data

def run_map_reduce(input_data):
    """
    Runs a map-reduce job to count the number of flights for each PassengerID.

    Parameters:
        input_data (pandas.DataFrame): The input data.
        
    Returns:
        pandas.DataFrame: A DataFrame containing the total number of flights for each PassengerID.
    """
    print("Calculating number of mappers...")
    num_mappers = os.cpu_count()
    print(f"Number of mappers: {num_mappers}")

    print("Calculating number of cores...")
    num_cores = os.cpu_count()
    print(f"Number of cores: {num_cores}")

    print("Calculating number of combiners...")
    num_combiners = 1
    print(f"Number of combiners: {num_combiners}")

    print("Calculating number of chunks...")
    num_chunks = num_cores
    print(f"Number of chunks: {num_chunks}")

    # Divide the input_data into equal chunks, one per core
    data_chunks = []
    chunk_size = len(input_data) // num_chunks
    for i in range(num_chunks):
        if i == num_chunks - 1:
            # last chunk includes any remaining rows
            data_chunk = input_data[i * chunk_size:]
        else:
            data_chunk = input_data[i * chunk_size:(i + 1) * chunk_size]
        data_chunks.append(data_chunk)

    # Use ThreadPoolExecutor to run the mapper and reducer across all processor cores
    with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        # Map each chunk to a key-value pair using the mapper
        mapped_data_chunks = list(executor.map(mapper, data_chunks))

    # Flatten the mapped data
    mapped_data = [item for sublist in mapped_data_chunks for item in sublist]

    # Use the combiner to group and reduce the mapped data
    reduced_data = combiner(mapped_data)

    # Convert the reduced data to a pandas DataFrame and sort by 'TotalFlights' in descending order
    result_df = pd.DataFrame(reduced_data, columns=['PassengerID', 'TotalFlights'])
    result_df = result_df.sort_values(by='TotalFlights', ascending=False)

    # Print the top 10 passengers with the most flights
    print("")
    print("Top 10 passengers with the most flights:")
    print("")
    
    print(result_df.head(10).to_string(index=False))
