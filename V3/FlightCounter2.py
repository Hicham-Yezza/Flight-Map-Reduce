{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "fea9c0b4",
   "metadata": {},
   "outputs": [],
   "source": [
    "#main.py 1 May 2023"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 1,
   "id": "96ec8e39",
   "metadata": {},
   "outputs": [],
   "source": [
    "import pandas as pd\n",
    "\n",
    "def mapper(passenger_id):\n",
    "    return (passenger_id, 1)\n",
    "\n",
    "def intermediate(data):\n",
    "    passenger_id, count = data\n",
    "    return (passenger_id, count)\n",
    "\n",
    "def reducer(passenger_id, count_list):\n",
    "    total_flights = sum(count_list)\n",
    "    return (passenger_id, total_flights)\n",
    "\n",
    "def run_map_reduce(input_data):\n",
    "    # Map each PassengerID to a key-value pair using the mapper\n",
    "    mapped_data = list(map(mapper, input_data['PassengerID']))\n",
    "\n",
    "    # Group the data by PassengerID using the intermediate function\n",
    "    grouped_data = {}\n",
    "    for passenger_id, count in mapped_data:\n",
    "        if passenger_id not in grouped_data:\n",
    "            grouped_data[passenger_id] = []\n",
    "        grouped_data[passenger_id].append(count)\n",
    "\n",
    "    # Reduce the data by calculating the total number of flights for each PassengerID using the reducer\n",
    "    reduced_data = list(map(lambda x: reducer(x[0], x[1]), grouped_data.items()))\n",
    "\n",
    "    # Return the reduced data as a DataFrame\n",
    "    return pd.DataFrame(reduced_data, columns=['PassengerID', 'TotalFlights'])\n",
    "\n",
    "def clean_data(df):\n",
    "    # Add a header row to the DataFrame\n",
    "    df.columns = ['PassengerID', 'FlightID', 'Origin', 'Destination', 'Depart_Time', 'Duration']\n",
    "\n",
    "    # Removing duplicated rows\n",
    "    df = df.drop_duplicates()\n",
    "\n",
    "    return df"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 2,
   "id": "21d5e01c",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "\n",
      "Top 10 passengers with the most flights:\n",
      "\n",
      "PassengerID  TotalFlights\n",
      " UES9151GS5            17\n",
      " SPR4484HA6            17\n",
      " DAZ3029XA0            17\n",
      " HCA3158QA6            17\n",
      " EZC9678QI6            17\n",
      " CKZ3132BR4            16\n",
      " HGO4350KK1            16\n",
      " JJM4724RF7            15\n",
      " WBE6935NU3            15\n",
      " PUD8209OG3            15\n"
     ]
    }
   ],
   "source": [
    "import pandas as pd\n",
    "from concurrent.futures import ThreadPoolExecutor\n",
    "# from common import mapper, intermediate, reducer, run_map_reduce, clean_data\n",
    "\n",
    "if __name__ == '__main__':\n",
    "    # Load the input data from a CSV file into a pandas DataFrame\n",
    "    df = pd.read_csv('AComp_Passenger_data_no_error.csv', header=None, names=['PassengerID', 'FlightID', 'Origin', 'Destination', 'Depart_Time', 'Duration'])\n",
    "\n",
    "    # Clean the data\n",
    "    df = clean_data(df)\n",
    "\n",
    "    # Use ThreadPoolExecutor to run the mapper and reducer across 8 processor cores\n",
    "    with ThreadPoolExecutor(max_workers=8) as executor:\n",
    "        # Map each PassengerID to a key-value pair using the mapper\n",
    "        mapped_data = list(executor.map(mapper, df['PassengerID']))\n",
    "\n",
    "    # Group the data by PassengerID using the intermediate function\n",
    "    grouped_data = {}\n",
    "    for passenger_id, count in map(intermediate, mapped_data):\n",
    "        if passenger_id not in grouped_data:\n",
    "            grouped_data[passenger_id] = []\n",
    "        grouped_data[passenger_id].append(count)\n",
    "\n",
    "    # Use ThreadPoolExecutor to run the reducer across 8 processor cores\n",
    "    with ThreadPoolExecutor(max_workers=8) as executor:\n",
    "        # Reduce the data by calculating the total number of flights for each PassengerID using the reducer\n",
    "        reduced_data = list(executor.map(lambda x: reducer(x[0], x[1]), grouped_data.items()))\n",
    "\n",
    "    # Convert the reduced data to a pandas DataFrame and sort by 'TotalFlights' in descending order\n",
    "    result_df = pd.DataFrame(reduced_data, columns=['PassengerID', 'TotalFlights'])\n",
    "    result_df = result_df.sort_values(by='TotalFlights', ascending=False)\n",
    "\n",
    "    # Print the top 10 passengers with the most flights\n",
    "    print(\"\")\n",
    "    print(\"Top 10 passengers with the most flights:\")\n",
    "    print(\"\")\n",
    "    \n",
    "    print(result_df.head(10).to_string(index=False))\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "49e6cf19",
   "metadata": {},
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.8.8"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
