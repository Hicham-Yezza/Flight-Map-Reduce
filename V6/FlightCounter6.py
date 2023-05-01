{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 5,
   "id": "fce3bae6",
   "metadata": {},
   "outputs": [],
   "source": [
    "# Making the csv filename an argument to the main function instead of defined inside it"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 15,
   "id": "04378156",
   "metadata": {},
   "outputs": [],
   "source": [
    "import pandas as pd\n",
    "from concurrent.futures import ThreadPoolExecutor\n",
    "\n",
    "def process_data(df):\n",
    "    # Add a header row to the DataFrame\n",
    "    df.columns = ['PassengerID', 'FlightID', 'Origin', 'Destination', 'Depart_Time', 'Duration']\n",
    "\n",
    "    # Removing duplicated rows\n",
    "    df = df.drop_duplicates()\n",
    "\n",
    "    return df\n",
    "\n",
    "def mapper(passenger_id):\n",
    "    return (passenger_id, 1)\n",
    "\n",
    "def combiner(mapped_data):\n",
    "    # Use a dictionary to group the data by PassengerID\n",
    "    grouped_data = {}\n",
    "    for passenger_id, count in mapped_data:\n",
    "        if passenger_id not in grouped_data:\n",
    "            grouped_data[passenger_id] = []\n",
    "        grouped_data[passenger_id].append(count)\n",
    "\n",
    "    # Return the grouped data\n",
    "    return grouped_data\n",
    "\n",
    "\n",
    "def reducer(passenger_id, count_list):\n",
    "    total_flights = sum(count_list)\n",
    "    return (passenger_id, total_flights)\n",
    "\n",
    "\n",
    "def run_map_reduce(input_data):\n",
    "    # Add a header row to the DataFrame\n",
    "    input_data.columns = ['PassengerID', 'FlightID', 'Origin', 'Destination', 'Depart_Time', 'Duration']\n",
    "    \n",
    "   \n",
    "    # Use ThreadPoolExecutor to run the mapper and reducer across 8 processor cores\n",
    "    with ThreadPoolExecutor(max_workers=8) as executor:\n",
    "        # Map each PassengerID to a key-value pair using the mapper\n",
    "        mapped_data = list(executor.map(mapper, input_data['PassengerID']))\n",
    "\n",
    "        # Use the combiner to group the data by PassengerID\n",
    "        grouped_data = combiner(mapped_data)\n",
    "\n",
    "        # Use the reducer to calculate the total number of flights for each PassengerID\n",
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
    "    print(result_df.head(10).to_string(index=False))\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 16,
   "id": "4c87bc05",
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
    "def main(filename):\n",
    "    # Load the input data from a CSV file into a pandas DataFrame\n",
    "    df = pd.read_csv(filename, header=None, names=['PassengerID', 'FlightID', 'Origin', 'Destination', 'Depart_Time', 'Duration'])\n",
    "\n",
    "    # Pre-process the data\n",
    "    df = process_data(df)\n",
    "    \n",
    "    # Run the map-reduce job on the input data\n",
    "    result_df = run_map_reduce(df)    \n",
    "\n",
    "if __name__ == '__main__':\n",
    "    filename = 'AComp_Passenger_data_no_error.csv'\n",
    "    main(filename)\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "8d7147ef",
   "metadata": {},
   "outputs": [],
   "source": []
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "d0f04b0f",
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
