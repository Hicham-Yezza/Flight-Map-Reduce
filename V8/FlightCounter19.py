{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 27,
   "id": "b2b6abe1",
   "metadata": {},
   "outputs": [],
   "source": [
    "# Adding print statements to monitor multi-threading in action"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 29,
   "id": "c6a71f0a",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Calculating number of mappers...\n",
      "Number of mappers: 8\n",
      "Calculating number of cores...\n",
      "Number of cores: 8\n",
      "Calculating number of combiners...\n",
      "Number of combiners: 1\n",
      "Calculating number of chunks...\n",
      "Number of chunks: 8\n",
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
    "import threading\n",
    "import os\n",
    "from functools import reduce\n",
    "from concurrent.futures import ThreadPoolExecutor\n",
    "\n",
    "def clean_data(df):\n",
    "    # Add a header row to the DataFrame\n",
    "    df.columns = ['PassengerID', 'FlightID', 'Origin', 'Destination', 'Depart_Time', 'Duration']\n",
    "\n",
    "    # Removing duplicated rows\n",
    "    df = df.drop_duplicates()\n",
    "\n",
    "    return df\n",
    "\n",
    "def mapper(data_chunk):\n",
    "    thread_id = threading.get_ident() % os.cpu_count() + 1\n",
    "    mapped_data = []\n",
    "    for passenger_id in data_chunk['PassengerID']:\n",
    "        mapped_data.append((passenger_id, 1))\n",
    "    return mapped_data\n",
    "\n",
    "def combiner(mapped_data):\n",
    "    # Use a dictionary to group the data by PassengerID\n",
    "    grouped_data = {}\n",
    "    for passenger_id, count in mapped_data:\n",
    "        if passenger_id not in grouped_data:\n",
    "            grouped_data[passenger_id] = []\n",
    "        grouped_data[passenger_id].append(count)\n",
    "\n",
    "    # Use ThreadPoolExecutor to run the reduce function across all processor cores\n",
    "    with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:\n",
    "        # Reduce the data by calculating the total number of flights for each PassengerID using the reduce function\n",
    "        reduced_data = list(executor.map(lambda x: (x[0], reduce(lambda a, b: a + b, x[1])), grouped_data.items()))\n",
    "\n",
    "    return reduced_data\n",
    "\n",
    "def run_map_reduce(input_data):\n",
    "    print(\"Calculating number of mappers...\")\n",
    "    num_mappers = os.cpu_count()\n",
    "    print(f\"Number of mappers: {num_mappers}\")\n",
    "\n",
    "    print(\"Calculating number of cores...\")\n",
    "    num_cores = os.cpu_count()\n",
    "    print(f\"Number of cores: {num_cores}\")\n",
    "\n",
    "    print(\"Calculating number of combiners...\")\n",
    "    num_combiners = 1\n",
    "    print(f\"Number of combiners: {num_combiners}\")\n",
    "\n",
    "    print(\"Calculating number of chunks...\")\n",
    "    num_chunks = num_cores\n",
    "    print(f\"Number of chunks: {num_chunks}\")\n",
    "\n",
    "    # Divide the input_data into equal chunks, one per core\n",
    "    data_chunks = []\n",
    "    chunk_size = len(input_data) // num_chunks\n",
    "    for i in range(num_chunks):\n",
    "        if i == num_chunks - 1:\n",
    "            # last chunk includes any remaining rows\n",
    "            data_chunk = input_data[i * chunk_size:]\n",
    "        else:\n",
    "            data_chunk = input_data[i * chunk_size:(i + 1) * chunk_size]\n",
    "        data_chunks.append(data_chunk)\n",
    "\n",
    "    # Use ThreadPoolExecutor to run the mapper and reducer across all processor cores\n",
    "    with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:\n",
    "        # Map each chunk to a key-value pair using the mapper\n",
    "        mapped_data_chunks = list(executor.map(mapper, data_chunks))\n",
    "\n",
    "    # Flatten the mapped data\n",
    "    mapped_data = [item for sublist in mapped_data_chunks for item in sublist]\n",
    "\n",
    "    # Use the combiner to group and reduce the mapped data\n",
    "    reduced_data = combiner(mapped_data)\n",
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
    "    print(result_df.head(10).to_string(index=False))\n",
    "    \n",
    "    \n",
    "#from common import run_map_reduce\n",
    "\n",
    "if __name__ == '__main__':\n",
    "    \n",
    "    # Load the input data from a CSV file into a pandas DataFrame\n",
    "    df = pd.read_csv('AComp_Passenger_data_no_error.csv', header=None, names=['PassengerID', 'FlightID', 'Origin', 'Destination', 'Depart_Time', 'Duration'])\n",
    "    \n",
    "    # Preprocess the data     \n",
    "    df = clean_data(df)\n",
    "    \n",
    "    # call run_map_reduce\n",
    "\n",
    "    run_map_reduce(df)\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "75a22279",
   "metadata": {},
   "outputs": [],
   "source": []
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "4266592f",
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
