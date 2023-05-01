{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "762a13f3",
   "metadata": {},
   "outputs": [],
   "source": [
    "# Merging grouping and reducer functions into a combiner function"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 1,
   "id": "4a5eaead",
   "metadata": {},
   "outputs": [],
   "source": [
    "import pandas as pd\n",
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
    "def mapper(passenger_id):\n",
    "    return (passenger_id, 1)\n",
    "\n",
    "def combiner(passenger_data):\n",
    "    # Group the data by passenger ID\n",
    "    grouped_data = {}\n",
    "    for passenger_id, count in passenger_data:\n",
    "        if passenger_id not in grouped_data:\n",
    "            grouped_data[passenger_id] = 0\n",
    "        grouped_data[passenger_id] += count\n",
    "    \n",
    "    # Calculate the total flights for each passenger\n",
    "    reduced_data = [(passenger_id, total_flights) for passenger_id, total_flights in grouped_data.items()]\n",
    "    \n",
    "    return reduced_data\n",
    "\n",
    "   # Use the combiner to group the data by PassengerID\n",
    "    grouped_data = {}\n",
    "    for passenger_id, count in mapped_data:\n",
    "        if passenger_id not in grouped_data:\n",
    "            grouped_data[passenger_id] = []\n",
    "        grouped_data[passenger_id].append(count)\n",
    "    combined_data = [combiner(key, value) for key, value in grouped_data.items()]\n",
    "\n",
    "    # Reduce the data by calculating the total number of flights for each PassengerID using the combiner/reducer\n",
    "    reduced_data = [(passenger_id, sum(count_list)) for passenger_id, count_list in combined_data]\n",
    "\n",
    "    # Return the reduced data as a DataFrame\n",
    "    return pd.DataFrame(reduced_data, columns=['PassengerID', 'TotalFlights'])"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 2,
   "id": "710bce93",
   "metadata": {},
   "outputs": [],
   "source": [
    "def main(filename):\n",
    "    # Load the input data from a CSV file into a pandas DataFrame\n",
    "    df = pd.read_csv(filename, header=None, names=['PassengerID', 'FlightID', 'Origin', 'Destination', 'Depart_Time', 'Duration'])\n",
    "\n",
    "    # Run the map-reduce job on the input data\n",
    "    result_df = run_map_reduce(df)\n",
    "\n",
    "    # Sort the result by 'TotalFlights' in descending order\n",
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
   "execution_count": null,
   "id": "035f767d",
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
