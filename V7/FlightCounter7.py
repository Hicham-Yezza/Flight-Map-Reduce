{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "a94bcfc8",
   "metadata": {},
   "outputs": [],
   "source": [
    "# Updating run_map_reduce function\n",
    "# This function now takes a cleaned DataFrame as input\n",
    "# uses the mapper and combiner to group the data by PassengerID\n",
    "# and then uses the combiner-reducer to calculate the total number of flights for each PassengerID\n",
    "# The output is then formatted as a string with the passenger IDs ranked by total number of flights\n",
    "# with each row in the form \"Rank X: Passenger ID: ABC (XX flights)\".\n",
    "\n",
    "def run_map_reduce(df):\n",
    "    # Map each PassengerID to a key-value pair using the mapper\n",
    "    mapped_data = list(map(mapper, df['PassengerID']))\n",
    "\n",
    "    # Use the combiner to group the data by PassengerID\n",
    "    grouped_data = combiner(mapped_data)\n",
    "\n",
    "    # Use ThreadPoolExecutor to run the reducer across 8 processor cores\n",
    "    with ThreadPoolExecutor(max_workers=8) as executor:\n",
    "        # Reduce the data by calculating the total number of flights for each PassengerID using the combiner-reducer\n",
    "        reduced_data = list(executor.map(lambda x: combiner_reducer(x[0], x[1]), grouped_data.items()))\n",
    "\n",
    "    # Convert the reduced data to a pandas DataFrame and sort by 'TotalFlights' in descending order\n",
    "    result_df = pd.DataFrame(reduced_data, columns=['PassengerID', 'TotalFlights'])\n",
    "    result_df = result_df.sort_values(by='TotalFlights', ascending=False)\n",
    "    \n",
    "    # Format the result as a string with \"Rank X: Passenger ID: ABC (XX flights)\"\n",
    "    result_str = \"\"\n",
    "    for i, row in result_df.iterrows():\n",
    "        result_str += f\"Rank {i+1}: Passenger ID: {row['PassengerID']} ({row['TotalFlights']} flights)\\n\"\n",
    "\n",
    "    return result_str"
   ]
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
