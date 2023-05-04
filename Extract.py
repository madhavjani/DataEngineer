import os
import pandas as pd
#import Transform
# Set the path to the directory containing the files
etfs_path = '/users/madhavjani/PycharmProjects/DataEngineer/Stocks/etfs'
# stock_path = '/users/madhavjani/PycharmProjects/DataEngineer/Stocks/stocks'
# symbol_path = '/users/madhavjani/PycharmProjects/DataEngineer/Stocks'

# Get a list of all files in the directory
file_list = os.listdir(etfs_path)
print(file_list)
# Create an empty list to store the dataframes
df_list = []

print(df_list)

# Loop through each file and load it into a dataframe
for f in file_list:
     file_path = os.path.join(etfs_path, f)
     print(file_path)
#     if os.path.isfile(file_path):
#         # Load the file into a dataframe
#         df = pd.read_csv(file_path)
#         # Append the dataframe to the list
#         df_list.append(df)
#
# # Concatenate all the dataframes into a single dataframe
# df = pd.concat(df_list, ignore_index=True)
#
# # Do whatever you need to do with the combined dataframe
# print(df.head())
# print(df.count())
