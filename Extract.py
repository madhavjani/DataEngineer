# import os
# import pandas as pd
# # List all files in a directory
# path = '/users/madhavjani/PycharmProjects/DataEngineer/Stocks/etfs'
# files = []
#
# for f in os.listdir(path):
#     if os.path.isfile(os.path.join(path, f)):
#         # Load the file into a dataframe
#             df = pd.read_csv(files)
#         # Append the dataframe to the list
#             files.append(f)
#
# print(files)
# #print(*files, sep="\n")

import os
import pandas as pd
import Transform
# Set the path to the directory containing the files
etfs_path = '/users/madhavjani/PycharmProjects/DataEngineer/Stocks/etfs'
stock_path = '/users/madhavjani/PycharmProjects/DataEngineer/Stocks/etfs'

# Get a list of all files in the directory
file_list = os.listdir(etfs_path)

# Create an empty list to store the dataframes
df_list = []

# Loop through each file and load it into a dataframe
for file_name in file_list:
    file_path = os.path.join(etfs_path, file_name)
    if os.path.isfile(file_path):
        # Load the file into a dataframe
        df = pd.read_csv(file_path)
        # Append the dataframe to the list
        df_list.append(df)

# Concatenate all the dataframes into a single dataframe
df = pd.concat(df_list, ignore_index=True)

# Do whatever you need to do with the combined dataframe
print(df.head())
print(df.count())
