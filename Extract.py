import os
import pandas as pd
from Transform import Transform
from pyspark.sql import SparkSession
import glob

spark = SparkSession.builder.appName("myApp").getOrCreate()

# Set the path to the directory containing the files
etfs_path = '/users/madhavjani/PycharmProjects/DataEngineer/Stocks/etfs'
stock_path = '/users/madhavjani/PycharmProjects/DataEngineer/Stocks/stocks'
symbol_path = '/users/madhavjani/PycharmProjects/DataEngineer/Stocks/symbols_valid_meta.csv'

# Get a list of all files in the directory
etfs_list = os.listdir(etfs_path)
stock_list = os.listdir(stock_path)


# Create an empty list to store the dataframes
df_etfs_list = []
df_stocks_list = []

## SYMBOL FILE ##
df_symbol = spark.read.csv(symbol_path, header=True, inferSchema=True)
df_symbol.write.format("parquet").mode("overwrite").save("Symbol_Raw_Data.parquet")
df_symbol=Transform().symbols_valid_meta(df_symbol)
df_symbol.write.format("parquet").mode("overwrite").save("Symbol_Transformed_Data.parquet")

#print(df_symbol.show(truncate=False))

## ETFS FILE ##
for f in etfs_list:
    file_path = os.path.join(etfs_path, f)
    if os.path.isfile(file_path):
        pd_etfs = pd.read_csv(file_path)
        pd_etfs["Symbol"]=f
        df_etfs_list.append(pd_etfs)

pd_etfs = pd.concat(df_etfs_list, ignore_index=True)
df_etfs=spark.createDataFrame(pd_etfs)
df_etfs.write.format("parquet").mode("overwrite").save("ETF_Raw_Data.parquet")

df_etfs=Transform().etfs(df_etfs,df_symbol)
df_etfs.write.format("parquet").mode("overwrite").save("ETF_Transformed_Data.parquet")


#print(df_etfs.printSchema())
# print(df_etfs.filter(df_etfs.Symbol == "ACSI").show(50,truncate=False))
# print(df_etfs.groupby("Symbol","Security_Name").count().show(truncate=False))

# STOCKS FILE ##
for f in stock_list:
    file_path = os.path.join(stock_path, f)
    if os.path.isfile(file_path):
        pd_stock = pd.read_csv(file_path)
        pd_stock["Symbol"] = f
        df_stocks_list.append(pd_stock)

pd_stock = pd.concat(df_stocks_list, ignore_index=True)

df_stocks=spark.createDataFrame(pd_stock)
df_stocks.write.format("parquet").mode("overwrite").save("Stocks_Raw_Data.parquet")

df_stocks=Transform().stocks(df_stocks,df_symbol)
df_stocks.write.format("parquet").mode("overwrite").save("Stocks_Transformed_Data.parquet")

# print(df_stocks.printSchema())
# print(df_stocks.groupby("Symbol","Security_Name").count().show())
print(df_stocks.show(50,truncate=False))
