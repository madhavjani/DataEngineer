from pyspark.sql import SQLContext
from pyspark.sql.functions import col,to_date
import pyspark.sql.types as T
from pyspark.sql.functions import *
from pyspark.sql.window import Window
# import Extract

class Transform:

    def symbols_valid_meta(self,df_symbol):
        columns = ['Nasdaq Traded', 'Listing Exchange',
         'Market Category', 'ETF', 'Round Lot Size', 'Test Issue',
         'Financial Status', 'CQS Symbol', 'NASDAQ Symbol', 'NextShares']

        compute_df=df_symbol.withColumnRenamed("Security Name","Security_Name").drop(*columns)

        return compute_df

    def stocks(self,df_stocks,df_symbol):
        df_stocks = df_stocks.withColumn("Date", to_date(col("Date")).cast("date")) \
            .withColumn("High", col("High").cast("float")) \
            .withColumn("Open", col("Open").cast("float")) \
            .withColumn("Low", col("Low").cast("float")) \
            .withColumn("Close", col("Close").cast("float")) \
            .withColumn("Adj Close", col("Adj Close").cast("float")) \
            .withColumn("Symbol", regexp_replace("Symbol", ".csv$", "")) \
            .withColumnRenamed("Adj Close", "Adj_Close")

        compute_stocks = df_symbol.select("Symbol", "Security_Name").join(df_stocks, "Symbol")
        return compute_stocks

    def etfs(self,df_etfs,df_symbol):
        df_etfs=df_etfs.withColumn("Date",to_date(col("Date")).cast("date"))\
            .withColumn("High",col("High").cast("float")) \
            .withColumn("Open", col("Open").cast("float")) \
            .withColumn("Low", col("Low").cast("float")) \
            .withColumn("Close", col("Close").cast("float")) \
            .withColumn("Adj Close", col("Adj Close").cast("float")) \
            .withColumn("Symbol",regexp_replace("Symbol",".csv$",""))\
            .withColumnRenamed("Adj Close","Adj_Close")

        compute_etfs=df_symbol.select("Symbol","Security_Name").join(df_etfs,"Symbol")

        return compute_etfs