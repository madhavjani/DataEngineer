# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType
# from pyspark.sql.functions import lit
# from pyspark.sql import SparkSession
#
# # Create a SparkSession
# spark = SparkSession.builder.appName('Append Data to Table').getOrCreate()
#
# # Define the schema
# schema = StructType([
#     StructField("Symbol", StringType(), True),
#     StructField("Security_Name", IntegerType(), True),
#     StructField("Date", DateType(), True),
#     StructField("Open", FloatType(), True),
#     StructField("High", FloatType(), True),
#     StructField("Low", FloatType(), True),
#     StructField("Close", FloatType(), True),
#     StructField("Adj_Close", FloatType(), True),
#     StructField("Volumne", FloatType(), True),
# ])