import unittest
import pyspark
import Transform

def test_symbol_date(spark):
    symbol_ip_path = spark.read.csv('/users/madhavjani/PycharmProjects/DataEngineer/Stocks/symbols_valid_meta.csv')
    symbol_op_path = spark.read.csv('/users/madhavjani/PycharmProjects/DataEngineer/Stocks/symbols_valid_meta.csv')

    symbol=Transform.symbols_valid_meta(symbol_ip_path)

    out_columns = list(symbol_op_path.toPandas().column)
    expected_out = ()
