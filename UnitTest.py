import unittest
import pyspark
import pandas as pd
from Transform import Transform

def get_sorted_data_frame(dataframe,columnList):
    return dataframe.sort_values(columnList).reset_index(drop=True)

def test_symbol_date(spark):
    symbol_ip_path = spark.read.csv('/users/madhavjani/PycharmProjects/DataEngineer/UnitTest/symbols_valid_meta.csv')
    symbol_op_path = spark.read.csv('/users/madhavjani/PycharmProjects/DataEngineer/UnitTest/symbols_valid_meta.csv')

    t=Transform()

    symbol=t.symbols_valid_meta(symbol_ip_path)

    out_columns = list(symbol_op_path.toPandas().column)
    expected_out = get_sorted_data_frame(symbol_op_path.toPandas(),out_columns)

    real_out=get_sorted_data_frame(symbol.toPandas(),out_columns)

    pd.testing.assert_frame_equal(expected_out,real_out,checkline=True, check_dtype=False)