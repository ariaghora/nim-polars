import unittest

import nim_polars

test "functions not nil":
  check not isNil(nim_polars.rs_dataframe_to_str)
  check not isNil(nim_polars.rs_series_to_str)
  check not isNil(nim_polars.readCsv)

test "csv load":
  let df = readCsv("tests/files/test.csv")
  check not df.rsData.isNil