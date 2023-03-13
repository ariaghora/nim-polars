import unittest

import nim_polars

test "functions not nil":
  check not nim_polars.rs_columns.isNil()
  check not nim_polars.rs_dataframe_to_str.isNil()
  check not nim_polars.rs_series_to_str.isNil()
  check not nim_polars.rs_read_csv.isNil()
  check not nim_polars.rs_scan_csv.isNil()

test "csv read":
  let df = readCSV("tests/files/test.csv")
  check not df.rsData.isNil()

test "csv scan":
  let lf = scanCSV("tests/files/test.csv")
  check not lf.rsData.isNil()

  let df = lf.collect()
  check not df.rsData.isNil()
  let a = df.columns("a")
  check not a.rsData.isNil()