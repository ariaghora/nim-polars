import strformat

## The low-level binding to Polars structs. Since we don't care about its
## internal structure, we can simply use a raw pointer to the structs.
type
  RsSeries* = ptr object
  RsDataFrame* = ptr object
  RsLazyFrame* = ptr object

const dynLibPath =
  when defined macosx: "libnim_polars.dylib"
  elif defined linux: "libnim_polars.so"
  elif defined windows: "libnim_polars.dll"
  else: raise newException(Exception, "libnim_polars shared library is not found or platform is unsupported")

proc rs_collect*(lf: RsLazyFrame): RsDataFrame {.cdecl, importc: "collect",
    dynlib: dynLibPath.}
proc rs_columns*(df: RsDataFrame, names: cstringArray,
    len: cint): RsDataFrame {.cdecl, importc: "columns", dynlib: dynLibPath.}
proc rs_dataframe_to_str*(df: RsDataFrame): cstring {.cdecl,
    importc: "dataframe_to_str", dynlib: dynLibPath.}
proc rs_free_series*(df: RsSeries) {.cdecl, importc: "free_series",
    dynlib: dynLibPath.}
proc rs_head*(df: RsDataFrame, length: uint): RsDataFrame {.cdecl,
    importc: "head", dynlib: dynLibPath.}
proc rs_read_csv*(path: cstring): RsDataFrame {.cdecl, importc: "read_csv",
    dynlib: dynLibPath.}
proc rs_scan_csv*(path: cstring): RsLazyFrame {.cdecl, importc: "scan_csv",
    dynlib: dynLibPath.}
proc rs_series_to_str*(df: RsSeries): cstring {.cdecl, importc: "series_to_str",
    dynlib: dynLibPath.}
proc rs_sort_by*(df: RsDataFrame,
    names: cstringArray, len_names: cint,
    reverse: openArray[cint], len_reverse: cint): RsDataFrame {.cdecl,
        importc: "sort_by",
    dynlib: dynLibPath.}

## High-level wrappers
## ===================

type
  Series* = object
    rsData: RsSeries
  DataFrame* = object
    rsData*: RsDataFrame
  LazyFrame* = object
    rsData*: RsLazyFrame

method `$`(df: Series): string {.base.} =
  let x = rs_series_to_str(df.rsData)
  return &"{x}"

method `$`*(df: DataFrame): string {.base.} =
  let x = rs_dataframe_to_str(df.rsData)
  return &"{x}"

method `$`*(lf: LazyFrame): string {.base.} =
  return "<LazyFrame obj>"

proc collect*(lf: LazyFrame): DataFrame =
  result = DataFrame(rsData: rs_collect(lf.rsData))
  if result.rsData.isNil():
    raise newException(Exception, "Failed to collect")

proc columns*(df: DataFrame, names: varargs[string]): DataFrame =
  let colNames = allocCStringArray(names)

  result = DataFrame(
    rsData: rs_columns(df.rsData, colNames, names.len().cint())
  )

  deallocCStringArray(colNames)

proc head*(df: DataFrame, length: uint = 5): DataFrame =
  return DataFrame(
    rsData: rs_head(df.rsData, length)
  )

proc readCSV*(path: string): DataFrame =
  result = DataFrame(rsData: rs_read_csv(path))
  if result.rsData.isNil():
    raise newException(Exception, &"Cannot load CSV {path}")

proc scanCSV*(path: string): LazyFrame =
  result = LazyFrame(rsData: rs_scan_csv(path))
  if result.rsData.isNil():
    raise newException(Exception, &"Cannot scan CSV {path}")

proc sortBy*(df: DataFrame, names: seq[string], reverse: seq[bool]): DataFrame =
  if names.len != reverse.len:
    if reverse.len != 1:
      raise newException(Exception, "`Reverse` argument must have length of one, or the same length with `names`")

  let colNames = allocCStringArray(names)
  let colNamesLen = names.len().cint()

  var rev: seq[cint] = @[]
  for r in reverse:
    rev.add(r.cint())

  result = DataFrame(
    rsData: rs_sort_by(df.rsData,
      colNames, colNamesLen,
      rev, reverse.len().cint()
    )
  )

  deallocCStringArray(colNames)

proc sortBy*(df: DataFrame, names: seq[string], reverse: bool): DataFrame =
  result = sortBy(df, names, @[reverse])

proc sortBy*(df: DataFrame, name: string, reverse: bool): DataFrame =
  result = sortBy(df, @[name], @[reverse])