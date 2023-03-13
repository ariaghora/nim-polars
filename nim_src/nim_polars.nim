import strformat

## The low-level binding to Polars structs. Since we don't care about its
## internal structure, we can simply use a raw pointer to the structs.
type
  RsSeries* =  ref object
  RsDataFrame* =  ref object
  RsLazyFrame* =  ref object

const dynLibPath = 
  when defined macosx: "libnim_polars.dylib" 
  elif defined linux: "libnim_polars.so" 
  elif defined windows: "libnim_polars.dll" 
  else: raise newException(Exception, "libnim_polars shared library is not found or platform is unsupported")

proc rs_collect*(lf: RsLazyFrame): RsDataFrame {.cdecl, importc: "collect", dynlib: dynLibPath .}
proc rs_columns*(df: RsDataFrame, names: openArray[cstring], len:cint): RsDataFrame {.cdecl, importc: "columns", dynlib: dynLibPath .}
proc rs_dataframe_to_str*(df: RsDataFrame): cstring {.cdecl, importc: "dataframe_to_str", dynlib: dynLibPath .}
proc rs_free_dataframe*(df: RsDataFrame) {.cdecl, importc: "free_dataframe", dynlib: dynLibPath .}
proc rs_free_lazyframe*(df: RsLazyFrame) {.cdecl, importc: "free_lazyframe", dynlib: dynLibPath .}
proc rs_free_series*(df: RsSeries) {.cdecl, importc: "free_series", dynlib: dynLibPath .}
proc rs_read_csv*(path: cstring): RsDataFrame {.cdecl, importc: "read_csv", dynlib: dynLibPath .}
proc rs_scan_csv*(path: cstring): RsLazyFrame {.cdecl, importc: "scan_csv", dynlib: dynLibPath .}
proc rs_series_to_str*(df: RsSeries): cstring {.cdecl, importc: "series_to_str", dynlib: dynLibPath .}

## High-level wrappers
## ===================

## Series
type
  Series* = object 
    rsData: RsSeries

method `$`(df: Series): string {.base.}=
  let x = rs_series_to_str(df.rsData)
  return &"{x}"

## Dataframes

type
  DataFrame* = object 
    rsData*: RsDataFrame
  LazyFrame* = object 
    rsData*: RsLazyFrame

proc `=destroy`(x: var DataFrame) =
  rs_free_dataframe(x.rsData)

proc `=destroy`(x: var Series) =
  rs_free_series(x.rsData)

method `$`*(df: DataFrame): string {.base.}=
  let x = rs_dataframe_to_str(df.rsData)
  return &"{x}"

method `$`*(lf: LazyFrame): string {.base.}=
  return "<LazyFrame obj>"

proc collect*(lf: LazyFrame): DataFrame = 
  result = DataFrame(rsData: rs_collect(lf.rsData))
  if result.rsData.isNil():
    raise newException(Exception, "Failed to collect")

proc columns*(df: DataFrame, names: varargs[string]): DataFrame = 
  var colNames: seq[cstring]= @[]
  for name in names:
    colNames.add(cstring(name))

  return DataFrame(
    rsData:rs_columns(df.rsData, colNames, cint(len(colNames)))
  )

## Misc

proc readCSV*(path: string): DataFrame = 
  result = DataFrame(rsData: rs_read_csv(path))
  if result.rsData.isNil():
    raise newException(Exception, &"Cannot load CSV {path}")

proc scanCSV*(path: string): LazyFrame = 
  result = LazyFrame(rsData: rs_scan_csv(path))
  if result.rsData.isNil():
    raise newException(Exception, &"Cannot scan CSV {path}")