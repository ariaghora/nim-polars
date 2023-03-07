import strformat

## The low-level binding to Polars structs. Since we don't care about its
## internal structure, we can simply use a raw pointer to the structs.
type
  RsSeries* =  ref object
  RsDataFrame* =  ref object

const dynLibPath = "libnim_polars.dylib" 
proc rs_free_dataframe*(df: RsDataFrame) {.cdecl, importc: "rs_free_dataframe", dynlib: dynLibPath .}
proc rs_dataframe_to_str*(df: RsDataFrame): cstring {.cdecl, importc: "rs_dataframe_to_str", dynlib: dynLibPath .}
proc rs_read_csv*(path: cstring): RsDataFrame {.cdecl, importc: "rs_read_csv", dynlib: dynLibPath .}
proc rs_series_to_str*(df: RsSeries): cstring {.cdecl, importc: "rs_series_to_str", dynlib: dynLibPath .}

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

proc `=destroy`(x: var DataFrame) =
  rs_free_dataframe(x.rsData)

method `$`*(df: DataFrame): string {.base.}=
  let x = rs_dataframe_to_str(df.rsData)
  return &"{x}"

## Misc

proc readCsv*(path: string): DataFrame = 
  result = DataFrame(rsData: rs_read_csv(path))
  if result.rsData.isNil():
    raise newException(Exception, &"Cannot load CSV {path}")
