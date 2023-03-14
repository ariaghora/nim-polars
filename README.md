# Nim Polars

DataFrame for Nim powered by Polars

## Installation

You will need rust compiler and make.

I don't know the best way to do this yet.
But what works for me (and tested on my macOS M1 aarch64):

- Clone this repository and `cd` into it
- Run `make`
- Run `sudo make install`

## Example

```nim
import nim_polars as pl

when isMainModule:
  let df1 = pl.readCsv("titanic.csv")
  echo df1

  let df2 = df1
    .columns("Name", "Fare", "Cabin", "Embarked", "Survived")
    .sortBy(names = @["Embarked", "Name"], reverse = @[false, true])
    .head(40)

  echo df2
```

## API

### `DataFrame`

`columns`, `head`, `sortBy`

### `LazyFrame`

`scanCsv`, `collect`