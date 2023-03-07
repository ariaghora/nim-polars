# Nim Polars

DataFrame for Nim powered by Polars

## Installation

You will need rust compiler and make.

I don't know the best way to do this yet.
But what works for me (and tested on my macOS M1 aarch64):

- Clone this repository and `cd` into it
- Run `make`
- Run `sudo make install`

## Getting started

```nim
when isMainModule:
    let df = readCsv("dataset.csv")
    echo(df.columns("col1", "col3"))
```