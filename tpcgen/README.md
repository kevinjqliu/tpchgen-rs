# TPC Data Generator CLI

`tpcgen` is a command line interface for generating TPC benchmark data.

## Install

```shell
pip install tpcgen
```

## Examples

```shell
tpcgen tpch -s 1 --output-dir /tmp/tpch
tpcgen tpch parquet -s 100 --tables lineitem --parts 10 --output-dir /tmp/tpch
tpcgen tpcds -s 1 --output-dir /tmp/tpcds
```
