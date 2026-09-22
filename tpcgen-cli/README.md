# tpcgen-cli

`tpcgen-cli` provides the command line interface for generating TPC-H
and TPC-DS benchmark data.

## Install

```shell
pip install tpcgen-cli
```

## Examples

```shell
tpcgen-cli tpch -s 1 --output-dir /tmp/tpch
tpcgen-cli tpch csv -s 1 --output-dir /tmp/tpch
tpcgen-cli tpch parquet -s 100 --tables lineitem --parts 10 --output-dir /tmp/tpch
tpcgen-cli tpch parquet -s 1 --tables lineitem --column-encoding=l_comment=DELTA_LENGTH_BYTE_ARRAY --output-dir /tmp/tpch
tpcgen-cli tpcds csv -s 1 --output-dir /tmp/tpcds
tpcgen-cli tpcds csv -s 1 --delimiter='\t' --output-dir /tmp/tpcds
```

TPC-DS DAT and CSV generation buffers output in parallel chunks. Use
`--chunk-bytes` to tune their approximate in-memory target size in bytes
(default: `8388608`, or 8 MiB). Smaller chunks reduce peak memory but increase
scheduling overhead:

```shell
tpcgen-cli tpcds dat -s 10 --chunk-bytes 67108864 --output-dir /tmp/tpcds
tpcgen-cli tpcds csv -s 10 --chunk-bytes 67108864 --output-dir /tmp/tpcds
```

This setting does not split output files or change their contents. Parquet
generation uses the separate `--row-group-bytes` option.
