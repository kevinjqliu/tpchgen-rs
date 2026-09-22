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

## TPC-DS text chunk sizing

TPC-DS DAT and CSV output is generated in parallel, using in-memory chunks.
`--chunk-bytes` sets the approximate target size of each chunk in bytes. The
default is `8388608` (8 MiB).

Smaller chunks provide finer scheduling granularity and use less memory per
chunk, but add scheduling overhead. Larger chunks reduce that overhead but may
use more memory and expose less parallelism.

```shell
tpcgen-cli tpcds dat -s 10 --chunk-bytes 64MiB --output-dir /tmp/tpcds
tpcgen-cli tpcds csv -s 10 --chunk-bytes 64MiB --output-dir /tmp/tpcds
```

The value is a planning target, not a memory limit or output file size. It does
not split files or change their contents. TPC-DS Parquet generation uses the
separate `--row-group-bytes` option.

`--chunk-bytes` accepts raw byte counts or human-readable sizes, such as `8mb`
or `8MiB`.
