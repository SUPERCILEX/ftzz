Unspecified gen dir:

```console
$ ftzz
? 2
Generate a random directory hierarchy with some number of files

Usage: ftzz[EXE] [OPTIONS] --files <NUM_FILES> <ROOT_DIR>

Arguments:
  <ROOT_DIR>  The directory in which to generate files

Options:
  -n, --files <NUM_FILES>              The number of files to generate
      --files-exact                    Whether or not to generate exactly N files
  -b, --total-bytes <NUM_BYTES>        The total amount of random data to be distributed across the
                                       generated files [default: 0]
      --fill-byte <FILL_BYTE>          Specify a specific fill byte to be used instead of
                                       deterministically random data
      --bytes-exact                    Whether or not to generate exactly N bytes
  -e, --exact                          Whether or not to generate exactly N files and bytes
  -d, --max-depth <MAX_DEPTH>          The maximum directory tree depth [default: 5]
  -r, --ftd-ratio <FILE_TO_DIR_RATIO>  The number of files to generate per directory (default: files
                                       / 1000)
      --seed <SEED>                    Change the PRNG's starting seed [default: 0]
  -h, --help                           Print help (use `--help` for more detail)
  -q, --quiet...                       Decrease logging verbosity
  -v, --verbose...                     Increase logging verbosity
  -V, --version                        Print version

```

Negative num files:

```console
$ ftzz -n -1 dir
? 2
error: unexpected argument '-1' found

  tip: to pass '-1' as a value, use '-- -1'

Usage: ftzz[EXE] [OPTIONS] --files <NUM_FILES> <ROOT_DIR>

$ ftzz -n "-1" dir
? 2
error: unexpected argument '-1' found

  tip: to pass '-1' as a value, use '-- -1'

Usage: ftzz[EXE] [OPTIONS] --files <NUM_FILES> <ROOT_DIR>

```

Negative max depth:

```console
$ ftzz -n 1 dir --depth -1
? 2
error: unexpected argument '-1' found

  tip: to pass '-1' as a value, use '-- -1'

Usage: ftzz[EXE] [OPTIONS] --files <NUM_FILES> <ROOT_DIR>

$ ftzz -n 1 dir --depth "-1"
? 2
error: unexpected argument '-1' found

  tip: to pass '-1' as a value, use '-- -1'

Usage: ftzz[EXE] [OPTIONS] --files <NUM_FILES> <ROOT_DIR>

```

Negative ratio:

```console
$ ftzz -n 1 dir --ftd-ratio -1
? 2
error: unexpected argument '-1' found

  tip: to pass '-1' as a value, use '-- -1'

Usage: ftzz[EXE] [OPTIONS] --files <NUM_FILES> <ROOT_DIR>

$ ftzz -n 1 dir --ftd-ratio "-1"
? 2
error: unexpected argument '-1' found

  tip: to pass '-1' as a value, use '-- -1'

Usage: ftzz[EXE] [OPTIONS] --files <NUM_FILES> <ROOT_DIR>

```

Negative seed:

```console
$ ftzz -n 1 dir --seed -1
? 2
error: unexpected argument '-1' found

  tip: to pass '-1' as a value, use '-- -1'

Usage: ftzz[EXE] [OPTIONS] --files <NUM_FILES> <ROOT_DIR>

$ ftzz -n 1 dir --seed "-1"
? 2
error: unexpected argument '-1' found

  tip: to pass '-1' as a value, use '-- -1'

Usage: ftzz[EXE] [OPTIONS] --files <NUM_FILES> <ROOT_DIR>

```

`files-exact` and `exact` conflict:

```console
$ ftzz -n 1 dir --files-exact --exact
? 2
error: the argument '--files-exact' cannot be used with '--exact'

Usage: ftzz[EXE] --files <NUM_FILES> --files-exact <ROOT_DIR>

```

`bytes-exact` and `exact` conflict:

```console
$ ftzz -n 1 dir --bytes-exact --exact
? 2
error: the argument '--bytes-exact' cannot be used with '--exact'

Usage: ftzz[EXE] --files <NUM_FILES> --bytes-exact <--total-bytes <NUM_BYTES>> <ROOT_DIR>

```

`bytes-exact` cannot be used without `num-bytes`:

```console
$ ftzz -n 1 dir --bytes-exact
? 2
error: the following required arguments were not provided:
  <--total-bytes <NUM_BYTES>>

Usage: ftzz[EXE] --files <NUM_FILES> --bytes-exact <--total-bytes <NUM_BYTES>> <ROOT_DIR>

```

`fill-byte` cannot be used without `num-bytes`:

```console
$ ftzz -n 1 dir --fill-byte 42
? 2
error: the following required arguments were not provided:
  <--total-bytes <NUM_BYTES>>

Usage: ftzz[EXE] --files <NUM_FILES> --fill-byte <FILL_BYTE> <--total-bytes <NUM_BYTES>> <ROOT_DIR>

```

Number overflow:

```console
$ ftzz -n 1 dir -d 999999999999999999999999999
? 2
error: invalid value '999999999999999999999999999' for '--max-depth <MAX_DEPTH>': number too large to fit in target type

```
