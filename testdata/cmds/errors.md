Unspecified gen dir:

```console
$ ftzz generate
? 2
error: The following required arguments were not provided:
  --files <NUM_FILES>
  <ROOT_DIR>

Usage: ftzz[EXE] generate --files <NUM_FILES> <ROOT_DIR>

```

Negative num files:

```console
$ ftzz generate -n -1 dir
? 2
error: Found argument '-1' which wasn't expected, or isn't valid in this context

  If you tried to supply '-1' as a value rather than a flag, use '-- -1'

Usage: ftzz[EXE] generate [OPTIONS] --files <NUM_FILES> <ROOT_DIR>

$ ftzz generate -n "-1" dir
? 2
error: Found argument '-1' which wasn't expected, or isn't valid in this context

  If you tried to supply '-1' as a value rather than a flag, use '-- -1'

Usage: ftzz[EXE] generate [OPTIONS] --files <NUM_FILES> <ROOT_DIR>

```

Negative max depth:

```console
$ ftzz generate -n 1 dir --depth -1
? 2
error: Found argument '-1' which wasn't expected, or isn't valid in this context

  If you tried to supply '-1' as a value rather than a flag, use '-- -1'

Usage: ftzz[EXE] generate [OPTIONS] --files <NUM_FILES> <ROOT_DIR>

$ ftzz generate -n 1 dir --depth "-1"
? 2
error: Found argument '-1' which wasn't expected, or isn't valid in this context

  If you tried to supply '-1' as a value rather than a flag, use '-- -1'

Usage: ftzz[EXE] generate [OPTIONS] --files <NUM_FILES> <ROOT_DIR>

```

Negative ratio:

```console
$ ftzz generate -n 1 dir --ftd-ratio -1
? 2
error: Found argument '-1' which wasn't expected, or isn't valid in this context

  If you tried to supply '-1' as a value rather than a flag, use '-- -1'

Usage: ftzz[EXE] generate [OPTIONS] --files <NUM_FILES> <ROOT_DIR>

$ ftzz generate -n 1 dir --ftd-ratio "-1"
? 2
error: Found argument '-1' which wasn't expected, or isn't valid in this context

  If you tried to supply '-1' as a value rather than a flag, use '-- -1'

Usage: ftzz[EXE] generate [OPTIONS] --files <NUM_FILES> <ROOT_DIR>

```

Negative seed:

```console
$ ftzz generate -n 1 dir --seed -1
? 2
error: Found argument '-1' which wasn't expected, or isn't valid in this context

  If you tried to supply '-1' as a value rather than a flag, use '-- -1'

Usage: ftzz[EXE] generate [OPTIONS] --files <NUM_FILES> <ROOT_DIR>

$ ftzz generate -n 1 dir --seed "-1"
? 2
error: Found argument '-1' which wasn't expected, or isn't valid in this context

  If you tried to supply '-1' as a value rather than a flag, use '-- -1'

Usage: ftzz[EXE] generate [OPTIONS] --files <NUM_FILES> <ROOT_DIR>

```

`files-exact` and `exact` conflict:

```console
$ ftzz generate -n 1 dir --files-exact --exact
? 2
error: The argument '--files-exact' cannot be used with '--exact'

Usage: ftzz[EXE] generate --files <NUM_FILES> --files-exact <ROOT_DIR>

```

`bytes-exact` and `exact` conflict:

```console
$ ftzz generate -n 1 dir --bytes-exact --exact
? 2
error: The argument '--bytes-exact' cannot be used with '--exact'

Usage: ftzz[EXE] generate --files <NUM_FILES> --bytes-exact <--total-bytes <NUM_BYTES>> <ROOT_DIR>

```

`bytes-exact` cannot be used without `num-bytes`:

```console
$ ftzz generate -n 1 dir --bytes-exact
? 2
error: The following required arguments were not provided:
  <--total-bytes <NUM_BYTES>>

Usage: ftzz[EXE] generate --files <NUM_FILES> --bytes-exact <--total-bytes <NUM_BYTES>> <ROOT_DIR>

```

`fill-byte` cannot be used without `num-bytes`:

```console
$ ftzz generate -n 1 dir --fill-byte 42
? 2
error: The following required arguments were not provided:
  <--total-bytes <NUM_BYTES>>

Usage: ftzz[EXE] generate --files <NUM_FILES> --fill-byte <FILL_BYTE> <--total-bytes <NUM_BYTES>> <ROOT_DIR>

```

Number overflow:

```console
$ ftzz generate -n 1 dir -d 999999999999999999999999999
? 2
error: Invalid value '999999999999999999999999999' for '--max-depth <MAX_DEPTH>': number too large to fit in target type

```
