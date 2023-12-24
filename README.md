# FTZZ

File Tree Fuzzer creates a pseudo-random directory hierarchy filled with some number of files.

## Installation

### Use prebuilt binaries

Binaries for a number of platforms are available on the
[release page](https://github.com/SUPERCILEX/ftzz/releases/latest).

### Build from source

```console,ignore
$ cargo +nightly install ftzz
```

> To install cargo, follow [these instructions](https://doc.rust-lang.org/cargo/getting-started/installation.html).

## Usage

Generate a reproducibly random tree in the current directory with *approximately* 1 million files:

```console
$ ftzz ./simple -n 1M
About 1,000,000 files will be generated in approximately 1,000 directories distributed across a tree of maximum depth 5 where each directory contains approximately 4 other directories.
Created 1,003,229 files across 1,259 directories.

```

Generate *exactly* 1 million files:

```console
$ ftzz ./exact -en 1M
Exactly 1,000,000 files will be generated in approximately 1,000 directories distributed across a tree of maximum depth 5 where each directory contains approximately 4 other directories.
Created 1,000,000 files across 1,259 directories.

```

Generate ~10_000 files with ~1 MB of random data spread across them:

```console
$ ftzz ./with_data -n 10K -b 1M
About 10,000 files will be generated in approximately 1,000 directories distributed across a tree of maximum depth 5 where each directory contains approximately 4 other directories. Each file will contain approximately 100 bytes of random data.
Created 9,312 files (924.6 KB) across 1,570 directories.

```

Because FTZZ creates reproducible outputs, the generated directory will always have the same
structure given the same inputs. To generate variations on a structure with the same parameters,
change the starting seed:

```console
$ ftzz ./unseeded -n 100
About 100 files will be generated in approximately 100 directories distributed across a tree of maximum depth 5 where each directory contains approximately 3 other directories.
Created 45 files across 198 directories.

$ ftzz ./seeded -n 100 --seed 42 # Or $RANDOM
About 100 files will be generated in approximately 100 directories distributed across a tree of maximum depth 5 where each directory contains approximately 3 other directories.
Created 83 files across 110 directories.

```

Other parameters can be found in the built-in docs:

```console
$ ftzz --help
Generate a random directory hierarchy with some number of files

A pseudo-random directory hierarchy will be generated (seeded by this command's input parameters)
containing approximately the target number of files. The exact configuration of files and
directories in the hierarchy is probabilistically determined to mostly match the specified
parameters.

Generated files and directories are named using monotonically increasing numbers, where files are
named `n` and directories are named `n.dir` for a given natural number `n`.

By default, generated files are empty, but random data can be used as the file contents with the
`total-bytes` option.

Usage: ftzz[EXE] [OPTIONS] --files <NUM_FILES> <ROOT_DIR>

Arguments:
  <ROOT_DIR>
          The directory in which to generate files
          
          The directory will be created if it does not exist.

Options:
  -n, --files <NUM_FILES>
          The number of files to generate
          
          Note: this value is probabilistically respected, meaning any number of files may be
          generated so long as we attempt to get close to N.

      --files-exact
          Whether or not to generate exactly N files

  -b, --total-bytes <NUM_BYTES>
          The total amount of random data to be distributed across the generated files
          
          Note: this value is probabilistically respected, meaning any amount of data may be
          generated so long as we attempt to get close to N.
          
          [default: 0]

      --fill-byte <FILL_BYTE>
          Specify a specific fill byte to be used instead of deterministically random data
          
          This can be used to improve compression ratios of the generated files.

      --bytes-exact
          Whether or not to generate exactly N bytes

  -e, --exact
          Whether or not to generate exactly N files and bytes

  -d, --max-depth <MAX_DEPTH>
          The maximum directory tree depth
          
          [default: 5]

  -r, --ftd-ratio <FILE_TO_DIR_RATIO>
          The number of files to generate per directory (default: files / 1000)
          
          Note: this value is probabilistically respected, meaning not all directories will have N
          files).

      --seed <SEED>
          Change the PRNG's starting seed
          
          For example, you can use bash's `$RANDOM` function.
          
          [default: 0]

  -h, --help
          Print help (use `-h` for a summary)

  -q, --quiet...
          Decrease logging verbosity

  -v, --verbose...
          Increase logging verbosity

  -V, --version
          Print version

```
