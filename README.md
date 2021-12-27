# FTZZ

File Tree Fuzzer allows you to create a pseudo-random directory hierarchy filled with some number of
files.

## Installation

### Use prebuilt binaries

Binaries for a number of platforms are available on the
[release page](https://github.com/SUPERCILEX/ftzz/releases).

### Build from source

```sh
$ cargo +nightly install ftzz
```

> To install cargo, see [these instructions](https://doc.rust-lang.org/cargo/getting-started/installation.html).

## Usage

Generate a reproducibly random tree in the current directory with *approximately* 1 million files:

```sh
$ ftzz g . -n 1M
```

Generate *exactly* 1 million files:

```sh
$ ftzz g . -en 1M
```

Because FTZZ generates reproducible outputs, the generated directory will always have the same
structure given the same inputs. To generate variations on a structure with the same parameters, add
some entropy:

```sh
$ ftzz g . -n 1M --entropy $RANDOM
```

Other parameters can be found in the built-in docs:

```sh
$ ftzz help
```
