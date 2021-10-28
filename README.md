# FTZZ

File Tree Fuzzer allows you to create a pseudo-random directory hierarchy filled with some number of
files.

## Installation

```sh
$ cargo +nightly install ftzz
```

> To install cargo, see [these instructions](https://doc.rust-lang.org/cargo/getting-started/installation.html).

## Usage

Generate a reproducibly random tree in the current directory with about 1 million files:

```sh
$ ftzz g . -n 1M
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

## Areas for improvement (a.k.a. limitations)

### Exact # of files

FTZZ currently cannot generate an exact number of files. For my purposes, maximizing performance was
the primary goal (FTZZ is extremely fast!) and I didn't need the outputs to be exact, but this could
be an interesting feature.

I think the best way to implement it while still maximizing performance would be using a channel as
an op queue: we precompute the target directory structure and then create the files/dirs. However,
instead of doing this in two phases, we send a message to a coordinator thread each time we know the
parameters for one directory which in turn spawns the blocking thread that actually creates files.
The main complication is keeping track of dependencies so that `create_dir` doesn't fail, but I'm
guessing it should be fine to use `create_dir_all` without causing a performance hit. The
precomputing algorithm would stop if it reaches the target number of files, or sprinkle in the
remaining files in randomly chosen directories (or maybe the leftovers could all be dumped in the
root dir).
