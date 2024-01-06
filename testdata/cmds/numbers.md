Plain nums:

```console
$ ftzz --files 1000 --max-depth 10 --ftd-ratio 100 plain
About 1,000 files will be generated in approximately 10 directories distributed across a tree of maximum depth 10 where each directory contains approximately 1 other directory.
Created 996 files across 42 directories.

```

SI numbers:

```console
$ ftzz --files 1K --max-depth 0.01K --ftd-ratio 0.0001M si
About 1,000 files will be generated in approximately 10 directories distributed across a tree of maximum depth 10 where each directory contains approximately 1 other directory.
Created 996 files across 42 directories.

```

Underscores:

```console
$ ftzz --files 1_000 --max-depth 1_0 --ftd-ratio 1_0_0 underscores
About 1,000 files will be generated in approximately 10 directories distributed across a tree of maximum depth 10 where each directory contains approximately 1 other directory.
Created 996 files across 42 directories.

```
