Plain nums:

```console
$ ftzz generate --files 1000 --max-depth 10 --ftd-ratio 100 plain
About 1,000 files will be generated in approximately 10 directories distributed across a tree of maximum depth 10 where each directory contains approximately 1 other directory.
Created 5,821 files across 60 directories.

```

SI numbers:

```console
$ ftzz generate --files 1K --max-depth 0.01K --ftd-ratio 0.0001M si
About 1,000 files will be generated in approximately 10 directories distributed across a tree of maximum depth 10 where each directory contains approximately 1 other directory.
Created 5,821 files across 60 directories.

```

Commas:

```console
$ ftzz generate --files 1,000 --max-depth 0,010 --ftd-ratio 1,0,0 commas
About 1,000 files will be generated in approximately 10 directories distributed across a tree of maximum depth 10 where each directory contains approximately 1 other directory.
Created 5,821 files across 60 directories.

```

Underscores:

```console
$ ftzz generate --files 1_000 --max-depth 1_0 --ftd-ratio 1_0_0 underscores
About 1,000 files will be generated in approximately 10 directories distributed across a tree of maximum depth 10 where each directory contains approximately 1 other directory.
Created 5,821 files across 60 directories.

```
