`files-exact` and `bytes-exact` can be used together:

```console
$ ftzz generate -n 1K -b 100 --files-exact --bytes-exact all-exact-explicit
Exactly 1,000 files will be generated in approximately 1,000 directories distributed across a tree of maximum depth 5 where each directory contains approximately 4 other directories. Each file will contain approximately 0 bytes of random data totaling exactly 100 bytes.
Created 1,000 files (100 B) across 1,018 directories.

```

Flat dir:

```console
$ ftzz generate -n 1K --depth 0 flat
About 1,000 files will be generated in approximately 1 directory distributed across a tree of maximum depth 0 where each directory contains approximately 0 other directories.
Created 1,034 files across 0 directories.

```

Verbose output:

```console
$ ftzz generate -vvv -n 1K verbose
About 1,000 files will be generated in approximately 1,000 directories distributed across a tree of maximum depth 5 where each directory contains approximately 4 other directories.
INFO  [ftzz::generator] Starting config config=Configuration { root_dir: "verbose", files: 1000, bytes: 0, files_exact: false, bytes_exact: false, files_per_dir: 1.0, dirs_per_dir: 3.9810717055349727, bytes_per_file: 0.0, max_depth: 5, seed: 0, informational_dirs_per_dir: 4, informational_total_dirs: 1000, informational_bytes_per_files: 0 }
DEBUG [ftzz::generator] Starting seed seed=5005
DEBUG [ftzz::core::scheduler] Entry allocations task_queue=255 object_pool.dirs=5 object_pool.paths=127 object_pool.file_sizes=0
Created 1,379 files across 1,381 directories.

```