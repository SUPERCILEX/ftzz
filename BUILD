load(
    "@rules_rust//rust:defs.bzl",
    "rust_binary",
    "rust_clippy",
    "rust_library",
    "rust_test",
    "rustfmt_test",
)

rust_library(
    name = "ftzz_lib",
    srcs = [
        "src/errors.rs",
        "src/generator.rs",
        "src/lib.rs",
    ],
    crate_name = "ftzz",
    proc_macro_deps = [
        "//third_party/cargo:derive_new",
    ],
    deps = [
        "//third_party/cargo:anyhow",
        "//third_party/cargo:clap_num",
        "//third_party/cargo:exitcode",
        "//third_party/cargo:futures",
        "//third_party/cargo:log",
        "//third_party/cargo:nix",
        "//third_party/cargo:num_cpus",
        "//third_party/cargo:num_format",
        "//third_party/cargo:rand",
        "//third_party/cargo:rand_distr",
        "//third_party/cargo:rand_xorshift",
        "//third_party/cargo:structopt",
        "//third_party/cargo:tokio",
    ],
)

rust_binary(
    name = "ftzz",
    srcs = [
        "src/main.rs",
    ],
    rustc_flags = select({
        "//tools/config:release_build": [
            "-Copt-level=3",
            "-Clto",
            "-Ccodegen-units=1",
            "-Zstrip=symbols",
        ],
        "//conditions:default": [],
    }),
    deps = [
        ":ftzz_lib",
        "//third_party/cargo:clap_verbosity_flag",
        "//third_party/cargo:simple_logger",
        "//third_party/cargo:structopt",
    ],
)

rust_test(
    name = "ftzz_test",
    srcs = [
        "tests/generator.rs",
    ],
    data = glob(["testdata/**/*.hash"]),
    proc_macro_deps = [
        "//third_party/cargo:rstest",
    ],
    rustc_flags = ["--cfg=bazel"],
    deps = [
        ":ftzz_lib",
        "//third_party/cargo:seahash",
        "//third_party/cargo:tempfile",
        "@rules_rust//tools/runfiles",
    ],
)
