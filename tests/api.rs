#[test]
fn fmt() {
    supercilex_tests::fmt();
}

#[test]
fn clippy() {
    supercilex_tests::clippy();
}

#[test]
fn api() {
    supercilex_tests::api();
}

#[test]
#[cfg_attr(miri, ignore)] // https://github.com/rayon-rs/rayon/issues/952
fn readme() {
    trycmd::TestCases::new().case("README.md");
}

#[test]
#[cfg_attr(miri, ignore)] // https://github.com/rayon-rs/rayon/issues/952
fn cli() {
    trycmd::TestCases::new().case("testdata/cmds/*.md");
}
