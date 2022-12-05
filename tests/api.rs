use expect_test::expect_file;
use public_api::PublicApi;

#[test]
#[cfg_attr(miri, ignore)] // gnu_get_libc_version breaks miri
fn api() {
    let json_path = rustdoc_json::Builder::default()
        .all_features(true)
        .build()
        .unwrap();

    let golden = PublicApi::from_rustdoc_json(json_path, public_api::Options::default()).unwrap().to_string();

    expect_file!["../api.golden"].assert_eq(&golden);
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
