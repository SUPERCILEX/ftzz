use std::{fs::read_to_string, io::Write};

use goldenfile::Mint;
use public_api::PublicApi;

#[test]
#[cfg_attr(miri, ignore)] // gnu_get_libc_version breaks miri
fn api() {
    let json_path = rustdoc_json::Builder::default()
        .all_features(true)
        .build()
        .unwrap();

    let mut mint = Mint::new(".");
    let mut goldenfile = mint.new_goldenfile("api.golden").unwrap();

    let json = read_to_string(json_path).unwrap();
    let api = PublicApi::from_rustdoc_json_str(&json, public_api::Options::default()).unwrap();
    for public_item in api.items {
        writeln!(goldenfile, "{public_item}").unwrap();
    }
}

#[test]
#[cfg_attr(miri, ignore)]
fn readme() {
    trycmd::TestCases::new().case("README.md");
}

#[test]
#[cfg_attr(miri, ignore)]
fn cli() {
    trycmd::TestCases::new().case("testdata/cmds/*.md");
}
