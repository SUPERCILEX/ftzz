use std::{fs::read_to_string, io::Write};

use goldenfile::Mint;
use public_api::public_api_from_rustdoc_json_str;

#[test]
fn api() {
    println!("Run before test:\n$ cargo +nightly rustdoc --all-features -- -Zunstable-options --output-format json");

    let mut mint = Mint::new(".");
    let mut goldenfile = mint.new_goldenfile("api.txt").unwrap();

    let json = read_to_string("target/doc/ftzz.json").unwrap();
    let items = public_api_from_rustdoc_json_str(&json, public_api::Options::default()).unwrap();
    for public_item in items {
        writeln!(goldenfile, "{}", public_item).unwrap();
    }
}
