fn main() {
    cc::Build::new()
        .file("native/custom_scan.c")
        .include("native")
        .warnings(true)
        .compile("pg_koldstore_custom_scan");

    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("macos") {
        println!("cargo:rustc-cdylib-link-arg=-Wl,-undefined,dynamic_lookup");
    }
}
