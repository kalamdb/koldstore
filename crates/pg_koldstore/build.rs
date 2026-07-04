fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=native/custom_scan.c");
    println!("cargo:rerun-if-changed=native/custom_scan.h");

    cc::Build::new()
        .file("native/custom_scan.c")
        .include("native")
        .warnings(true)
        .compile("pg_koldstore_custom_scan");

    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("macos") {
        println!("cargo:rustc-cdylib-link-arg=-Wl,-undefined,dynamic_lookup");
    }
}
