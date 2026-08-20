fn main() {
    // The `link-urma` feature is the opt-in for targets that reference the
    // extern symbols (examples, the ub P2P backend): only then do we link
    // liburma, so building the bindings alone never requires the library.
    if std::env::var_os("CARGO_FEATURE_LINK_URMA").is_some() {
        println!("cargo:rustc-link-lib=dylib=urma");
    }
}
