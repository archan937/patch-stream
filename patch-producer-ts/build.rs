use std::{env, path::PathBuf, process::Command};

fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    // stream-rs lives next to patch-stream: ../../stream-rs/bun/
    let bun_root = manifest_dir
        .parent()  // patch-stream/
        .unwrap()
        .parent()  // Sources/
        .unwrap()
        .join("stream-rs/bun");

    let entry = bun_root.join("src/runtime/index.ts");
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    let out_file = out_dir.join("page_runtime.js");

    println!("cargo:rerun-if-changed={}", bun_root.join("src/runtime/index.ts").display());
    println!("cargo:rerun-if-changed={}", bun_root.join("src/runtime/polyfill.ts").display());
    println!("cargo:rerun-if-changed={}", bun_root.join("src/page/generate.ts").display());
    println!("cargo:rerun-if-changed={}", bun_root.join("src/page/catalog.ts").display());

    let status = Command::new("bun")
        .args([
            "build",
            entry.to_str().unwrap(),
            "--outfile",
            out_file.to_str().unwrap(),
            "--target",
            "browser",
            "--format",
            "iife",
            "--minify",
        ])
        .current_dir(&bun_root)
        .status()
        .expect("failed to run `bun build` — is bun installed?");

    assert!(status.success(), "bun build failed");
}
