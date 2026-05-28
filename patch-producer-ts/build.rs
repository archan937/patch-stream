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

    let bridge_ts = manifest_dir.join("src/bridge.ts");
    let app_entry = bun_root.join("src/runtime/index.ts");
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    let out_file = out_dir.join("page_runtime.js");

    // Generate a synthetic entry that loads bridge.ts first, then index.ts.
    // index.ts still imports ./polyfill, but all the guards (`if (!globalThis.xxx)`)
    // make that a no-op once bridge.ts has already set everything up.
    let synth = out_dir.join("entry.ts");
    std::fs::write(
        &synth,
        format!(
            "import {:?};\nimport {:?};\n",
            bridge_ts.to_str().unwrap(),
            app_entry.to_str().unwrap(),
        ),
    )
    .expect("failed to write synthetic entry.ts");

    println!("cargo:rerun-if-changed=src/bridge.ts");
    println!("cargo:rerun-if-changed={}", bun_root.join("src/runtime/index.ts").display());
    println!("cargo:rerun-if-changed={}", bun_root.join("src/page/generate.ts").display());
    println!("cargo:rerun-if-changed={}", bun_root.join("src/page/catalog.ts").display());

    let status = Command::new("bun")
        .args([
            "build",
            synth.to_str().unwrap(),
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
