#[cfg(feature = "codegen")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::path::PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?);
    let output = root.join("src/generated");
    std::fs::create_dir_all(&output)?;
    let mut config = prost_build::Config::new();
    config.protoc_executable(protoc_bin_vendored::protoc_bin_path()?);
    config.bytes(["."]);
    config.out_dir(output);
    config.compile_protos(
        &[
            root.join("proto/common.proto"),
            root.join("proto/public.proto"),
            root.join("proto/internal.proto"),
        ],
        &[root.join("proto")],
    )?;
    Ok(())
}

#[cfg(not(feature = "codegen"))]
fn main() {
    println!("cargo:rerun-if-changed=proto");
}
