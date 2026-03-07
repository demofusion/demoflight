use std::io::Result;

fn main() -> Result<()> {
    let proto_files = &["proto/demoflight/v1/demoflight.proto"];
    let include_dirs = &["proto"];

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .out_dir("src/proto")
        .compile_protos(proto_files, include_dirs)?;

    println!("cargo::rerun-if-changed=proto/");

    Ok(())
}
