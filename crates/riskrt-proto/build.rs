//! Compiles `proto/riskrt/v1/risk.proto` with protox, a pure-Rust protobuf
//! compiler, so no system `protoc` is needed to build the workspace.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto = "proto/riskrt/v1/risk.proto";
    println!("cargo:rerun-if-changed={proto}");
    let fds = protox::compile([proto], ["proto"])?;
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_fds(fds)?;
    Ok(())
}
