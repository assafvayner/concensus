fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(false)
        .bytes(["."])
        .compile_protos(
            &["../daccord-demo/proto/consensus.proto"],
            &["../daccord-demo/proto"],
        )?;
    Ok(())
}
