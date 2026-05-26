fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=../../proto/orchestrator.proto");
    println!("cargo:rerun-if-changed=../../proto/benchmark.proto");

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile(
            &[
                "../../proto/orchestrator.proto",
                "../../proto/benchmark.proto",
            ],
            &["../../proto"],
        )?;

    Ok(())
}