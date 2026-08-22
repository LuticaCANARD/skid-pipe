#[path = "../benches/support/footprint.rs"]
mod footprint;

fn main() {
    println!(
        "Async direct={} B pipeline={} B",
        footprint::skid_pipe_measure_direct_async_future_bytes(0),
        footprint::skid_pipe_measure_pipeline_async_future_bytes(0),
    );
    println!(
        "TryAsync direct={} B pipeline={} B",
        footprint::skid_pipe_measure_direct_try_async_future_bytes(0),
        footprint::skid_pipe_measure_pipeline_try_async_future_bytes(0),
    );
}
