# One computation, multiple environments

This demo shares one `no_std`, allocation-free sensor computation between native
replay, a Wasm CLI simulator, and a firmware-facing ADC adapter. It uses only
`skid-pipe` and Rust `core` in its library. The optional `host` binary uses `std`
for arguments and CSV output; the host is not a no-allocation firmware example.

```text
little-endian ADC sample
    → decode → zero-offset calibration → smoothing
    → level and delta features → hysteresis classification
```

[`src/lib.rs`](src/lib.rs) is the only definition of the computation.
`preprocessing`, `feature_extractor`, and `classifier` return opaque `impl Chain`
values. `sensor_pipeline` assembles them using `from_chain` and `then_chain`.
Both native and Wasm run [`src/main.rs`](src/main.rs) against that library.

## Run and verify

Use the repository's Rust 1.98.1 toolchain, Python 3, and Node.js 24 or newer.
Install the validation targets once:

```sh
rustup target add wasm32-wasip1 wasm32v1-none thumbv6m-none-eabi thumbv7em-none-eabihf riscv32imac-unknown-none-elf
python3 scripts/check-portable-sensor.py
```

The script runs library tests, builds and executes native and Wasm binaries,
compares their CSV observations for three streams (77 samples), and checks the
shared library on core-only Wasm, Cortex-M0, Cortex-M4F, and RISC-V targets.
The default stream must also match explicit expected observations, so parity
alone cannot hide a shared regression in that case. Build failures, execution
failures, and output differences cause a nonzero exit.

Run the default replay or supply your own ordered ADC samples:

```sh
cargo run --manifest-path examples/portable-sensor/Cargo.toml --features host --bin sensor-replay
cargo run --manifest-path examples/portable-sensor/Cargo.toml --features host --bin sensor-replay -- 100 3100 4100 100
```

After the validation script has built Wasm, run the same simulated inputs there:

```sh
node scripts/run-sensor-wasm.mjs target/portable-sensor/wasm32-wasip1/debug/sensor-replay.wasm 100 3100 4100 100
```

The runner uses [Node's WASI preview1 API](https://nodejs.org/api/wasi.html).
This is executable Wasm replay in a CLI; it is not a browser visualization.

## State and reproducibility

The default constructed input is `[100, 1100, 3100, 4100, 4100, 1100, 100]`.
It is synthetic data, not a hardware recording. Calibration subtracts 100 and
clamps into 0..4095. The first sample initializes the filter; later samples
use `(previous + calibrated) / 2`, rounded down. Features report the filtered
level and its delta. Classification enters High at 3000 and leaves at 2000.

```csv
raw,level,delta,high
100,0,0,false
1100,500,500,false
3100,1750,1250,false
4100,2875,1125,false
4100,3437,562,true
1100,2218,-1219,true
100,1109,-1109,false
```

Retain one pipeline per sensor stream. Rebuilding resets both filter and
classifier state. Integer arithmetic makes these observations exactly
comparable across targets. Samples must arrive in acquisition order at a fixed
interval; missing inputs are not interpolated. For irregular sampling, define
timestamps and elapsed-time behavior in the shared computation first.

## Firmware boundary

Firmware can retain `sensor_pipeline(offset)` and call `process_adc(&mut pipeline,
raw)` for each reading. The adapter encodes the ADC count as little-endian input
for the very same shared chain. HAL setup, interrupts, sampling cadence, and
result delivery belong to board code.

CI only compile-checks this boundary on ARM and RISC-V. There is no STM32 board
runner in this demo and no claim of physical sensor execution, flash size,
stack usage, or real-time deadlines. Validate those on a chosen device before
treating the demo as firmware evidence.
