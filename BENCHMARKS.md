# Benchmarks

Latest measurement: 2026-09-13, Rust 1.98.1.

Measured source: working tree based on `753b6f68fbd583d2bfd47683fe1077a499e532ba`; includes the uncommitted `from_chain`/`then_chain` API and module-composition benchmark.

## Current run: 2026-09-13

This run measures the rebased source after adding synchronous chain-as-stage
composition. It is a new WSL2/Linux measurement, so its values should be
compared with one another rather than treated as a time-series continuation of
the Apple M1 snapshot below.

### Environment and method

- Intel Core i9-9900K, 4 logical CPUs exposed to WSL2; Ubuntu 26.04 LTS on AC power.
- Rust 1.98.1, LLVM 22.1.8, `x86_64-unknown-linux-gnu`; Cargo 1.98.1.
- Criterion 0.8.2; futures 0.3.34; tower 0.5.3. Default features; `wide` and `tokio` disabled.
- Each suite used a 1 second warm-up, 3 second measurement, and 100 samples. Suites ran sequentially as `vs_futures`, `composition`, `vs_tower`, `vs_futures`.
- Pipeline construction stayed outside timed loops. Async comparisons use the same `core::future::Ready` stages on both sides. `module_composed` uses the same synchronous stage functions split into two opaque `impl Chain` modules and joined with `from_chain`/`then_chain`.
- Estimates below are Criterion point estimates in nanoseconds. The two `vs_futures` runs are shown as point-estimate ranges; they are not confidence intervals. The host was not isolated or affinity-pinned.

### Direct composition: current run

| Case | Direct | Pipe | Module-composed | Module / direct − 1 |
|---|---:|---:|---:|---:|
| 3-stage sync | 3.3697 ns | 3.3813 ns | 3.6877 ns | +9.4% |
| 1-stage fallible success | 7.5031 ns | 7.4772 ns | — | — |
| 3-stage fallible success | 14.704 ns | 15.857 ns | — | — |
| 8-stage fallible success | 32.402 ns | 32.745 ns | — | — |
| Type-changing fallible success | 6.5240 ns | 6.5091 ns | — | — |
| Fallible error, first | 8.0028 ns | 7.9238 ns | — | — |
| Fallible error, middle | 18.511 ns | 18.167 ns | — | — |
| Fallible error, last | 31.841 ns | 33.751 ns | — | — |
| 100-stage sync success | 269.70 ns | 271.84 ns | — | — |
| 100-stage fallible success | 334.84 ns | 338.81 ns | — | — |
| 100-stage async Ready success | 362.85 ns | 423.20 ns | — | — |
| 100-stage TryAsync Ready success | 341.47 ns | 404.86 ns | — | — |
| 100-stage TryAsync error, first | 7.8659 ns | 21.343 ns | — | — |
| 100-stage TryAsync error, middle | 172.58 ns | 213.29 ns | — | — |
| 100-stage TryAsync error, last | 339.74 ns | 399.81 ns | — | — |

The new module-composed case is about 9.4% slower than the direct call and
9.1% slower than the flat `Pipe` in this build. It measures the explicit
module boundary and adapter as well as the stages; it is evidence about the
cost of this composition shape, not a universal overhead guarantee.

### `futures` comparison: two current runs

| Case | Direct async fn | skid-pipe | futures | futures / skid-pipe |
|---|---:|---:|---:|---:|
| Async, 3 stages | 9.516–10.153 ns | 9.474–9.858 ns | 30.452–30.490 ns | 3.09–3.22x |
| Try async, 3 stages | 14.068–16.277 ns | 14.101–15.801 ns | 33.444–34.076 ns | 2.12–2.42x |
| Try async, 3 stages, first error | 8.348–8.566 ns | 8.326–8.856 ns | 18.411–21.453 ns | 2.08–2.58x |
| Async, 10 stages | 31.751–40.519 ns | 32.061–32.775 ns | 102.23–139.48 ns | 3.12–4.35x |
| Try async, 10 stages, first error | 9.269–18.866 ns | 11.879–11.950 ns | 53.741–79.262 ns | 4.50–6.67x |

### Tower comparison: current run

| Case | Direct async fn | skid-pipe | Tower ready + call |
|---|---:|---:|---:|
| Try async, 3 stages, success | 12.654 ns | 13.896 ns | 35.703 ns |

Tower/skid-pipe is 2.57x in this fixture. Tower still provides a readiness and
service contract; this benchmark does not make it interchangeable with a local
computation pipeline.

### Future layout: current run

The existing footprint example measured the same inline storage as the previous
snapshot: Async direct 8 B / pipeline 120 B, and TryAsync direct 8 B / pipeline
216 B. This measures future storage, not heap allocation or total peak stack use.

### Reproduce current run

Use the working tree based on `753b6f68fbd583d2bfd47683fe1077a499e532ba`, Rust
1.98.1, and the following commands. A generated `Cargo.lock` is ignored by the
repository and pins the dependency resolution for this local run.

```sh
cargo +1.98.1 generate-lockfile
cargo +1.98.1 update -p futures --precise 0.3.34
cargo +1.98.1 update -p tokio --precise 1.53.1
cargo +1.98.1 bench --locked --no-run --benches
for bench in vs_futures composition vs_tower vs_futures; do
  cargo +1.98.1 bench --locked --bench "$bench" -- \
    --warm-up-time 1 --measurement-time 3 --sample-size 100 --noplot
done
cargo +1.98.1 run --locked --release --example measure_footprint
```

## Previous run: 2026-09-12 (Apple M1)

The following section is retained for historical comparison. It is not the
latest measurement.

### Result

Across two runs, the ordinary async pipeline took 67–70% less time than the
futures combinator chain at three stages, and 59–61% less at ten stages.
The direct async fn and skid-pipe results were close: the ordering at three
stages reversed between runs, and the ten-stage estimates were within 3%.
There is no consistent direct-call speed advantage to claim for skid-pipe.
The first-error ten-stage comparison favors skid-pipe over futures by 5.19–5.99x.

Long chains still have unfavorable cases. In the composition benchmark,
100-stage TryAsyncPipe rejecting at the first stage took 17.087 ns versus
4.522 ns directly (3.78x as long), and rejecting in the middle took 663.789 ns
versus 363.949 ns (+82.4%). These are separate workloads from vs_futures.

### Environment and method

- Apple M1, 8 CPU cores, 16 GiB RAM; macOS 26.4.1 (25E253), AC power.
- Rust 1.98.1, LLVM 22.1.8, aarch64-apple-darwin; Cargo 1.98.1.
- Criterion 0.8.2; futures 0.3.34; tower 0.5.3. Tokio 1.53.1 was resolved but its feature was disabled.
- Default features, optimized Cargo bench profile; neither wide nor tokio enabled.
- Precompiled the three benchmarks before timing, then ran vs_futures, composition, and vs_tower sequentially. Repeated vs_futures afterward with identical settings. The footprint example ran after the timed measurements.
- Warm-up 1 second, measurement 3 seconds per case, 100 samples. Main run: 58 cases; repeat: 15 cases. Each command exited successfully.
- Values use Criterion's slope point estimate where available, otherwise its mean. All 73 estimates include 95% confidence intervals in the appendix below.
- These are existing Ready-future microbenchmarks with black_box barriers and non-inlined stage functions. Pipeline construction is outside the timed loop; futures combinators are rebuilt inside it. They do not measure I/O, executor scheduling, end-to-end application throughput, or run_send/spawn latency.
- The host was not isolated and CPU affinity was not pinned. Some cases have outliers and wide intervals. Cross-run ranges below are ranges of point estimates, not confidence intervals; small differences are not a stable performance promise.
- The older Intel/WSL2 + Rust 1.86 snapshot uses different hardware, OS, compiler, and sources. Do not attribute differences from it solely to the Rust upgrade or the recent fixes.

### futures comparison: previous run

| Case | Direct async fn | skid-pipe | futures | futures / skid-pipe |
|---|---:|---:|---:|---:|
| Async, 3 stages | 7.81–8.33 ns | 8.21–8.90 ns | 26.87–27.00 ns | 3.02–3.29x |
| Try async, 3 stages | 14.50–15.05 ns | 13.66–14.15 ns | 27.85–29.09 ns | 2.04–2.06x |
| Try async, 3 stages, first error | 4.30–4.60 ns | 4.18–4.53 ns | 8.52–9.38 ns | 2.04–2.07x |
| Async, 10 stages | 38.43–38.50 ns | 38.39–39.59 ns | 95.96–98.40 ns | 2.42–2.56x |
| Try async, 10 stages, first error | 4.51–4.70 ns | 4.55–4.56 ns | 23.65–27.28 ns | 5.19–5.99x |

### Direct composition: first run

| Case | Direct | Pipeline | Pipeline / direct − 1 |
|---|---:|---:|---:|
| `async_three_stage_ready` | 4.985 ns | 4.724 ns | -5.2% |
| `fallible_error/first` | 3.459 ns | 2.375 ns | -31.4% |
| `fallible_error/last` | 9.260 ns | 10.902 ns | +17.7% |
| `fallible_error/middle` | 11.092 ns | 10.727 ns | -3.3% |
| `fallible_success/1_stage` | 1.544 ns | 1.610 ns | +4.3% |
| `fallible_success/3_stage` | 3.543 ns | 3.991 ns | +12.6% |
| `fallible_success/8_stage` | 9.452 ns | 11.091 ns | +17.3% |
| `fallible_type_changing_success` | 4.404 ns | 4.482 ns | +1.8% |
| `hundred_stage/async_ready_success` | 782.396 ns | 765.782 ns | -2.1% |
| `hundred_stage/fallible_error/first` | 6.194 ns | 9.298 ns | +50.1% |
| `hundred_stage/fallible_error/last` | 780.661 ns | 753.457 ns | -3.5% |
| `hundred_stage/fallible_error/middle` | 548.861 ns | 468.834 ns | -14.6% |
| `hundred_stage/fallible_success` | 741.556 ns | 765.688 ns | +3.3% |
| `hundred_stage/sync_success` | 602.950 ns | 566.867 ns | -6.0% |
| `hundred_stage/try_async_error/first` | 4.522 ns | 17.087 ns | +277.9% |
| `hundred_stage/try_async_error/last` | 969.315 ns | 859.078 ns | -11.4% |
| `hundred_stage/try_async_error/middle` | 363.949 ns | 663.789 ns | +82.4% |
| `hundred_stage/try_async_ready_success` | 739.018 ns | 836.134 ns | +13.1% |
| `sync_three_stage` | 2.248 ns | 2.303 ns | +2.4% |
| `try_async_three_stage_ready_success` | 7.015 ns | 7.296 ns | +4.0% |

### Tower: first run

| Case | Direct async fn | skid-pipe | Tower ready + call |
|---|---:|---:|---:|
| Try async, 3 stages, success | 7.563 ns | 7.688 ns | 18.637 ns |

Tower/skid-pipe = 2.42x in this fixture. Tower includes a readiness
and service contract; this comparison does not make skid-pipe a substitute for
backpressure or middleware. Its stage payload differs from the vs_futures
benchmark, so compare implementations within each table.

### 100-stage future layout

Measured with the existing measure_footprint example, on the same host and
default features. This measures inline future storage, not heap allocation or
total peak stack use.

| Kind | Direct | Pipeline |
|---|---:|---:|
| Async | 8 B | 120 B |
| TryAsync | 8 B | 216 B |

### Reproduction

Use the measured source commit and Rust 1.98.1. This library does not commit a
Cargo.lock, so a fresh checkout resolves transitive dependencies again;
matching the direct dependency versions does not guarantee identical results.
Preserve any existing local lockfile when reproducing a prior environment.

Resolve the dependencies, then compile all benchmark executables before timing:

```sh
cargo +1.98.1 generate-lockfile
cargo +1.98.1 update -p futures --precise 0.3.34
cargo +1.98.1 update -p tokio --precise 1.53.1
cargo +1.98.1 bench --locked --no-run \
  --bench vs_futures --bench composition --bench vs_tower
```

Run the suites sequentially, repeating the futures comparison after the other
suites. Use the same settings for every run:

```sh
for bench in vs_futures composition vs_tower vs_futures; do
  cargo +1.98.1 bench --locked --bench "$bench" -- \
    --warm-up-time 1 --measurement-time 3 --sample-size 100 --noplot
done
cargo +1.98.1 run --locked --release --example measure_footprint
```

Criterion writes its local reports beneath the ignored `target/criterion`
directory. The measured values and confidence intervals are retained in this
document.

### All estimates and 95% confidence intervals

| Run | Benchmark | Estimate (ns) | 95% lower | 95% upper |
|---|---|---:|---:|---:|
| first | `async_three_stage_ready/async_pipe` | 4.7236 | 4.5887 | 4.8870 |
| first | `async_three_stage_ready/direct` | 4.9847 | 4.6960 | 5.3941 |
| first | `fallible_error/first/direct` | 3.4591 | 2.8519 | 4.1834 |
| first | `fallible_error/first/try_pipe` | 2.3746 | 2.2470 | 2.5326 |
| first | `fallible_error/last/direct` | 9.2601 | 8.7724 | 9.9257 |
| first | `fallible_error/last/try_pipe` | 10.9016 | 10.6270 | 11.2476 |
| first | `fallible_error/middle/direct` | 11.0922 | 9.8576 | 12.4561 |
| first | `fallible_error/middle/try_pipe` | 10.7267 | 9.1892 | 12.6822 |
| first | `fallible_success/1_stage/direct` | 1.5437 | 1.4932 | 1.5956 |
| first | `fallible_success/1_stage/try_pipe` | 1.6102 | 1.5628 | 1.6566 |
| first | `fallible_success/3_stage/direct` | 3.5433 | 3.3443 | 3.8742 |
| first | `fallible_success/3_stage/try_pipe` | 3.9906 | 3.9374 | 4.0565 |
| first | `fallible_success/8_stage/direct` | 9.4519 | 8.8999 | 10.2812 |
| first | `fallible_success/8_stage/try_pipe` | 11.0912 | 10.7497 | 11.5081 |
| first | `fallible_type_changing_success/direct` | 4.4044 | 4.2690 | 4.5727 |
| first | `fallible_type_changing_success/try_pipe` | 4.4824 | 4.3231 | 4.6778 |
| first | `hundred_stage/async_ready_success/async_pipe` | 765.7816 | 757.2986 | 781.5069 |
| first | `hundred_stage/async_ready_success/direct` | 782.3963 | 739.2321 | 863.0778 |
| first | `hundred_stage/fallible_error/first/direct` | 6.1941 | 5.4047 | 7.2657 |
| first | `hundred_stage/fallible_error/first/try_pipe` | 9.2978 | 7.3156 | 11.9608 |
| first | `hundred_stage/fallible_error/last/direct` | 780.6612 | 754.6460 | 813.6696 |
| first | `hundred_stage/fallible_error/last/try_pipe` | 753.4568 | 745.2099 | 764.8597 |
| first | `hundred_stage/fallible_error/middle/direct` | 548.8608 | 500.4146 | 611.9560 |
| first | `hundred_stage/fallible_error/middle/try_pipe` | 468.8342 | 397.1819 | 561.9993 |
| first | `hundred_stage/fallible_success/direct` | 741.5557 | 738.4623 | 745.5538 |
| first | `hundred_stage/fallible_success/try_pipe` | 765.6876 | 748.8195 | 790.0269 |
| first | `hundred_stage/sync_success/direct` | 602.9498 | 573.8946 | 649.7853 |
| first | `hundred_stage/sync_success/pipe` | 566.8670 | 557.9241 | 581.3901 |
| first | `hundred_stage/try_async_error/first/direct` | 4.5215 | 4.1456 | 5.0516 |
| first | `hundred_stage/try_async_error/first/try_async_pipe` | 17.0870 | 16.0854 | 18.6629 |
| first | `hundred_stage/try_async_error/last/direct` | 969.3146 | 819.6727 | 1216.4631 |
| first | `hundred_stage/try_async_error/last/try_async_pipe` | 859.0780 | 804.2614 | 941.4640 |
| first | `hundred_stage/try_async_error/middle/direct` | 363.9490 | 356.7868 | 373.6511 |
| first | `hundred_stage/try_async_error/middle/try_async_pipe` | 663.7893 | 616.7580 | 712.9173 |
| first | `hundred_stage/try_async_ready_success/direct` | 739.0180 | 733.9778 | 745.7180 |
| first | `hundred_stage/try_async_ready_success/try_async_pipe` | 836.1341 | 811.5408 | 888.5358 |
| first | `sync_three_stage/direct` | 2.2484 | 2.2432 | 2.2551 |
| first | `sync_three_stage/pipe` | 2.3033 | 2.2517 | 2.3691 |
| first | `try_async_three_stage_ready_success/direct` | 7.0155 | 6.9246 | 7.1256 |
| first | `try_async_three_stage_ready_success/try_async_pipe` | 7.2964 | 6.9764 | 7.7683 |
| first | `vs_futures/async_10_stage_success/direct_async_fn` | 38.4984 | 38.0816 | 39.0618 |
| first | `vs_futures/async_10_stage_success/futures_then` | 95.9638 | 94.6020 | 97.6748 |
| first | `vs_futures/async_10_stage_success/skid_pipe` | 39.5885 | 38.6649 | 40.7070 |
| first | `vs_futures/async_3_stage_success/direct_async_fn` | 7.8149 | 7.6693 | 8.0002 |
| first | `vs_futures/async_3_stage_success/futures_then` | 26.8743 | 25.5362 | 28.6475 |
| first | `vs_futures/async_3_stage_success/skid_pipe` | 8.9022 | 8.0942 | 10.2960 |
| first | `vs_futures/try_async_10_stage_first_error/direct_async_fn` | 4.7049 | 4.3482 | 5.1397 |
| first | `vs_futures/try_async_10_stage_first_error/futures_and_then` | 23.6532 | 23.3786 | 24.0721 |
| first | `vs_futures/try_async_10_stage_first_error/skid_pipe` | 4.5570 | 4.3423 | 4.8994 |
| first | `vs_futures/try_async_3_stage_first_error/direct_async_fn` | 4.3017 | 4.1586 | 4.6109 |
| first | `vs_futures/try_async_3_stage_first_error/futures_and_then` | 8.5152 | 8.3699 | 8.6914 |
| first | `vs_futures/try_async_3_stage_first_error/skid_pipe` | 4.1788 | 4.1595 | 4.2130 |
| first | `vs_futures/try_async_3_stage_success/direct_async_fn` | 15.0472 | 14.4889 | 15.5978 |
| first | `vs_futures/try_async_3_stage_success/futures_and_then` | 27.8530 | 26.5347 | 29.4683 |
| first | `vs_futures/try_async_3_stage_success/skid_pipe` | 13.6593 | 13.1513 | 14.2016 |
| first | `vs_tower/try_async_3_stage_success/direct_async_fn` | 7.5632 | 7.3338 | 7.8320 |
| first | `vs_tower/try_async_3_stage_success/skid_pipe` | 7.6885 | 7.4257 | 8.0463 |
| first | `vs_tower/try_async_3_stage_success/tower_ready_call` | 18.6365 | 18.2246 | 19.1190 |
| repeat | `vs_futures/async_10_stage_success/direct_async_fn` | 38.4264 | 38.1932 | 38.7527 |
| repeat | `vs_futures/async_10_stage_success/futures_then` | 98.4047 | 95.3031 | 104.1902 |
| repeat | `vs_futures/async_10_stage_success/skid_pipe` | 38.3883 | 38.1979 | 38.6577 |
| repeat | `vs_futures/async_3_stage_success/direct_async_fn` | 8.3305 | 8.0675 | 8.6509 |
| repeat | `vs_futures/async_3_stage_success/futures_then` | 27.0045 | 25.9610 | 29.1020 |
| repeat | `vs_futures/async_3_stage_success/skid_pipe` | 8.2128 | 7.9438 | 8.5624 |
| repeat | `vs_futures/try_async_10_stage_first_error/direct_async_fn` | 4.5055 | 4.4469 | 4.5712 |
| repeat | `vs_futures/try_async_10_stage_first_error/futures_and_then` | 27.2806 | 25.4363 | 29.9836 |
| repeat | `vs_futures/try_async_10_stage_first_error/skid_pipe` | 4.5510 | 4.4469 | 4.6744 |
| repeat | `vs_futures/try_async_3_stage_first_error/direct_async_fn` | 4.5993 | 4.4368 | 4.8041 |
| repeat | `vs_futures/try_async_3_stage_first_error/futures_and_then` | 9.3843 | 8.9081 | 10.0575 |
| repeat | `vs_futures/try_async_3_stage_first_error/skid_pipe` | 4.5260 | 4.4310 | 4.6397 |
| repeat | `vs_futures/try_async_3_stage_success/direct_async_fn` | 14.5014 | 13.9298 | 15.0794 |
| repeat | `vs_futures/try_async_3_stage_success/futures_and_then` | 29.0887 | 28.3094 | 30.0810 |
| repeat | `vs_futures/try_async_3_stage_success/skid_pipe` | 14.1478 | 13.5650 | 14.8361 |

## Historical results and optimization notes

The material below describes older source revisions measured primarily on
Intel/WSL2 with Rust 1.86. Its tables and implementation descriptions are
historical, including the old async state machines and state-retention notes.
Use the current results above for the latest comparison. The Cortex-M size
probes and optimization experiments below were not repeated in this run.

This is a machine-local comparison, not a performance guarantee. Every row
uses equivalent stage functions on both sides, keeps pipeline construction
outside the measured loop, and passes input and output through `black_box`.
Async rows use the same `core::future::Ready` stages for the direct and
pipeline implementations.

Snapshot environment (measured 2026-08-23; every row and the Cortex-M
code-size table come from the same machine and settings, with a 3 second
measurement window and 100 samples):

- Rust 1.86.0, LLVM 19.1.7
- x86_64 Linux under WSL2
- Intel Core i9-9900K, 4 logical CPUs exposed to the guest
- Criterion 0.8.2, 1 second warm-up, 3 second measurement, 100 samples

The original snapshot used this command on its historical checkout:

```sh
cargo +1.86 bench --bench composition -- \
  --warm-up-time 1 --measurement-time 3 --sample-size 100
```

### Short chains

Times are Criterion point estimates. Delta is `(pipeline / direct) - 1`.

| Case | Direct | Pipeline | Delta |
|---|---:|---:|---:|
| `Pipe`, 3-stage success | 3.4235 ns | 3.448 ns | +0.72% |
| `TryPipe`, 1-stage success | 7.3616 ns | 7.3805 ns | +0.26% |
| `TryPipe`, 3-stage success | 14.28 ns | 15.145 ns | +6.06% |
| `TryPipe`, 8-stage success | 29.365 ns | 32.238 ns | +9.78% |
| `TryPipe`, 3-stage type-changing success | 6.2231 ns | 6.5288 ns | +4.91% |
| `TryPipe`, 8-stage first error | 7.8854 ns | 7.824 ns | -0.78% |
| `TryPipe`, 8-stage middle error | 18.39 ns | 18.173 ns | -1.18% |
| `TryPipe`, 8-stage last error | 29.162 ns | 31.619 ns | +8.43% |
| `AsyncPipe`, 3 ready stages | 9.1414 ns | 7.4888 ns | -18.08% |
| `TryAsyncPipe`, 3 ready success stages | 11.387 ns | 20.352 ns | +78.73% |

The `TryAsyncPipe` row is the worst in this snapshot, but it overstates the
fallible machinery's own cost: this group's stages are not the ones the
`AsyncPipe` row above uses, so the two rows are not comparable to each other.
Measured against an infallible pipeline over the *same* payload and stage
shape, the fallible one costs about 20 percentage points more, not 110.

### 100-stage chains

| Case | Direct | Pipeline | Delta |
|---|---:|---:|---:|
| `Pipe`, success | 272.74 ns | 273.16 ns | +0.15% |
| `TryPipe`, success | 694.14 ns | 694.1 ns | -0.01% |
| `TryPipe`, first error | 6.6112 ns | 6.6189 ns | +0.12% |
| `TryPipe`, middle error | 346.68 ns | 347.5 ns | +0.24% |
| `TryPipe`, last error | 691.44 ns | 695.09 ns | +0.53% |
| `AsyncPipe`, ready success | 338.34 ns | 366.11 ns | +8.21% |
| `TryAsyncPipe`, ready success | 700.68 ns | 716.1 ns | +2.20% |
| `TryAsyncPipe`, first error | 7.7117 ns | 23.868 ns | +209.50% |
| `TryAsyncPipe`, middle error | 372.95 ns | 364.2 ns | -2.35% |
| `TryAsyncPipe`, last error | 695.91 ns | 712.8 ns | +2.43% |

The first-error async result is the one case where a long `TryAsyncPipe` chain
still pays a fixed cost no direct call has. That cost belongs to static async
composition rather than to this crate in particular: measured on the same stage
shape, `futures`' `and_then` pays more of it. See "Against the `futures`
combinators" below. Creating the run future writes one
pointer and one state tag per group of eight stages, and the first stage's error
is then propagated back out through every one of those groups in the same poll.
No later stage is called. It is far cheaper than it was — the same row measured
94.350 ns when the future stored one reference per stage — but very long chains
remain a poor fit for latency-hot early-rejection paths.

Differences around one percent in this snapshot should be treated as
equivalent at this machine-local resolution, not as a stable win or loss.

The `TryPipe` re-measurement moved no pipeline arm by more than one percent in
absolute time; where a delta shifted, it is the direct baseline that moved
between runs. The 3-stage type-changing row is the clearest case: the pipeline
went from 6.4680 ns to 6.5238 ns while its direct baseline went from 6.3608 ns
to 6.0441 ns, so the delta grew from +1.69% to +7.94% without the pipeline
getting slower. At six nanoseconds these arms are dominated by run-to-run
variation, and the delta column should not be read as a regression.

### Future layout and Cortex-M code size

`size_of_val` measurements for the 100-stage ready-future workload were:

| Target (`u16` fixture) | Direct future | Pipeline future |
|---|---:|---:|
| x86_64 Linux | 8 B | 240 B |
| `thumbv6m-none-eabi` | 8 B | 124 B |

The Thumb values are identical for the infallible and fallible variants in
this fixture. These values are concrete-type layouts, not heap allocations.
They normally become part of an executor task's stack or task storage.

A Rust 1.86 `--release` Thumb symbol-size probe, with the same `Ready` stages,
one-poll driver, and shared non-inlined stage function, produced:

| Ten-stage entry point | Direct | Pipeline | Difference |
|---|---:|---:|---:|
| `AsyncPipe` | 66 B | 116 B | +50 B (+75.76%) |
| `TryAsyncPipe` | 164 B | 324 B | +160 B (+97.56%) |

| 100-stage entry point | Direct | Pipeline | Difference |
|---|---:|---:|---:|
| `AsyncPipe` | 606 B | 2236 B | +1630 B (+268.98%) |
| `TryAsyncPipe` | 1460 B | 2444 B | +984 B (+67.40%) |

These are the eight entry-point symbol sizes, not total linked-image size or a
reachability analysis; out-of-line helpers are not attributed to either row.

The pipeline rows are larger than the direct ones on purpose. `Pipe`,
`AsyncPipe`, `TryAsyncPipe` and the internal state machines mark their hot
methods `#[inline(always)]`, so a chain is flattened into its caller instead of
being executed as a tower of `poll` calls. That is what makes the runtime table
above competitive, and the multiplier grows with chain length. Building the
same fixture with `opt-level = "z"` does not undo it: the pipeline entry points
measure 328 B and 356 B at ten stages and 2408 B and 3340 B at a hundred.

`TryPipe` is the one exception: its methods are plain `#[inline]`, because
forcing them measured slower on the fallible synchronous rows.

100-stage support is still stated as a compile-time and behavioral boundary
rather than an embedded footprint target, and a shorter chain with explicit
await boundaries is still the recommendation on a constrained target. On a
constrained MCU, measure the final linked image. Actual code size changes with
stage diversity, inlining, LTO, optimization level, panic strategy, and
target.

The measurement source is checked in at `benches/support/footprint.rs` and is
compiled by the cross-target fixture, so both chain lengths are reproducible
from a checkout. Reproduce the host layouts with:

```sh
cargo +1.86 run --release --example measure_footprint
```

For the Thumb code-size symbols, use a repository checkout to build the no_std
fixture as one codegen unit and inspect the four `skid_pipe_measure_*` one-poll
entry points:

```sh
probe_dir=$(mktemp -d)
CARGO_TARGET_DIR="$probe_dir" cargo +1.86 rustc \
  --manifest-path tests/fixtures/no_std/Cargo.toml \
  --target thumbv6m-none-eabi --release --lib -- \
  -C codegen-units=1 --emit=obj
nm -S --size-sort \
  "$probe_dir"/thumbv6m-none-eabi/release/deps/skid_pipe_no_std_fixture-*.o \
  | grep 'skid_pipe_measure_.*\(direct\|pipeline\).*async$'
```

The fixture also exports four `*_future_bytes` functions so a target
disassembler can verify the returned layout constants without executing the
firmware image.

### Rejected optimizations

Three changes aimed at the two `TryAsyncPipe` rows above were implemented and
measured against this snapshot's machine. Two are recorded here so the same
ground is not retried from the same reasoning; the third became the
`lazy-construction` feature, which 0.3.0 removed because the async-block
rewrite makes construction free unconditionally (see the last section).

The `benches/diagnose.rs` groups split those rows into the costs behind them:
creating a run future without polling it, an infallible and a fallible pipeline
over identical payloads, and a first-error short-circuit at 1, 3, 10 and 100
stages. That last group is what makes the target concrete. The cost is not
proportional to chain length:

| First error at stage 1 | Time | Over direct |
|---|---:|---:|
| direct call | 13.195 ns | — |
| 1-stage `TryAsyncPipe` | 13.327 ns | +0.13 ns |
| 3-stage | 19.229 ns | +6.03 ns |
| 10-stage | 19.982 ns | +6.79 ns |
| 100-stage | 33.559 ns | +20.36 ns |

A single-stage chain matches the direct call, because it is a bare
`FirstStageFuture` with no link machine. The first link machine adds about 6 ns
and each further group about 1.2 ns. The fixed entry cost, not the chain
length, is what the 3-stage row pays.

**`#[inline(always)]` on the generated `Drop`.** On the short-circuit path every
enclosing machine drops a child that has already cleared its own tag, so those
drops are inert; they were the only methods in `future.rs` still out of line.
Inlining them regressed the 100-stage first error by 28.1% and the 10-stage one
by 6.1% (p = 0.00), and improved only the 3-stage row, by 4.3%. The per-layer
saving is real but the accumulated code growth costs more, which is consistent
with the entry-point size note above: these paths are already at the inlining
budget, so any change that grows them needs measuring rather than reasoning.

**Dispatching on a register-resident state tag.** Each machine's `poll` loops
on `match this.state`, re-reading the tag it stored on the previous transition.
Hoisting the tag into a local, updating it alongside the field, and deriving
`this` once outside the loop leaves the field writes — and so the drop protocol
— untouched while removing that store-to-load dependency. It changed nothing:
every row moved by at most 1.7%, and the `first_error_depth/direct` control,
which contains no pipeline code, moved 1.7% in the same run. LLVM was already
forwarding the stores.

Two of the three were predicted to help from reading the code and did not.
Treat the per-layer costs above as measured, and anything about why they are
what they are as a hypothesis until a benchmark says otherwise.

### Against the `futures` combinators

`benches/vs_futures.rs` puts three arms on identical stage bodies, payloads and
`Ready` futures. `skid_pipe` builds its pipeline once outside the loop and calls
`run` inside it. `futures_then` and `futures_and_then` rebuild their chain every
iteration, because one `await` consumes it — that is what composing futures
rather than functions costs a caller who runs the same computation twice.
`direct_async_fn` is a plain `async fn`: reusable, composing the same stages,
needing no dependency at all, and so the baseline both crates have to beat.

| Group | `direct_async_fn` | `skid_pipe` | `futures` | `futures` / `skid_pipe` |
|---|---:|---:|---:|---:|
| async, 3 stages, success | 12.750 ns | 23.518 ns | 41.611 ns | 1.77x |
| `try` async, 3 stages, success | 35.221 ns | 43.561 ns | 49.326 ns | 1.13x |
| `try` async, 3 stages, first error | 11.715 ns | 19.292 ns | 25.322 ns | 1.31x |
| async, 10 stages, success | 44.015 ns | 57.716 ns | 153.390 ns | 2.66x |
| `try` async, 10 stages, first error | 11.464 ns | 19.583 ns | 74.135 ns | 3.79x |

No two arms' confidence intervals overlap in any group.

`skid-pipe` beats the combinators in every group, and by more as the chain
grows: 1.1x to 1.8x at three stages, 2.7x to 3.8x at ten. The gap scales with
stage count because the rebuild the combinator arm performs each run is
proportional to the chain, while the pipeline is already built and `run` only
issues a future for it.

The first-error rows matter most for reading the rest of this file. The 3-stage
`TryAsyncPipe` row costs 64.7% over a direct call and the 10-stage one 70.8%,
which the sections above treat as this crate's weakest result. `and_then` on the
same shape costs 116.2% and 546.7%. The short-circuit overhead is real, and it
is what static async composition costs; the ecosystem's usual answer costs more
of it.

The baseline wins everywhere, and that is the honest headline. A plain
`async fn` is faster than both crates in all five groups, by 24% to 85% against
`skid-pipe`. What `skid-pipe` sells against it is a composed computation that is
a value: built in one place and returned as `impl AsyncChain<Input, Output = O>`,
assembled conditionally, and typed at every connection. Not speed.

It does not sell state retention here, and the arms above should not be read as
if it did. `AsyncStep`'s blanket implementation maps a stage to
`type Future<'a> = Fut`, which does not borrow the closure, so an async stage
that captures state by move gets a copy per call and silently accumulates
nothing — the same trap a plain async closure has, and the reason the crate
docs route async state through a `Cell` captured by shared reference. That
`Cell` works just as well without this crate. Only the synchronous `Pipe` and
`TryPipe` keep `FnMut` state across runs on their own.

Reach for the `async fn` when the chain is short and lives in one place.

Ten stages is the ceiling here because a combinator chain nests its type once
per stage, which is the same wall that made this crate flatten its own chains in
groups of eight.

Reproduce with:

```sh
cargo bench --bench vs_futures -- \
  --warm-up-time 1 --measurement-time 3 --sample-size 100
```

### Against Tower services

Tower is a useful adjacent comparison, but not a substitute for this crate.
Its unit of composition is a request/response `Service` with a readiness
protocol; it requires `std`, and it owns middleware concerns such as retry,
timeout, and backpressure. `skid-pipe` composes local functions, is `no_std` by
default, and has none of that protocol.

`benches/vs_tower.rs` measures a small but fair overlap: three fallible,
immediately-ready stages with the identical `Value -> Ready<Result<Value,
Infallible>>` bodies in all arms. The Tower arm is a reusable
`ServiceBuilder::and_then` stack over an immediately-ready terminal service and
uses Tower's normal `ready().await.call(input).await` invocation. Thus its
number includes the readiness contract a real Tower caller must drive; it does
not include HTTP, I/O, retries, or a Tokio scheduler.

This is a separate 2026-08-24 revalidation run, so compare arms within this
table only rather than to the earlier `futures` snapshot above:

| Group | plain `async fn` | `skid-pipe` | Tower ready + call | Tower / `skid-pipe` |
|---|---:|---:|---:|---:|
| try async, 3 stages, success | 11.913 ns | 20.050 ns | 30.783 ns | 1.54x |

The direct `async fn` remains the lowest-cost fixed computation. For a
reusable, typed local chain, `skid-pipe` avoids the service protocol and its
extra future combinators. Use Tower when the workload actually needs its
service semantics, not to run local computation stages faster.

Run the comparison with:

```sh
cargo +1.86 bench --bench vs_tower -- \
  --warm-up-time 1 --measurement-time 3 --sample-size 100
```

### Other pipeline libraries

The following crates should not be put in the nanosecond single-item table;
their execution models answer different questions. A valid comparison needs a
throughput and memory/backpressure workload with many items and realistic
pending work, rather than a one-item `Ready` microbenchmark.

| Library family | Model | Appropriate benchmark |
|---|---|---|
| `async-pipes`, `pumps`, `pipelines` | worker tasks/threads and channels | items/s, p50/p99 end-to-end latency, bounded-queue memory, and scaling by worker count |
| `pipeline-toolkit` | async steps over a type-keyed dynamic context | workflow wiring, context access, and error-path latency on a representative workflow |
| `pipexec` | reusable scratchpad stage executor | same-context synchronous stage latency, static vs dynamic dispatch, and per-stage instrumentation cost |
| `pipeline`, `pipe-trait`, `pipeop`, `apply` | immediate value-piping macros/traits | the direct-call baseline: they do not construct a reusable pipeline value |

Mixing any of these into the tables above would make their task/channel,
allocation, context lookup, or dispatch strategy look like a defect rather
than the feature the caller chose. Add a workload-specific suite before making
a throughput claim across those categories.

### The async-block rewrite (0.3.0)

0.3.0 replaced the hand-written state machines in `src/future.rs` with one
`async` block per group of stages. The composition shape is the one those
machines used, one group wider — arities one to sixteen terminate on `End`,
longer chains fold sixteen at a time — so this measures the machine, not the
algorithm. Both columns come from the
same machine and session, `benches/composition.rs` and `benches/diagnose.rs`
run against each tree in turn.

| | 0.2.1 hand-written | 0.3.0 `async` block |
|---|---:|---:|
| `async_three_stage_ready/async_pipe` | 5.2975 ns | 9.2050 ns |
| `try_async_three_stage_ready_success/try_async_pipe` | 20.820 ns | 11.447 ns |
| `hundred_stage/async_ready_success/async_pipe` | 361.82 ns | 385 – 419 ns |
| `hundred_stage/try_async_ready_success/try_async_pipe` | 397.61 ns | 408.38 ns |
| `hundred_stage/try_async_error/first/try_async_pipe` | 23.744 ns | 21.755 ns |
| Create a 3-stage run future | 1.3221 ns | 0.9023 ns |
| Create a 100-stage run future | 7.0211 ns | 0.8768 ns |

Construction no longer scales with chain length: an `async` block does nothing
until its first poll, so the `O(stages / 8)` stores the old machines wrote at
`run` are gone and the `lazy-construction` feature has nothing left to buy.

Run-future size, from `examples/measure_footprint.rs` and a host-side
`size_of_val` probe over the shorter chains an embedded target actually builds:

| Stages | 0.2.1 `AsyncPipe` | 0.3.0 | 0.2.1 `TryAsyncPipe` | 0.3.0 |
|---:|---:|---:|---:|---:|
| 2 | 32 B | 24 B | 32 B | 24 B |
| 4 | 48 B | 24 B | 48 B | 24 B |
| 8 | 64 B | 24 B | 64 B | 32 B |
| 100 | 240 B | 120 B | 240 B | 216 B |

The compiler overlaps the stage futures of a group into one slot, so a group's
future stops growing with its arity. The rows up to eight are 64-bit host
measurements; the 100-stage row is the `no_std` footprint example.

That last row is why `run` returns an `async` block instead of being an
`async fn`. The two spell the same thing, but the `async fn` form measures
320 B and 416 B on the same example against the block form's 216 B and 312 B,
so clippy's `manual_async_fn` is allowed at each `run` rather than taken.

Group width is the second lever, and the larger one. Measured at four, eight,
sixteen and thirty-two with everything else fixed. The build column is a clean
`cargo build` of this crate alone, taken only for the two widths that were
candidates to ship:

| Width | 100-stage success | 100-stage first error | Run future | `.text`, v7em | Clean build |
|---:|---:|---:|---:|---:|---:|
| 4 | 517.25 ns | 54.603 ns | 608 B | 6,651 B | |
| 8 | 466.66 ns | 35.200 ns | 320 B | 6,721 B | |
| 16 | 385 – 419 ns | 21.755 ns | 120 B | 6,045 B | 1.70 s |
| 32 | 364.11 ns | 18.501 ns | 72 B | 5,803 B | 8.08 s |

Nothing turns over until 32, where the crate's own compile time is what pays
for the last step: a clean `cargo build` goes from 1.70 s to about 8 s. That is
also where the ladder stops. At 64 the macro needs `#![recursion_limit]` raised
inside this crate and the same build takes 65 s, so the cost roughly eights per
doubling while the rows it buys are already close to flat. That is the only row with a trade in it, so 32 is the
`wide` feature and 16 is the default. Every chain of 16 stages or fewer — the
shape this crate is actually for — is identical either way.

`wide` is additive, which is the only reason it can be a Cargo feature at all —
but not because nothing is removed. Turning it on does delete four impls: the
16-wide `rest` arms are gated `#[cfg(not(feature = "wide"))]`, and they have to
be, since they cover the same seventeen-layer chains the new arity-17 impls do
and would overlap them. What stays fixed is the *set of types* implementing the
trait; only the impl covering a long chain is swapped for a wider one. That is
the invariant a future width has to preserve: every chain that resolved before
still resolves, so a build that worked without the feature still works with it.
A pair of mutually exclusive `width-N` features would not.

The macro is what makes any of this movable. Writing 34 impls per trait out by
hand is what kept the width at eight.

The two three-stage rows above should not be read as a property of this
change. Compiling the exact shape each one benchmarks — the same three
`#[inline(never)]` stages, the pipeline held outside the timed closure — and
disassembling it gives, for the infallible group, 21 instructions and three
indirect calls on both trees, differing only in whether the first poll's tag
check is spelled `testb $0x1, %al` or `cmpl $0x1, %eax`. Identical work, a
74% slower row. The fallible group does shrink, 52 instructions to 46, which
matches the direction of its 45% faster row but not its size.

At five to twenty nanoseconds around three non-inlinable calls, those rows
measure how the benchmark binary inlines and places the `run` call, not what
the composed pipeline compiles to. The rows that do carry signal are the
100-stage ones, the footprints and the `.text` totals.

How the body is written matters as much as how wide it is. The stages must be
separate `let` statements in one scope. Nesting them as a single expression
keeps every stage's future alive to the end of the statement and the run future
grows with the group — 912 B against 120 B at width sixteen. Nesting them as
recursive blocks instead costs the error path, because an early `?` unwinds one
scope per stage: 36.663 ns against 21.755 ns on the 100-stage first error. So
the macro accumulates the body into one expansion and emits it flat.

Flash, `tests/fixtures/no_std` built at `opt-level = "z"`, `.text` totals:

| Target | 0.2.1 | 0.3.0 | 0.3.0 `wide` |
|---|---:|---:|---:|
| `thumbv7em-none-eabihf` | 10,375 B | 6,045 B | 5,803 B |
| `thumbv6m-none-eabi` | 12,327 B | 9,531 B | |

The fixture's synchronous 100-stage paths are identical in both trees, so the
async-only saving is larger than these totals show.

The `Send` ladder is a second walk over the composition rather than a
delegation to the first. `fn run_send(..) -> impl Future<..> + Send {
AsyncChain::run(self, input) }` is the obvious shrink, and it works for the
arities that terminate on `End` — but not for the arm that folds a group over
a shorter chain. Proving that return type is `Send` means looking through
`run`'s opaque future, which for the folding arm contains the tail chain's own
opaque future, so the compiler searches `Chain` impls for a generic tail with
inference variables and recurses until it overflows; raising `recursion_limit`
to let it search further segfaults rustc instead of finishing. Both walks stay,
but they share one accumulator.

One limit worth naming: a chain longer than 127 stages now needs
`#![recursion_limit]` raised in the calling crate, where the hand-written
machines did not. The crate's own 100-stage tests compile without it.
