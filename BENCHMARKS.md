# Benchmark snapshot

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

Run the maintained benchmark with:

```sh
cargo +1.86 bench --bench composition -- \
  --warm-up-time 1 --measurement-time 3 --sample-size 100
```

## Short chains

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

## 100-stage chains

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
still pays a fixed cost no direct call has. Creating the run future writes one
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

## Future layout and Cortex-M code size

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

## Rejected optimizations

Three changes aimed at the two `TryAsyncPipe` rows above were implemented and
measured against this snapshot's machine. None landed. They are recorded here
so the same ground is not retried from the same reasoning.

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

**Lazy tail construction.** Each machine's `new` builds its tail chain's future
immediately, so creating a 100-stage run future costs 9.7967 ns before any
stage runs. Parking the input in the slot union instead, behind a new `Input`
state, and building the tail on the first poll cuts that to 1.2431 ns, an 87.6%
reduction, and makes a run future that is dropped before its first poll O(1).
The work is not removed, only moved: it lands in `poll`, where one extra state
per layer costs more than it saved. The 100-stage first error regressed 19.8%,
the 10-stage 14.4%, the 3-stage 9.0%, and the same-shape success rows 11.6%
(infallible) and 3.8% (fallible), all at p = 0.00. This is a trade, not a
failure — it is the right change for a workload that creates and cancels run
futures — but it is a loss on end-to-end latency, which is what these rows
track.

Two of the three were predicted to help from reading the code and did not. The
one that did exactly what it was designed to do still lost overall. Treat the
per-layer costs above as measured, and anything about why they are what they
are as a hypothesis until a benchmark says otherwise.
