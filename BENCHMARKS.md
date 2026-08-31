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

## Against the `futures` combinators

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

## Against Tower services

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

## Other pipeline libraries

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

## The async-block rewrite (0.3.0)

0.3.0 replaced the hand-written state machines in `src/future.rs` with one
`async` block per group of eight stages. The composition shape is unchanged —
arities one to eight terminate on `End`, longer chains fold eight at a time —
so this measures the machine, not the algorithm. Both columns come from the
same machine and session, `benches/composition.rs` and `benches/diagnose.rs`
run against each tree in turn.

| | 0.2.1 hand-written | 0.3.0 `async` block |
|---|---:|---:|
| `async_three_stage_ready/async_pipe` | 5.2975 ns | 10.133 ns |
| `try_async_three_stage_ready_success/try_async_pipe` | 20.820 ns | 11.303 ns |
| `hundred_stage/async_ready_success/async_pipe` | 361.82 ns | 422.26 ns |
| `hundred_stage/try_async_ready_success/try_async_pipe` | 397.61 ns | 435.27 ns |
| `hundred_stage/try_async_error/first/try_async_pipe` | 23.744 ns | 30.913 ns |
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
| 100 | 240 B | 216 B | 240 B | 312 B |

The compiler overlaps the stage futures of a group into one slot, so a group's
future stops growing with its arity. The rows up to eight are 64-bit host
measurements; the 100-stage row is the `no_std` footprint example.

That last row is why `run` returns an `async` block instead of being an
`async fn`. The two spell the same thing, but the `async fn` form measures
320 B and 416 B on the same example against the block form's 216 B and 312 B,
so clippy's `manual_async_fn` is allowed at each `run` rather than taken.

Group width is a second lever on the same rows. At width four the 100-stage
success costs 517.25 ns and the future is 608 B; at width sixteen, 386.30 ns
and 176 B, with `.text` on `thumbv7em-none-eabihf` falling from 6,651 B to
6,101 B. Wider was better on every row measured, so there is no trade to
expose as a knob — only a width to pick, and eight is a conservative one that
keeps the generated impl count at nine per trait.

Flash, `tests/fixtures/no_std` built at `opt-level = "z"`, `.text` totals:

| Target | 0.2.1 | 0.3.0 |
|---|---:|---:|
| `thumbv7em-none-eabihf` | 10,375 B | 6,651 B |
| `thumbv6m-none-eabi` | 12,327 B | 9,737 B |

The fixture's synchronous 100-stage paths are identical in both trees, so the
async-only saving is larger than these totals show.

One limit worth naming: a chain longer than 127 stages now needs
`#![recursion_limit]` raised in the calling crate, where the hand-written
machines did not. The crate's own 100-stage tests compile without it.
