# Changelog

All notable changes to this project are documented in this file.

The project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Because the public API is still pre-1.0, minor releases may contain breaking
API changes. See the compatibility policy in the README before upgrading.

## [Unreleased]

These changes target 0.2.0 because the async execution traits have a breaking
GAT migration. No 0.2.0 release is implied until this section is dated.

### Added

- Added the opt-in `lazy-construction` feature. Each link future then parks its
  input and builds its tail chain's future on its own first poll, so creating a
  run future is one layer's work regardless of chain length and one dropped
  before its first poll is `O(1)`. Creating a 100-stage run future drops from
  9.8489 ns to 1.2309 ns; the work moves into `poll`, where the extra state per
  layer costs more than it saved, so the 100-stage first error regresses 19.6%
  and the three-stage success rows 9.2% and 1.6%. It is off by default because
  the default build optimizes end-to-end latency. The public API, the run
  future's 240-byte layout, and the guarantee that no stage runs before the
  first poll are identical either way.
- Added `TryAsyncPipe` for statically composing asynchronous
  `Result<T, E>` stages with first-error short-circuiting.
- Added the opt-in `tokio` feature with `spawn` and `spawn_local` extension
  traits for owned `AsyncChain` and `TryAsyncChain` tasks. The default build
  remains dependency-free and `no_std`.
- Documented the exclusive mutable borrow held by an async pipeline's returned
  future, including cancellation and sequential state reuse.
- Added compile-fail and runtime coverage for async borrowing, pending futures,
  cancellation, short-circuiting, and stateful stages.
- Expanded direct-comparison benchmarks across fallible chain lengths, error
  positions, 100-stage chains, and `TryAsyncPipe` ready futures.
- Added 100-stage execution coverage for all four pipeline variants, including
  `no_std` WebAssembly and Cortex-M compilation.
- Added compile-time coverage for the `Send + 'static` future boundary used by
  `tokio::spawn` and the non-`Send` boundary used by `LocalSet::spawn_local`,
  without adding a Tokio dependency.
- Made public API documentation warnings fail CI on both Rust 1.86 and stable.
- Added a CI check that fails the build if any file outside `src/future.rs`
  opts back into `unsafe`, making the crate's unsafe-isolation claim enforced
  rather than documented.
- Added the docs.rs configuration that marks the `tokio` items with their
  feature requirement, so they are no longer shown as unconditionally
  available.
- Added pinned-nightly Miri coverage for async pin projection, pending-stage
  cancellation, first-error short-circuiting, and 100-stage chains.
- Added a reproducible Cortex-M fixture for comparing direct and pipeline
  future layout and one-poll code size, at ten and at 100 stages.
- Added a benchmark against the `futures` combinators on identical stages.
  `skid-pipe` measures 1.1x to 1.8x faster at three stages and 2.7x to 3.8x at
  ten, the gap growing because a combinator chain is consumed by one `await`
  and so is rebuilt per run. First-error short-circuiting, this crate's weakest
  result against direct calls, costs `and_then` more on the same shape. A plain
  `async fn` beats both crates in every group.

### Changed

- Reduced `TryPipe` success-path overhead without changing its public API or
  first-error behavior.
- Flattened async execution in groups of eight stages so 100-stage
  `AsyncPipe` and `TryAsyncPipe` values compile under rustc's default recursion
  limit. The safe public API is unchanged; unsafe pin projection is confined
  to one denied-by-default internal module.
- Made async runs lazy from their first stage while retaining an exclusive
  pipeline borrow until the returned future completes or is dropped.
- Changed `AsyncStep` and `AsyncChain` to expose their concrete future through
  a generic associated `Future<'a>` type. Ordinary async functions and
  closures continue to use the blanket implementation unchanged.
- Marked every async run future `#[must_use]`. Discarding a run future was
  silently a no-op after 0.1.2 replaced the `impl Future` return types, because
  `#[must_use]` does not propagate to a named type.
- Changed the five `#[inline(always)]` attributes in `TryPipe`, `TryStep`, and
  `TryChain` to plain `#[inline]`, matching `Pipe` and `AsyncPipe`. Benchmarks
  show no regression on any fallible arm.
- Documented that creating a run future, while lazy in the sense that no stage
  runs before the first poll, still costs `O(stages)` struct moves to build.
- Collapsed the six hand-written async state machines into one generator macro,
  reducing the crate's only unsafe module from 1,275 to 670 lines with no
  change to the public API and no change to the pin-projection or drop
  protocol.
- Changed each async link future to borrow the sub-pipeline it drives as a
  single pointer and project one stage out of it when that stage starts, instead
  of storing one reference per stage. A 100-stage run future is 240 bytes on
  x86_64 and 124 bytes on `thumbv6m-none-eabi`, down from 1016 and 512, and
  creating one writes about a tenth as many stores. Together with the wider
  groups this cut the 100-stage `TryAsyncPipe` first-error run from 94.350 ns to
  23.868 ns against a 7.7117 ns direct baseline, and brought the 100-stage
  `AsyncPipe` and `TryAsyncPipe` success rows to +8.2% and +2.2% over direct
  calls. The 100-stage entry-point code size fell as well.
- Marked the hot methods of `Pipe`, `AsyncPipe`, `TryAsyncPipe` and the internal
  async state machines `#[inline(always)]`, so a chain flattens into its caller
  instead of running as a tower of `poll` calls. The 100-stage `TryAsyncPipe`
  rows went from roughly +55% over equivalent direct calls to roughly +9%, and
  the three-stage `Pipe` and 100-stage `TryPipe` rows now match or beat direct
  calls. Entry-point code size grows with chain length as a result; see the
  benchmark snapshot. `TryPipe` keeps plain `#[inline]`, which measured faster
  for it.
- Replaced the `Option<&mut Step>` slot that each async link used to park its
  stage with the reference held by value. `Option::take` wrote `None` back on
  every stage transition even though the state tag already records which steps
  are consumed; dropping that store cut the 100-stage `TryAsyncPipe` first-error
  run by a further 18%.
- Removed the panic path that every async stage transition emitted while taking
  its step out of an `Option`. The step is present by the same state-tag
  invariant the surrounding union already relies on, so the check was
  unreachable. A three-stage `AsyncPipe` run over `Ready` stages is 15% faster,
  a 100-stage one 51% faster, and the ten-stage `thumbv6m-none-eabi` entry
  point is smaller at every optimization level measured. It also made each
  `poll` small enough for LLVM to flatten, which is the effect the
  `#[inline(always)]` change above then made unconditional.

### Removed

- Removed documentation that outlived what it described: the `TryAsyncChain`
  contrast with an "artificial successful future", which referred to the
  deleted `TryAsyncChain for End` impl, and a sentence in `AsyncChain` that
  repeated its own first paragraph. The `README.md` validation command list now
  matches what CI runs.
- **Breaking:** Removed the `AsyncChain for End` and `TryAsyncChain for End`
  implementations. They became unreachable when single-stage pipelines were
  rewired to run their head stage directly, and `TryAsyncChain for End` was
  uncallable without a turbofish because `Error` appeared neither in the
  arguments nor in the caller's return position. Removing them also removes the
  only type that implemented both Tokio extension traits, so `End.spawn(..)` no
  longer fails with an ambiguous-method error. `Chain for End` and
  `TryChain for End` are unaffected.

Existing `Pipe`, `TryPipe`, and ordinary-function `AsyncPipe` users require no
migration. A hand-written `AsyncStep` or `AsyncChain` implementation must add
its `type Future<'a>` and return `Self::Future<'_>` from `call` or `run`.
`TryAsyncPipe` is additive. Code that called `AsyncChain::run` or
`TryAsyncChain::run` on `End` directly must call the pipeline instead; no such
call could reach a stage. Because the async execution traits changed, this set
of changes is intended for the next pre-1.0 minor release.

## [0.1.2] - 2026-08-22

### Changed

- Made the direct and `AsyncPipe` Criterion comparisons use the same
  `core::future::Ready`-returning stages.

No API migration is required from 0.1.1.

## [0.1.1] - 2026-08-22

### Changed

- Changed the crate's license from `MIT OR Apache-2.0` to MIT-only.

No code migration is required from 0.1.0.

## [0.1.0] - 2026-08-22

### Added

- Initial release of the `no_std`, dependency-free static pipeline core.
- Added synchronous `Pipe`, fallible `TryPipe`, and executor-independent
  `AsyncPipe` composition with `FnMut` state.
- Added native, WebAssembly, and embedded target validation with Rust 1.86 as
  the minimum supported Rust version (MSRV).

[Unreleased]: https://github.com/LuticaCANARD/skid-pipe/compare/v0.1.2...HEAD
[0.1.2]: https://github.com/LuticaCANARD/skid-pipe/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/LuticaCANARD/skid-pipe/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/LuticaCANARD/skid-pipe/releases/tag/v0.1.0
