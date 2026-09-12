#!/usr/bin/env python3
"""Execute the same Rust sensor module on native and Wasm; check MCU builds."""
from pathlib import Path
import os
import subprocess

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "examples/portable-sensor/Cargo.toml"
TARGET = ROOT / "target/portable-sensor"


def run(*args, capture=False):
    return subprocess.run(
        args, cwd=ROOT, check=True, text=True,
        stdout=subprocess.PIPE if capture else None,
    ).stdout


def cargo(*args):
    run("cargo", *args, "--manifest-path", str(MANIFEST), "--target-dir", str(TARGET))


cargo("test", "--lib")
cargo("build", "--features", "host", "--bin", "sensor-replay")
cargo("build", "--features", "host", "--bin", "sensor-replay", "--target", "wasm32-wasip1")
native = TARGET / "debug" / ("sensor-replay.exe" if os.name == "nt" else "sensor-replay")
wasm = TARGET / "wasm32-wasip1/debug/sensor-replay.wasm"
expected = """raw,level,delta,high
100,0,0,false
1100,500,500,false
3100,1750,1250,false
4100,2875,1125,false
4100,3437,562,true
1100,2218,-1219,true
100,1109,-1109,false
"""
for samples in [[], ["0", "65535", "65535", "0", "100", "3100"], ["100"] * 64]:
    native_output = run(str(native), *samples, capture=True)
    wasm_output = run("node", str(ROOT / "scripts/run-sensor-wasm.mjs"), str(wasm), *samples, capture=True)
    if native_output != wasm_output:
        raise SystemExit("Native/Wasm outputs differ")
    if not samples and native_output != expected:
        raise SystemExit("Default replay differs from expected observations")
print("PASS: native and Wasm agree on 77 samples across 3 fresh streams", flush=True)
for target in ["wasm32v1-none", "thumbv6m-none-eabi", "thumbv7em-none-eabihf", "riscv32imac-unknown-none-elf"]:
    cargo("check", "--lib", "--target", target)
print("PASS: shared no_std library compiles for core-only Wasm, ARM, and RISC-V", flush=True)
print("MCU hardware execution, timing, and memory use are not measured.")
