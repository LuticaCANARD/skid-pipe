// Node's WASI preview1 host: https://nodejs.org/api/wasi.html
import { readFile } from 'node:fs/promises';
import { WASI } from 'node:wasi';

const [path, ...samples] = process.argv.slice(2);
if (!path) {
  throw new Error('Usage: node scripts/run-sensor-wasm.mjs <sensor-replay.wasm> [samples...]');
}
const wasi = new WASI({ version: 'preview1', args: ['sensor-replay', ...samples] });
const module = await WebAssembly.compile(await readFile(path));
const instance = await WebAssembly.instantiate(module, wasi.getImportObject());
process.exitCode = wasi.start(instance);
