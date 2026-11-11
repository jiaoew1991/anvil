# anvil-rs Build Instructions

## Prerequisites

- Rust toolchain (cargo, rustc)
- Python 3.10+
- maturin (`pip install maturin`)

## Building

### Production Build

Build a wheel package:

```bash
maturin build --release
```

The wheel will be created in `target/wheels/`.

### Development Build

Install the package in development mode (editable install):

```bash
maturin develop
```

## Important Notes

### DO NOT use `cargo build` directly

This is a PyO3 extension module and must be built with `maturin`, not `cargo build`. 
Using `cargo build` will fail with linker errors about missing Python symbols.

## Troubleshooting

### Linker errors about missing Python symbols

You're probably using `cargo build` instead of `maturin build`. Use maturin.
