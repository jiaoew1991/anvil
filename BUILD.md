# anvil-rs Build Instructions

## Prerequisites

- Rust toolchain (cargo, rustc)
- Python 3.10+
- maturin (`pip install maturin`)

## Building

### Production Build

Build a wheel package:

```bash
PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1 maturin build --release
```

The wheel will be created in `target/wheels/`.

### Development Build

Install the package in development mode (editable install):

```bash
PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1 maturin develop
```

## Important Notes

### DO NOT use `cargo build` directly

This is a PyO3 extension module and must be built with `maturin`, not `cargo build`. 
Using `cargo build` will fail with linker errors about missing Python symbols.

### Python 3.14 Compatibility

PyO3 0.23 officially supports Python up to 3.13. The environment variable 
`PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1` allows building with Python 3.14 
using the stable ABI.

## Environment Variables

- `PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1`: Enable forward compatibility with newer Python versions

## Troubleshooting

### Linker errors about missing Python symbols

You're probably using `cargo build` instead of `maturin build`. Use maturin.

### "Python 3.14 is newer than PyO3's maximum supported version"

Set `PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1` environment variable.
