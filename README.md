# consistent
[![Build Status](https://travis-ci.org/DavidCai1993/consistent.svg?branch=master)](https://travis-ci.org/DavidCai1993/consistent)

Consistent hash package for Rust.

### Installation

```toml
[dependencies]
consistent_rs = "0.1.1"
```

### Documentation

See: https://docs.rs/consistent-rs/0.1.1/consistent_rs/

### Example

```rust
let mut consistant = Consistant::default();
consistant.add("cacheA");
consistant.add("cacheB");
consistant.add("cacheC");

println!("david => {:?}", consistant.get("david"));
println!("james => {:?}", consistant.get("james"));
println!("kelly => {:?}", consistant.get("kelly"));
```
