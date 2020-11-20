# Search Engine

Web Info search engine project

## Build

Prerequisites: rust, cargo

```bash
cargo build --release
```

## Run

First, generate index

```
cargo run --release --bin index dataset/maildir
```

`dataset/maildir` is the directory of data

Bool Search

```
cargo run --release --bin bool_search
```

When seeing `Q>`, type query and hit enter

For the present, only support AND logic