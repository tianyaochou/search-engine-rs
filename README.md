# Search Engine

Web Info search engine project

## Build

Prerequisites: rust, cargo

```bash
cargo build --release
```

## Run

First, link data directory in dataset, then generate index.

Running time is estimated 10 minutes on 2.3 GHz Quad-Core Intel Core i5 and memory usage is peaked at 7 GB.
Generated index takes up about 10 GB.

```
# PWD: project root
ln -s path-to-maildir dataset
cargo run --release --bin index dataset/maildir
```

`dataset/maildir` is the directory of data

Bool Search

Estimated memory usage is 2.5GB.

```
cargo run --release --bin bool_search
```

When seeing `Q>`, type query and hit enter

For the present, only support AND logic