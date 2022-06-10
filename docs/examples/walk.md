# Walk Dir

OpenDAL has native walk dir support via [`BatchOperator`](/opendal/struct.BatchOperator.html).

OpenDAL supports two ways to walk a dir:

- bottom up
- top down

## Bottom up

`Bottom up` will list from the most inner dirs.

Given the following file tree:

```text
.
├── dir_x/
│   ├── dir_y/
│   │   ├── dir_z/
│   │   └── file_c
│   └── file_b
└── file_a
```

The output could be:

```text
dir_x/dir_y/dir_z/file_c
dir_x/dir_y/dir_z/
dir_x/dir_y/file_b
dir_x/dir_y/
dir_x/file_a
dir_x/
```

Refer to [`BottomUpWalker`](/opendal/io_util/struct.BottomUpWalker.html) for more information.

```rust
let mut ds = op.batch().walk_bottom_up();

while let Some(de) = ds.try_next().await? {
    match de.mode() {
        ObjectMode::FILE => {
            println!("Handling file")
        }
        ObjectMode::DIR => {
            println!("Handling dir like start a new list via meta.path()")
        }
        ObjectMode::Unknown => continue,
    }
}
```

## Top down

`Top down` will list from the most outer dirs.

Given the following file tree:

```text
.
├── dir_x/
│   ├── dir_y/
│   │   ├── dir_z/
│   │   └── file_c
│   └── file_b
└── file_a
```

The output could be:

```text
dir_x/
dir_x/file_a
dir_x/dir_y/
dir_x/dir_y/file_b
dir_x/dir_y/dir_z/
dir_x/dir_y/dir_z/file_c
```

Refer to [`TopDownWalker`](/opendal/io_util/struct.TopDownWalker.html) for more information.

```rust
let mut ds = op.batch().walk_top_down();

while let Some(de) = ds.try_next().await? {
    match de.mode() {
        ObjectMode::FILE => {
            println!("Handling file")
        }
        ObjectMode::DIR => {
            println!("Handling dir like start a new list via meta.path()")
        }
        ObjectMode::Unknown => continue,
    }
}
```
