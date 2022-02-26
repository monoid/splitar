# Content-aware splitting of tar archives

`splitar` splits large tar archive, writing each volume as (almost)
independent tar archive, never splitting it in the middle of a file.

`splitar` works in a streaming fashion, never loading into the memory
more than necessary.

## Features
+ Split tar files, generating valid tar files of limited data size.
+ Reading data from stdin if file is `-` (writing to stdout is not possible,
  obviously).
+ Compress filter (or any other kind) for the output.
+ Optionally recreate directory entries for each new volume.
+ Optionally fail when file too large is found.

## Limitations
+ While chunks are limited by size, it is the size of contained data, not
  file output.
+ If the input archive contains file larger than chunk size limit, the output
  chunk will inevitably contains the entire file, as `splitar` never splits
  contained files.  You may use option `--fail-on-large-file` if you want
  the util to fail on such a file.
+ If a volume contains a hardlink or symlink to file in some previous volume,
  `tar` will refuse creating this link if the target does not exists (e.g.
  it was not unpacked from one of previous volumes).

# Installation

`splitar` is written in Rust, and having the
[`cargo` installed](https://doc.rust-lang.org/cargo/getting-started/installation.html),
you can install it with `cargo install splitar` command.  No manual
installation of any additional dependency is required.
