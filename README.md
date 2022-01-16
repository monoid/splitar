# Content-aware splitting of tar archives

`splitar` splits large tar archive, writing each volume as an independent
tar archive, never splitting it in the middle of a file.

`splitar` works in a streaming fashion, never loading into the memory
more than necessary.

## Features
+ Split tar files, generating valid tar files of limited data size.
+ Reading data from stdin (writing to stdout is not possible, obviously).
+ Compress filter (or any other kind) for the output.
+ Recreate directory entries for each volume, making them truly independent.

## TODO
+ Optionally fail when file too large is found.

## Limitations
+ While chunks are limited by size, it is the size of contained data, not
  file output.
+ If the input archive contains file larger than chunk size limit, the output
  chunk will inevitably contains the entire file, as `splitar` never splits
  contained files.
+ If a volume contains a link to file in some previous volume, `tar` with
  default arguments will refuse creating this link if the target does not 
  exists (e.g. it was not unpacked from the previous volume).
