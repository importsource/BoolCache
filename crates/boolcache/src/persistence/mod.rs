pub mod aof;
pub mod rdb;

use std::path::Path;

/// Atomically replace `dst` with `src`.
///
/// On Unix this is a single `rename(2)` syscall which is atomic.
/// On Windows `rename` fails if the destination already exists, so we
/// remove it first.  There is a small TOCTOU window, but it is
/// acceptable for our use-case (persistence files).
pub(crate) fn atomic_rename(src: &Path, dst: &Path) -> std::io::Result<()> {
    #[cfg(windows)]
    let _ = std::fs::remove_file(dst); // ignore error if dst doesn't exist yet
    std::fs::rename(src, dst)
}
