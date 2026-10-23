"""One polling process per database, on Windows and POSIX."""

from contextlib import contextmanager
from pathlib import Path
import os


@contextmanager
def instance_lock(db_path: str):
    path = Path(db_path).with_suffix(Path(db_path).suffix + ".lock")
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+b")
    try:
        if os.name == "nt":
            import msvcrt

            handle.seek(0)
            if not handle.read(1):
                handle.write(b"0")
                handle.flush()
            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        handle.close()
        raise RuntimeError("База уже используется другим процессом бота") from None
    try:
        yield
    finally:
        # Closing releases the OS lock. Keep the inode to avoid unlink races.
        handle.close()
