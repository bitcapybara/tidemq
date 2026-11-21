"""Advise Linux to release clean file pages from one private CI Cargo target."""

import os
import stat
import sys


def release_clean_pages(target):
    root = os.stat(target, follow_symlinks=False)
    if (os.path.realpath(target) != target or not stat.S_ISDIR(root.st_mode)
            or root.st_uid != os.getuid() or root.st_mode & 0o077):
        raise ValueError("The CI Cargo target must be a private, owned directory")

    root_fd = os.open(target, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        if not os.path.samestat(root, os.fstat(root_fd)):
            raise ValueError("The CI Cargo target changed after validation")
        return advise_files(root_fd, root)
    finally:
        os.close(root_fd)


def advise_files(root_fd, root):
    advised = 0
    for _, directories, names, directory in os.fwalk(".", follow_symlinks=False, dir_fd=root_fd):
        directories[:] = [
            name for name in directories
            if os.stat(name, dir_fd=directory, follow_symlinks=False).st_dev == root.st_dev
        ]
        for name in names:
            entry = os.stat(name, dir_fd=directory, follow_symlinks=False)
            if (not stat.S_ISREG(entry.st_mode) or entry.st_dev != root.st_dev
                    or entry.st_uid != root.st_uid or entry.st_nlink != 1):
                continue
            fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
            try:
                current = os.fstat(fd)
                if os.path.samestat(entry, current) and current.st_nlink == 1:
                    os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
                    advised += 1
            finally:
                os.close(fd)
    return advised


if __name__ == "__main__":
    print(f"Advised release of clean Cargo cache pages for {release_clean_pages(sys.argv[1])} files")
