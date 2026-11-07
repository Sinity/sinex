"""Enter the selected artifact by directory descriptors before the root Borg job runs."""
import argparse
import os
from pathlib import PurePosixPath
import pwd
import re


def open_directory(path):
    """No intermediate component may redirect a privileged reader through a symlink."""
    parts = PurePosixPath(path).parts
    if not parts or parts[0] != "/" or ".." in parts:
        raise ValueError("backup artifact root must be an absolute directory")
    fd = os.open("/", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        for part in parts[1:]:
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            os.close(fd)
            fd = child
        return fd
    except BaseException:
        os.close(fd)
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root")
    parser.add_argument("run")
    parser.add_argument("component", choices=("main", "keys"))
    parser.add_argument("user")
    parser.add_argument("script")
    args = parser.parse_args()
    if not re.fullmatch(r"[0-9a-f-]{36}", args.run):
        raise ValueError("backup run is not a UUID directory")
    uid = pwd.getpwnam(args.user).pw_uid
    fd = open_directory(args.root)
    try:
        for name in (args.run, args.component):
            child = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            os.close(fd)
            fd = child
            metadata = os.fstat(fd)
            if metadata.st_uid != uid or metadata.st_mode & 0o077:
                raise ValueError("backup artifact directories must be private to the daemon")
        # The cwd inode remains pinned even if the daemon renames its parents.
        # Borg's ordinary Linux walk opens children with O_NOFOLLOW; no
        # --read-special option is enabled by this module.
        os.fchdir(fd)
    finally:
        os.close(fd)
    env = dict(os.environ, SINEX_BACKUP_CWD=f"{args.root}/{args.run}/{args.component}")
    os.execve(args.script, [args.script], env)


if __name__ == "__main__":
    main()
