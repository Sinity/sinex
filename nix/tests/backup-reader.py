"""Directory replacement cannot redirect the privileged Borg root."""
import os
from pathlib import Path
import runpy
import sys
import tempfile
import unittest

open_directory = runpy.run_path(sys.argv.pop(1))["open_directory"]


class ArtifactDirectory(unittest.TestCase):
    def test_symlinked_intermediate_parent_is_refused(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "private").mkdir()
            (root / "selected").symlink_to(root / "private", target_is_directory=True)
            with self.assertRaises(OSError):
                open_directory(root / "selected")

    def test_opened_cwd_keeps_its_inode_when_the_parent_is_replaced(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "selected" / "main").mkdir(parents=True)
            (root / "private" / "main").mkdir(parents=True)
            fd = open_directory(root / "selected" / "main")
            try:
                inode = os.fstat(fd).st_ino
                (root / "selected").rename(root / "moved")
                (root / "selected").symlink_to(root / "private", target_is_directory=True)
                self.assertEqual(os.fstat(fd).st_ino, inode)
                self.assertNotEqual(inode, (root / "private" / "main").stat().st_ino)
                with self.assertRaises(OSError):
                    open_directory(root / "selected" / "main")
            finally:
                os.close(fd)


if __name__ == "__main__":
    unittest.main()
