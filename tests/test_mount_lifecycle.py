"""
Test for mounting lifecycle:
1. Ensure clean initial state (mount stop)
2. accli mount start <project_slug>
3. Capture directory structure (ensure not empty) & read any file
4. accli mount stop
5. accli mount start <project_slug> again
6. Capture directory structure (must not be empty) & read any file
"""
import os
import sys
import time
import platform
import subprocess
import unittest
from pathlib import Path


class TestMountLifecycle(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.project_slug = os.environ.get("ACCLI_TEST_PROJECT", "test")
        cls.mount_drive = os.environ.get("ACCLI_TEST_DRIVE", "W:")
        cls.mount_arg = cls.mount_drive.rstrip("\\/")
        if platform.system() == "Windows" and len(cls.mount_arg) == 1 and cls.mount_arg.isalpha():
            cls.mount_arg += ":"
        cls.mount_path = Path(f"{cls.mount_arg}\\") if platform.system() == "Windows" else Path.home() / "accelerator" / "mnt" / cls.project_slug

    def run_cli(self, args: list, timeout: int):
        cmd = [sys.executable, "-m", "accli"] + args
        print(f"\n[TEST EXEC] {' '.join(cmd)}")
        result = subprocess.run(
            cmd,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            errors="replace",
            timeout=timeout
        )
        print(f"[EXIT CODE] {result.returncode}")
        if result.stdout:
            print(f"[STDOUT]\n{result.stdout.strip()}")
        if result.stderr:
            print(f"[STDERR]\n{result.stderr.strip()}")
        return result

    def get_structure(self, target_path: Path):
        """List files and folders at the mounted location and ensure it is not empty."""
        entries = []
        for _ in range(10):
            try:
                if target_path.exists():
                    entries = [p.name for p in target_path.iterdir()]
                    if entries:
                        break
            except OSError:
                pass
            time.sleep(0.5)

        self.assertTrue(
            target_path.exists(),
            f"Mount path '{target_path}' does not exist or drive is not mapped."
        )
        print(f"[MOUNT STRUCTURE at {target_path}]: {entries}")
        return entries

    def read_any_file(self, target_path: Path):
        """Locate any readable file under the mounted project path and read its content."""
        search_dirs = [target_path / self.project_slug, target_path]
        for sdir in search_dirs:
            if not sdir.exists():
                continue
            for root, dirs, files in os.walk(sdir):
                for file_name in files:
                    candidate = Path(root) / file_name
                    try:
                        print(f"[TEST READ FILE] Attempting to read: {candidate}")
                        with open(candidate, "rb") as fp:
                            data = fp.read(1024)
                        print(f"[TEST READ FILE] Successfully read {len(data)} bytes from '{candidate.name}'.")
                        return candidate, len(data)
                    except Exception as e:
                        print(f"[TEST READ FILE] Skipping unreadable candidate {candidate}: {e}")

        self.fail(f"Could not read any file under mounted path '{target_path}'.")

    def tearDown(self):
        try:
            self.run_cli(["mount", "stop", self.mount_arg], 30)
        except Exception:
            pass

    def test_mount_start_stop_start_lifecycle(self):
        # Step 0: Ensure clean initial state
        print("\n--- Step 0: Ensure clean initial state ---")
        self.run_cli(["mount", "stop", self.mount_arg], 30)
        time.sleep(2)

        # Step 1: First mount start
        print(f"\n--- Step 1: Starting mount for '{self.project_slug}' at '{self.mount_arg}' ---")
        start1 = self.run_cli(["mount", "start", self.project_slug, self.mount_arg], 60)
        self.assertEqual(start1.returncode, 0, f"First mount start failed: {start1.stderr or start1.stdout}")

        # Step 2: Check structure is not empty and try to read any file
        print("\n--- Step 2: Checking structure & reading file after 1st mount ---")
        structure1 = self.get_structure(self.mount_path)
        self.assertGreater(
            len(structure1), 0,
            f"Expected structure to be not empty after 1st mount, got: {structure1}"
        )
        file1, bytes_read1 = self.read_any_file(self.mount_path)
        self.assertIsNotNone(file1)

        # Step 3: Mount stop (clean)
        print(f"\n--- Step 3: Stopping mount at '{self.mount_arg}' ---")
        stop = self.run_cli(["mount", "stop", self.mount_arg], 30)
        self.assertEqual(stop.returncode, 0, f"Mount stop failed: {stop.stderr or stop.stdout}")
        time.sleep(3)

        # Step 4: Second mount start
        print(f"\n--- Step 4: Starting mount for '{self.project_slug}' at '{self.mount_arg}' (2nd time) ---")
        start2 = self.run_cli(["mount", "start", self.project_slug, self.mount_arg], 120)
        self.assertEqual(start2.returncode, 0, f"Second mount start failed: {start2.stderr or start2.stdout}")

        # Step 5: Check structure is not empty and try to read any file after clean & remount
        print("\n--- Step 5: Checking structure & reading file after 2nd mount ---")
        structure2 = self.get_structure(self.mount_path)
        self.assertGreater(
            len(structure2), 0,
            f"Expected structure to be not empty after 2nd mount, got: {structure2}"
        )
        file2, bytes_read2 = self.read_any_file(self.mount_path)
        self.assertIsNotNone(file2)

        print("\n[SUCCESS] Mount start -> read -> stop -> start -> read lifecycle test completed successfully!")


if __name__ == "__main__":
    unittest.main()
