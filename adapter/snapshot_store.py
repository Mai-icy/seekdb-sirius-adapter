"""Immutable Parquet generations on the compute host; no GPU pin state."""
from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import time


def private_directory(path):
    path = Path(path).resolve(strict=True)
    info = path.stat()
    if not path.is_dir() or info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise ValueError("Parquet storage must be a private directory owned by this UID")
    return path


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sync_directory(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def lock_until(file, operation, deadline):
    while True:
        try:
            fcntl.flock(file, operation | fcntl.LOCK_NB)
            return
        except BlockingIOError:
            if time.monotonic() >= deadline:
                raise TimeoutError("Timed out waiting for GPU snapshot publication")
            time.sleep(0.05)


class SnapshotStore:
    def __init__(self, root, capacity):
        self.root = private_directory(root)
        self.capacity = capacity

    @contextmanager
    def acquire(self, identity, label, catalog, work, deadline):
        # Schema is deliberately not part of the key: changing a table must
        # reject a named generation, rather than silently replace its data.
        key = hashlib.sha256(canonical([identity, label]).encode()).hexdigest()
        destination = self.root / key
        with (self.root / (key + ".lock")).open("a+b") as lock:
            lock_until(lock, fcntl.LOCK_SH, deadline)
            if not destination.exists():
                fcntl.flock(lock, fcntl.LOCK_UN)
                lock_until(lock, fcntl.LOCK_EX, deadline)
            manifest = None
            if destination.exists():
                manifest = json.loads((destination / "manifest.json").read_text())
                if manifest.get("version") != 1 or manifest["catalog"] != catalog:
                    raise ValueError("GPU snapshot schema changed; select a new gpu_snapshot name")
                for table in manifest["tables"]:
                    for part in table["parts"]:
                        path = destination / part["file"]
                        if path.parent != destination or not path.is_file() or path.stat().st_size != part["bytes"]:
                            raise ValueError("GPU snapshot file is missing or damaged; explicitly replace this generation")
                fcntl.flock(lock, fcntl.LOCK_SH)
            lease = dict(key=key, destination=destination, manifest=manifest, work=Path(work),
                         identity=identity, label=label, catalog=catalog, lock=lock)
            yield lease

    def publish(self, lease, manifest, deadline):
        staging = lease["work"]
        manifest.update(version=1, identity=lease["identity"], label=lease["label"], catalog=lease["catalog"])
        payload = canonical(manifest)
        with (staging / "manifest.json").open("x") as file:
            file.write(payload)
            file.flush()
            os.fsync(file.fileno())
        sync_directory(staging)
        with (self.root / "quota.lock").open("a+b") as quota:
            lock_until(quota, fcntl.LOCK_EX, deadline)
            occupied = 0
            for directory in self.root.iterdir():
                if directory.is_dir() and re.fullmatch(r"[0-9a-f]{64}", directory.name):
                    saved = json.loads((directory / "manifest.json").read_text())
                    occupied += saved["bytes"]
            if occupied + manifest["bytes"] > self.capacity:
                raise ValueError("GPU snapshot cache is full; release an unused generation")
            os.rename(staging, lease["destination"])
            sync_directory(self.root)
        lease["manifest"] = manifest
        fcntl.flock(lease["lock"], fcntl.LOCK_SH)
        return lease["destination"]


def main():
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--drop", help="Drop this snapshot ID; fail while it is in use")
    args = parser.parse_args()
    root = private_directory(args.root)
    if args.drop:
        if not re.fullmatch(r"[0-9a-f]{64}", args.drop):
            parser.error("--drop requires a snapshot ID printed by the list command")
        with (root / (args.drop + ".lock")).open("a+b") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            # Use the same order as publication: generation lock, then quota.
            with (root / "quota.lock").open("a+b") as quota:
                fcntl.flock(quota, fcntl.LOCK_EX)
                shutil.rmtree(root / args.drop)
                sync_directory(root)
        print(json.dumps({"dropped": args.drop}))
    else:
        for path in sorted(root.glob("*/manifest.json")):
            if not re.fullmatch(r"[0-9a-f]{64}", path.parent.name):
                continue
            with (root / (path.parent.name + ".lock")).open("a+b") as lock:
                fcntl.flock(lock, fcntl.LOCK_SH)
                if path.exists():
                    value = json.loads(path.read_text())
                    print(json.dumps(dict(id=path.parent.name, label=value["label"],
                        identity=value["identity"], snapshot=value["snapshot"], bytes=value["bytes"])))


if __name__ == "__main__":
    main()
