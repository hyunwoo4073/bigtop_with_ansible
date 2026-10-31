#!/usr/bin/env python3
"""Fail fast when an Iceberg Spark runtime JAR requires newer than Java 11."""
import pathlib
import struct
import sys
import zipfile


def check(path):
    p = pathlib.Path(path)
    if not p.is_file() or p.stat().st_size < 10_000_000:
        raise ValueError(f"Missing/too-small Iceberg runtime JAR: {p}")
    with zipfile.ZipFile(p) as z:
        names = z.namelist()
        expected = "org/apache/iceberg/spark/SparkCatalog.class"
        if expected not in names:
            raise ValueError(f"Not the expected Spark Iceberg runtime: missing {expected}")
        checked = 0
        max_major = 0
        for name in names:
            if not name.startswith("org/apache/iceberg/") or not name.endswith(".class"):
                continue
            with z.open(name) as f:
                h = f.read(8)
            if len(h) < 8 or h[:4] != b"\xca\xfe\xba\xbe":
                raise ValueError(f"Corrupt class header: {name}")
            major = struct.unpack(">H", h[6:8])[0]
            if major > 55:
                raise ValueError(f"{name}: class file version {major}; Java 11 supports at most 55")
            max_major = max(max_major, major)
            checked += 1
        print(f"ICEBERG JAVA11 PREFLIGHT PASS: {checked} Iceberg classes checked, max major={max_major}, file={p}")


if __name__ == "__main__":
    try:
        if len(sys.argv) != 2:
            raise ValueError("Usage: 53-check-iceberg-java11.py <iceberg-runtime.jar>")
        check(sys.argv[1])
    except (OSError, zipfile.BadZipFile, ValueError) as e:
        print(f"ICEBERG JAVA11 PREFLIGHT FAIL: {e}", file=sys.stderr)
        sys.exit(1)
