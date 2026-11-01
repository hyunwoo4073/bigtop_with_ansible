#!/usr/bin/env python3
"""Step 55 read-only VM inventory probe; deliberately excludes credentials."""
import json
import os
import platform
import shutil
import socket
import subprocess
import xml.etree.ElementTree as ET
from pathlib import Path

SERVICES = (
    "trino", "hive-metastore", "hive-metastore-server", "hive-server2",
    "postgresql", "docker", "prometheus", "grafana-server",
    "hadoop-hdfs-namenode", "hadoop-hdfs-datanode",
    "hadoop-yarn-resourcemanager", "hadoop-yarn-nodemanager",
)
BINARIES = (
    "java", "spark-submit", "hdfs", "yarn", "trino", "trino-cli",
    "psql", "pg_isready", "docker", "beeline", "hive", "ss",
)
PORTS = (5432, 8080, 8081, 8088, 8181, 8182, 9000, 9083, 9090, 3000, 8585)
XML_WHITELIST = {
    "fs.defaultFS", "hive.metastore.uris", "hive.metastore.warehouse.dir"
}


def run(args, timeout=4):
    try:
        p = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                           text=True, timeout=timeout, check=False)
        return {"rc": p.returncode, "output": (p.stdout or p.stderr).strip()[:400]}
    except (OSError, subprocess.TimeoutExpired) as e:
        return {"rc": None, "output": type(e).__name__}


def mem_kib():
    out = {}
    try:
        for line in Path("/proc/meminfo").read_text().splitlines():
            key, _, val = line.partition(":")
            if key in ("MemTotal", "MemAvailable", "SwapTotal", "SwapFree"):
                out[key + "_kib"] = int(val.split()[0])
    except OSError:
        pass
    return out


def xml_settings():
    out = {}
    for path in ("/etc/hadoop/conf/core-site.xml", "/etc/hive/conf/hive-site.xml",
                 "/etc/spark/conf/hive-site.xml", "/etc/hadoop/conf/hdfs-site.xml"):
        p = Path(path)
        values = {}
        try:
            if not p.is_file():
                continue
            root = ET.parse(path).getroot()
            for prop in root.findall("property"):
                name = prop.findtext("name")
                if name in XML_WHITELIST:
                    values[name] = (prop.findtext("value") or "").strip()[:250]
            out[path] = values
        except (ET.ParseError, OSError, PermissionError):
            out[path] = {"parse": "unavailable"}
    return out


def local_ports():
    r = run(["ss", "-H", "-lnt"], 4)
    if r["rc"] is not None and r["rc"] != 0:
        return {"probe": "unavailable"}
    # ss output can be many lines; request full output rather than clipped version.
    try:
        proc = subprocess.run(["ss", "-H", "-lnt"], capture_output=True,
                              text=True, timeout=4, check=False)
        tokens = [line.split()[3] for line in proc.stdout.splitlines() if len(line.split()) > 3]
        return {str(port): any(t.rsplit(":", 1)[-1] == str(port) for t in tokens)
                for port in PORTS}
    except (OSError, subprocess.TimeoutExpired):
        return {"probe": "unavailable"}


def safe_path_exists(path):
    """Report inaccessible paths without mistaking them for nonexistent ones."""
    try:
        return Path(path).exists()
    except PermissionError:
        return "permission_denied"
    except OSError:
        return "unavailable"


def main():
    binaries = {name: bool(shutil.which(name)) for name in BINARIES}
    java = run(["java", "-version"])
    java_line = java["output"].splitlines()[0] if java["output"] else "unavailable"
    services = {}
    for name in SERVICES:
        r = run(["systemctl", "is-active", name])
        services[name] = r["output"].splitlines()[0] if r["output"] else "unknown"
    jdk_candidates = []
    for base in (Path("/usr/lib/jvm"), Path("/opt")):
        try:
            if not base.is_dir():
                continue
            for candidate in base.iterdir():
                if candidate.is_dir() and (candidate / "bin/java").exists():
                    jdk_candidates.append(str(candidate))
        except OSError:
            # Restricted directories are not proof that Java is absent.
            continue
    stat_paths = (
        "/opt/bigtop-iceberg/iceberg-spark-runtime-3.5_2.12-1.10.1.jar",
        "/etc/trino/config.properties", "/etc/trino/catalog",
        "/etc/hadoop/conf/core-site.xml", "/etc/hadoop/conf/hdfs-site.xml",
        "/etc/hive/conf/hive-site.xml", "/etc/spark/conf/hive-site.xml",
        "/opt/bigtop-governance/openmetadata",
    )
    report = {
        "host": socket.gethostname(),
        "os": platform.platform(),
        "cpu_count": os.cpu_count(),
        "memory": mem_kib(),
        "java_version": java_line,
        "java_candidates": sorted(jdk_candidates),
        "binaries_available": binaries,
        "services": services,
        "listening_ports": local_ports(),
        "paths_exist": {p: safe_path_exists(p) for p in stat_paths},
        "nonsecret_hadoop_hive_settings": xml_settings(),
    }
    print(json.dumps(report, sort_keys=True, ensure_ascii=False))


if __name__ == "__main__":
    main()
