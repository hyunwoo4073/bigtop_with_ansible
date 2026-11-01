#!/usr/bin/env python3
"""Read and summarize Step 55 report; never use credentials or change services."""
import argparse
import json
from pathlib import Path


def gib(kib):
    return "%.2f GiB" % (int(kib or 0) / 1024 / 1024)


def lines(report):
    out = ["STEP 55 — Iceberg / Trino deployment preflight",
           "Status: inspection only; no Trino or catalog service installed.", ""]
    hosts = report["hosts"]
    for name in ("master", "worker1", "worker2", "worker3", "metadata"):
        if name not in hosts:
            continue
        r = hosts[name]
        m = r.get("memory", {})
        out.append("[%s] CPU=%s RAM=%s available=%s" % (
            name, r.get("cpu_count"), gib(m.get("MemTotal_kib")),
            gib(m.get("MemAvailable_kib"))))
        out.append("  Java: %s" % r.get("java_version", "unknown"))
        active = [x for x, value in r.get("services", {}).items() if value == "active"]
        out.append("  Active systemd: %s" % (", ".join(active) or "none reported"))
        ports = [p for p, used in r.get("listening_ports", {}).items() if used is True]
        out.append("  Selected listening ports: %s" % (", ".join(sorted(ports, key=int)) or "none"))
        hms = r.get("nonsecret_hadoop_hive_settings", {})
        for file, properties in sorted(hms.items()):
            if "hive.metastore.uris" in properties:
                out.append("  Hive Metastore URI (%s): %s" % (file, properties["hive.metastore.uris"]))
    out.extend(["", "Architecture decisions after reviewing this report:",
                "  1) Choose REST or existing HMS catalog; HadoopCatalog is lab-only.",
                "  2) Identify a VM with enough free RAM for Trino; avoid overloading worker3.",
                "  3) Install JDK 25 for Trino only; do not replace Spark/YARN Java 11.",
                "  4) Validate HDFS access (incl. permissions) from every future Trino node.",
                "  5) Deploy shared catalog and Trino in later steps, then cross-query Iceberg.",
                "  6) Step 53 HDFS proof: %s" % report.get("master_iceberg_step53_hdfs_proof_exists")])
    return "\n".join(out)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("report", type=Path)
    args = parser.parse_args()
    payload = json.loads(args.report.read_text(encoding="utf-8"))
    print(lines(payload))


if __name__ == "__main__":
    main()
