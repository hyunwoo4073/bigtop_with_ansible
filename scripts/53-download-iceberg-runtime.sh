#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
version=1.10.1
artifact="iceberg-spark-runtime-3.5_2.12-${version}.jar"
base="https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${version}"
mkdir -p vendor/iceberg
if [[ -f "vendor/iceberg/$artifact" ]]; then
  echo "Existing vendor/iceberg/$artifact; verify checksum before use:"
else
  curl --fail --location --retry 3 --connect-timeout 15 \
    --output "vendor/iceberg/${artifact}.tmp" \
    "${base}/${artifact}"
  mv "vendor/iceberg/${artifact}.tmp" "vendor/iceberg/$artifact"
fi
python3 - "vendor/iceberg/$artifact" <<'PY'
import os, sys, zipfile
p=sys.argv[1]
if os.path.getsize(p)<10000000 or not zipfile.is_zipfile(p):
    raise SystemExit("Invalid Iceberg runtime jar; check network/download")
with zipfile.ZipFile(p) as z:
    assert z.testzip() is None, "Corrupt JAR"
    assert 'org/apache/iceberg/spark/SparkCatalog.class' in z.namelist(), "Not Iceberg Spark runtime"
print("ICEBERG JAR OK", p, os.path.getsize(p), "bytes")
PY
python3 scripts/53-check-iceberg-java11.py "vendor/iceberg/$artifact"
sha256sum "vendor/iceberg/$artifact"
echo "No automatic changes to Spark global jars; use playbooks/53-iceberg-hdfs-lab.yml"
