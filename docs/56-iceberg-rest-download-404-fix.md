# Step 56 — PostgreSQL JDBC checksum URL 404 fix

The previous download script requested `postgresql-42.7.11.jar.sha256` from Maven Central. Some Maven artifacts do not publish this sidecar; the HTTP 404 was caused by the checksum request, **not** by the JDBC JAR. The official Maven Resolver documentation describes Maven Central's usual SHA-1 and MD5 checksum mechanisms, which do not guarantee a SHA-256 sidecar for every artifact.

The patched downloader pins SHA-256 for PostgreSQL JDBC 42.7.11 (`1981b31d3993c58702783c1cddf10a34e48c1f413d70ff1cb6def0a143484647`), downloads the JAR directly from Maven Central and checks the contents before storing it. A matching previously downloaded Gravitino archive is reused. The existing full verifier is unchanged.

Run on worker3:

```bash
cd ~/bigtop-cluster-ansible
unzip -o 56_iceberg_rest_catalog_fetch_404_fix.zip -d .
python3 -m unittest discover -s tests -p 'test_iceberg_rest_catalog.py' -v
make lint
python3 scripts/56-fetch-iceberg-rest-assets.py
python3 scripts/56-verify-artifacts.py vendor/iceberg-rest
```

Only after `STEP56_ARTIFACTS_VERIFIED` proceed with the existing `playbooks/56-iceberg-rest-catalog.yml -e iceberg_rest_apply=true` deployment. Do not delete `vendor/iceberg-rest/`, modify installed JDKs, or change PostgreSQL settings to solve the download 404.

Note: checksum pins verify reproducibility/integrity of artifacts but are not a substitute for confirming the trustworthiness of the initial published checksum.
