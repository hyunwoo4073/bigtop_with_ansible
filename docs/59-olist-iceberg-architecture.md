# 59 — Olist Iceberg Gold WAP with Step 47-quality checks

## Scope and architecture

```
Step58 shared.olist58_v3 Silver (5) + Quarantine (1) ────┐
                                                          ├─ Spark 59 quality (29 Step47 rule IDs)
Step58 shared.olist58_v3.gold_daily_category_sales ────┘      ▲
          │ exact Iceberg source rows                           │
          ▼                                                    │
Step59 shared.olist59_v3.gold_daily_category_sales            │
  main    : 1 source row technical seed ──────────────┐        │
  audit_v3: full source Gold via branch append ───────┼────────┘
                                                      │ PASS + immutable stage proof
                                                      ▼
                      explicit `CALL shared.system.fast_forward` (single Gold table)
                                                      │
                                                      ▼
                         Trino iceberg_rest.olist59_v3.gold_daily_category_sales
```

**This is a REAL Gold-only WAP exercise on Olist data, not a full multitable atomic release.**
The 58 source tables and the original approved Parquet v3 are never modified.
Stage creates an *isolated* 59 release table and audit branch. Before publish its
`main` deliberately has a single **real source row** as a technical seed because
Iceberg branches require an existing snapshot. The technical main table **must not
be offered to BI users before publish**. The complete audited Gold data exists
only on the `audit_v3` branch until explicit publish.

## Quality rules

Re-runs all **29 Step47 rule IDs** using the Step58 Iceberg Silver/Quarantine
and Step59 `audit_v3` Gold candidate. This includes counts, uniqueness, fact
referential integrity, null/amount consistency, quarantine thresholds,
raw-row conservation using the approved v3 manifest, independent Gold Mart
recomputation, and sales units. It also requires **exact bidirectional multiset
parity** (`exceptAll`) against Step58 Gold, schema equality, source snapshot
stability, and the Step46/47/52 approved proof chain plus Step58 parity proof.

The stage report contains `status`, 29 detailed checks, a snapshot ID for each
of the seven Step58 source tables, prepublish Gold main snapshot ID, Gold audit
snapshot ID, and source proof fingerprints. It is written once at:

`/data/olist/iceberg-validation/step59/v3/_STAGE_REPORT.json`

Publish loads and validates the **immutable PASS** stage report, verifies that
both 58 sources and the 59 audit snapshot have not changed, reruns quality and
exact parity, and then calls Iceberg `fast_forward` on **only the 59 Gold table**.
It verifies that main == the audited snapshot and writes once:

`/data/olist/iceberg-validation/step59/v3/_PUBLISH_REPORT.json`

It is idempotent for re-execution against *identical* proofs and snapshot IDs.
If a stage FAIL report is written, do **not** delete or overwrite it to get a
PASS. Investigate first, then use a new versioned release design.

## Boundaries

- Does **not** re-execute Raw CSV → Silver ETL (Step46 remains the data producer).
- Does **not** convert all seven Iceberg tables to WAP; 59 publishes one Gold table.
- Does **not** make seven tables atomic; this requires a coordinated release
  pointer or consumer-view gating in a later step.
- Does **not** implement an authorization gateway: `shared.olist59_v3` is a
  technical namespace until published, and direct Iceberg SQL access to the
  branch or seed main is possible for authorized internal users.
- Does **not** make HDFS proof files cryptographically immutable; existing
  cluster permission and access-control settings govern those files.
- REST Catalog branch support with Gravitino 1.3.1 **must be proven on the live
  cluster**. A successful Step54 HadoopCatalog branch alone cannot confirm it.
