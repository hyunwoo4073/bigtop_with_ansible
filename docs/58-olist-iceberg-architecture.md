# Step 58: Olist v3 Silver/Gold Iceberg materialization

## Purpose and boundary

The Step 58 implementation **materializes already approved immutable Parquet outputs** from Step 46/51 into version-scoped Iceberg tables through the Step 56 Gravitino REST Catalog. This is the first controlled transition from HDFS Parquet directories to cataloged Silver/Gold Iceberg tables. **It does not yet replace the Raw CSV → ETL transformation implementation**: the existing Step 46 Spark transformation remains the producer of the v3 source. Future pipeline orchestration can be changed once the Iceberg write, quality gate and WAP process are proven.

```
Raw v1 CSV → Step 46 Spark ETL → immutable Parquet v3 → Step 47 DQ → Step 52 approval
                                                         │
                                          Step 58 Spark on YARN (reads only)
                                                         │
                                                         ▼
              Gravitino REST Catalog worker3:9001 / PostgreSQL worker3
                                                         │
                                                         ▼
                   shared.olist58_v3 (Iceberg metadata + HDFS Parquet files)
                                                         │
                                                         ▼
                    Trino 484 metadata:8080 / iceberg_rest.olist58_v3
```

The logical metastore and physical data are separate: Gravitino owns REST Catalog operations, PostgreSQL holds catalog registry data, and HDFS stores Iceberg metadata/manifests/data. Spark and Trino do **not** send Parquet data through Gravitino.

## Tables

| Approved v3 Parquet source | New Iceberg table (Spark) | Trino name |
|---|---|---|
| `curated/orders` | `shared.olist58_v3.silver_orders` | `iceberg_rest.olist58_v3.silver_orders` |
| `curated/order_items` | `shared.olist58_v3.silver_order_items` | `iceberg_rest.olist58_v3.silver_order_items` |
| `curated/customers` | `shared.olist58_v3.silver_customers` | `iceberg_rest.olist58_v3.silver_customers` |
| `curated/products` | `shared.olist58_v3.silver_products` | `iceberg_rest.olist58_v3.silver_products` |
| `curated/order_item_fact` | `shared.olist58_v3.silver_order_item_fact` | `iceberg_rest.olist58_v3.silver_order_item_fact` |
| `quarantine/rows` | `shared.olist58_v3.quality_quarantine_rows` | `iceberg_rest.olist58_v3.quality_quarantine_rows` |
| `mart/daily_category_sales` | `shared.olist58_v3.gold_daily_category_sales` | `iceberg_rest.olist58_v3.gold_daily_category_sales` |

The Gold table preserves the **Step 46 item-list-value definition**: only delivered orders, sum of order item price/freight, not settled payment revenue. No extra joins are introduced. 58 keeps the original column types and names.

## Correctness and recovery

1. Before Spark reads: require Step 52 approval, verify its SHA256 link to the v3 ETL manifest and the approved quality report; enforce the seven `_SUCCESS` markers and the 29 PASS checks.
2. For each Parquet source verify the manifest row count, create a **new** Iceberg table with provenance table properties only if absent, and validate its schema and row count.
3. Validate exact multiset equality with bidirectional `exceptAll` (including duplicates). These checks are shuffle-intensive on the small YARN cluster but stronger than just count or hashes.
4. After all seven pass, write a versioned immutable HDFS proof at `/data/olist/iceberg-validation/step58/v3/part-*`.
5. Ansible copies this nonsecret proof JSON to worker3 `artifacts/58-iceberg-v3-proof.json`; an independent Trino query checks all seven counts and the Gold SUM.

Failures do not overwrite existing Iceberg tables or v3 Parquet. Partial deployment may leave a subset of new Iceberg tables; rerunning verifies their provenance and exact contents before creating the missing ones. A mismatch fails closed. Concurrent runs targeting the namespace are **not supported**; ensure a single scheduled or manual invocation. The proof is version scoped and immutable.

**Not yet a published Iceberg release:** Step 52 approved the source Parquet; it does *not* automatically approve the new Iceberg tables, provide a multi-table atomic commit or move an audit branch to main. Step 59 will implement the Iceberg quality gate/WAP. Current tables live in a versioned isolated namespace, not a consumer-facing stable release namespace.

For the internal on-premises environment, existing service identities and HDFS settings are reused. Production access control, a catalog authorization policy, backups, and failure recovery remain further work.
