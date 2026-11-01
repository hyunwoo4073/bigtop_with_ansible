#!/usr/bin/env python3
"""Small idempotent Spark-on-YARN write/read against the shared REST Catalog."""
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName('step56-iceberg-rest-smoke').getOrCreate()
    try:
        spark.sql('CREATE NAMESPACE IF NOT EXISTS shared.olist56')
        spark.sql('CREATE TABLE IF NOT EXISTS shared.olist56.catalog_smoke (id BIGINT, source STRING) USING iceberg')
        previous = spark.sql('SELECT id, source FROM shared.olist56.catalog_smoke').collect()
        if not previous:
            spark.sql("INSERT INTO shared.olist56.catalog_smoke VALUES (56, 'spark')")
            print('STEP56_REST_SPARK_CHANGED: inserted smoke row')
        elif len(previous) != 1 or previous[0]['id'] != 56 or previous[0]['source'] != 'spark':
            raise RuntimeError('existing smoke table has unexpected data; do not overwrite')
        rows = spark.sql('SELECT id, source FROM shared.olist56.catalog_smoke').collect()
        assert len(rows) == 1 and rows[0]['id'] == 56, rows
        print('STEP56_REST_SPARK_PASS: shared.olist56.catalog_smoke rows=1')
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
