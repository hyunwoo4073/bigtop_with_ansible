"""Run without Spark, Hadoop or Ansible: python3 -m unittest discover ..."""
import argparse
import ast
import pathlib
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
TARGET = ROOT / "roles" / "iceberg_spark_lab" / "files" / "iceberg_hdfs_smoke.py"


def load_functions():
    # Exercise standalone guard/idempotence functions without installing PySpark.
    module = ast.parse(TARGET.read_text(encoding="utf-8"))
    items = [n for n in module.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name in ("guard_args", "smoke_row_ids", "should_insert")]
    ns = {"re": __import__("re"), "CATALOG_RE": __import__("re").compile(r"^[a-z][a-z0-9_]{0,20}$"), "HDFS_PREFIX": "hdfs://master:9000/data/olist/"}
    exec(compile(ast.Module(body=items, type_ignores=[]), str(TARGET), "exec"), ns)
    return ns


N = load_functions()


class TestGuard(unittest.TestCase):
    def setUp(self):
        self.a = argparse.Namespace(catalog="lab", source="hdfs://master:9000/data/olist/warehouse/v3/mart/daily_category_sales", approval="hdfs://master:9000/data/olist/published/v3/_APPROVAL.json", proof="hdfs://master:9000/data/olist/iceberg-validation/step53")

    def test_valid(self):
        N["guard_args"](self.a)

    def test_invalid_catalog(self):
        self.a.catalog = "lab; DROP TABLE"
        with self.assertRaises(ValueError): N["guard_args"](self.a)

    def test_nonapproved_version(self):
        self.a.source = self.a.source.replace("/v3/", "/v2/")
        with self.assertRaises(ValueError): N["guard_args"](self.a)

    def test_wrong_mart(self):
        self.a.source = self.a.source.replace("daily_category_sales", "orders")
        with self.assertRaises(ValueError): N["guard_args"](self.a)

    def test_wrong_approval(self):
        self.a.approval = self.a.approval.replace("/v3/", "/v1/")
        with self.assertRaises(ValueError): N["guard_args"](self.a)

    def test_wrong_proof(self):
        self.a.proof = self.a.proof.replace("step53", "step52")
        with self.assertRaises(ValueError): N["guard_args"](self.a)


class TestIdempotence(unittest.TestCase):
    def test_create_first(self): self.assertTrue(N["should_insert"]([], 1))
    def test_create_second(self): self.assertTrue(N["should_insert"]([1], 2))
    def test_no_duplicate_first(self): self.assertFalse(N["should_insert"]([1], 1))
    def test_no_duplicate_second(self): self.assertFalse(N["should_insert"]([1, 2], 2))
    def test_reject_rogue_members(self):
        with self.assertRaises(ValueError): N["should_insert"]([3], 2)
    def test_reject_extra_rows(self):
        with self.assertRaises(ValueError): N["should_insert"]([1, 2, 3], 2)
    def test_sort(self):
        data=[argparse.Namespace(id=2), argparse.Namespace(id=1)]
        self.assertEqual(N["smoke_row_ids"](data), [1, 2])


class TestPackage(unittest.TestCase):
    def test_readme(self):
        self.assertTrue((ROOT / 'docs/53-iceberg-apply.md').is_file())
    def test_runtime_separate(self):
        role=(ROOT/'roles/iceberg_spark_lab/tasks/main.yml').read_text()
        self.assertIn('spark-submit', role)
        self.assertIn('--jars', role)
        self.assertNotIn('/usr/lib/spark/jars/', role)
        self.assertIn('hdfs://{{ iceberg_spark_lab_hdfs_address }}', role)
    def test_lab_paths_and_read_only_mart(self):
        txt=TARGET.read_text()
        self.assertIn('source = spark.read.parquet(args.source)', txt)
        self.assertIn('source.coalesce(2).writeTo(mart).using("iceberg").create()',txt)
        self.assertIn('spark.read.option("snapshot-id", str(first_id))', txt)
    def test_no_global_deploy(self):
        txt=(ROOT/'roles/iceberg_spark_lab/defaults/main.yml').read_text()
        self.assertIn('iceberg_spark_lab_run_smoke: false', txt)


class TestJava11Compatibility(unittest.TestCase):
    def test_pinned_version(self):
        defaults = (ROOT / "roles/iceberg_spark_lab/defaults/main.yml").read_text()
        tasks = (ROOT / "roles/iceberg_spark_lab/tasks/main.yml").read_text()
        dl = (ROOT / "scripts/53-download-iceberg-runtime.sh").read_text()
        self.assertIn("iceberg_spark_lab_version: '1.10.1'", defaults)
        self.assertIn("iceberg_spark_lab_version == '1.10.1'", tasks)
        self.assertIn("version=1.10.1", dl)
        self.assertIn("53-check-iceberg-java11.py", tasks)

    def test_bytecode_guard_present(self):
        t = (ROOT / "scripts/53-check-iceberg-java11.py").read_text()
        self.assertIn("major > 55", t)
        self.assertIn("SparkCatalog.class", t)
