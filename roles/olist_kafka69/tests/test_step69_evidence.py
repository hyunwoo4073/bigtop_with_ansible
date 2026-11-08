import importlib.util
import pathlib
import unittest
from contextlib import redirect_stdout, redirect_stderr
from io import StringIO

PATH = pathlib.Path(__file__).resolve().parents[1] / 'files' / 'step69_evidence.py'
spec = importlib.util.spec_from_file_location('step69_evidence', PATH)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)


def progress(kafka=24, spark=24, batch=2):
    return {'topic': 'test', 'partitions': {'0': {'kafka_end': kafka, 'spark_committed': spark, 'lag': kafka - spark}}, 'spark_batch_id': batch}


def checkpoint(batch=2):
    return {'checkpoint': 'hdfs://master:9000/path', 'offsets_ls': f'-rw-r--r-- 1 hadoop hadoop 123 2026-11-08 12:00 /path/offsets/{batch}\n', 'commits_ls': f'-rw-r--r-- 1 hadoop hadoop 123 2026-11-08 12:00 /path/commits/{batch}\n'}


class Step69Tests(unittest.TestCase):
    def runcompare(self, *args):
        with redirect_stdout(StringIO()), redirect_stderr(StringIO()):
            return mod.compare(*args)

    def test_stable_is_not_replay_proof(self):
        self.assertEqual(0, self.runcompare(progress(), progress(), checkpoint(), checkpoint(), False, True))
        self.assertEqual(1, self.runcompare(progress(), progress(), checkpoint(), checkpoint(), True, True))

    def test_catch_up_with_input(self):
        self.assertEqual(0, self.runcompare(progress(), progress(30, 30, 3), checkpoint(), checkpoint(3), True, True))

    def test_unfinished_lag(self):
        self.assertEqual(1, self.runcompare(progress(), progress(30, 24, 2), checkpoint(), checkpoint(), True, True))

    def test_regressing_spark_offset(self):
        self.assertEqual(1, self.runcompare(progress(24, 24), progress(25, 20), checkpoint(), checkpoint(), False, False))

    def test_mismatched_checkpoint(self):
        c = checkpoint()
        a = checkpoint()
        a['checkpoint'] = 'hdfs://master:9000/other'
        self.assertEqual(1, self.runcompare(progress(), progress(), c, a, False, False))

    def test_empty_checkpoint_ls(self):
        c = checkpoint()
        c['commits_ls'] = 'Found 0 items\n'
        self.assertEqual(1, self.runcompare(progress(), progress(), checkpoint(), c, False, False))

    def test_checkpoint_filename_parsing(self):
        self.assertEqual({2, 10}, mod.checkpoint_batches('-rw-r--r-- /abc/2\n-rw-r--r-- /abc/10\n'))

if __name__ == '__main__':
    unittest.main()
