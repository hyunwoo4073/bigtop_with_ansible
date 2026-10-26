import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / 'roles/web_console_metadata/files/sync_metadata.py'
spec = importlib.util.spec_from_file_location('sync_metadata', SCRIPT)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

class TestMetadata(unittest.TestCase):
    def test_inventory_and_service_catalog(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root/'inventory.json').write_text(json.dumps({
                'cluster_slug':'lab','hosts':[
                    {'name':'master','address':'192.0.2.10','groups':['master']},
                    {'name':'worker1','address':'192.0.2.11','groups':['workers']}] }))
            (root/'app.py').write_text("COMPONENTS = [{'group':'HDFS','component':'NameNode','host':'master','service':'nn'}]\n")
            slug, hosts = module.read_inventory(root/'inventory.json')
            services = module.read_services(root/'app.py', hosts)
            self.assertEqual(slug, 'lab')
            self.assertEqual(len(hosts), 2)
            self.assertEqual(len(services), 1)
    def test_unknown_host_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'app.py'
            path.write_text("COMPONENTS = [{'group':'HDFS','component':'NameNode','host':'nonexistent','service':'nn'}]\n")
            with self.assertRaises(ValueError):
                module.read_services(path, {'master':{}})
    def test_dynamic_registry_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'app.py'
            path.write_text('COMPONENTS = get_components()\n')
            with self.assertRaises(ValueError):
                module.read_services(path, {'master':{}})
    def test_duplicate_host_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'inv.json'
            path.write_text(json.dumps({'cluster_slug':'lab', 'hosts':[
                {'name':'master','address':'a','groups':[]},
                {'name':'master','address':'b','groups':[]}]}))
            with self.assertRaises(ValueError):
                module.read_inventory(path)

if __name__ == '__main__':
    unittest.main()
