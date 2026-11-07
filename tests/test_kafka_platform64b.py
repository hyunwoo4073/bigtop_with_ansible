import importlib.util
import json
import stat
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('step64b',ROOT/'scripts/64b-kafka-platform.py')
mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod)
QUORUM='''ClusterId: demo\nLeaderId: 2\nCurrentVoters: [{"id":1,"endpoints":["CONTROLLER://a:9093"]},\n{"id":2,"endpoints":["CONTROLLER://b:9093"]},\n{"id":3,"endpoints":["CONTROLLER://c:9093"]}]\n'''
TOPICS='''Topic: __consumer_offsets TopicId: abc PartitionCount: 1 ReplicationFactor: 3 Configs: ...\nTopic: __consumer_offsets Partition: 0 Leader: 1 Replicas: 1,2,3 Isr: 1,2,3 Elr: LastKnownElr:\nTopic: smoke TopicId: abc PartitionCount: 1 ReplicationFactor: 3 Configs:\nTopic: smoke Partition: 0 Leader: 2 Replicas: 1,2,3 Isr: 1,2 Elr: LastKnownElr:\n'''
class Tests(unittest.TestCase):
    def test_quorum_nested(self):self.assertEqual(mod.parse_quorum(QUORUM),{'leader':2,'voters':[1,2,3]})
    def test_quorum_simple(self):self.assertEqual(mod.parse_quorum('LeaderId: 1\nCurrentVoters: [1,2,3]')['leader'],1)
    def test_no_leader(self):
        with self.assertRaises(mod.Step64BError):mod.parse_quorum('LeaderId: -1\nCurrentVoters: [1,2,3]')
    def test_voters_mismatch(self):
        with self.assertRaises(mod.Step64BError):mod.parse_quorum('LeaderId: 1\nCurrentVoters: [1,2,4]')
    def test_topics(self):
        parsed=mod.parse_topic_describe(TOPICS)
        self.assertEqual(parsed['smoke']['partitions'][0]['isr'],{1,2})
    def test_topics_empty(self):self.assertEqual(mod.parse_topic_describe(''),{})
    def test_topics_garbage(self):
        with self.assertRaises(mod.Step64BError):mod.parse_topic_describe('not Kafka output')
    def test_inspect(self):
        with patch.object(mod,'run_cli',side_effect=[QUORUM,TOPICS]),patch.object(mod,'tcp_up',return_value=1):s=mod.inspect(mod.DEFAULT_BOOTSTRAP)
        self.assertEqual(s['topic_count'],2);self.assertEqual(s['under_replicated'],1)
    def test_metrics(self):
        with patch.object(mod,'run_cli',side_effect=[QUORUM,TOPICS]),patch.object(mod,'tcp_up',return_value=1):s=mod.inspect(mod.DEFAULT_BOOTSTRAP)
        self.assertIn('bigtop_kafka64_under_replicated_partitions 1',mod.prometheus_content(s,100))
    def test_failure_metrics(self):
        self.assertNotIn('bigtop_kafka64_under_replicated_partitions',mod.prometheus_content(None,100))
    def test_atomic_write(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'metrics.prom';mod.atomic_write(p,'test\n')
            self.assertEqual(p.read_text(),'test\n');self.assertEqual(stat.S_IMODE(p.stat().st_mode),0o644)
    def test_atomic_missing_dir(self):
        with tempfile.TemporaryDirectory() as d:
            with self.assertRaises(mod.Step64BError):mod.atomic_write(Path(d)/'none'/'a.prom','ok')
    def test_om_skip_internal(self):
        state={'topics':{'__consumer_offsets':{},'demo':{'partitions':{0:{}},'rf':3}}}
        calls=[]
        def mock_http(method,path,payload=None,token=None,base=None):
            calls.append((method,path,payload))
            if method=='GET' and path.startswith('services/'):return 200,{'serviceType':'Kafka'}
            if method=='GET':return 404,None
            return 201,{'name':'demo'}
        with patch.object(mod,'http',side_effect=mock_http):self.assertEqual(mod.sync_openmetadata(state,'token',mod.DEFAULT_OM),1)
        self.assertNotIn('__consumer_offsets',str(calls))
    def test_om_create(self):
        state={'topics':{'demo':{'partitions':{0:{}},'rf':3}}}
        def mock_http(method,path,payload=None,token=None,base=None):return (404,None) if method=='GET' else (201,{'name':payload['name']})
        with patch.object(mod,'http',side_effect=mock_http) as mocker:
            self.assertEqual(mod.sync_openmetadata(state,'token',mod.DEFAULT_OM),1)
            self.assertEqual(len([c for c in mocker.call_args_list if c.args[0]=='POST']),2)
    def test_om_token_permissions(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'token';p.write_text('secret');p.chmod(0o644)
            with self.assertRaises(mod.Step64BError):mod.read_token(p)
    def test_dashboard(self):
        s=(ROOT/'roles/kafka_platform64b/templates/kafka-kraft64-platform.json.j2').read_text()
        d=json.loads(s.replace('{{ kafka_platform64b_datasource_uid }}','prometheus'))
        self.assertEqual(d['uid'],'bigtop-kafka-kraft64');self.assertEqual(len(d['panels']),9)
    def test_yaml(self):
        import yaml
        for p in list((ROOT/'roles/kafka_platform64b').rglob('*.yml'))+[ROOT/'playbooks/64b-kafka-platform-integration.yml']:
            self.assertIsInstance(yaml.safe_load(p.read_text()),(dict,list))
    def test_playbook_mode_default_safe_for_outer_when(self):
        import yaml
        pb = yaml.safe_load((ROOT/'playbooks/64b-kafka-platform-integration.yml').read_text())
        tasks = pb[0]['tasks']
        conditions = [task['when'] for task in tasks if 'when' in task]
        self.assertEqual(len(conditions), 2)
        self.assertTrue(all("kafka_platform64b_mode | default('plan')" in str(cond) for cond in conditions))
        self.assertTrue(any("'apply'" in cond for cond in conditions))
        self.assertTrue(any("'verify'" in cond for cond in conditions))
    def test_never_restart_or_reformat(self):
        task_text='\n'.join(p.read_text() for p in (ROOT/'roles/kafka_platform64b/tasks').glob('*.yml'))
        self.assertNotIn('state: restarted',task_text);self.assertNotIn('kafka-storage.sh',task_text)
if __name__=='__main__': unittest.main()
