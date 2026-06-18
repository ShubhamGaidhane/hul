import unittest
from unittest.mock import MagicMock, patch
import sys
import os

# Mock azure modules before importing adf_scanner
sys.modules["azure"] = MagicMock()
sys.modules["azure.identity"] = MagicMock()
sys.modules["azure.mgmt"] = MagicMock()
sys.modules["azure.mgmt.datafactory"] = MagicMock()
sys.modules["azure.mgmt.datafactory.models"] = MagicMock()

from adf_scanner import UnifiedADFScanner

class TestADFScanner(unittest.TestCase):
    def setUp(self):
        self.subscription_id = "test-sub"
        self.tenant_id = "test-tenant"
        self.client_id = "test-client"
        self.client_secret = "test-secret"
        self.rg_name = "test-rg"
        self.factory_name = "test-factory"

        self.mock_client = MagicMock()
        with patch('azure.identity.ClientSecretCredential'):
            with patch('azure.mgmt.datafactory.DataFactoryManagementClient', return_value=self.mock_client):
                self.scanner = UnifiedADFScanner(
                    self.subscription_id,
                    self.tenant_id,
                    self.client_id,
                    self.client_secret
                )
                self.scanner.client = self.mock_client

    def test_collect_pipeline_insights(self):
        act1 = MagicMock()
        act1.name = 'act1'
        act1.type = 'Copy'
        act1.inputs = []
        act1.outputs = []
        act1.serialize.return_value = {'name': 'act1', 'type': 'Copy', 'typeProperties': {}}

        pipe_detail = MagicMock()
        pipe_detail.name = 'test_pipe'
        pipe_detail.activities = [act1]
        pipe_detail.parameters = {}
        pipe_detail.variables = {}
        pipe_detail.as_dict.return_value = {'name': 'test_pipe'}

        self.mock_client.pipelines.list_by_factory.return_value = [pipe_detail]

        self.scanner.collect_pipeline_insights(self.rg_name, self.factory_name)
        self.assertEqual(len(self.scanner.catalog['pipelines']), 1)

    def test_collect_datasets(self):
        ds_detail = MagicMock()
        ds_detail.name = 'ds1'
        ds_detail.as_dict.return_value = {
            'properties': {
                'type': 'DelimitedText',
                'linkedServiceName': {'referenceName': 'ls1'},
                'typeProperties': {'columnDelimiter': ','}
            }
        }

        self.mock_client.datasets.list_by_factory.return_value = [ds_detail]

        self.scanner.collect_datasets(self.rg_name, self.factory_name)
        self.assertEqual(len(self.scanner.catalog['datasets']), 1)
        self.assertEqual(self.scanner.catalog['datasets'][0]['delimiter'], ',')

    def test_collect_triggers(self):
        tr_detail = MagicMock()
        tr_detail.name = 'tr1'
        tr_detail.serialize.return_value = {
            'properties': {
                'type': 'ScheduleTrigger',
                'runtimeState': 'Started',
                'pipelines': [{'pipelineReference': {'referenceName': 'p1'}}],
                'typeProperties': {
                    'recurrence': {'startTime': '2023-01-01T00:00:00Z'}
                }
            }
        }

        self.mock_client.triggers.list_by_factory.return_value = [tr_detail]

        self.scanner.collect_trigger_info(self.rg_name, self.factory_name)
        self.assertEqual(len(self.scanner.catalog['triggers']), 1)
        self.assertEqual(self.scanner.catalog['triggers'][0]['trigger_time'], '2023-01-01T00:00:00Z')

if __name__ == '__main__':
    unittest.main()
