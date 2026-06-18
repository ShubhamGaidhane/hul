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

        # Manually inject a mock client
        self.mock_client = MagicMock()
        with patch('azure.identity.ClientSecretCredential'):
            with patch('azure.mgmt.datafactory.DataFactoryManagementClient', return_value=self.mock_client):
                self.scanner = UnifiedADFScanner(
                    self.subscription_id,
                    self.tenant_id,
                    self.client_id,
                    self.client_secret
                )
                self.scanner.client = self.mock_client # Ensure it uses our mock

    def test_collect_pipeline_insights(self):
        # Mock activities
        act1 = MagicMock()
        act1.name = 'act1'
        act1.type = 'Copy'
        act1.inputs = [MagicMock(reference_name='ds_in')]
        act1.outputs = [MagicMock(reference_name='ds_out')]
        act1.serialize.return_value = {'name': 'act1', 'type': 'Copy'}

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
        ds_detail.properties.type = 'DelimitedText'
        ds_detail.properties.linked_service_name.reference_name = 'ls1'
        # Mock type_properties with column_delimiter
        ds_detail.properties.type_properties = MagicMock()
        ds_detail.properties.type_properties.column_delimiter = ','
        ds_detail.as_dict.return_value = {'name': 'ds1'}

        self.mock_client.datasets.list_by_factory.return_value = [ds_detail]

        self.scanner.collect_datasets(self.rg_name, self.factory_name)
        self.assertEqual(len(self.scanner.catalog['datasets']), 1)
        self.assertEqual(self.scanner.catalog['datasets'][0]['delimiter'], ',')

    def test_collect_triggers(self):
        tr_detail = MagicMock()
        tr_detail.name = 'tr1'
        tr_detail.properties.type = 'ScheduleTrigger'
        tr_detail.properties.runtime_state = 'Started'
        tr_detail.properties.pipelines = [MagicMock()]
        tr_detail.properties.pipelines[0].pipeline_reference.reference_name = 'p1'
        # Mock type_properties with recurrence start_time
        tr_detail.properties.type_properties = MagicMock()
        tr_detail.properties.type_properties.recurrence.start_time = '2023-01-01T00:00:00Z'
        tr_detail.serialize.return_value = {'name': 'tr1', 'properties': {'type': 'ScheduleTrigger'}}

        self.mock_client.triggers.list_by_factory.return_value = [tr_detail]

        self.scanner.collect_trigger_info(self.rg_name, self.factory_name)
        self.assertEqual(len(self.scanner.catalog['triggers']), 1)
        self.assertEqual(self.scanner.catalog['triggers'][0]['trigger_time'], '2023-01-01T00:00:00Z')

if __name__ == '__main__':
    unittest.main()
