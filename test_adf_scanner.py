import unittest
from unittest.mock import MagicMock, patch
import sys

# Mock azure modules before importing adf_scanner
mock_azure = MagicMock()
sys.modules["azure"] = mock_azure
sys.modules["azure.identity"] = MagicMock()
sys.modules["azure.mgmt"] = MagicMock()
sys.modules["azure.mgmt.datafactory"] = MagicMock()

from adf_scanner import UnifiedADFScanner

class TestADFScanner(unittest.TestCase):
    def setUp(self):
        self.subscription_id = "test-sub"
        self.rg_name = "test-rg"
        self.factory_name = "test-factory"

        # Manually inject a mock client
        self.mock_client = MagicMock()
        with patch('azure.identity.DefaultAzureCredential'):
            with patch('azure.mgmt.datafactory.DataFactoryManagementClient', return_value=self.mock_client):
                self.scanner = UnifiedADFScanner(self.subscription_id)
                self.scanner.client = self.mock_client # Ensure it uses our mock

    def test_collect_pipeline_insights(self):
        # Mock activities
        act1 = MagicMock()
        act1.name = 'act1'
        act1.type = 'Copy'
        act1.inputs = [MagicMock(reference_name='ds_in')]
        act1.outputs = [MagicMock(reference_name='ds_out')]
        act1.serialize.return_value = {'name': 'act1', 'type': 'Copy'}

        # ExecutePipelineActivity
        exec_act = MagicMock()
        exec_act.name = 'exec_pipe'
        exec_act.type = 'ExecutePipeline'
        if hasattr(exec_act, 'type_properties'):
            del exec_act.type_properties
        exec_act.pipeline.reference_name = 'sub_pipe'
        exec_act.serialize.return_value = {'name': 'exec_pipe', 'type': 'ExecutePipeline'}

        pipe_detail = MagicMock()
        pipe_detail.name = 'test_pipe'
        pipe_detail.activities = [act1, exec_act]
        pipe_detail.parameters = {}
        pipe_detail.variables = {}
        pipe_detail.as_dict.return_value = {'name': 'test_pipe'}

        self.mock_client.pipelines.list_by_factory.return_value = [pipe_detail]

        self.scanner.collect_pipeline_insights(self.rg_name, self.factory_name)

        self.assertEqual(len(self.scanner.catalog['pipelines']), 1)
        lineage = self.scanner.lineage_map['test_pipe']
        self.assertIn(("dataset", "ds_in"), lineage)
        self.assertIn(("dataset", "ds_out"), lineage)
        self.assertIn(("pipeline", "sub_pipe"), lineage)

    def test_collect_datasets(self):
        ds_detail = MagicMock()
        ds_detail.name = 'ds1'
        ds_detail.properties.type = 'AzureBlob'
        ds_detail.properties.linked_service_name.reference_name = 'ls1'
        ds_detail.as_dict.return_value = {'name': 'ds1'}

        self.mock_client.datasets.list_by_factory.return_value = [ds_detail]

        self.scanner.collect_datasets(self.rg_name, self.factory_name)
        self.assertEqual(len(self.scanner.catalog['datasets']), 1)
        self.assertEqual(self.scanner.catalog['datasets'][0]['linked_service'], 'ls1')

    def test_collect_linked_services(self):
        ls_detail = MagicMock()
        ls_detail.name = 'ls1'
        ls_detail.properties.type = 'AzureBlobStorage'
        ls_detail.serialize.return_value = {'name': 'ls1', 'properties': {'type': 'AzureBlobStorage'}}

        self.mock_client.linked_services.list_by_factory.return_value = [ls_detail]

        self.scanner.collect_linked_service_info(self.rg_name, self.factory_name)
        self.assertEqual(len(self.scanner.catalog['linked_services']), 1)

    def test_collect_triggers(self):
        tr_detail = MagicMock()
        tr_detail.name = 'tr1'
        tr_detail.properties.type = 'ScheduleTrigger'
        tr_detail.properties.runtime_state = 'Started'
        tr_detail.properties.pipelines = [MagicMock()]
        tr_detail.properties.pipelines[0].pipeline_reference.reference_name = 'p1'
        tr_detail.serialize.return_value = {'name': 'tr1', 'properties': {'type': 'ScheduleTrigger'}}

        self.mock_client.triggers.list_by_factory.return_value = [tr_detail]

        self.scanner.collect_trigger_info(self.rg_name, self.factory_name)
        self.assertEqual(len(self.scanner.catalog['triggers']), 1)

    def test_collect_integration_runtimes(self):
        ir_resource = MagicMock()
        ir_resource.name = 'ir1'

        ir_detail = MagicMock()
        ir_detail.properties.type = 'Managed'
        ir_detail.serialize.return_value = {'name': 'ir1', 'properties': {'type': 'Managed'}}

        self.mock_client.integration_runtimes.list_by_factory.return_value = [ir_resource]
        self.mock_client.integration_runtimes.get.return_value = ir_detail

        self.scanner.collect_integration_runtimes(self.rg_name, self.factory_name)
        self.assertEqual(len(self.scanner.catalog['integration_runtimes']), 1)
        self.assertEqual(self.scanner.catalog['integration_runtimes'][0]['type'], 'Managed')

if __name__ == '__main__':
    unittest.main()
