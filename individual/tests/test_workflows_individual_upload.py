from django.test import TestCase
from core.test_helpers import create_test_interactive_user
from individual.models import (
    IndividualDataSource,
    IndividualDataSourceUpload,
    IndividualDataUploadRecords,
)
from individual.workflows.base_individual_upload import process_import_individuals_workflow
from unittest.mock import patch
import uuid


class ProcessImportIndividualsWorkflowTest(TestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        # Patch validate_dataframe_headers as it is already tested separately
        cls.patcher = patch(
            "individual.workflows.utils.BasePythonWorkflowExecutor.validate_dataframe_headers",
            lambda self: None
        )
        cls.patcher.start()

    @classmethod
    def tearDownClass(cls):
        cls.patcher.stop()
        super().tearDownClass()

    def setUp(self):
        self.user = create_test_interactive_user(username="admin")
        self.user_uuid = self.user.id

        self.upload = IndividualDataSourceUpload(
            source_name='csv',
            source_type='upload',
            status="PENDING",
        )
        self.upload.save(user=self.user)
        self.upload_uuid = self.upload.id

        upload_record = IndividualDataUploadRecords(
            data_upload=self.upload,
            workflow='my workflow',
            json_ext={"group_aggregation_column": None}
        )
        upload_record.save(user=self.user.user)

        self.valid_data_source = IndividualDataSource(
            upload_id=self.upload_uuid,
            json_ext={"first_name": "John", "last_name": "Doe", "dob": "1980-01-01"}
        )
        self.valid_data_source.save(user=self.user)

        self.invalid_data_source = IndividualDataSource(
            upload_id=self.upload_uuid,
            json_ext={"first_name": "Jane Workflow"}
        )
        self.invalid_data_source.save(user=self.user)

    @patch('individual.services.IndividualConfig.enable_maker_checker_for_individual_upload', False)
    def test_process_import_individuals_workflow_successful_execution(self):
        process_import_individuals_workflow(self.user_uuid, self.upload_uuid)

        upload = IndividualDataSourceUpload.objects.get(id=self.upload_uuid)

        # Check that the status is 'FAIL' due to missing fields in one entry
        self.assertEqual(upload.status, "FAIL")
        self.assertIsNotNone(upload.error)
        errors = upload.error['errors']
        self.assertIn("Invalid entries", errors['error'])

        # Check that the correct failing entries are logged in the error field
        for key in [
            "failing_entries_last_name", "failing_entries_dob"
        ]:
            self.assertIn(key, errors)
            self.assertIn(str(self.invalid_data_source.id), errors[key])
            self.assertNotIn(str(self.valid_data_source.id), errors[key])

        # individual_id should not be assigned for any data sources
        data_entries = IndividualDataSource.objects.filter(upload_id=self.upload_uuid)
        for entry in data_entries:
            self.assertIsNone(entry.individual_id)

    @patch('individual.services.IndividualConfig.enable_maker_checker_for_individual_upload', False)
    def test_process_import_individuals_workflow_with_all_valid_entries(self):
        # Update invalid entry in IndividualDataSource to valid data
        IndividualDataSource.objects.filter(
                upload_id=self.upload_uuid, json_ext={"first_name": "Jane Workflow"}
        ).update(
            json_ext={
                "first_name": "Jane Workflow", "last_name": "Doe", "dob": "1982-01-01"
            }
        )

        process_import_individuals_workflow(self.user_uuid, self.upload_uuid)

        upload = IndividualDataSourceUpload.objects.get(id=self.upload_uuid)

        self.assertEqual(upload.status, "SUCCESS")
        self.assertEqual(upload.error, {})

        # Verify that individual IDs have been assigned to data entries in IndividualDataSource
        data_entries = IndividualDataSource.objects.filter(upload_id=self.upload_uuid)
        for entry in data_entries:
            self.assertIsNotNone(entry.individual_id)

    @patch('individual.services.IndividualConfig.enable_maker_checker_for_individual_upload', True)
    def test_process_import_individuals_workflow_with_all_valid_entries_with_maker_checker(self):
        # Update invalid entry in IndividualDataSource to valid data
        IndividualDataSource.objects.filter(
                upload_id=self.upload_uuid, json_ext={"first_name": "Jane Workflow"}
        ).update(
            json_ext={
                "first_name": "Jane Workflow", "last_name": "Doe", "dob": "1982-01-01"
            }
        )

        process_import_individuals_workflow(self.user_uuid, self.upload_uuid)

        upload = IndividualDataSourceUpload.objects.get(id=self.upload_uuid)

        self.assertEqual(upload.status, "WAITING_FOR_VERIFICATION")
        self.assertEqual(upload.error, {})

        # Verify that individual IDs not yet assigned to data entries in IndividualDataSource
        data_entries = IndividualDataSource.objects.filter(upload_id=self.upload_uuid)
        for entry in data_entries:
            self.assertIsNone(entry.individual_id)
