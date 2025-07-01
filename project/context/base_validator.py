import great_expectations as gx
from great_expectations.checkpoint import SlackNotificationAction, UpdateDataDocsAction
import uuid
from pathlib import Path
from abc import ABC, abstractmethod
class BaseValidator(ABC):
    def __init__(self, context=None):
        self.context = context
        self.action_list = [
            # SlackNotificationAction(
            #     name="send_slack_notification_on_failed_expectations",
            #     slack_token="${validation_notification_slack_webhook}",
            #     slack_channel="${validation_notification_slack_channel}",
            #     notify_on="failure",
            #     show_failed_expectations=True,
            # ),
            UpdateDataDocsAction(
                name="update_all_data_docs",
            ),
        ]
        
        self.result_format = "COMPLETE"
        
        self.site_config = {
            "class_name": "SiteBuilder",
            "site_index_builder": {"class_name": "DefaultSiteIndexBuilder"},
            "store_backend": {
                "class_name": "TupleFilesystemStoreBackend",
                "base_directory": "result"
            },
        }
    
    @abstractmethod
    def create_data_source(self, data_source_name, **kwargs):
        pass
    
    @abstractmethod
    def create_data_asset(self, data_source, data_asset_name, **kwargs):
        pass

    @abstractmethod
    def create_batch_definition(self, data_asset, batch_definition_name, **kwargs):

        pass
    
    def create_suite(self, suite_name, expectations):
        try:
            suite = gx.ExpectationSuite(name=suite_name)
            self.context.suites.add(suite)
        except:
            self.context.suites.delete(name=suite_name)
            suite = self.context.suites.get(suite_name)
        
        for expectation in expectations:
            suite.add_expectation(expectation)
        
        return suite
    
    def create_validation_definition(self, batch_definition, suite, validation_definition_name):
        validation_definition = gx.ValidationDefinition(
            data=batch_definition, suite=suite, name=validation_definition_name
        )
        validation = self.context.validation_definitions.add(validation_definition)
        return validation
    
    def create_checkpoint(self, checkpoint_name, validation):
        try:
            checkpoint = self.context.checkpoints.get(checkpoint_name)
        except:
            checkpoint = gx.Checkpoint(
                name=checkpoint_name,
                validation_definitions=[validation],
                actions=self.action_list,
                result_format=self.result_format
            )
            checkpoint = self.context.checkpoints.add(checkpoint)
        return checkpoint
    
    def build_and_open_docs(self, site_name=None):
        if site_name is None:
            site_name = "default_site"  # 设置默认站点名称
        existing_sites = self.context.list_data_docs_sites()
        if site_name not in existing_sites:
            self.context.add_data_docs_site(site_name=site_name, site_config=self.site_config)

        self.context.build_data_docs(site_names=[site_name])
        self.context.open_data_docs(site_name=site_name)


    def generate_unique_names(self, base_name=None):
        return {
            'data_asset_name':base_name or str(uuid.uuid4()),
            'data_source_name': 'data',
            'batch_definition_name': str(uuid.uuid4()),
            'suite_name': str(uuid.uuid4()),
            'validation_definition_name': str(uuid.uuid4()),
            'checkpoint_name': str(uuid.uuid4()),
            'site_name': "my_site"
        }
    @abstractmethod
    def validate_data(self, expectations, **kwargs):
        pass