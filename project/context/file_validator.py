import os
from pathlib import Path
from .base_validator import BaseValidator

class FileValidator(BaseValidator):

    def create_data_source(self, data_source_name, data_source_path):
        data_source_name="file_" + data_source_name
        try:
            data_source = self.context.data_sources.add_pandas_filesystem(
                name=data_source_name,
                base_directory=data_source_path
            )
        except:
            data_source = self.context.data_sources.get(data_source_name)
        return data_source
    
    def create_data_asset(self, data_source, data_asset_name, asset_type):
        try:
            if asset_type == "csv":
                data_asset = data_source.add_csv_asset(name=data_asset_name)
            elif asset_type == "parquet":
                data_asset = data_source.add_parquet_asset(name=data_asset_name)
            else:
                raise ValueError(f"不支持的 asset_type 类型：{asset_type}")
        except:
            data_asset = data_source.get_asset(data_asset_name)
        return data_asset
    
    def create_batch_definition(self, data_asset, batch_definition_name, batch_path):
        try:
            batch_definition = data_asset.add_batch_definition_path(
                name=batch_definition_name,
                path=batch_path
            )
        except:
            batch_definition = data_asset.get_batch_definition(batch_definition_name)
        return batch_definition
    
    def validate_data(self,asset_type,data_source_path, batch_path, expectations):
        data_source_path = os.path.abspath(data_source_path)
        data_asset_name = Path(batch_path).stem
        names = self.generate_unique_names(data_asset_name)
        names['batch_definition_name'] = f"{data_asset_name}_batch"
        data_source = self.create_data_source(names['data_source_name'], data_source_path)
        data_asset = self.create_data_asset(data_source, names['data_asset_name'], asset_type)
        batch_definition = self.create_batch_definition(data_asset, names['batch_definition_name'], batch_path)
        suite = self.create_suite(names['suite_name'], expectations)
        validation_definition = self.create_validation_definition(
            batch_definition, suite, names['validation_definition_name']
        )
        checkpoint = self.create_checkpoint(names['checkpoint_name'], validation_definition)
        results = checkpoint.run()

        # self.build_and_open_docs(names['site_name'])
        self.context.build_data_docs()
        return results


def validate_file_data(context, data_source_path, asset_type, batch_path, expectations):
    validator = FileValidator(context)
    return validator.validate_data(asset_type, data_source_path, batch_path,expectations)