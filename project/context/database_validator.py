import great_expectations as gx
from context.base_validator import BaseValidator
from typing import Dict, Any, Optional

class DatabaseValidator(BaseValidator):
    def __init__(self, context=None):
        super().__init__(context)
    def create_data_source(self, data_source_name, db_type, connection_url):
        data_source_name=db_type+"_"+data_source_name
        try:
            if db_type=='postgresql':
                data_source = self.context.data_sources.add_postgres(
                    name=data_source_name,
                    connection_string=connection_url)
            elif db_type=='mysql':
                data_source = self.context.data_sources.add_sql(
                    name=data_source_name,
                    connection_string=connection_url)
            else:
                raise ValueError(f"不支持的数据库类型：{db_type}")
        except Exception:
                data_source = self.context.data_sources.get(data_source_name)
        
        return data_source
    
    def create_data_asset(self, data_source, data_asset_name, table_name):
        try:
            data_asset = data_source.add_table_asset(table_name=table_name, name=data_asset_name)
        except:
            data_asset = data_source.get_asset(data_asset_name)
        return data_asset
    
    def create_batch_definition(self, data_asset, batch_definition_name):
        try:
            batch_definition = data_asset.add_batch_definition_whole_table(
                name=batch_definition_name
            )
        except:
            batch_definition = data_asset.get_batch_definition(batch_definition_name)
        return batch_definition
    
    def validate_data(self, db_type:str=None, table_name:str=None,connection_url:str=None,expectations:list=None):
        names = self.generate_unique_names(table_name)
        names['batch_definition_name'] = f"{table_name}_batch"
        # 创建验证流程
        data_source = self.create_data_source(names['data_source_name'], db_type, connection_url)
        data_asset = self.create_data_asset(data_source, names['data_asset_name'], table_name=table_name)
        batch_definition = self.create_batch_definition(data_asset, names['batch_definition_name'])
        suite = self.create_suite(names['suite_name'], expectations)
        validation_definition = self.create_validation_definition(batch_definition, suite, names['validation_definition_name'])
        checkpoint = self.create_checkpoint(names['checkpoint_name'], validation_definition)
        # 运行验证
        results = checkpoint.run()
        # 构建并打开文档
        self.build_and_open_docs(names['site_name'])

        return results
def validate_postgresql_data(context, connection_url, expectations,
                           table_name=None):
    validator = DatabaseValidator(context)
    return validator.validate_data(
        db_type='postgresql',
        table_name=table_name,
        connection_url=connection_url,
        expectations=expectations,
    )

def validate_mysql_data(context, connection_url, expectations,
                       table_name=None):
    validator = DatabaseValidator(context)
    return validator.validate_data(
        db_type='mysql',
        table_name=table_name,
        connection_url=connection_url,
        expectations=expectations,
    )