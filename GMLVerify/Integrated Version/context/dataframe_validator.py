import great_expectations as gx
from .base_validator import BaseValidator
import great_expectations.expectations as gxe


class DataFrameValidator(BaseValidator):
    def create_data_source(self, df_type:str="pandas",data_source_name:str=None):
        data_source_name=df_type+"_"+data_source_name
        try:
            if df_type == "spark":
               data_source = self.context.data_sources.add_spark(name=data_source_name)
            elif df_type == "pandas":
                data_source = self.context.data_sources.add_pandas(name=data_source_name)
            else:
                raise ValueError(f"不支持的数据库类型：{df_type}")
        except:
            data_source = self.context.data_sources.get(data_source_name)
        return data_source
    
    def create_data_asset(self, data_source, data_asset_name):
        try:
            data_asset = data_source.add_dataframe_asset(name=data_asset_name)
        except:
            data_asset = data_source.get_asset(data_asset_name)
        return data_asset
    
    def create_batch_definition(self, data_asset, batch_definition_name):
        try:
            batch_definition = data_asset.add_batch_definition_whole_dataframe(
                batch_definition_name
            )
        except:
            batch_definition = data_asset.get_batch_definition(batch_definition_name)
        return batch_definition
    
    def validate_data(self,df_type,dataframe_batch, expectations):
        # 生成唯一名称
        names = self.generate_unique_names()
        # 创建验证流程
        data_source = self.create_data_source(df_type,names['data_source_name'])
        data_asset = self.create_data_asset(data_source, names['data_asset_name'])
        batch_definition = self.create_batch_definition(data_asset, names['batch_definition_name'])
        suite = self.create_suite(names['suite_name'], expectations)
        validation_definition = self.create_validation_definition(
            batch_definition, suite, names['validation_definition_name']
        )
        checkpoint = self.create_checkpoint(names['checkpoint_name'], validation_definition)
        
        # 运行验证
        results = checkpoint.run(dataframe_batch)
        
        # 构建并打开文档
        self.build_and_open_docs(names['site_name'])
        
        return results
def validate_dataframe_data(context, batch_parameters, expectations):
    validator = DataFrameValidator(context)
    return validator.validate_data(expectations, batch_parameters)