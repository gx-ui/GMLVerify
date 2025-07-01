import great_expectations as gx
from context.file_validator import FileValidator
from context.database_validator import DatabaseValidator
from enum import Enum


class ValidatorType(Enum):
    """验证器类型枚举"""
    FILE = "file"
    SPARK = "spark"
    DATABASE = "database"
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"

class ValidatorFactory:   
    @staticmethod
    def create_validator(validator_type: ValidatorType, context=None, **kwargs):
        if context is None:
            context = gx.get_context(mode="file", project_root_dir="../")
        
        if validator_type == ValidatorType.FILE:
            return FileValidator(context, **kwargs)
        elif validator_type == ValidatorType.SPARK:
            return SparkValidator(context, **kwargs)
        elif validator_type in [ValidatorType.DATABASE, ValidatorType.POSTGRESQL, ValidatorType.MYSQL]:
            return DatabaseValidator(context, **kwargs)
        else:
            raise ValueError(f"不支持的验证器类型：{validator_type}")
    
    @staticmethod
    def create_file_validator(context=None):
        return ValidatorFactory.create_validator(ValidatorType.FILE, context)
    
    @staticmethod
    def create_spark_validator(context=None):

        return ValidatorFactory.create_validator(ValidatorType.SPARK, context)

def get_file_validator(context=None):
    return ValidatorFactory.create_file_validator(context)


def get_dateframe_validator(context=None):

    factory = ValidatorFactory()
    return factory.create_validator(ValidatorType.SPARK, context)


def get_database_validator(context=None, db_config=None):

    factory = ValidatorFactory()
    return factory.create_validator(ValidatorType.DATABASE, context, db_config=db_config)


def get_postgresql_validator(context=None, db_config=None):

    factory = ValidatorFactory()
    return factory.create_validator(ValidatorType.POSTGRESQL, context, db_config=db_config)


def get_mysql_validator(context=None, db_config=None):

    factory = ValidatorFactory()
    return factory.create_validator(ValidatorType.MYSQL, context, db_config=db_config)