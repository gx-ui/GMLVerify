import great_expectations as gx
import great_expectations.expectations as gxe
import pandas as pd
# from pyspark.sql import SparkSession
from context.dataframe_validator import DataFrameValidator
from context.file_validator import FileValidator
def example_dataframe_validator():
    # 创建上下文
    context = gx.get_context(mode="file", project_root_dir="./")
    # 创建Spark验证器
    dataframe_validator = DataFrameValidator(context)
    # 定义期望
    expectations = [
        gxe.ExpectColumnValuesToMatchRegex(
            column="score",
            regex=r"^(?:[1-9]\d*|0*[1-9]\d*)$"
        ),
        gxe.ExpectColumnValueLengthsToBeBetween(
            column="body",
            min_value=1,
            max_value=1000
        ),
        gxe.ExpectColumnValuesToBeInSet(
            column="subreddit",
            value_set=["technology", "politics", "science", "machinelearning"]
        )
    ]
    # spark = SparkSession.builder \
    #     .appName("Reddit数据简单预处理以及演示") \
    #     .master("local[*]") \
    #     .config("spark.sql.shuffle.partitions", 10) \
    #     .config("spark.sql.adaptive.enabled", "false") \
    #     .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
    #     .getOrCreate()
    data_path = "./data/tax_data.csv"

    df = pd.read_csv(data_path)

    results = dataframe_validator.validate_data(
        df_type="pandas",
        expectations=expectations,
        dataframe_batch={"dataframe": df}
    )
    # spark.stop()
    return results

def example_file_validator():
    context = gx.get_context(mode="file", project_root_dir="./")
    # 创建Spark验证器
    file_validator = FileValidator(context)

    expectations = [
        gxe.ExpectColumnValuesToBeBetween(
            column="passenger_count",
            min_value=1,
            max_value=1000
        ),  
    ]
    result=file_validator.validate_data("csv","./data","tax_data.csv",expectations)

    return result


if __name__ == '__main__':

     result=example_file_validator()
     print(f"验证结果: {result}\n")



