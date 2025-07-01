# Great Expectations
## 项目结构
greatexpectations/
├── context/                    # 核心验证器模块
│   ├── base_validator.py       # 抽象基类
│   ├── dataframe_validator.py  # DataFrame验证器（目前支持pandas,pyspark）
│   ├── file_validator.py       # 文件验证器（目前支持csv,parquet）
│   ├── database_validator.py   # 数据库验证器（目前支持postgresql,mysql）
│   └── validator_factory.py    # 验证器工厂
├── gx/                         # Great Expectations 配置
│   ├── great_expectations.yml  # 主配置文件
│   ├── expectations/           # 期望定义
│   ├── checkpoints/           # 检查点配置
│   ├── result/                # 验证结果
│   └── plugins/               # 自定义插件
│       └── custom_data_docs/  
├── data/                      # 示例数据
│   ├── filtered/              
│   └── reddit/               # Reddit 数据集
│   └── tax_data.csv           # 示例CSV文件
├── test/                      # 测试和示例
│   └── example_usage.py       # 使用示例
└── README.md                  # 项目说明
```

## 快速开始

### 1. 环境准备

确保您的环境满足以下要求：
- Python 3.9.0+
- Great Expectations 
- PySpark 3.0 (可选，用于 Spark 数据验证)

### 2. 安装依赖

```bash
pip install great-expectations
pip install pandas
pip install pyspark  # 可选
```

### 3. 基本使用示例
## 核心组件

### 验证器类型

1. **DataFrameValidator**: 用于 Pandas 和 Spark DataFrame 验证
2. **FileValidator**: 用于文件数据验证
3. **DatabaseValidator**: 用于数据库数据验证


## 配置说明

### Great Expectations 配置

主要配置文件 `gx/great_expectations.yml` 包含：

- **数据源配置**: 支持 Pandas 和 Spark 数据源
- **存储配置**: 期望、验证结果和检查点的存储位置
- **数据文档站点**: 多个站点配置，包括中文站点
- **插件目录**: 自定义插件和扩展

## 示例数据

项目包含示例数据用于测试和演示：
- **Reddit 数据集**: 包含帖子评分、内容和子版块信息
- **过滤数据**: 预处理后的 Parquet 格式数据

### 自定义期望（举例）
```python
import great_expectations.expectations as gxe
gxe.ExpectColumnValuesToMatchRegex(column="score", regex=r"^[0-9]+$")

# 长度范围验证
gxe.ExpectColumnValueLengthsToBeBetween(column="body", min_value=1, max_value=1000)

# 值集合验证
gxe.ExpectColumnValuesToBeInSet(column="category", value_set=["A", "B", "C"])

# 空值检查
gxe.ExpectColumnValuesToNotBeNull(column="id")

# 数值范围验证
gxe.ExpectColumnValuesToBeBetween(column="age", min_value=0, max_value=120)
```

可以创建自定义的数据期望来满足特定的验证需求。

**注意**: 本项目基于 Great Expectations 1.x 版本构建，与 0.x 版本的 API 存在差异。请确保使用正确的版本和 API 调用方式。