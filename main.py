from utils.bronze_silver_functions.etl_functions.bronze.bronze_class import BronzeTransformations
from utils.bronze_silver_functions.etl_functions.silver.silver_class import SilverTransformations

from databricks.connect.session import DatabricksSession as SparkSession

spark = SparkSession.builder.getOrCreate()

data = [
        (1, "Alice",   "HR",      "2024-01-10"),
        (2, "Bob",     "Finance", "2024-01-11"),
        (3, "NULL",    "IT",      "2024-01-12"),   # null-like name
        (4, "Diana",   None,      "2024-01-13"),   # null department
        (2, "Bob",     "Finance", "2024-01-15"),   # duplicate id=2, newer date
        (5, "Eve",     "HR",      "2024-01-14"),
        (1, "Alice",   "HR",      "2024-01-09"),   # duplicate id=1, older date
    ]
df_raw = spark.createDataFrame(data, schema=["id", "name", "department", "event_date"])
df_raw.show()

df_bronze = (
    df_raw
        .transform(lambda df: BronzeTransformations.timestamp_bach_column(df))
        .transform(lambda df: BronzeTransformations.uuid_batch_column(df))
)

df_bronze.show()

df_silver = (
    df_bronze
        .transform(lambda df: SilverTransformations.deduplicate(df, ["id"]))
)

df_silver.show()