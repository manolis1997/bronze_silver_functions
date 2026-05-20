from pyspark.sql import DataFrame
from pyspark.sql import functions as F
 
 
class SilverTransformations:
 
    @staticmethod
    def deduplicate(df: DataFrame, key_cols: list[str], order_col: str = "_ingestion_at") -> DataFrame:
        """
        Keep the most recent row per unique combination of key_cols,
        breaking ties by order_col descending (defaults to Bronze ingestion timestamp).
        """
        from pyspark.sql import Window
 
        w = Window.partitionBy(*key_cols).orderBy(F.col(order_col).desc())
        return (
            df.withColumn("_rn", F.row_number().over(w))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )
 