from pyspark.sql import DataFrame
from pyspark.sql import functions as F

import uuid

from datetime import datetime, timezone

class BronzeTransformations:
    @staticmethod
    def uuid_batch_column(df: DataFrame) -> DataFrame:
        batch_id = str(uuid.uuid4())
        return df.withColumn('_batch_id', F.lit(batch_id))
    
    @staticmethod
    def timestamp_bach_column(df: DataFrame) -> DataFrame:
        ingested_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        return df.withColumn('_ingestion_at', F.lit(ingested_at).cast("timestamp"))