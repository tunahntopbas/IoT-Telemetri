# -*- coding: utf-8 -*-
"""Z7: s3a://tablolar/iot Delta tablosunun geçmişi ve zaman yolculuğu.

  docker compose exec spark spark-submit analiz/delta_gecmis.py
"""
from pyspark.sql import SparkSession, functions as F

TABLO = "s3a://tablolar/iot"
spark = SparkSession.builder.appName("delta-gecmis").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

gecmis = spark.sql(f"DESCRIBE HISTORY delta.`{TABLO}`")
gecmis.select("version", "timestamp", "operation", "operationParameters", "operationMetrics.numOutputRows",
              "operationMetrics.numRemovedFiles", "operationMetrics.numAddedFiles").show(truncate=False)
for r in gecmis.orderBy("version").collect():
    v = r["version"]
    df = spark.read.format("delta").option("versionAsOf", v).load(TABLO)
    gun16 = df.filter(F.col("tarih") == "2026-11-16").count()
    print(f"versionAsOf {v}: {df.count():,} satır · sütun sayısı {len(df.columns)} · 2026-11-16 günü {gun16:,} satır")
spark.stop()
