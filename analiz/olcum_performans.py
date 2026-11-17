# -*- coding: utf-8 -*-
"""Z5 performans kararı: boyut tablosu join'i broadcast mı sort-merge mi, shuffle bölüm sayısı 8 mi 200 mü.

Aynı veri (s3a://ham/iot/, 3.000.000 satır), aynı temizleme hattı; sonuç "noop" çıktıya yazılır
(plan tamamen çalışır ama hiçbir yere dosya yazılmaz). Her durum 1 ısınma + 3 ölçüm.

  docker compose exec spark spark-submit analiz/olcum_performans.py [--bekle 120]
"""
import argparse, statistics as st, sys, time
sys.path.insert(0, "/kit/analiz")
from pyspark.sql import SparkSession, functions as F
from ortak_spark import HAM, BOYUT, SEMA, BOYUT_SEMA, sure, olcum

ap = argparse.ArgumentParser()
ap.add_argument("--tekrar", type=int, default=3, help="her durum kaç kez ölçülsün")
ap.add_argument("--bekle", type=int, default=0, help="sonda Spark UI açık kalsın diye kaç saniye beklensin")
a = ap.parse_args()

spark = SparkSession.builder.appName("olcum-performans").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


def hat():
    ham = spark.read.schema(SEMA).json(HAM).withColumn("olay_zamani", F.to_timestamp("olay_zamani"))
    tekil = ham.dropDuplicates(["olay_zamani", "sensor_id", "bolge", "sicaklik", "nem", "titresim"])
    boyut = spark.read.schema(BOYUT_SEMA).option("header", True).csv(BOYUT).withColumnRenamed("bolge", "b_bolge")
    return tekil.filter(F.col("sensor_id").isNotNull()).join(boyut, "sensor_id", "left")


def calistir():
    hat().write.format("noop").mode("overwrite").save()


def strateji():
    p = spark._jvm.org.apache.spark.sql.api.python.PythonSQLUtils.explainString(hat()._jdf.queryExecution(), "simple")
    return "BroadcastHashJoin" if "BroadcastHashJoin" in p else ("SortMergeJoin" if "SortMergeJoin" in p else "?")


durumlar = [
    ("broadcast_shuffle8", {"spark.sql.autoBroadcastJoinThreshold": "10485760", "spark.sql.shuffle.partitions": "8"}),
    ("sortmerge_shuffle8", {"spark.sql.autoBroadcastJoinThreshold": "-1", "spark.sql.shuffle.partitions": "8"}),
    ("broadcast_shuffle200", {"spark.sql.autoBroadcastJoinThreshold": "10485760", "spark.sql.shuffle.partitions": "200"}),
]
for ad, ayar in durumlar:
    for k, v in ayar.items():
        spark.conf.set(k, v)
    s = sure(calistir, tekrar=a.tekrar)
    olcum(f"z5_perf_{ad}", round(st.mean(s), 3), "saniye",
          f"temizleme hatti (json oku + dropDuplicates + boyut join) -> noop; join={strateji()}; " +
          "; ".join(f"{k}={v}" for k, v in ayar.items()), a.tekrar, "kosular: " + " / ".join(f"{x:.3f}" for x in s))

if a.bekle:
    print(f"Spark UI açık: http://localhost:4040  ({a.bekle} s bekleniyor)", flush=True)
    time.sleep(a.bekle)
spark.stop()
