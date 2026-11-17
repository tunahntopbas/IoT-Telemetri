# -*- coding: utf-8 -*-
"""Toplu sonuçları ve boyut tablosunu servis deposuna yükler; geçmiş günlerin dakikalık özetini de doldurur.

  docker compose --profile servis-ts up -d
  docker compose exec spark spark-submit is/servis_yukle.py

Yazılanlar (hepsi yeniden çalıştırılabilir, aynı sonucu verir):
  sensorler      ← s3a://ham/boyut/iot/sensorler.csv
  toplu1, toplu2 ← s3a://tablolar/iot_toplu1, iot_toplu2 (Delta)
  sensor_dakika  ← s3a://tablolar/iot (Delta), geçmiş günler sensör × dakika özetine çevrilerek
"""
import sys, time
sys.path.insert(0, "/kit/is"); sys.path.insert(0, "/kit/analiz")
from pyspark.sql import SparkSession, functions as F
from ortak_spark import BOYUT, BOYUT_SEMA
from db import JDBC, calistir

spark = SparkSession.builder.appName("servis-yukle").config("spark.sql.session.timeZone", "UTC").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


def yukle(df, tablo):
    t0 = time.time()
    calistir(spark, f"TRUNCATE {tablo}")
    df.write.format("jdbc").options(**JDBC, dbtable=tablo, batchsize="10000").mode("append").save()
    print(f"{tablo}: {df.count():,} satır ({time.time() - t0:.1f} s)", flush=True)


yukle(spark.read.schema(BOYUT_SEMA).option("header", True).csv(BOYUT), "sensorler")
yukle(spark.read.format("delta").load("s3a://tablolar/iot_toplu1"), "toplu1")
yukle(spark.read.format("delta").load("s3a://tablolar/iot_toplu2")
      .select("sensor_id", "gelen", "anomali", "model", "kurulum_yili", "beklenen", "kayip_orani", "anomali_orani"), "toplu2")

# Geçmiş günler: akış işinin yazdığı biçimin aynısı (sensör × 1 dakika). Toplu veride Kafka'ya varış zamanı yok;
# son_varis olarak olay zamanı yazılıyor.
olay = spark.read.format("delta").load("s3a://tablolar/iot")
dakika = (olay.groupBy(F.window("olay_zamani", "1 minute").start.alias("pencere_baslangic"), "sensor_id")
          .agg(F.max("bolge").alias("bolge"), F.count("*").cast("int").alias("olay_sayisi"),
               F.coalesce(F.sum(F.col("anomali").cast("int")), F.lit(0)).cast("int").alias("anomali_sayisi"),
               F.max("titresim").alias("max_titresim"), F.avg("sicaklik").alias("ort_sicaklik"),
               F.max("olay_zamani").alias("son_olay"), F.max("olay_zamani").alias("son_varis")))
aralik = olay.agg(F.min("tarih"), F.max("tarih")).first()
t0 = time.time()
calistir(spark, f"DELETE FROM sensor_dakika WHERE pencere_baslangic >= '{aralik[0]}' AND pencere_baslangic < DATE '{aralik[1]}' + 1")
dakika.write.format("jdbc").options(**JDBC, dbtable="sensor_dakika", batchsize="10000").mode("append").save()
print(f"sensor_dakika (geçmiş {aralik[0]} – {aralik[1]}): {dakika.count():,} satır ({time.time() - t0:.1f} s)", flush=True)
spark.stop()
