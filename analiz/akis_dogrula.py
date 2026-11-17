# -*- coding: utf-8 -*-
"""Akış deneylerinin doğrulaması: Kafka'daki gerçek veriyle akış işinin yazdığı tabloları karşılaştırır.

  docker compose exec spark spark-submit analiz/akis_dogrula.py --konu akis-gec-kart --tablo deney_gec_kart_wm120 --tablo deney_gec_kart_wm30
  ... --pencere   # pencere pencere karşılaştır (yeniden başlatma deneyi)
  ... --s3 s3a://islenmis/akis/deney_yeniden/   # dosya çıktısını da karşılaştır

Gerçek değer: konudaki bütün mesajlar toplu okunup akış işiyle aynı kuralla (sensor_id ve olay zamanı dolu,
bütün alanları aynı olanlardan biri) ayıklanır.
"""
import argparse, sys
sys.path.insert(0, "/kit/is"); sys.path.insert(0, "/kit/analiz")
from pyspark.sql import SparkSession, functions as F
from db import JDBC
from ortak_spark import olcum

ap = argparse.ArgumentParser()
ap.add_argument("--konu", required=True)
ap.add_argument("--tablo", action="append", default=[])
ap.add_argument("--s3", default=None)
ap.add_argument("--pencere", action="store_true")
ap.add_argument("--deney", default="akis")
a = ap.parse_args()

spark = SparkSession.builder.appName("akis-dogrula").config("spark.sql.session.timeZone", "UTC").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
SEMA = "olay_zamani STRING, sensor_id STRING, bolge STRING, sicaklik DOUBLE, nem DOUBLE, titresim DOUBLE"

ham = (spark.read.format("kafka").option("kafka.bootstrap.servers", "kafka:29092")
       .option("subscribe", a.konu).option("startingOffsets", "earliest").load())
olay = (ham.select(F.from_json(F.col("value").cast("string"), SEMA).alias("o"), "timestamp").select("o.*", "timestamp")
        .withColumn("olay_zamani", F.to_timestamp("olay_zamani"))
        .filter(F.col("olay_zamani").isNotNull() & F.col("sensor_id").isNotNull()))
n_mesaj = ham.count()
tekil = olay.dropDuplicates(["olay_zamani", "sensor_id", "bolge", "sicaklik", "nem", "titresim"])
n_gecerli, n_tekil = olay.count(), tekil.count()
gec = tekil.filter((F.col("timestamp").cast("double") - F.col("olay_zamani").cast("double")) > 5).count()
print(f"Kafka mesajı: {n_mesaj:,} · sensor_id/zaman dolu: {n_gecerli:,} · tekrarsız: {n_tekil:,} · tekrar: {n_gecerli - n_tekil:,} · 5 sn'den geç varan: {gec:,}")
olcum(f"{a.deney}_kafka_mesaj", n_mesaj, "mesaj", f"konu {a.konu}, toplu okuma", 1, f"gecerli {n_gecerli}, tekrarsiz {n_tekil}, gec(>5s) {gec}")

dogru = tekil.groupBy(F.window("olay_zamani", "1 minute").start.alias("pencere_baslangic"), "sensor_id").agg(F.count("*").alias("dogru"))

for t in a.tablo:
    # Tabloda bu konudan önceki (ör. toplu veriden doldurulmuş geçmiş) pencereler olabilir: yalnızca konunun zaman aralığı
    ilk = dogru.agg(F.min("pencere_baslangic")).first()[0]
    df = spark.read.format("jdbc").options(**JDBC, dbtable=t).load().filter(F.col("pencere_baslangic") >= F.lit(ilk))
    toplam = df.agg(F.sum("olay_sayisi")).first()[0] or 0
    print(f"[{t}] sayılan olay: {toplam:,} · sayıma girmeyen: {n_tekil - toplam:,} (%{100 * (n_tekil - toplam) / n_tekil:.3f})")
    olcum(f"{a.deney}_{t}_sayima_girmeyen", n_tekil - toplam, "olay", f"tekrarsiz Kafka olayi ({n_tekil}) - tablodaki olay_sayisi toplami ({toplam})", 1,
          f"oran %{100 * (n_tekil - toplam) / n_tekil:.3f}")
    if a.pencere:
        k = dogru.join(df.select("pencere_baslangic", "sensor_id", "olay_sayisi"), ["pencere_baslangic", "sensor_id"], "full")
        k = k.fillna(0, ["dogru", "olay_sayisi"])
        eksik = k.filter(F.col("olay_sayisi") < F.col("dogru")).agg(F.count("*"), F.sum(F.col("dogru") - F.col("olay_sayisi"))).first()
        fazla = k.filter(F.col("olay_sayisi") > F.col("dogru")).agg(F.count("*"), F.sum(F.col("olay_sayisi") - F.col("dogru"))).first()
        print(f"  pencere karşılaştırması: {k.count():,} pencere · eksik sayılan pencere {eksik[0]} ({eksik[1] or 0} olay) · fazla sayılan pencere {fazla[0]} ({fazla[1] or 0} olay)")
        olcum(f"{a.deney}_{t}_pencere_eksik_fazla", f"eksik {eksik[1] or 0} / fazla {fazla[1] or 0}", "olay",
              "sensor x 1 dk pencere bazinda tablo ile Kafka'dan hesaplanan dogru deger", 1, f"{k.count()} pencere")

if a.s3:
    s = spark.read.parquet(a.s3)
    son = s.agg(F.max("pencere_baslangic")).first()[0]
    k = dogru.filter(F.col("pencere_baslangic") <= son).join(s.select("pencere_baslangic", "sensor_id", "olay_sayisi"),
                                                              ["pencere_baslangic", "sensor_id"], "full").fillna(0, ["dogru", "olay_sayisi"])
    eksik = k.filter(F.col("olay_sayisi") < F.col("dogru")).agg(F.count("*"), F.sum(F.col("dogru") - F.col("olay_sayisi"))).first()
    fazla = k.filter(F.col("olay_sayisi") > F.col("dogru")).agg(F.count("*"), F.sum(F.col("olay_sayisi") - F.col("dogru"))).first()
    cift = s.groupBy("pencere_baslangic", "sensor_id").count().filter("count > 1").count()
    print(f"[s3 {a.s3}] kesinleşmiş pencere {s.count():,} (son {son}) · iki kez yazılan pencere {cift} · eksik {eksik[1] or 0} olay · fazla {fazla[1] or 0} olay")
    olcum(f"{a.deney}_s3_pencere_eksik_fazla_cift", f"eksik {eksik[1] or 0} / fazla {fazla[1] or 0} / cift pencere {cift}", "olay",
          "append ciktisinda kesinlesmis pencereler ile Kafka'dan hesaplanan dogru deger", 1, f"{s.count()} pencere")
spark.stop()
