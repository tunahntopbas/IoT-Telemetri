# -*- coding: utf-8 -*-
"""Z3 ölçümleri: aday bölüm anahtarlarının 8 bölüme dağılımı, tuzlama, tekrar eden olayların etkisi.

  docker compose exec spark spark-submit analiz/olcum_bolumleme.py
"""
import sys
sys.path.insert(0, "/kit/analiz")
from pyspark.sql import SparkSession, functions as F
from ortak_spark import HAM, SEMA, olcum

spark = SparkSession.builder.appName("olcum-bolumleme").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

ham = (spark.read.schema(SEMA).json(HAM)
       .withColumn("ts", F.to_timestamp("olay_zamani"))
       .withColumn("saat", F.hour(F.from_utc_timestamp("ts", "Europe/Istanbul"))))
toplam = ham.count()
olcum("z3_ham_satir", toplam, "satir", "spark.read.json(s3a://ham/iot/) count", 1, "tekrarlar dahil")


def dagilim(df, ad, sutunlar, aciklama):
    """repartition(8, sütunlar) → Spark'ın hash bölümleyicisi (Murmur3). En dolu bölüm ÷ ortalama."""
    say = (df.repartition(8, *sutunlar).withColumn("bolum", F.spark_partition_id())
             .groupBy("bolum").count().collect())
    n = {r["bolum"]: r["count"] for r in say}
    dolu = [n.get(i, 0) for i in range(8)]
    ort = sum(dolu) / 8
    olcum(f"z3_en_dolu_ort_{ad}", round(max(dolu) / ort, 3), "oran", f"repartition(8, {aciklama}) + spark_partition_id", 1,
          "bolumler: " + "/".join(str(x) for x in dolu) + f"; bos bolum {dolu.count(0)}")


dagilim(ham, "sensor_id", ["sensor_id"], "sensor_id")
dagilim(ham, "bolge", ["bolge"], "bolge")
dagilim(ham.withColumn("tarih", F.to_date("ts")), "tarih", ["tarih"], "tarih")
dagilim(ham, "saat", ["saat"], "saat")
dagilim(ham, "sensor_id_saat", ["sensor_id", "saat"], "sensor_id, saat")
# Tuzlama: bolge'ye 0..3 arası rastgele bir tuz ekleyip bileşik anahtar yapmak
tuzlu = ham.withColumn("tuz", (F.rand(42) * 4).cast("int"))
dagilim(tuzlu, "bolge_tuz4", ["bolge", "tuz"], "bolge, tuz 0-3")
tuzlu8 = ham.withColumn("tuz", (F.rand(42) * 8).cast("int"))
dagilim(tuzlu8, "bolge_tuz8", ["bolge", "tuz"], "bolge, tuz 0-7")

# Bölge başına kaç farklı değer var?
olcum("z3_farkli_deger_sayisi", "sensor_id={} bolge={} saat={}".format(
    ham.select("sensor_id").distinct().count(), ham.select("bolge").distinct().count(), ham.select("saat").distinct().count()),
    "adet", "distinct count", 1, "null dahil")

# ── Tekrar eden olaylar sonucu değiştiriyor mu? ────────────────────────────
tekil = ham.dropDuplicates(["olay_zamani", "sensor_id", "bolge", "sicaklik", "nem", "titresim"])
n_tekil = tekil.count()
olcum("z3_tekrar_eden_satir", toplam - n_tekil, "satir", "count - dropDuplicates(tum alanlar).count", 1,
      f"oran %{100 * (toplam - n_tekil) / toplam:.3f}")


def ozet(df):
    return df.agg(F.count("*").alias("n"), F.avg("sicaklik").alias("ort"),
                  F.sum(F.when(F.col("titresim") > 1.0, 1).otherwise(0)).alias("anomali")).first()


a, b = ozet(ham), ozet(tekil)
olcum("z3_tekrar_etkisi_ort_sicaklik", f"{a['ort']:.5f} -> {b['ort']:.5f}", "C", "tum veri ortalama sicaklik: tekrarli -> tekrarsiz", 1,
      f"fark {a['ort'] - b['ort']:.5f}")
olcum("z3_tekrar_etkisi_anomali_sayisi", f"{a['anomali']} -> {b['anomali']}", "kayit", "titresim>1.0 sayisi: tekrarli -> tekrarsiz", 1, "")
# Sensör başına sayımda en çok etkilenen sensör
s1 = ham.groupBy("sensor_id").count().withColumnRenamed("count", "tekrarli")
s2 = tekil.groupBy("sensor_id").count().withColumnRenamed("count", "tekrarsiz")
en = (s1.join(s2, "sensor_id").withColumn("fark", F.col("tekrarli") - F.col("tekrarsiz"))
        .orderBy(F.desc("fark")).first())
olcum("z3_tekrar_etkisi_sensor_sayim_en_fazla", en["fark"], "olay", "sensor basina count: tekrarli - tekrarsiz, en buyuk fark", 1,
      f"{en['sensor_id']}: {en['tekrarli']} -> {en['tekrarsiz']}")
spark.stop()
