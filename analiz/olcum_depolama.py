# -*- coding: utf-8 -*-
"""Z2 ve Z4 ölçümleri: JSON ile Parquet boyutu ve sorgu süresi, sıkıştırma türleri, dosya sayısı.

Çalıştırma (önce toplu veri üretilmiş ve is/toplu_is.py bir kez çalışmış olmalı):
  docker compose exec spark spark-submit analiz/olcum_depolama.py
"""
import statistics as st, sys
sys.path.insert(0, "/kit/analiz")
from pyspark.sql import SparkSession, functions as F
from ortak_spark import HAM, SEMA, dosyalar, veri_dosyalari, sure, olcum

spark = SparkSession.builder.appName("olcum-depolama").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

KOD_YOL = "s3a://islenmis/iot_codec/"


def hazirla(df):
    return (df.withColumn("olay_zamani", F.to_timestamp("olay_zamani"))
              .withColumn("saat", F.hour(F.from_utc_timestamp("olay_zamani", "Europe/Istanbul"))))


def sorgu(df):
    # Aynı soru iki biçimde: Akdeniz'deki sensörlerin Türkiye saatine göre ortalama sıcaklığı
    return (hazirla(df).filter(F.col("bolge") == "Akdeniz")
            .groupBy("saat").agg(F.avg("sicaklik").alias("ort_sicaklik")).collect())


# ── 1. Ham JSON boyutu ──────────────────────────────────────────────────────
json_dos = veri_dosyalari(spark, HAM, ".jsonl")
json_bayt = sum(b for _, b in json_dos)
olcum("z2_json_toplam_boyut", json_bayt, "bayt", f"Hadoop FS listFiles s3a://ham/iot/ ({len(json_dos)} dosya)", 1, "3.000.000 satir, 14 gun")

# ── 2. Kitin iskeletinin yazdığı Parquet (snappy, tarih bölümlü, 8 yazıcı) ──
pq_dos = veri_dosyalari(spark, "s3a://islenmis/iot/", ".parquet")
pq_bayt = sum(b for _, b in pq_dos)
olcum("z2_parquet_snappy_iskelet_boyut", pq_bayt, "bayt", f"Hadoop FS listFiles s3a://islenmis/iot/ ({len(pq_dos)} dosya)", 1,
      f"JSON/Parquet orani {json_bayt / pq_bayt:.2f}; iskelet dropDuplicates sonrasi")
olcum("z4_iskelet_dosya_sayisi", len(pq_dos), "dosya", "partitionBy(tarih), kitin iskeleti", 1, "14 tarih klasoru")
olcum("z4_iskelet_dosya_ort_boyut", round(pq_bayt / len(pq_dos)), "bayt", "toplam / dosya sayisi", 1,
      f"en kucuk {min(b for _, b in pq_dos)} / en buyuk {max(b for _, b in pq_dos)}")

# ── 3. Aynı sorgu: JSON (şema elle) ve Parquet ─────────────────────────────
js = spark.read.schema(SEMA).json(HAM)
pq = spark.read.parquet("s3a://islenmis/iot/")
for ad, df in [("json", js), ("parquet_snappy", pq)]:
    s = sure(lambda: sorgu(df))
    olcum(f"z2_sorgu_suresi_{ad}", round(st.mean(s), 3), "saniye",
          "bolge=Akdeniz filtresi + saate gore ortalama sicaklik, collect(); 1 isinma + 3 olcum, ortalama", 3,
          "kosular: " + " / ".join(f"{x:.3f}" for x in s))

# ── 4. Sıkıştırma türleri ──────────────────────────────────────────────────
temiz = spark.read.parquet("s3a://islenmis/iot/")
for kod in ["snappy", "zstd", "gzip", "uncompressed"]:
    yol = KOD_YOL + kod + "/"
    s_yaz = sure(lambda: temiz.write.mode("overwrite").option("compression", kod).partitionBy("tarih").parquet(yol), tekrar=1, isinma=0)
    d = veri_dosyalari(spark, yol, ".parquet")
    b = sum(x for _, x in d)
    olcum(f"z2_codec_{kod}_boyut", b, "bayt", f"ayni temiz veri, partitionBy(tarih), {len(d)} dosya", 1,
          f"JSON/bu orani {json_bayt / b:.2f}; yazma {s_yaz[0]:.1f} s")
    df = spark.read.parquet(yol)
    s = sure(lambda: sorgu(df))
    olcum(f"z2_codec_{kod}_okuma_suresi", round(st.mean(s), 3), "saniye", "ayni Akdeniz sorgusu; 1 isinma + 3 olcum, ortalama", 3,
          "kosular: " + " / ".join(f"{x:.3f}" for x in s))
    s = sure(lambda: df.agg(F.sum("sicaklik"), F.sum("nem"), F.sum("titresim")).collect())
    olcum(f"z2_codec_{kod}_tam_tarama_suresi", round(st.mean(s), 3), "saniye", "uc olcum sutununun toplami (filtresiz); 1 isinma + 3 olcum", 3,
          "kosular: " + " / ".join(f"{x:.3f}" for x in s))

# ── 5. Küçük dosya: tarih başına tek dosyaya birleştirme ───────────────────
BIR = "s3a://islenmis/iot_birlesik/"
temiz.repartition("tarih").write.mode("overwrite").partitionBy("tarih").parquet(BIR)
d = veri_dosyalari(spark, BIR, ".parquet")
olcum("z4_birlesik_dosya_sayisi", len(d), "dosya", "repartition(tarih) + partitionBy(tarih)", 1, "")
olcum("z4_birlesik_dosya_ort_boyut", round(sum(b for _, b in d) / len(d)), "bayt", "toplam / dosya sayisi", 1, "")
for ad, yol in [("iskelet", "s3a://islenmis/iot/"), ("birlesik", BIR)]:
    s = sure(lambda: spark.read.parquet(yol).count())
    olcum(f"z4_count_suresi_{ad}", round(st.mean(s), 3), "saniye", "spark.read.parquet(yol).count(); 1 isinma + 3 olcum", 3,
          "kosular: " + " / ".join(f"{x:.3f}" for x in s))

# ── 6. Pushdown görünümü ───────────────────────────────────────────────────
print("=== EXPLAIN JSON ===")
hazirla(js).filter(F.col("bolge") == "Akdeniz").groupBy("saat").agg(F.avg("sicaklik")).explain(mode="formatted")
print("=== EXPLAIN PARQUET ===")
hazirla(pq).filter(F.col("bolge") == "Akdeniz").groupBy("saat").agg(F.avg("sicaklik")).explain(mode="formatted")
spark.stop()
