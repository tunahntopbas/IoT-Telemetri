# -*- coding: utf-8 -*-
"""TOPLU HAT — IoT sensör telemetrisi (grup G221210024, şablon iot).

ham JSON (S3) → temizleme → boyut tablosuyla join → Delta tablosu → iki toplu soru.

  Toplu 1: Bölgelere göre günün saatine (Türkiye saati) bağlı sıcaklık ve titreşim deseni
  Toplu 2: Sensör başına veri kaybı ve anomali oranı, sensör modeli ve kurulum yılına göre

Çalıştırma:
  docker compose exec araclar python araclar/uretici.py --grup G221210024 --sablon iot --toplu 3000000 --gun 14
  docker compose exec spark spark-submit is/toplu_is.py
  docker compose exec spark spark-submit is/toplu_is.py --tarih 2026-11-17      # yalnızca o günü işle, tabloda o günü değiştir

Çıktılar:
  s3a://tablolar/iot              temiz olaylar (Delta, tarih bölümlü)
  s3a://tablolar/iot_karantina    sensor_id'si olmayan kayıtlar (Delta)
  s3a://tablolar/iot_toplu1       Toplu 1 sonucu (Delta)
  s3a://tablolar/iot_toplu2       Toplu 2 sonucu, sensör başına (Delta)
  analiz/cikti/toplu1.csv, toplu2.csv   grafikler için küçük özetler
  kanit/explain-1.txt             iki sorunun fiziksel planı
"""
import argparse, glob, os, shutil, sys, time
sys.path.insert(0, "/kit/analiz")
from pyspark.sql import SparkSession, functions as F
from ortak_spark import HAM, BOYUT, SEMA, BOYUT_SEMA

ap = argparse.ArgumentParser()
ap.add_argument("--sablon", default="iot", choices=["iot"])
ap.add_argument("--tarih", default=None, help="yalnızca bu günü işle (YYYY-MM-DD); Delta'da yalnızca o gün değişir")
ap.add_argument("--explain", default="/kit/kanit/explain-1.txt")
a = ap.parse_args()

# Parquet dosyaları zstd ile sıkıştırılıyor: aynı veride snappy'den %20 küçük (rapor bölüm 5, z2_codec_*_boyut).
spark = (SparkSession.builder.appName("toplu-iot")
         .config("spark.sql.parquet.compression.codec", "zstd").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

TABLO = "s3a://tablolar/iot"
KARANTINA = "s3a://tablolar/iot_karantina"
TOPLU1 = "s3a://tablolar/iot_toplu1"
TOPLU2 = "s3a://tablolar/iot_toplu2"
CIKTI = "/kit/analiz/cikti"

# Titreşim eşiği. Normal ölçümler 0,2 civarında toplanıyor; 0,5 ile 1,5 arasında neredeyse hiç kayıt yok,
# anomaliler 1,5'in üstünde. Eşiği bu boşluğun ortasına koyduk (dağılım aşağıda ekrana basılıyor).
TITRESIM_ESIK = 1.0

t_bas = time.time()

# ── 1. Ham veriyi oku ──────────────────────────────────────────────────────
# Şema elle veriliyor: çıkarım bütün veriyi bir kez fazladan okur. Klasör adındaki tarih=… sütunu
# Spark tarafından kendiliğinden eklenir ve --tarih filtresinde yalnızca o klasör okunur.
ham = spark.read.schema(SEMA).json(HAM)
if a.tarih:
    ham = ham.filter(F.col("tarih") == F.lit(a.tarih).cast("date"))
ham = (ham.withColumn("olay_zamani", F.to_timestamp("olay_zamani"))
          .withColumn("tarih", F.to_date("olay_zamani"))
          .withColumn("saat_tr", F.hour(F.from_utc_timestamp("olay_zamani", "Europe/Istanbul"))))
n_ham = ham.count()
print(f"ham satır: {n_ham:,}")

# ── 2. Temizle ─────────────────────────────────────────────────────────────
# a) Tekrar eden olay: bütün alanları aynı olan satırlardan biri kalır.
OLCUM_ALANLARI = ["sicaklik", "nem", "titresim"]
tekil = ham.dropDuplicates(["olay_zamani", "sensor_id", "bolge"] + OLCUM_ALANLARI)
# b) sensor_id'si olmayan kayıt hiçbir sensöre bağlanamaz: atılmıyor, karantinaya yazılıyor.
karantina = tekil.filter(F.col("sensor_id").isNull())
gecerli = tekil.filter(F.col("sensor_id").isNotNull())
# c) bolge eksikse boyut tablosundan dolduruluyor (sensörün bölgesi sabit). Boyut tablosu 200 satır:
#    broadcast join ile büyük tablo shuffle edilmiyor.
boyut = spark.read.schema(BOYUT_SEMA).option("header", True).csv(BOYUT)
temiz = (gecerli.join(F.broadcast(boyut.select("sensor_id", F.col("bolge").alias("b_bolge"), "model", "kurulum_yili")),
                      "sensor_id", "left")
         .withColumn("bolge_dolduruldu", F.col("bolge").isNull() & F.col("b_bolge").isNotNull())
         .withColumn("bolge", F.coalesce("bolge", "b_bolge"))
         .drop("b_bolge")
         .withColumn("anomali", F.col("titresim") > TITRESIM_ESIK))
# d) Ölçüm alanı (sicaklik/nem/titresim) eksik olan kayıt tutuluyor: sayımda yer alır, ortalamalara
#    null olarak girmez (avg null'ları atlar). Yüksek titreşimli kayıtlar gerçek anomali, ATILMIYOR.

# Kirli veri sayıları (rapor için)
kusur = ham.agg(*[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in ["sensor_id", "bolge"] + OLCUM_ALANLARI]).first()
n_tekil = tekil.count()
n_karantina = karantina.count()
n_dolduruldu = temiz.filter("bolge_dolduruldu").count()
print(f"tekrar eden (atılan): {n_ham - n_tekil:,}")
print(f"sensor_id yok (karantina): {n_karantina:,}")
print(f"bolge boyut tablosundan dolduruldu: {n_dolduruldu:,}")
print("alan bazında null/eksik (ham): " + ", ".join(f"{c}={kusur[c]}" for c in kusur.asDict()))
print("titreşim dağılımı (ham): " + ", ".join(
    f"{ad}={ham.filter(kosul).count():,}" for ad, kosul in [
        ("<0.5", F.col("titresim") < 0.5), ("0.5-1.0", (F.col("titresim") >= 0.5) & (F.col("titresim") < 1.0)),
        ("1.0-1.5", (F.col("titresim") >= 1.0) & (F.col("titresim") < 1.5)), (">=1.5", F.col("titresim") >= 1.5)]))

# ── 3. Delta tablosuna yaz ─────────────────────────────────────────────────
# Tarihe göre bölümlü: sorular gün aralığıyla filtreleniyor, yeni gün eklemek de tek klasörü etkiliyor.
# --tarih verilirse yalnızca o günün bölümü değişir (replaceWhere): iş iki kez çalışsa da veri katlanmaz.
t0 = time.time()
yaz = temiz.write.format("delta").partitionBy("tarih")
if a.tarih:
    yaz = yaz.mode("overwrite").option("replaceWhere", f"tarih = '{a.tarih}'")
else:
    yaz = yaz.mode("overwrite").option("overwriteSchema", "true")   # tam yeniden yazımda şema da yenilenir
yaz.save(TABLO)
# Karantina da tarih bölümlü ve aynı kuralla yazılıyor: bir günü yeniden işlemek o günün karantinasını çoğaltmasın.
kyaz = karantina.write.format("delta").partitionBy("tarih")
if a.tarih:
    kyaz = kyaz.mode("overwrite").option("replaceWhere", f"tarih = '{a.tarih}'")
else:
    kyaz = kyaz.mode("overwrite").option("overwriteSchema", "true")
kyaz.save(KARANTINA)
print(f"Delta yazıldı: {TABLO}   ({time.time() - t0:.1f} s)")

tablo = spark.read.format("delta").load(TABLO)

# ── 4. Toplu 1: bölge × saat ───────────────────────────────────────────────
toplu1 = (tablo.groupBy("bolge", "saat_tr")
          .agg(F.count("*").alias("olay"),
               F.round(F.avg("sicaklik"), 3).alias("ort_sicaklik"),
               F.round(F.avg("titresim"), 4).alias("ort_titresim"),
               F.round(F.percentile_approx("titresim", 0.95), 4).alias("p95_titresim"),
               F.sum(F.col("anomali").cast("int")).alias("anomali"))
          .orderBy("bolge", "saat_tr"))

# ── 5. Toplu 2: sensör başına kayıp ve anomali ─────────────────────────────
# Gelen: tekrarlar atıldıktan sonra sensörün olay sayısı. Beklenen: bütün sensörlerin ortanca olay sayısı
# (sensörler aynı sıklıkla ölçüm yapıyor; ortanca, az gönderenlerden etkilenmeyen bir "normal sensör" ölçüsü).
sensor = (tablo.groupBy("sensor_id")
          .agg(F.count("*").alias("gelen"),
               F.sum(F.col("anomali").cast("int")).alias("anomali")))
beklenen = sensor.agg(F.percentile_approx("gelen", 0.5)).first()[0]
toplu2 = (sensor.join(F.broadcast(boyut.select("sensor_id", "model", "kurulum_yili")), "sensor_id", "left")
          .withColumn("beklenen", F.lit(beklenen))
          .withColumn("kayip_orani", F.round(F.greatest(F.lit(0.0), 1 - F.col("gelen") / F.col("beklenen")), 4))
          .withColumn("anomali_orani", F.round(F.col("anomali") / F.col("gelen"), 5)))
print(f"beklenen olay / sensör (ortanca): {beklenen:,}")

# ── 6. Fiziksel planlar ────────────────────────────────────────────────────
def plan(df):
    return spark._jvm.org.apache.spark.sql.api.python.PythonSQLUtils.explainString(df._jdf.queryExecution(), "formatted")

with open(a.explain, "w", encoding="utf-8") as f:
    f.write("# Toplu 1: bölge × saat (s3a://tablolar/iot üzerinde)\n\n" + plan(toplu1) + "\n\n")
    f.write("# Toplu 2: sensör başına kayıp ve anomali, boyut tablosuyla join\n\n" + plan(toplu2) + "\n\n")
    f.write("# Temizleme hattı: ham JSON → tekrar ayıklama → boyut tablosuyla join\n\n" + plan(temiz) + "\n")
print(f"explain yazıldı: {a.explain}")

# ── 7. Sonuçları yaz ───────────────────────────────────────────────────────
t0 = time.time()
toplu1.write.format("delta").mode("overwrite").save(TOPLU1)
toplu2.write.format("delta").mode("overwrite").save(TOPLU2)
print(f"toplu sonuçlar yazıldı ({time.time() - t0:.1f} s)")


def csv_yaz(df, ad):
    """Küçük sonucu tek bir CSV dosyası olarak analiz/cikti/ altına koyar (grafik için).
    Geçici klasör kapsayıcının kendi diskinde (/tmp): paylaşılan kit/ klasöründe Spark'ın _temporary klasörü
    bilgisayar tarafında kalabiliyordu ve bir sonraki çalıştırma "Unable to clear output directory" ile düşüyordu
    (kanit/kirilma-deneyi-bellek-*.txt)."""
    gecici = f"/tmp/_csv_{ad}_{os.getpid()}"
    df.coalesce(1).write.mode("overwrite").option("header", True).csv("file://" + gecici)
    os.makedirs(CIKTI, exist_ok=True)
    shutil.copyfile(glob.glob(f"{gecici}/part-*.csv")[0], f"{CIKTI}/{ad}.csv")
    shutil.rmtree(gecici, ignore_errors=True)


csv_yaz(spark.read.format("delta").load(TOPLU1), "toplu1")
csv_yaz(spark.read.format("delta").load(TOPLU2).orderBy("sensor_id"), "toplu2")

spark.read.format("delta").load(TOPLU2).groupBy("model").agg(
    F.count("*").alias("sensor"), F.round(F.avg("kayip_orani"), 4).alias("ort_kayip"),
    F.round(F.sum("anomali") / F.sum("gelen"), 5).alias("anomali_orani")).orderBy(F.desc("anomali_orani")).show()
print(f"toplam süre: {time.time() - t_bas:.1f} s")
spark.stop()
