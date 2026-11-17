# -*- coding: utf-8 -*-
"""AKIŞ HATTI — Kafka → Spark Structured Streaming → sensör başına 1 dakikalık pencere → servis deposu.

Grup G221210024, şablon iot. İki akış sorusu aynı pencere özetinden cevaplanıyor:
  Akış 1: Son 10 dakikada titreşim eşiğini (1,0) aşan sensörler   → görünüm anomali_son10dk
  Akış 2: 60 saniyedir olayı Kafka'ya ulaşmayan (susan) sensörler  → görünüm susan_sensorler
(Görünümler servis/sema.sql içinde; 10 dakikalık ve 60 saniyelik pencere orada, dakikalık özetin üstünde.)

Çalıştırma:
  docker compose --profile servis-ts up -d
  docker compose exec araclar python araclar/uretici.py --grup G221210024 --sablon iot --hiz 50
  docker compose exec spark spark-submit is/akis_iskelet.py

Seçenekler:
  --cikti db|s3|konsol   (birden fazla verilebilir: --cikti db --cikti s3)
  --watermark "2 minutes" (birden fazla verilirse her biri ayrı sorgu ve ayrı tablo: deney için)
  --konu, --tablo        deneylerde ayrı konu ve tablo kullanmak için

Teslim garantisi: Spark ofsetleri ve pencere durumunu checkpoint'te tutuyor; çöken iş son tamamlanmamış
yığını yeniden işliyor (en az bir kez). Veritabanına yazma (sensör, pencere) anahtarıyla upsert olduğu için
yeniden işlenen yığın aynı satırları aynı değerlerle yazıyor: sonuç tam bir kez yazılmış gibi.
"""
import argparse, json, os, time, sys
sys.path.insert(0, "/kit/is")
from pyspark.sql import SparkSession, functions as F
from db import baglanti

ap = argparse.ArgumentParser()
ap.add_argument("--sablon", default="iot", choices=["iot"])
ap.add_argument("--konu", default="sensorler")
ap.add_argument("--cikti", action="append", choices=["db", "s3", "konsol"])
ap.add_argument("--tablo", default="sensor_dakika")
ap.add_argument("--watermark", action="append")
ap.add_argument("--tetik", default="10 seconds")
ap.add_argument("--baslangic", default="earliest", choices=["earliest", "latest"])
# Checkpoint yerel diskte (kapsayıcıya bağlı kit/veri/ klasörü). S3'teyken durum deposunun her yığında S3'e
# yazılması yığın başına ~10 sn tutuyordu ve uyarı gecikmesi 30 sn sınırına dayanıyordu (rapor bölüm 8.3).
ap.add_argument("--checkpoint", default="file:///kit/veri/checkpoint",
                help="checkpoint kökü (ör. s3a://islenmis/_checkpoint/akis)")
ap.add_argument("--ilerleme", default=None, help="mikro yığın ilerlemesinin yazılacağı .jsonl dosyası")
ap.add_argument("--cokme-batch", type=int, default=None,
                help="DENEY: bu yığın veritabanına yazılıp onaylandıktan hemen sonra süreci öldür (Spark ofseti kaydedemeden)")
a = ap.parse_args()
a.cikti = a.cikti or ["db"]
a.watermark = a.watermark or ["2 minutes"]

SEMA = "olay_zamani STRING, sensor_id STRING, bolge STRING, sicaklik DOUBLE, nem DOUBLE, titresim DOUBLE"
TITRESIM_ESIK = 1.0          # toplu veride 0,5–1,5 arasında 3 milyon kayıttan yalnızca 1'i var (bölüm 7)

spark = (SparkSession.builder.appName(f"akis-{a.konu}")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# ── 1. Kafka'dan oku ───────────────────────────────────────────────────────
# "earliest": ilk çalıştırmada konunun başından başlar. Yeniden başlatmada bu ayar kullanılmaz;
# Spark checkpoint'teki ofsetten devam eder.
ham = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", "kafka:29092")
       .option("subscribe", a.konu)
       .option("startingOffsets", a.baslangic)
       .option("failOnDataLoss", "true")
       .load())

olaylar = (ham.select(F.from_json(F.col("value").cast("string"), SEMA).alias("o"),
                      F.col("timestamp").alias("varis"))            # Kafka'ya ulaştığı an
              .select("o.*", "varis")
              .withColumn("olay_zamani", F.to_timestamp("olay_zamani"))
              .filter(F.col("olay_zamani").isNotNull() & F.col("sensor_id").isNotNull()))


def ozet(wm):
    """Watermark + tekrar ayıklama + sensör başına 1 dakikalık sabit pencere."""
    return (olaylar
            .withWatermark("olay_zamani", wm)
            # Tekrar eden olay bütün alanlarıyla aynı. Olay zamanı anahtarda olduğu için ayıklama durumu
            # watermark geçince siliniyor; durum sınırsız büyümüyor.
            .dropDuplicates(["olay_zamani", "sensor_id", "bolge", "sicaklik", "nem", "titresim"])
            .groupBy(F.window("olay_zamani", "1 minute"), "sensor_id")
            .agg(F.max("bolge").alias("bolge"),
                 F.count("*").alias("olay_sayisi"),
                 F.coalesce(F.sum((F.col("titresim") > TITRESIM_ESIK).cast("int")), F.lit(0)).alias("anomali_sayisi"),
                 F.max("titresim").alias("max_titresim"),
                 F.avg("sicaklik").alias("ort_sicaklik"),
                 F.max("olay_zamani").alias("son_olay"),
                 F.max("varis").alias("son_varis"))
            .select(F.col("window.start").alias("pencere_baslangic"), "sensor_id", "bolge", "olay_sayisi",
                    "anomali_sayisi", "max_titresim", "ort_sicaklik", "son_olay", "son_varis"))


UPSERT = """INSERT INTO {t} (pencere_baslangic, sensor_id, bolge, olay_sayisi, anomali_sayisi, max_titresim,
                            ort_sicaklik, son_olay, son_varis, yazilma)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, now())
ON CONFLICT (sensor_id, pencere_baslangic) DO UPDATE SET
  bolge = EXCLUDED.bolge, olay_sayisi = EXCLUDED.olay_sayisi, anomali_sayisi = EXCLUDED.anomali_sayisi,
  max_titresim = EXCLUDED.max_titresim, ort_sicaklik = EXCLUDED.ort_sicaklik, son_olay = EXCLUDED.son_olay,
  son_varis = EXCLUDED.son_varis, yazilma = now()"""


def db_yazici(tablo, sorgu_adi):
    if tablo != "sensor_dakika":      # deney tabloları: ana tabloyla aynı yapı
        c = baglanti(spark)
        c.createStatement().execute(
            f"CREATE TABLE IF NOT EXISTS {tablo} (LIKE sensor_dakika INCLUDING ALL)")
        c.close()

    def yaz(df, batch_id):
        satirlar = (df.select(F.unix_millis("pencere_baslangic").alias("p"), "sensor_id", "bolge", "olay_sayisi",
                              "anomali_sayisi", "max_titresim", "ort_sicaklik",
                              F.unix_millis("son_olay").alias("so"), F.unix_millis("son_varis").alias("sv"))
                    .collect())
        jvm = spark._jvm
        c = baglanti(spark)
        try:
            c.setAutoCommit(False)
            ps = c.prepareStatement(UPSERT.format(t=tablo))
            for r in satirlar:
                ps.setTimestamp(1, jvm.java.sql.Timestamp(r["p"]))
                ps.setString(2, r["sensor_id"])
                ps.setString(3, r["bolge"])
                ps.setInt(4, int(r["olay_sayisi"]))
                ps.setInt(5, int(r["anomali_sayisi"]))
                ps.setObject(6, r["max_titresim"])
                ps.setObject(7, r["ort_sicaklik"])
                ps.setTimestamp(8, jvm.java.sql.Timestamp(r["so"]))
                ps.setTimestamp(9, jvm.java.sql.Timestamp(r["sv"]))
                ps.addBatch()
            if satirlar:
                ps.executeBatch()
            g = c.prepareStatement("INSERT INTO batch_gunlugu (sorgu, batch_id, satir, max_varis) VALUES (?, ?, ?, ?)")
            g.setString(1, sorgu_adi)
            g.setLong(2, batch_id)
            g.setInt(3, len(satirlar))
            g.setObject(4, jvm.java.sql.Timestamp(max(r["sv"] for r in satirlar)) if satirlar else None)
            g.execute()
            c.commit()          # yığının bütün satırları tek işlemde: ya hepsi ya hiçbiri
        finally:
            c.close()
        if a.cokme_batch is not None and batch_id == a.cokme_batch:
            print(f"DENEY: yığın {batch_id} veritabanına yazıldı, süreç şimdi öldürülüyor", flush=True)
            os._exit(1)
    return yaz


def saniye(wm):
    sayi, birim = wm.split()
    return int(sayi) * (60 if birim.startswith("minute") else 1)


sorgular = []
for wm in a.watermark:
    ek = "" if len(a.watermark) == 1 else f"_wm{saniye(wm)}"
    tablo = a.tablo + ek
    df = ozet(wm)
    if "db" in a.cikti:
        ad = f"{tablo}-db"
        sorgular.append(df.writeStream.queryName(ad.replace("-", "_")).outputMode("update")
                        .foreachBatch(db_yazici(tablo, ad))
                        .option("checkpointLocation", f"{a.checkpoint}/{tablo}-db/")
                        .trigger(processingTime=a.tetik).start())
    if "s3" in a.cikti:
        # Dosya çıktısı yalnızca "append": pencere watermark geçince kesinleşir ve bir kez yazılır.
        # Spark'ın dosya yazıcısı _spark_metadata günlüğüyle hangi yığının yazıldığını tutuyor (tam bir kez).
        sorgular.append(df.writeStream.queryName(f"{tablo}_s3").outputMode("append").format("parquet")
                        .option("path", f"s3a://islenmis/akis/{tablo}/")
                        .option("checkpointLocation", f"{a.checkpoint}/{tablo}-s3/")
                        .trigger(processingTime=a.tetik).start())
    if "konsol" in a.cikti:
        sorgular.append(df.writeStream.outputMode("update").format("console").option("truncate", False)
                        .trigger(processingTime=a.tetik).start())

# Mikro yığın ilerlemesini dosyaya yaz (gecikme ve işlem süresi ölçümü için)
gorulen = {}
while any(q.isActive for q in sorgular):
    for q in sorgular:
        p = q.lastProgress
        if p and gorulen.get(q.name) != p["batchId"]:
            gorulen[q.name] = p["batchId"]
            if a.ilerleme:
                with open(a.ilerleme, "a") as f:
                    f.write(json.dumps({"sorgu": q.name, "batch": p["batchId"], "zaman": p["timestamp"],
                                        "girdi": p["numInputRows"], "sure_ms": p["durationMs"],
                                        "girdi_hizi": p.get("inputRowsPerSecond"), "islem_hizi": p.get("processedRowsPerSecond"),
                                        "kaynak": [{"bas": x.get("startOffset"), "son": x.get("endOffset"), "en_son": x.get("latestOffset")} for x in p.get("sources", [])],
                                        "olay_zamani": p.get("eventTime", {}),
                                        "durum": [s.get("numRowsDroppedByWatermark") for s in p.get("stateOperators", [])],
                                        "durum_ms": [{k: s.get(k) for k in ("commitTimeMs", "allUpdatesTimeMs", "allRemovalsTimeMs")}
                                                     for s in p.get("stateOperators", [])]}) + "\n")
    time.sleep(2)
for q in sorgular:
    if q.exception():
        raise q.exception()
