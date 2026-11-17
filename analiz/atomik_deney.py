# -*- coding: utf-8 -*-
"""Z7 atomik yazma deneyi: aynı 3 milyon satırı Delta tablosuna ve çıplak Parquet klasörüne yazarken
işi yarıda kesip (kill) tabloyu okumak.

  docker compose exec spark spark-submit analiz/atomik_deney.py hazirla        # iki hedefi de tam yaz (surum=1)
  docker compose exec spark spark-submit analiz/atomik_deney.py yaz delta      # surum=2 ile üzerine yaz (yarıda kesilecek)
  docker compose exec spark spark-submit analiz/atomik_deney.py yaz parquet
  docker compose exec spark spark-submit analiz/atomik_deney.py oku            # iki hedefin durumu
"""
import sys, time
sys.path.insert(0, "/kit/analiz")
from pyspark.sql import SparkSession, functions as F
from ortak_spark import dosyalar

KAYNAK = "s3a://tablolar/iot"
HEDEF = {"delta": "s3a://tablolar/iot_atomik", "parquet": "s3a://islenmis/iot_atomik"}

spark = SparkSession.builder.appName("atomik-deney").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
komut = sys.argv[1]


def yaz(tur, surum):
    # 64 parçaya bölünüyor: yazma birçok dosyaya yayılır, yarıda kesmek için zaman olur
    df = spark.read.format("delta").load(KAYNAK).withColumn("surum", F.lit(surum)).repartition(64)
    w = df.write.mode("overwrite")
    (w.format("delta").save(HEDEF[tur]) if tur == "delta" else w.parquet(HEDEF[tur]))


def oku():
    for tur, yol in HEDEF.items():
        d = dosyalar(spark, yol)
        veri = [x for x in d if x[0].endswith(".parquet") and "/_temporary/" not in x[0]]
        gecici = [x for x in d if "/_temporary/" in x[0]]
        print(f"[{tur}] {yol}")
        print(f"  dosya: toplam {len(d)}, veri .parquet {len(veri)}, _temporary altında {len(gecici)}, "
              f"_SUCCESS {'var' if any(x[0].endswith('_SUCCESS') for x in d) else 'yok'}")
        try:
            df = spark.read.format("delta").load(yol) if tur == "delta" else spark.read.parquet(yol)
            print("  okunan satır: " + ", ".join(f"surum={r['surum']}: {r['count']:,}" for r in df.groupBy("surum").count().orderBy("surum").collect())
                  + f"   toplam {df.count():,}")
        except Exception as e:
            print(f"  OKUMA HATASI: {type(e).__name__}: {str(e).splitlines()[0][:200]}")
        if tur == "delta":
            spark.sql(f"DESCRIBE HISTORY delta.`{yol}`").select("version", "timestamp", "operation").show(truncate=False)


if komut == "hazirla":
    for tur in HEDEF:
        t0 = time.time()
        yaz(tur, 1)
        print(f"{tur} tam yazma süresi: {time.time() - t0:.1f} s", flush=True)
    oku()
elif komut == "yaz":
    print(f"yazma başladı: {sys.argv[2]}", flush=True)
    yaz(sys.argv[2], 2)
    print("yazma bitti (kesilmedi)")
elif komut == "oku":
    oku()
spark.stop()
