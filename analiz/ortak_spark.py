# -*- coding: utf-8 -*-
"""Ölçüm betiklerinin ortak parçaları: şema, S3 dosya listeleme, süre ölçme, ölçüm satırı yazma."""
import time
from datetime import date
from pyspark.sql import types as T

HAM = "s3a://ham/iot/"
BOYUT = "s3a://ham/boyut/iot/sensorler.csv"

# Ham olayın şeması elle veriliyor: çıkarım bütün veriyi bir kez fazladan okur.
SEMA = T.StructType([
    T.StructField("olay_zamani", T.StringType()),
    T.StructField("sensor_id", T.StringType()),
    T.StructField("bolge", T.StringType()),
    T.StructField("sicaklik", T.DoubleType()),
    T.StructField("nem", T.DoubleType()),
    T.StructField("titresim", T.DoubleType()),
])

BOYUT_SEMA = T.StructType([
    T.StructField("sensor_id", T.StringType()),
    T.StructField("bolge", T.StringType()),
    T.StructField("model", T.StringType()),
    T.StructField("kurulum_yili", T.IntegerType()),
])


def dosyalar(spark, yol):
    """Bir S3 yolunun altındaki bütün dosyalar: [(yol, bayt)]. Gizli dosyalar (_delta_log, _SUCCESS) dahil."""
    jvm = spark._jvm
    p = jvm.org.apache.hadoop.fs.Path(yol)
    fs = p.getFileSystem(spark._jsc.hadoopConfiguration())
    if not fs.exists(p):
        return []
    it = fs.listFiles(p, True)
    sonuc = []
    while it.hasNext():
        f = it.next()
        sonuc.append((f.getPath().toString(), f.getLen()))
    return sonuc


def veri_dosyalari(spark, yol, uzanti):
    return [(y, b) for y, b in dosyalar(spark, yol) if y.endswith(uzanti) and "/_" not in y.split(yol, 1)[-1]]


def sure(fonk, tekrar=3, isinma=1):
    """fonk'u önce isinma kez (ölçülmeden), sonra tekrar kez çalıştırır; saniye listesi döndürür."""
    for _ in range(isinma):
        fonk()
    olculen = []
    for _ in range(tekrar):
        t0 = time.time()
        fonk()
        olculen.append(time.time() - t0)
    return olculen


def olcum(ad, deger, birim, yontem, tekrar=1, not_=""):
    """olcumler.csv'ye aktarılacak satırı ekrana basar (OLCUM| ile başlayan satırlar toplanır)."""
    print("OLCUM|" + "|".join(str(x) for x in [date.today().isoformat(), ad, deger, birim, yontem, tekrar, not_]))
