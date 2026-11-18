# -*- coding: utf-8 -*-
"""Omurga şeması (Şekil 3.1): rapor/sekiller/mimari.png
Çalıştırma: docker compose exec araclar python analiz/mimari_sekli.py
Sürümler çalışan kapsayıcılardan okundu (rapor bölüm 3)."""
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch

fig, ax = plt.subplots(figsize=(16, 7.4), dpi=150)
ax.set_xlim(0, 160); ax.set_ylim(0, 74); ax.axis("off")

RENK = {"kaynak": "#e8eef7", "kuyruk": "#fdf0dc", "isleme": "#e5f4e7", "depo": "#f3e6f5", "sunum": "#fde8e8"}


def kutu(x, y, w, h, baslik, satirlar, tur):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.4,rounding_size=1.2",
                                fc=RENK[tur], ec="#444", lw=1.2))
    ax.text(x + w / 2, y + h - 2.3, baslik, ha="center", va="top", fontsize=10.5, weight="bold")
    ax.text(x + w / 2, y + h - 6.2, "\n".join(satirlar), ha="center", va="top", fontsize=8.3, linespacing=1.35)


def ok(x1, y1, x2, y2, yazi="", dx=0, dy=1.6):
    ax.add_patch(FancyArrowPatch((x1, y1), (x2, y2), arrowstyle="-|>", mutation_scale=16, lw=1.4, color="#333"))
    if yazi:
        ax.text((x1 + x2) / 2 + dx, (y1 + y2) / 2 + dy, yazi, ha="center", va="bottom", fontsize=7.6, color="#222",
                bbox=dict(fc="white", ec="none", pad=0.6))


ax.text(2, 72, "AKIŞ HATTI", fontsize=11, weight="bold", color="#555")
ax.text(2, 34.5, "TOPLU HAT", fontsize=11, weight="bold", color="#555")

# Akış hattı (üst sıra)
kutu(2, 46, 26, 22, "Olay kaynağı", ["200 sensör (benzetim)", "araclar/uretici.py", "Python 3.12.16", "confluent-kafka 2.6.1", "acks=all, idempotent"], "kaynak")
kutu(36, 46, 28, 22, "Olay günlüğü", ["Apache Kafka 4.0.0 (KRaft)", "tek düğüm, kopya 1", "konu sensorler: 8 bölüm", "anahtar sensor_id", "saklama 7 gün"], "kuyruk")
kutu(72, 46, 32, 22, "Akış işleme", ["Spark 3.5.3 Structured Streaming", "is/akis_iskelet.py", "watermark 2 dk, update modu", "sensör × 1 dk pencere", "tekrar ayıklama, tetik 10 sn", "checkpoint: yerel disk (kit/veri)"], "isleme")
kutu(112, 46, 28, 22, "Servis deposu", ["PostgreSQL 16.6", "+ TimescaleDB 2.17.2", "sensor_dakika (hypertable)", "upsert (sensör, pencere)", "görünümler: Akış 1, Akış 2"], "depo")
kutu(143, 26, 15, 18, "Pano", ["Grafana 11.3.1", "6 panel", "10 sn'de", "bir yenileme", "salt okunur"], "sunum")

ok(28.8, 57, 35.2, 57, "JSON, lz4", dy=1.4)
ok(64.8, 57, 71.2, 57, "en az bir kez", dy=1.4)
ok(104.8, 57, 111.2, 57, "JDBC upsert", dy=1.4)
ok(140.8, 50, 150, 44.6, "SQL", dx=3.4, dy=-0.5)

# Toplu hat (alt sıra)
kutu(2, 6, 26, 25, "Toplu kaynak", ["uretici.py --toplu", "3.000.000 olay, 14 gün", "JSONL", "+ boyut tablosu", "sensorler.csv (200 satır)"], "kaynak")
kutu(36, 6, 28, 25, "Nesne deposu", ["SeaweedFS 4.48 (S3 uyumlu)", "tek düğüm, çoğaltma yok", "s3://ham/iot/tarih=…/", "s3://ham/boyut/iot/", "s3://tablolar/ (Delta)"], "depo")
kutu(72, 6, 32, 25, "Toplu işleme", ["Spark 3.5.3 (local[*], 8 çekirdek)", "+ Delta Lake 3.2.1", "is/toplu_is.py", "tekrar ayıklama, karantina", "broadcast join (boyut)", "Delta: tarih bölümlü, zstd"], "isleme")
kutu(112, 6, 28, 25, "Yükleme", ["is/servis_yukle.py", "Spark 3.5.3", "PostgreSQL JDBC 42.7.4", "toplu1, toplu2,", "sensorler, geçmiş", "sensor_dakika"], "isleme")

ok(28.8, 18.5, 35.2, 18.5, "S3 PUT", dy=1.4)
ok(64.8, 21, 71.2, 21, "okuma", dy=1.4)
ok(71.2, 13, 64.8, 13, "Delta yazma", dy=-4)
ok(126, 31.8, 126, 45.2, "toplu sonuçlar", dx=8.5, dy=-1)
ok(104.8, 18.5, 111.2, 18.5, "Delta okuma", dy=1.4)

ax.text(80, 1.2, "Hepsi tek makinede: Apple M1, 8 GB RAM, Docker'a 4,786 GiB; Docker 29.9.2, Docker Compose 5.6.1",
        ha="center", fontsize=8.5, color="#555")
plt.tight_layout()
plt.savefig("/kit/rapor/sekiller/mimari.png", bbox_inches="tight")
print("yazıldı: rapor/sekiller/mimari.png")
