# BSM 461 Büyük Veriye Giriş — Dönem Projesi Raporu

## IoT sensör telemetrisi: Kafka, Spark, Delta Lake ve TimescaleDB ile toplu ve akış hattı

| | |
|---|---|
| Ders | BSM 461 Büyük Veriye Giriş, 2026-2027 Güz |
| Öğretim üyesi | Dr. Öğr. Üyesi Sümeyye Kaynak |
| Grup kodu | G221210024 |
| Şube | 1A |
| Üyeler | Veli Tunahan Topbaş (G221210024), Zeynep Azap (G231210056), Behiç Balkaya (G231210451) |
| Şablon | 3 — IoT sensör telemetrisi (`iot`) |
| Depo | https://github.com/tunahntopbas/IoT-Telemetri |
| Teslim commit'i | `teslim` etiketi: (teslim günü yazılacak) |

**Senaryo kartımız** (`kanit/senaryo.txt`):

| Koşul | Değer |
|---|---|
| Sistem | Sensör ağı (200 sensör) |
| Ortalama hız | Saniyede 2.000 ölçüm |
| Tepe yük | En yoğun saatte ortalamanın 2 katı |
| Uyarı süresi | Akış sorusunun cevabı en geç 30 sn'de |
| Geç veri | Olayların %5'i en fazla 2 dk geç |
| Kayıp sınırı | Sayıma girmeyen olay en fazla %1 |
| Saklama | Ham veri 1095 gün |
| Servis sorgusu | p99 en fazla 50 ms |

Raporda geçen her ölçüm `kanit/olcumler.csv` dosyasında; ölçümü üreten betik ve ham çıktı `analiz/` ve `kanit/` klasörlerinde.
