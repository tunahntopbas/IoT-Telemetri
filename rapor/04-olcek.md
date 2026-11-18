# 4. Ölçek (Z1)

## Girdi değerleri

| Değer | Sayı | Birim | Kaynak |
|---|---|---|---|
| Ortalama hız | 2.000 | olay/s | senaryo kartı (`kanit/senaryo.txt`) |
| Tepe yük çarpanı | 2 | × ortalama | senaryo kartı ("en yoğun saatte ortalamanın 2 katı") |
| Saklama süresi | 1.095 | gün | senaryo kartı |
| Olay başına bayt (JSON değeri) | 142,58 | bayt | ölçüm, `olcumler.csv` satırı `olay_deger_bayt_ortalama` (1934 Kafka mesajı) |
| Olay başına bayt (JSONL dosyasında, satır sonu dahil) | 143,57 | bayt | ölçüm, `olcumler.csv` satırı `toplu_dosya_bayt_satir_basi` (200.000 satır) |
| Mesaj anahtarı (`sensor_id`) | 10 | bayt | ölçüm, `olcumler.csv` satırı `olay_anahtar_bayt_ortalama` |
| Bir günün saniye sayısı | 86.400 | s | sabit |

Hesaplarda 142,58 bayt kullanıldı. Kafka'nın kayıt başlığı, anahtar ve sıkıştırma etkisi bu sayıya dahil değil. Kafka'nın diskte kapladığı yeri ayrıca ölçmedik: Kafka'da olaylar 7 gün tutuluyor (`retention.ms`), 1095 günlük saklama S3'te. Disk hesabını belirleyen S3 tarafı.

## Ham veri hacmi (ortalama hızla)

| Büyüklük | Hesap | Sonuç |
|---|---|---|
| Saniyelik veri | 2.000 olay/s × 142,58 bayt | 285.160 bayt/s ≈ 285,2 KB/s ≈ 2,28 Mbit/s |
| Günlük olay sayısı | 2.000 × 86.400 | 172,8 milyon olay/gün |
| Günlük ham veri | 285.160 bayt/s × 86.400 s | 24,64 GB/gün |
| 1095 günlük ham veri | 24,64 GB/gün × 1.095 gün | 26,98 TB |

(GB = 10⁹ bayt, TB = 10¹² bayt. Hepsi hesap; girdileri yukarıdaki tabloda.)

## Parquet'te ne olur?

Ölçüldü (bölüm 5.1, `kanit/olcumler.csv` satırları `z2_codec_*`). Aynı 3.000.000 satır (430,7 MB JSON) farklı biçimlerde yazıldı. 1095 günlük boyut, 26,98 TB'ın ölçülen orana bölünmesiyle hesaplandı. Parquet dosyaları tekrarlar atıldıktan sonra yazıldığı için oranlar binde 2 kadar iyimser.

| Biçim | Ölçülen boyut | Ölçülen oran (JSON ÷ biçim) | 1095 günlük boyut (hesap) |
|---|---|---|---|
| JSON (ham) | 430,7 MB | 1,00 | 26,98 TB |
| Parquet, sıkıştırmasız | 59,6 MB | 7,22 | 3,74 TB |
| Parquet + snappy | 44,5 MB | 9,68 | 2,79 TB |
| Parquet + gzip | 36,6 MB | 11,77 | 2,29 TB |
| Parquet + zstd | 35,5 MB | 12,12 | 2,23 TB |

Ham veriyi zstd'li Parquet olarak tutmak 3 yıllık hacmi yaklaşık 27 TB'tan 2,2 TB'a indiriyor. Günlük hacim 24,64 GB'tan 2,03 GB'a iniyor (24,64 ÷ 12,12). Bu da bizim 245 GB'lık diskte yaklaşık 120 güne karşılık geliyor; yani 3 yıl yine tek makineye sığmıyor.

## Ortalama mı, tepe mi?

İkisi farklı yerlerde kullanılıyor:
- Depolama ortalama hızla hesaplanır. Tepe yük günde bir saat sürüyor; diski günün toplamı doldurur. Bu yüzden 26,98 TB ortalamadan geliyor.
- İşleme kapasitesi (Kafka bölüm sayısı, akış işinin saniyede işleyebileceği olay, ağ) tepe yüke göre seçilir. Akış sorusunun cevabı 30 sn içinde çıkmalı. En yoğun saatte sistem geri kalırsa uyarı tam da en çok olay olan anda gecikir.

| Büyüklük | Ortalama | Tepe (×2) |
|---|---|---|
| Olay/s | 2.000 | 4.000 |
| Veri hızı | 285,2 KB/s (2,28 Mbit/s) | 570,3 KB/s (4,56 Mbit/s) |

Not: Kitin üreticisi `iot` şablonunda olayları günün saatlerine eşit dağıtıyor (`SAAT_AGIRLIK["iot"] = [1] * 24`). Yani kartın "tepe yük" koşulunu üretilen veri kendisi göstermiyor. Tepe yükü `--hiz` ile ayrı bir deneyde kendimiz oluşturduk; sonuç bölüm 11'de.

## Tek makine mi, dağıtık mı?

- Depolama: 1095 günde 26,98 TB ham JSON birikiyor. Bizim makinenin diski 245 GB (`df -H /`, 17.11.2026). Bu kadar disk ham verinin yaklaşık 10 gününü tutar (245 GB ÷ 24,64 GB/gün ≈ 9,9 gün). Ölçtüğümüz en iyi sıkıştırma (zstd'li Parquet, 12,12 kat) ile bile 3 yıl 2,23 TB ediyor ve 245 GB'a sığmıyor; bunun için 110 katın üstünde bir sıkıştırma gerekirdi. Bu yüzden ham veri için dağıtık nesne deposu (S3 uyumlu) gerekli.
- Akış: Ortalama 285 KB/s, tepe 570 KB/s veri hızı küçük bir hız. Tepe yük deneyinde (bölüm 11) akış işi 8.000 olay/s istenen hızda kayıpsız çalıştı (sayıma girmeyen olay 0). Checkpoint yerel diskteyken ortalama işleme hızı 15.752 olay/s ölçüldü. Yani bu hız tek makinede işleniyor. Ama tek makine olsa bile dayanıklılık için Kafka'nın en az 3 düğümde kopyalanması gerekiyor (kitte tek düğüm, tek kopya). Yani dağıtıklık burada hızdan çok kopyalama gereği.

## İlk hangi bileşen tıkanır?

Tepe yük deneyinde (bölüm 11) ilk tıkanan şey hız değil, uyarı gecikmesi oldu:
- Akış checkpoint'i S3'teyken: yığın başına ~10 sn checkpoint yazımı yüzünden uyarı gecikmesinin üst sınırı 2.000 olay/s'de bile 37,5 sn'ye çıktı ve 30 sn'lik sınırı aştı. Checkpoint'i yerel diske alınca 8.000 olay/s'de en fazla 16,1 sn oldu (bölüm 8.3).
- Spark akış işi ile toplu iş aynı 8 çekirdeği paylaşınca: toplu iş sürerken akış yığınları ortalama 3,0 sn'den 13,8 sn'ye, en uzunu 36,5 sn'ye çıktı (bölüm 11). Bellek taşmadı (toplam en fazla 4,02 GiB, OOM 0).
- Üretici: Tek Python süreci bu makinede en fazla ~7.780 olay/s gönderebildi (`--hiz 8000` istendi). Kartın tepe yükü 4.000 olay/s bunun altında, ama 10x için üretici de tek süreç olarak yetmez.
- Kafka: Tek düğüm, 8 bölüm (bölüm 8.1). Ölçtüğümüz hızlarda Kafka tarafında tıkanma görmedik.

## 10x ve 100x

| Ölçek | Ortalama olay/s | Tepe olay/s | Ortalama veri hızı | Tepe veri hızı | Günlük ham veri | 1095 günlük ham veri |
|---|---|---|---|---|---|---|
| 1x (kart) | 2.000 | 4.000 | 285,2 KB/s | 570,3 KB/s | 24,64 GB | 26,98 TB |
| 10x | 20.000 | 40.000 | 2,85 MB/s | 5,70 MB/s | 246,4 GB | 269,8 TB |
| 100x | 200.000 | 400.000 | 28,5 MB/s | 57,0 MB/s | 2,46 TB | 2,70 PB |

(Hepsi hesap: olay/s × 142,58 bayt; günlük için × 86.400; toplam için × 1.095. MB = 10⁶ bayt, PB = 10¹⁵ bayt.)

Ölçek büyüyünce ne değişir?
- 10x'te: Tepe 40.000 olay/s. Tek düğümlü Kafka'mızın aldığını gösterdiğimiz hız en az 18.000 olay/s (bölüm 8.1); 40.000 için bunu ayrıca göstermedik. Kafka birden çok broker'a, bölüm sayısı da tepe hıza göre yeniden hesaplanmalı. Akış işinin tek makinede ölçtüğümüz en yüksek ortalama hızı 15.752 olay/s, yani 10x'in tepesi tek makinenin ölçtüğümüz kapasitesinin üstünde. Akış işi birden çok yürütücüye dağıtılmalı. Üretici de tek süreç olarak (~7.780 olay/s) yetmez.
- 100x'te: 3 yılda petabayt düzeyine çıkılıyor. Ham JSON'u saklamak yerine sıkıştırılmış Parquet/Delta'ya geçmek ve eski veriyi daha ucuz depolama sınıfına taşımak zorunlu hale gelir. Akış işi çok düğümlü bir kümeye dağıtılmalı.
