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

Hesaplarda 142,58 bayt kullanıldı. Kafka'nın kayıt başlığı, anahtar ve sıkıştırma etkisi bu sayıya dahil değil. Kafka'daki gerçek disk kullanımı Z8'de ayrıca ölçülecek.

## Ham veri hacmi (ortalama hızla)

| Büyüklük | Hesap | Sonuç |
|---|---|---|
| Saniyelik veri | 2.000 olay/s × 142,58 bayt | 285.160 bayt/s ≈ 285,2 KB/s ≈ 2,28 Mbit/s |
| Günlük olay sayısı | 2.000 × 86.400 | 172,8 milyon olay/gün |
| Günlük ham veri | 285.160 bayt/s × 86.400 s | 24,64 GB/gün |
| 1095 günlük ham veri | 24,64 GB/gün × 1.095 gün | 26,98 TB |

(GB = 10⁹ bayt, TB = 10¹² bayt. Hepsi hesap; girdileri yukarıdaki tabloda.)

## Parquet'te ne olur?

Adım 6'da ölçülecek. Aynı veriyi JSON ve Parquet (snappy, zstd, gzip) olarak yazıp toplam boyutları karşılaştıracağız (Z2). Sıkıştırma oranı ölçülmeden 1095 günlük Parquet boyutu hesaplanmayacak.

| Biçim | Ölçülen oran (JSON'a göre) | 1095 günlük boyut |
|---|---|---|
| JSON (ham) | 1,00 | 26,98 TB |
| Parquet + snappy | ölçülecek | ölçülecek |
| Parquet + zstd | ölçülecek | ölçülecek |
| Parquet + gzip | ölçülecek | ölçülecek |

## Ortalama mı, tepe mi?

İkisi farklı yerlerde kullanılıyor:
- Depolama ortalama hızla hesaplanır. Tepe yük günde bir saat sürüyor; diski günün toplamı doldurur. Bu yüzden 26,98 TB ortalamadan geliyor.
- İşleme kapasitesi (Kafka bölüm sayısı, akış işinin saniyede işleyebileceği olay, ağ) tepe yüke göre seçilir. Akış sorusunun cevabı 30 sn içinde çıkmalı. En yoğun saatte sistem geri kalırsa uyarı tam da en çok olay olan anda gecikir.

| Büyüklük | Ortalama | Tepe (×2) |
|---|---|---|
| Olay/s | 2.000 | 4.000 |
| Veri hızı | 285,2 KB/s (2,28 Mbit/s) | 570,3 KB/s (4,56 Mbit/s) |

Not: Kitin üreticisi `iot` şablonunda olayları günün saatlerine eşit dağıtıyor (`SAAT_AGIRLIK["iot"] = [1] * 24`). Yani kartın "tepe yük" koşulunu üretilen veri kendisi göstermiyor. Tepe yükü `--hiz` ile ortalamanın 2 katı hızla ayrı bir deneyde kendimiz oluşturacağız (Z11).

## Tek makine mi, dağıtık mı?

- Depolama: 1095 günde 26,98 TB ham JSON birikiyor. Bizim makinenin diski 245 GB (`df -H /`, 17.11.2026). Bu kadar disk ham verinin yaklaşık 10 gününü tutar (245 GB ÷ 24,64 GB/gün ≈ 9,9 gün). Parquet sıkıştırması bu sayıyı değiştirecek ama 3 yılı tek makineye sığdırmak için 100 katın üstünde bir sıkıştırma gerekir. Bu yüzden ham veri için dağıtık nesne deposu (S3 uyumlu) gerekli.
- Akış: Ortalama 285 KB/s, tepe 570 KB/s veri hızı küçük bir hız. Bu hızın tek makinede işlenip işlenemeyeceğini Z11'deki tepe yük deneyinde ölçeceğiz. Ama tek makine olsa bile dayanıklılık için Kafka'nın en az 3 düğümde kopyalanması gerekiyor (kitte tek düğüm, tek kopya). Yani dağıtıklık burada hızdan çok kopyalama gereği.

## İlk hangi bileşen tıkanır?

Ölçülecek. Adaylar ve neden aday oldukları:
- Spark akış işi: Kitin README'sindeki ölçüme göre yük altında en çok belleği Spark kullanıyor (2,6 GB). Bizim Docker'a ayrılan bellek 4,786 GiB (`docker info`). Tepe yükte mikro yığın süresi 30 sn tetikleme aralığını aşarsa akış geri kalır.
- Üretici: Tek Python süreci. `--hiz 100` ile 20 sn'de 1930 olay gönderdi (`olcumler.csv`), ama 2.000 ve 4.000 olay/s'de ölçmedik.
- Kafka: Tek düğüm, 3 bölüm. Bölüm sayısı tüketici paralelliğini sınırlıyor.

Deney planı: üreticiyi 2.000 ve 4.000 olay/s ile çalıştırıp akış işinin Spark UI'daki "Input Rate" ve "Process Rate" değerlerini, mikro yığın sürelerini ve tüketici gecikmesini (consumer lag) kaydetmek. Sonuçlar `olcumler.csv`'ye yazılacak.

## 10x ve 100x

| Ölçek | Ortalama olay/s | Tepe olay/s | Ortalama veri hızı | Tepe veri hızı | Günlük ham veri | 1095 günlük ham veri |
|---|---|---|---|---|---|---|
| 1x (kart) | 2.000 | 4.000 | 285,2 KB/s | 570,3 KB/s | 24,64 GB | 26,98 TB |
| 10x | 20.000 | 40.000 | 2,85 MB/s | 5,70 MB/s | 246,4 GB | 269,8 TB |
| 100x | 200.000 | 400.000 | 28,5 MB/s | 57,0 MB/s | 2,46 TB | 2,70 PB |

(Hepsi hesap: olay/s × 142,58 bayt; günlük için × 86.400; toplam için × 1.095. MB = 10⁶ bayt, PB = 10¹⁵ bayt.)

Ölçek büyüyünce ne değişir? Bunları tepe yük deneyinden sonra ölçümle destekleyeceğiz:
- 10x'te: Kafka bölüm sayısı tepe hıza göre yeniden hesaplanmalı (Z8). Spark akış işi tek makinede kalabilir mi, ölçülecek.
- 100x'te: 3 yılda petabayt düzeyine çıkılıyor. Ham JSON'u saklamak yerine sıkıştırılmış Parquet/Delta'ya geçmek ve eski veriyi daha ucuz depolama sınıfına taşımak zorunlu hale gelir. Akış işi çok düğümlü bir kümeye dağıtılmalı.
