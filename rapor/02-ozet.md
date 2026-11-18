# 2. Özet

## Sorularımız

Senaryo kartımız: saniyede ortalama 2.000 ölçüm yapan 200 sensörlük bir ağ. Akış sorularının cevabı en geç 30 sn'de çıkmalı, olayların %5'i 2 dk'ya kadar geç geliyor ve sayıma girmeyen olay en fazla %1 olabilir. Ham veri 1095 gün saklanıyor, servis sorgusunda p99 en fazla 50 ms.

| Soru | Cevap nerede | Kısaca nasıl |
|---|---|---|
| **Akış 1:** Son 10 dakikada titreşimi eşiği (1,0) aşan sensörler | Görünüm `anomali_son10dk`, panoda ilk panel | Sensör başına 1 dk'lık pencere, update modu ile her tetikte yazılıyor; eşik toplu verideki boşluktan seçildi (bölüm 7) |
| **Akış 2:** 60 sn'dir olayı ulaşmayan (susan) sensörler | Görünüm `susan_sensorler` | Kafka'ya varış zamanına bakılıyor, olay zamanına değil: geç gelen olay sensörü susmuş göstermiyor; hiç olayı gelmeyen sensör de boyut tablosundan görünüyor |
| **Toplu 1:** Bölgelere göre günün saatine bağlı sıcaklık ve titreşim deseni | Delta `iot_toplu1`, panoda ısı haritası | 3 milyon olay, 14 gün, Türkiye saatine göre bölge × saat |
| **Toplu 2:** Sensör başına veri kaybı ve anomali oranı, model ve kurulum yılına göre | Delta `iot_toplu2` | Boyut tablosuyla broadcast join; beklenen olay sayısı = sensörlerin ortanca olay sayısı |

## Ne kurduk

Kitin üstüne iki hat kurduk (Şekil 3.1). **Toplu hat:** S3'teki ham JSON → Spark ile tekrar ayıklama, karantina ve boyut join'i → tarih bölümlü, zstd sıkıştırmalı Delta tablosu → iki toplu soru. **Akış hattı:** Kafka (8 bölüm, anahtar `sensor_id`) → Spark Structured Streaming (2 dk watermark, update modu, yerel checkpoint) → TimescaleDB'ye upsert. İki hattın sonucu aynı TimescaleDB'de; Grafana panosu 10 sn'de bir yenileniyor.

Kartın koşullarını ölçerek sağladık:
- Sayıma girmeyen olay %0: kart oranında, %30 ve %50 geç veride, yeniden başlatma ve çökmede (sınır %1).
- En kötü uyarı gecikmesi 13,4 sn (saniyede 50 olay), 16,1 sn (saniyede 8000 olay); sınır 30 sn.
- Servis sorgularında p99 en fazla 11,3 ms; sınır 50 ms.

## Üç bulgu

1. **Watermark uyarıyı geciktirmiyor, checkpoint geciktiriyor.** Kartın 30 sn'lik uyarı süresi ile 2 dk'lık geç veri sınırı ilk bakışta çelişiyor. Update modunda watermark yalnızca geç olayın ne kadar süre kabul edileceğini belirliyor; sonucun ne zaman yazılacağını belirlemiyor. 30 sn'lik watermark kart oranında bile olayların %4,13'ünü attı, 2 dk'lık watermark hiçbirini atmadı. Uyarıyı asıl geciktiren şey checkpoint'in S3'te olmasıydı: her yığında durumun S3'e yazılması ~10 sn sürüyordu ve en kötü gecikme 29,9–37,5 sn'ye çıkıyordu. Yerel diske alınca 13,4 sn'ye indi (bölüm 8.3).
2. **Akış hattı kesintide kayıpsız ama kendiliğinden dönmüyor.** Kafka 60 sn'den uzun ya da veritabanı kısa süre kapalı kalınca akış işi ölüyor. Aynı checkpoint ile yeniden başlatınca kayıp 0, tekrar 0. Checkpoint kaybolur da iş "yalnızca yeni olaylar" ayarıyla başlatılırsa kayıp %21,9: upsert eski pencereleri eksik sayılarla eziyor (bölüm 11).
3. **Veride iki desen var.** Titreşim anomalileri bölgeden bağımsız olarak hep saat 21.00'de yığılıyor. TX-200 modelinin anomali oranı (%1,17) diğer modellerin yaklaşık 4 katı. Veri kaybı ise modelle değil sensör numarasıyla ilgili: 13'ün katı olan 15 sensör olaylarının %28,7–%30,6'sını, 23'ün katı olan 8 sensör %7,3–%10,1'ini kaybediyor (bölüm 7, 10).
