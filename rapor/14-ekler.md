# Ek: Veri sözlüğü

Kaynak: `araclar/uretici.py` (şablon `iot`, grup `G221210024`). Değer aralıkları 17.11.2026'da üretilen 200.000 satırlık toplu örnekten (2 gün, `--toplu 200000 --gun 2`) DuckDB ile ölçüldü (`kanit/olcumler.csv`). Kafka konusu: `sensorler`, mesaj anahtarı: `sensor_id`.

## Olay: `sensorler` konusu / `s3://ham/iot/tarih=YYYY-MM-DD/*.jsonl`

Her olay tek satırlık bir JSON nesnesi. Ortalama boyut 142,58 bayt (akış, 1934 mesaj), 142,57 bayt (toplu, 200.000 satır).

| Alan | Anlam | Tip | Birim | Örnek | Ölçülen aralık / not |
|---|---|---|---|---|---|
| `olay_zamani` | Ölçümün sensörde alındığı an (olay zamanı) | metin, ISO 8601, milisaniyeli, UTC (`Z`) | — | `2026-11-17T17:17:18.855Z` | Akışta olayların %4,81'i Kafka'ya yazılma anından 5 sn'den daha eski geldi, en büyük gecikme 118,7 sn. Pencereleme bu alana göre yapılacak. |
| `sensor_id` | Sensörün kimliği | metin | — | `sensor-066` | `sensor-001` … `sensor-200`, 200 farklı değer. Kafka anahtarı. 41 kayıtta null, 39 kayıtta alan hiç yok. |
| `bolge` | Sensörün bulunduğu coğrafi bölge | metin (7 değer) | — | `İç Anadolu` | Marmara, Ege, Akdeniz, İç Anadolu, Karadeniz, Doğu Anadolu, Güneydoğu. Üreticide bölge sensör numarasından türetiliyor (`sensör no % 7`), yani her sensörün bölgesi sabit. |
| `sicaklik` | Ölçülen sıcaklık | ondalıklı sayı (2 basamak) | °C | `18.86` | min 5,66 · ortanca 20,71 · ort 21,71 · %99 36,37 · max 73,38. Üretici kodunda bölgeye göre taban değer ve gün içinde ±4 °C'lik bir döngü var; bunu veride Toplu 1 sorusuyla göstereceğiz. 45 °C üstü 500 kayıt (anomali). |
| `nem` | Bağıl nem | ondalıklı sayı (1 basamak) | % (varsayım) | `53.0` | min 33,2 · ort 52,2 · max 73,2. Kit birimi yazmıyor; 0–100 aralığında kaldığı için bağıl nem yüzdesi kabul ettik. |
| `titresim` | Titreşim şiddeti | ondalıklı sayı (3 basamak) | belirtilmemiş (g kabul edilebilir) | `0.271` | min 0,001 · ort 0,210 · %99 0,324 · max 3,998. Normalde ~0,2 civarında; 1,0 üstü 782 kayıt (anomali). Kit birimi yazmıyor, raporda varsayım olarak belirtilecek. |

## Boyut tablosu: `s3://ham/boyut/iot/sensorler.csv`

200 satır, 6.421 bayt. Olaylarla `sensor_id` üzerinden join yapılır.

| Alan | Anlam | Tip | Birim | Örnek | Not |
|---|---|---|---|---|---|
| `sensor_id` | Sensör kimliği (join anahtarı) | metin | — | `sensor-001` | 200 tekil değer, olaylardaki `sensor_id` ile aynı küme. |
| `bolge` | Sensörün bölgesi | metin | — | `Ege` | Bölge başına 28–29 sensör. Olaydaki `bolge` ile aynı olmalı; olayda null/eksik olduğunda buradan doldurulabilir. |
| `model` | Sensör modeli | metin (5 değer) | — | `SN-10` | SN-12 48, KM-5 45, SN-10 43, TX-100 40, TX-200 24 sensör. |
| `kurulum_yili` | Sensörün kurulduğu yıl | tam sayı | yıl | `2020` | 2018–2025. |
