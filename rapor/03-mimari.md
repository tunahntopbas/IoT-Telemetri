# 3. Mimari (Z0) — taslak

Sürümler `docker-compose.yml`, `araclar/Dockerfile` ve `spark-defaults.conf` dosyalarından okundu. Çalışan kapsayıcılarda da kontrol edildi (`kafka-topics.sh --version`, `spark-submit --version`, `weed version`, Python paket sürümleri; 17.11.2026). Henüz kurulmamış bileşenlerin sürümü "?" olarak bırakıldı.

## Omurga şeması

```
AKIŞ HATTI
┌──────────────────────────┐   ┌──────────────────────────────┐   ┌──────────────────────────────┐
│ Olay kaynağı             │   │ Toplama                      │   │ Günlük / kuyruk              │
│ 200 sensör (benzetim)    │──▶│ Kafka üreticisi              │──▶│ Apache Kafka 4.0.0 (KRaft)   │
│ araclar/uretici.py       │   │ confluent-kafka 2.6.1        │   │ konu: sensorler, 3 bölüm     │
│ Python 3.12.16           │   │ (librdkafka 2.6.1)           │   │ anahtar: sensor_id           │
└──────────────────────────┘   │ acks=all, idempotent, lz4    │   └──────────────┬───────────────┘
                               └──────────────────────────────┘                  │
                                                                                 ▼
┌──────────────────────────┐   ┌──────────────────────────────┐   ┌──────────────────────────────┐
│ Sunum                    │   │ Servis deposu                │   │ Akış işleme                  │
│ Grafana ?                │◀──│ PostgreSQL ? + TimescaleDB ? │◀──│ Spark 3.5.3 Structured       │
│                          │   │                              │   │ Streaming (Scala 2.12.18,    │
└──────────────────────────┘   └──────────────────────────────┘   │ Java 17.0.12)                │
                                                                  │ spark-sql-kafka 3.5.3        │
                                                                  │ is/akis_iskelet.py           │
                                                                  └──────────────────────────────┘
TOPLU HAT
┌──────────────────────────┐   ┌──────────────────────────────┐   ┌──────────────────────────────┐
│ Kaynak                   │   │ Ham depolama                 │   │ Toplu işleme                 │
│ uretici.py --toplu       │──▶│ SeaweedFS 4.48 (S3 uyumlu)   │──▶│ Spark 3.5.3 + Delta Lake     │
│ (geçmiş günler, JSONL)   │   │ s3://ham/iot/tarih=…/        │   │ 3.2.1, hadoop-aws 3.3.4      │
│ + boyut: sensorler.csv   │   │ s3://ham/boyut/iot/          │   │ is/toplu_is.py               │
└──────────────────────────┘   └──────────────────────────────┘   │ çıktı: s3://tablolar (Delta) │
                                                                  └──────────────┬───────────────┘
                                                                                 ▼
                                                                  ┌──────────────────────────────┐
                                                                  │ Servis deposu (aynı)         │
                                                                  │ PostgreSQL ? + TimescaleDB ? │
                                                                  └──────────────────────────────┘
Yardımcı: DuckDB 1.1.3, pyarrow 18.0.0, pandas 2.2.3, boto3 1.35.54 (araclar kapsayıcısı)
Altyapı: Docker 29.9.2, Docker Compose 5.6.1, Apple M1, 8 GB RAM, Docker'a 4.786 GiB
```

Açık sorular (karar verilecek):
- Akış işinin çıktısı doğrudan servis deposuna mı yazılacak, yoksa önce S3'e (kitte `--cikti s3` Parquet) yazılıp oradan mı taşınacak?
- Toplu hattın sonuçları servis deposuna hangi araçla ve hangi sıklıkla yüklenecek?
- PostgreSQL, TimescaleDB ve Grafana için kullanılacak imaj sürümleri compose'a eklendiğinde yazılacak.

## Garanti tablosu (iskelet)

| Ok | Kaç kopya (kitte → üretimde) | Sıra garantisi | Ağ bölünürse ne olur |
|---|---|---|---|
| Sensör → üretici | 1 → ? (sensör tarafında tampon olup olmadığı karar) | Tek sensörün olayları üretim sırasıyla gönderilir; ama olayların %5'i geç geldiği için olay zamanı sırası zaten bozuk (ölçüm: %4,81, en fazla 118,7 sn) | Kitte kaynak ile üretici aynı süreç. Gerçekte sensör bağlantıyı kaybederse ya tamponlar (geç olay) ya da veri kaybolur, ölçülecek |
| Üretici → Kafka | 1 → 3 (kitte `replication factor = 1`) | `acks=all` + idempotent üretici: aynı bölümde sıra korunur, yeniden denemede kopya oluşmaz. Sıra yalnızca bölüm içinde, yani aynı `sensor_id` için geçerli | Üretici yeniden dener; `acks=all` ile lider kopya ve ISR'ın yazdığından emin olunca onay gelir. Kitte tek düğüm olduğu için Kafka düşerse yazma durur. `docker compose stop kafka` deneyiyle ölçülecek (Z11) |
| Kafka → Spark akış | 1 → 3 | Bölüm içinde ofset sırası. Spark pencereleri olay zamanına göre kurar, varış sırasına göre değil | Spark ofsetleri checkpoint'e yazar, bağlantı dönünce kaldığı yerden okur. Kayıp ya da tekrar olup olmadığı YENİDEN BAŞLATMA DENEYİ ile ölçülecek (Z8) |
| Spark akış → servis deposu | ? | Yok (pencere sonuçları; sıra gerekmiyor, anahtar sensör + pencere) | ? (idempotent upsert ile tekrar yazma zararsız hale getirilecek, karar verilecek) |
| Üretici (toplu) → S3 ham | 1 → ? (kitte tek düğüm SeaweedFS, çoğaltma yok) | Sıra yok, dosya bazında | S3'e yazılamazsa yazma hata verir. Kısmi dosya tehlikesi Z4/Z7'de |
| S3 ham → Spark toplu → S3 Delta | 1 → ? | Sıra yok | Delta işlem günlüğü sayesinde yazma ya tamamen görünür ya hiç; çıplak Parquet ile farkı Z7'deki Ctrl+C deneyiyle gösterilecek |
| Delta → servis deposu | ? | ? | ? |
| Servis deposu → Grafana | ? | — | Grafana sorgu atamaz, pano boş/eski görünür |

## Kitin tek makine sadeleştirmeleri ve üretimde ne olmalı

Kitin README'sinde (§8) listelenenler ve bizim notlarımız:

| Kitte | Neden sorun | Üretimde |
|---|---|---|
| Kafka tek düğüm, `replication factor = 1` (`KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1`) | Düğüm ölürse veri gider | En az 3 broker, `replication.factor=3`, `min.insync.replicas=2` |
| Kafka konuları varsayılan 3 bölüm (`KAFKA_NUM_PARTITIONS: 3`) | Bölüm sayısı tüketici paralelliğinin üst sınırı | Kartın tepe hızına göre hesaplanmış bölüm sayısı (Z8) |
| SeaweedFS tek düğüm, çoğaltma yok (`replicaPlacement:000` günlükte görülüyor) | Disk bozulursa ham veri gider | Çoğaltmalı ya da silme kodlu (erasure coding) nesne deposu |
| Spark `local[*]`, `spark.driver.memory 1g` | Sürücü ve yürütücüler aynı süreçte, gerçek küme yok | Ayrı sürücü ve yürütücülerle küme (ör. Kubernetes ya da YARN) |
| Güvenlik kapalı: şifre yok, TLS yok, S3 anahtarı düz metin (`bsm461` / `bsm461-gizli`) | Ağdaki herkes okuyup yazabilir | TLS, kimlik doğrulama, gizli bilgilerin ayrı bir kasada tutulması |
| Üretici tek Python süreci | Sensör ağının kendisi değil, benzetimi | Binlerce bağımsız cihaz; ağ kopmaları ve saat kaymaları gerçek |
| Ortak bellek: Docker'a 4.786 GiB, hepsi tek makinede | Bir bileşen belleği doldurursa diğerleri de etkilenir | Bileşenler ayrı makinelerde, kaynak sınırları ayrı |

Bizim eklediğimiz tek değişiklik: `s3` servisinin sağlık denetimi `localhost` yerine `127.0.0.1` kullanıyor (README'deki "Kitten farklar" bölümü). Bu bir sadeleştirme değil, Apple M1 makinemizde S3 kapsayıcısının sağlıklı görünmesi için gerekliydi.
