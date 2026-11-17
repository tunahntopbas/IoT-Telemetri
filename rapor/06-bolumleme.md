# 6. Bölümleme (Z3)

Ölçümler `analiz/olcum_bolumleme.py` ile 3.000.000 satırlık ham veride yapıldı (`kanit/olcumler.csv`, `z3_*` satırları). Spark'ın kendi hash bölümleyicisi kullanıldı: `repartition(8, anahtar)` sonrası `spark_partition_id()` ile her bölümdeki satır sayıldı. Ölçü: en dolu bölüm ÷ ortalama. Değer 1,0'a ne kadar yakınsa dağılım o kadar dengeli. 8 bölümde ortalama 375.000 satır.

## 6.1 Aday bölüm anahtarları

| Anahtar | Farklı değer | 8 bölümdeki satırlar | Boş bölüm | En dolu ÷ ortalama |
|---|---|---|---|---|
| `sensor_id` | 200 (+ null) | 318.635 / 373.280 / 496.751 / 271.677 / 293.988 / 404.506 / 389.280 / 451.883 | 0 | 1,325 |
| `bolge` | 7 (+ null) | 0 / 1.288.793 / 1.170 / 853.849 / 0 / 0 / 420.418 / 435.770 | 3 | 3,437 |
| `tarih` | 14 | 0 / 428.580 / 428.570 / 214.285 / 1.071.425 / 428.570 / 214.285 / 214.285 | 1 | 2,857 |
| `saat` (Türkiye saati) | 24 | 499.075 / 627.469 / 250.199 / 499.353 / 0 / 374.913 / 374.687 / 374.304 | 1 | 1,673 |
| `sensor_id` + `saat` | 4.800 | 387.509 / 372.884 / 356.775 / 391.238 / 377.795 / 382.841 / 375.891 / 355.067 | 0 | 1,043 |
| `bolge` + tuz (0–3) | 28 | 754.792 / 217.538 / 108.936 / 641.842 / 535.762 / 209.939 / 108.771 / 422.420 | 0 | 2,013 |
| `bolge` + tuz (0–7) | 56 | 537.657 / 217.371 / 375.218 / 535.204 / 537.311 / 213.356 / 213.276 / 370.607 | 0 | 1,434 |

Ne görüyoruz?
- Veri anahtarlar arasında neredeyse eşit dağılıyor: sensör başına olay sayısı en çok ortalamanın 1,10 katı (bölüm 10). Sıcak noktayı tek tek anahtarlar değil, farklı değer sayısının az olması yaratıyor. 7 bölge 8 bölüme dağıtılınca bölgeler hash'e göre çakışıyor: 3 bölüm boş kalıyor, bir bölüme 3 bölge düşüyor (1.288.793 satır). `tarih` için de aynı durum geçerli: 14 gün 8 bölüme düşünce bir bölüm 5 günü taşıyor.
- `sensor_id`'de bile oran 1,325. 200 anahtar 8 bölüme her bölüme 25 anahtar düşecek kadar eşit dağılmıyor.
- `sensor_id` ve `saat` birlikte kullanılınca 4.800 farklı değer oluşuyor ve oran 1,043'e iniyor.

## 6.2 Sıcak nokta ve çözüm

En kötü aday `bolge`. Toplu 1 sorusu bölgeye göre gruplandığı için bu sorun gerçekten karşımıza çıkıyor. İki çözüm denedik:
- Tuzlama: Her satıra rastgele bir tuz (0–3 ya da 0–7) ekleyip `(bolge, tuz)` ile bölümlemek. Önce 3,437 olan oran 4 tuzla 2,013'e, 8 tuzla 1,434'e indi. Bedeli şu: aynı bölgenin satırları birkaç bölüme dağılıyor, bu yüzden toplulaştırma iki adımda yapılmalı (önce `bolge, tuz`, sonra `bolge`).
- Anahtar değişikliği: Toplu 1'i `(bolge, saat)` ile gruplamak. Bu, sorunun zaten istediği şey. Planda (`kanit/explain-1.txt`, Toplu 1) Spark'ın `hashpartitioning(bolge, saat_tr, 8)` ile bölümlediği görülüyor. 168 farklı değer olduğu için tek başına `bolge`'deki sorun ortaya çıkmıyor.

Toplu 1'de ikinci yolu kullandık, ek kod gerektirmiyor. Ayrıca Spark'ın AQE özelliği shuffle'dan sonra küçük bölümleri birleştiriyor. Tuzlamayı gerçek bir eşitsizlik gördüğümüz yer için (akışta bölgeye göre gruplama gerekirse) aklımızda tutuyoruz.

## 6.3 Kafka bölümleri

Kafka'da mesaj anahtarı `sensor_id`. Üreticimiz `confluent-kafka` (librdkafka 2.6.1) ve Spark'tan farklı bir bölümleyici kullanıyor. Bu yüzden Spark ölçümünü Kafka'ya taşımadık, Kafka'da ayrıca ölçtük:
- 3 bölümlü `olcum-bayt` konusu: 1.934 mesaj, 743 / 637 / 554 → oran 1,15 (bölüm 10'daki ölçüm).
- 8 bölümle elle açılan `bolum-deneme-8` konusu (`--hiz 500 --sure 20`, 9.773 olay, Kafka'da 9.793 mesaj): 1.256 / 1.264 / 1.103 / 1.223 / 1.184 / 1.207 / 1.255 / 1.301 → oran 1,063 (`z3_kafka_8bolum_dagilim`).

Asıl `sensorler` konusunun bölüm sayısını kartın hızına göre akış hattında (bölüm 8, Z8) hesaplayacağız. O konu üreticinin otomatik açtığı 3 bölümle duruyor; silinip doğru bölüm sayısıyla yeniden açılacak. Sıra garantisi Kafka'da yalnızca bölüm içinde geçerli. Anahtar `sensor_id` olduğu için tek bir sensörün mesajları hep aynı bölüme gidiyor ve kendi aralarında sıralı kalıyor.

## 6.4 Çoğaltma

| Bileşen | Kitte kopya | Üretimde | Neden |
|---|---|---|---|
| Kafka | 1 (`KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1`, konular da 1) | 3, `min.insync.replicas=2` | Bir broker düşünce veri kaybolmamalı ve yazma durmamalı. 3 kopyada biri giderse hâlâ 2 kopya kalıyor ve `acks=all` ile yazma sürebiliyor. |
| S3 (SeaweedFS) | 1 (günlükte `replicaPlacement:000`) | Çoğaltmalı ya da silme kodlu depo (ör. 3 kopya ya da eşdeğeri) | Ham veri 1095 gün saklanacak. Tek disk bu sürede bozulabilir ve ham veri yeniden üretilemez. |
| Servis deposu (PostgreSQL + TimescaleDB) | henüz kurulmadı | 1 birincil + en az 1 eşzamanlı yedek | Pano ve uyarılar bu depodan okunuyor; birincil düşerse yedek devralmalı. |

## 6.5 Ağ bölünmesi: C mi A mı? (CAP ve PACELC)

| Bileşen | Bölünmede (P) | Bölünme yokken (E) | Gerekçe |
|---|---|---|---|
| Kafka (`acks=all`, `min.insync.replicas=2`, `unclean.leader.election.enable=false`) | C: ISR 2'nin altına düşerse yazma reddedilir | C (L yerine): her yazma yeterli kopya onaylayınca tamamlanır, bu gecikmeyi artırır | Tutarlılık yerine erişilebilirlik isteseydik `acks=1` ve unclean leader election açık olurdu; bu da onaylanmış veri kaybı demek. |
| S3 uyumlu nesne deposu | Kurulum türüne bağlı; kitteki tek düğümde bölünme düğümün tamamen erişilemez olması demek | — | AWS S3 2020'den beri yazdıktan sonra okumada güçlü tutarlılık sağlıyor. Kendi kurduğumuz bir depoda bu ayrıca doğrulanmalı. |
| PostgreSQL + TimescaleDB (eşzamanlı yedekli) | C: yedeğe ulaşılamazsa onay bekleyen yazmalar durur | C: her yazma yedeğin onayını bekler | Uyarıların tutarlı olması, birkaç saniyelik yazma beklemesinden daha önemli. Pano okuması yedekten de yapılabilir. |

PACELC sınıfı olarak üçünü de PC/EC kabul ediyoruz. Bu sınıflandırma tasarım kararımız; Kafka için kırılma deneyinde (`docker compose stop kafka`, Z11) üreticinin davranışını ölçeceğiz.

## 6.6 Tekrar eden olaylar sonucu değiştiriyor mu? (idempotentlik)

| Ölçüm | Tekrarlı | Tekrarsız | Fark | Kaynak |
|---|---|---|---|---|
| Satır sayısı | 3.000.000 | 2.994.095 | 5.905 (%0,197) | `z3_tekrar_eden_satir` |
| Ortalama sıcaklık | 21,73041 °C | 21,73035 °C | 0,00006 °C | `z3_tekrar_etkisi_ort_sicaklik` |
| Titreşim > 1,0 kayıt sayısı | 12.076 | 12.054 | 22 | `z3_tekrar_etkisi_anomali_sayisi` |
| Sensör başına olay sayısı, en çok etkilenen | 15.586 | 15.535 (`sensor-149`) | 51 olay (%0,33) | `z3_tekrar_etkisi_sensor_sayim_en_fazla` |

Ortalamalar tekrarlardan neredeyse etkilenmiyor, çünkü tekrar eden olay da aynı değeri taşıyor. Sayım soruları ise doğrudan etkileniyor. Anomali sayısı ve sensör başına olay sayısı tekrarlar kadar şişiyor. Toplu 2'de kayıp oranı "gelen ÷ beklenen" ile hesaplandığı için tekrarlar kaybı olduğundan küçük gösterir. Bir sensörde 51 olaylık fark %0,33 kayıp oranı demek; bu da kartın %1'lik kayıp sınırının üçte biri. Bu yüzden toplu hatta tekrarları bütün alanlar üzerinden ayıklıyoruz. Bir günü yeniden işlerken de `replaceWhere` kullanıyoruz (bölüm 5.3); iş iki kez çalışsa da sonuç değişmiyor. Akışta aynı sorun Z8'de ele alınacak.
