# 9. Servis deposu ve pano (Z6, Z9)

## 9.1 Erişim desenleri

Servis deposunu kullananlar Grafana panosu ve operatörün elle attığı sorgular. Kartta p99 sınırı 50 ms. Gecikme, akış işi aynı tabloya yazarken ölçüldü. Her sorgu `pgbench` ile 1000 kez çalıştırıldı (`analiz/p99_olc.sh`, sorgular `servis/pgbench/`, sonuç `kanit/servis-p99.txt`). Ölçüm sırasında tabloda 14 günlük geçmiş vardı: 2,1 milyon satır, 363 MB.

| # | Sorgu | Kim, ne sıklıkla | Okunan veri | p50 | p99 | en kötü |
|---|---|---|---|---|---|---|
| q1 | Sensör X'in son 10 dakikası | Operatör, sensöre tıklayınca | 1 sensör × 10 satır | 0,25 ms | 1,30 ms | 20,9 ms |
| q2 | Sensör X'in geçmişteki bir 10 dakikası | Operatör, arıza incelerken | 1 sensör × 10 satır, 14 günün içinden | 2,03 ms | 8,29 ms | 18,7 ms |
| q3 | Son 10 dakikada anomali olan sensörler (Akış 1) | Pano, 10 sn'de bir | Son 10 dk: ~2.000 satır | 0,41 ms | 0,75 ms | 8,9 ms |
| q4 | Susan sensörler (Akış 2) | Pano, 10 sn'de bir | 200 sensör × son 30 dk | 1,55 ms | 11,34 ms | 37,5 ms |
| q5 | Bölgenin saatlik özeti (Toplu 1) | Pano, 10 sn'de bir | 24 satır | 0,17 ms | 2,61 ms | 27,0 ms |
| q6 | Dakika başına olay, son 1 saat | Pano, 10 sn'de bir | Son 1 saat: ~12.000 satır | 0,61 ms | 9,45 ms | 27,6 ms |

Altı sorgunun hepsi p99'da 50 ms'nin altında. En yavaşı q4 (11,3 ms): 200 sensörün her biri için ayrı bir indeks araması yapıyor (`kanit/servis-explain.txt`). En kötü tekil değerler (20–37 ms) akış işinin yazdığı anlara denk geliyor. Bunlar 1000 sorgudan birkaçı.

Yazma tarafı: akış işi her 10 sn'de bir, değişen pencereleri tek bir işlemde upsert ediyor. `--hiz 50`'de bu yığın başına ~200 satır.

## 9.2 Neden PostgreSQL + TimescaleDB

Sorguların hepsi iki biçimden biri: "bir sensörün bir zaman aralığı" ya da "son N dakikanın bütün sensörleri, gruplanmış". İkisi de zamana göre aralık okuması. Bunun yanında küçük boyut tablosuyla (`sensorler`, 200 satır) join var.

| Seçenek | Neden seçmedik / seçtik |
|---|---|
| **PostgreSQL + TimescaleDB** (seçilen) | Tablo zamana göre parçalara (chunk) bölünüyor [6]. Zaman aralıklı sorgu yalnızca ilgili parçayı okuyor: q2'de 15 parçanın biri okundu. Upsert (`ON CONFLICT`) hazır geliyor; akışın tekrar yazımlarını zararsız kılan da bu. Görünüm, join ve `time_bucket` SQL ile yazılıyor. Grafana'nın PostgreSQL veri kaynağı hazır. |
| MongoDB | Sensör başına belge modeli q1/q2'ye uyar. Ama q3, q4 ve q6 bütün sensörleri gruplayan sorgular, boyut tablosuyla da join istiyorlar. Bunlar aggregation pipeline'a dönüşür. Upsert var ama zaman parçalama yok. |
| Redis | "Son değer" ve "son varış" için çok hızlı. Ama 14 günlük geçmiş (q2) ve gruplama (q6) için ayrı bir depo gerekirdi. Bellekte 363 MB × büyüme demek. |
| Cassandra | Yazma ölçeği için uygun: `(sensor_id, gün)` bölüm anahtarı q1/q2'ye uyar. Ama q3, q4 ve q6 bütün bölümleri tarar. Join yok. Tek düğümlü kurulumda getirisi yok. |

Hacim küçük: `--hiz 50`'de saniyede 50 olay, dakikalık özette dakikada 200 satır. Tek düğümlü PostgreSQL bunu rahat kaldırıyor. Büyüme senaryosu bölüm 9.4'te.

## 9.3 Veri modeli ve anahtar

`servis/sema.sql`:

- **`sensor_dakika`**: sensör başına 1 dakikalık pencere. Birincil anahtar `(sensor_id, pencere_baslangic)`. Bu hem upsert'in çakışma anahtarı hem de q1, q2 ve q4'ün kullandığı indeks: önce sensör, sonra zaman aralığı. Hypertable, `pencere_baslangic` ile 1 günlük parçalara bölünüyor. `son_varis` penceredeki olayların Kafka'ya en son ulaştığı an; Akış 2 bunu kullanıyor. Olay zamanını kullanmıyoruz, çünkü geç gelen bir olay sensörü yanlışlıkla "susmuş" gösterirdi.
- **`anomali_son10dk`**, **`susan_sensorler`** (görünüm): iki akış sorusu. 10 dakikalık ve 60 saniyelik pencere burada uygulanıyor. Akış işi yalnızca dakikalık özeti yazıyor, bu yüzden pencereyi değiştirmek için işi yeniden başlatmak gerekmiyor. `susan_sensorler` boyut tablosundan başlıyor: hiç olayı gelmemiş sensör de listede görünüyor.
- **`toplu1`**, **`toplu2`**: toplu hattın sonuçları (Delta'dan `is/servis_yukle.py` ile yükleniyor).
- **`batch_gunlugu`**: her mikro yığın için bir satır. Bilerek birincil anahtarı yok: aynı yığının iki kez yazıldığını görmek için (bölüm 8.5).

**Upsert ve tekrar:** Akış işi pencerenin o ana kadarki toplamını yazıyor, artışı değil. Aynı yığının iki kez yazılması aynı satırı aynı değerle günceller. Bu yüzden en az bir kez teslim, tabloda tam bir kez sonuç veriyor (bölüm 8.5, deney 2).

## 9.4 Ölçeklenince bölüm/shard anahtarı

Bugün tek düğüm yetiyor. Sensör sayısı 100 katına çıkarsa (20.000 sensör, dakikada 20.000 satır) dağıtık kurulum düşünülür (ör. Citus ya da Cassandra):

- **Yalnızca zamanla bölmek (monoton artan anahtar) yanlış olur.** Bütün yeni yazılar her an tek bir bölüme ("şimdi"ye) gider. Bir düğüm yazı yükünün tamamını taşır, diğerleri boş durur (sıcak nokta). Pano sorguları da hep o bölüme gittiği için okuma yükü de oraya yığılır.
- **Önerilen:** dağıtım anahtarı `sensor_id`'nin hash'i, her düğümün içinde zamana göre parçalama (bugünkü hypertable). Böylece yazılar sensörlere göre bütün düğümlere yayılır. q1/q2 tek bir düğüme gider (sensör anahtarı sorguda var). q3, q4 ve q6 bütün düğümlere paralel gider ve her düğümde yalnızca son parçayı okur.
- Kafka tarafında da anahtar aynı (`sensor_id`, bölüm 8.1). Bir sensörün olayları aynı Kafka bölümünde ve aynı veritabanı düğümünde kalır.

Tek düğümde de "son parça" en çok yazılan parça. TimescaleDB'de bu bilinçli bir tasarım: son parça bellekte kalıyor, eski parçalar dokunulmadan duruyor. Tek makinede sorun değil, sorun yazının tek makineye sığmadığı an başlıyor.

## 9.5 Pano (Z9)

Grafana 11.3, `docker compose --profile servis-ts up -d` ile açılıyor: http://localhost:3000 (giriş istemez, salt okunur). Pano ve veri kaynağı dosyadan geliyor (`grafana/provisioning/`), elle ayar gerekmiyor. Varsayılan görünüm son 15 dakika, **10 sn'de bir yenileniyor**. Bu, akış işinin tetik aralığıyla aynı: daha sık yenilemek yeni veri göstermez.

| Panel | Ne gösteriyor | Veri nereden |
|---|---|---|
| Akış 1 · Son 10 dakikada titreşimi 1,0'ı aşan sensörler | Sensör, bölge, anomali sayısı, en yüksek titreşim, son varış | `anomali_son10dk` (akış) |
| Akış 2 · Susan sensörler | 60 sn'dir olayı ulaşmayan sensörler, kaç saniyedir sessiz | `susan_sensorler` (akış + boyut tablosu) |
| Dakika başına olay ve anomali sayısı | Zaman serisi, iki eksen | `sensor_dakika` (akış), `time_bucket('1 minute')` |
| Toplu 1 · Bölge × saat ortalama sıcaklık | Renkli tablo (ısı haritası), 7 bölge × 24 saat | `toplu1` (toplu hat) |
| Toplu 2 · Modele göre anomali oranı | Çubuk grafik | `toplu2` (toplu hat) |
| Toplu 2 · Veri kaybı %5'i aşan sensörler | Tablo | `toplu2` (toplu hat) |

Akış panellerinde bir olay, Kafka'ya varışından en geç 13–16 sn sonra veritabanında oluyor (bölüm 8.3, yerel checkpoint ile ölçüldü). Panonun 10 sn'lik yenilemesiyle birlikte ekrana en geç ~26 sn'de geliyor; kartın sınırı 30 sn. Toplu paneller ancak toplu iş ve `is/servis_yukle.py` yeniden çalıştırılınca değişiyor.

Ekran görüntüleri:
- `kanit/grafana-pano.png`: normal çalışma (üretici `--hiz 50`). Akış 2 boş, yani bütün sensörlerden olay geliyor. Toplu panellerde 14 günlük sonuçlar var.
- `kanit/kirilma-deneyi-kafka-grafana.png`: Kafka durdurulduğunda. Akış 2 panelinde bütün sensörler susmuş görünüyor, zaman serisi 23:55'te kesiliyor (bölüm 11).
