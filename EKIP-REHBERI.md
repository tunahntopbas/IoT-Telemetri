# Ekip rehberi: projede ne yaptık?

Bu dosya grubumuz için yazıldı: Tunahan, Zeynep, Behiç. Amaç, kodu açmadan projenin ne yaptığını, neyi neden seçtiğimizi ve sunumda hangi sayıları söyleyeceğimizi anlamak. Ayrıntı ve bütün ölçümler raporda (`rapor/BSM461_Proje_1A_G221210024.pdf`). Burada sadeleştirilmiş hâli var.

---

## 1. Konu ne?

Hoca her gruba bir "senaryo kartı" verdi (`kanit/senaryo.txt`). Bizimki bir **sensör ağı**: 200 sensör sürekli sıcaklık, nem ve titreşim ölçüyor. Kart bize şu koşulları koyuyor:

| Koşul | Anlamı |
|---|---|
| Saniyede 2.000 ölçüm, yoğun saatte 2 katı | Sistemin taşıması gereken yük |
| Uyarı en geç 30 sn'de çıkmalı | Bir sensörde sorun olunca 30 sn içinde ekranda görmeliyiz |
| Olayların %5'i 2 dk'ya kadar geç gelir | Bazı ölçümler ağda gecikiyor |
| Sayıma girmeyen olay en fazla %1 | Geç gelenleri de saymamız lazım, atarsak en fazla %1 |
| Ham veri 1095 gün (3 yıl) saklanır | Disk hesabı buna göre |
| Sorgu cevabı p99'da en fazla 50 ms | Sorguların %99'u 50 ms'nin altında dönmeli |

Gerçek sensörümüz yok. Kitteki `araclar/uretici.py` bu sensörleri taklit ediyor ve bizim grup koduna göre hep aynı veriyi üretiyor. Veride bilerek bozukluklar var: tekrar eden olaylar, eksik alanlar, geç gelen olaylar.

## 2. Hangi soruları cevaplıyoruz?

Hoca 2 "akış" (anlık) ve 2 "toplu" (geçmişe bakan) soru istedi.

- **Akış 1:** Son 10 dakikada titreşimi 1,0'ı geçen sensörler hangileri? (arıza belirtisi)
- **Akış 2:** Hangi sensör 60 saniyedir hiç veri göndermiyor? (susan sensör)
- **Toplu 1:** Bölgelere ve günün saatine göre sıcaklık ve titreşim nasıl değişiyor?
- **Toplu 2:** Hangi sensör ne kadar veri kaybediyor, hangi model daha çok anomali üretiyor?

## 3. Sistemin parçaları (bir benzetmeyle)

Şekil: `rapor/sekiller/mimari.png`. Soldan sağa okunuyor.

| Parça | Ne işe yarıyor | Benzetme |
|---|---|---|
| **Kafka** | Sensörlerden gelen olayları sırayla tutan bir kuyruk. Okuyan program çökerse kaldığı yerden devam edebiliyor | Postane: mektuplar sırayla rafa konuyor, alıcı gelince alıyor |
| **SeaweedFS (S3)** | Ham dosyaları saklayan depo. Amazon S3 ile aynı arayüz | Dosya arşivi |
| **Spark** | Büyük veriyi işleyen motor. Hem geçmiş veriyi (toplu) hem akan veriyi (akış) işliyor | Fabrika bandı |
| **Delta Lake** | Spark'ın yazdığı tablolara "ya hep ya hiç" garantisi ekliyor | Yarım kalan yazmayı geri alan bir kayıt defteri |
| **TimescaleDB** | Sonuçların durduğu veritabanı (PostgreSQL + zaman serisi eklentisi) | Vitrin: pano buradan okuyor |
| **Grafana** | Pano. Tabloları ve grafikleri 10 sn'de bir yeniliyor | Ekran |

**İki hat var:**
- **Toplu hat:** 3 milyon olaylık 14 günlük geçmiş → S3 → Spark ile temizle → Delta tablosu → Toplu 1 ve Toplu 2 → TimescaleDB.
- **Akış hattı:** Sensörler → Kafka → Spark Streaming (her 10 sn'de bir) → TimescaleDB → Grafana.

## 4. Önemli kararlarımız ve neden

Her kararın yanında bir ölçüm var, çünkü hoca "ölçülmemiş sayı = sıfır puan" diyor.

### 4.1 Watermark 2 dakika + "update" modu (en önemli karar)

**Sorun:** Kart hem "30 sn'de uyarı" hem "2 dk geç gelenleri de say" diyor. İlk bakışta çelişiyor: 2 dk beklersek uyarı geç kalır, beklemezsek geç gelenleri kaçırırız.

**Çözüm:** Spark'ta "update" modu var. Bu modda Spark pencerenin kapanmasını beklemeden her 10 sn'de o ana kadarki sayıyı veritabanına yazıyor. Geç gelen olay sonradan gelince aynı satırı güncelliyor. Watermark (2 dk) yalnızca "en fazla ne kadar geç gelen olayı kabul edeyim" sorusunun cevabı. Uyarının zamanını belirlemiyor.

**Ölçüm** (`analiz/gec_veri_deneyi.sh`): aynı veriyi iki ayarla işledik.

| Geç olay oranı | 2 dk watermark: kaybolan | 30 sn watermark: kaybolan |
|---|---|---|
| %5 (kart) | 0 | %4,13 |
| %30 | 0 | %23,1 |
| %50 | 0 | %38,7 |

30 sn seçseydik kartın %1 sınırını 4 kat aşacaktık.

### 4.2 Checkpoint'i yerel diske aldık

**Checkpoint nedir:** Spark'ın "nerede kaldım" defteri. Çökerse buradan devam ediyor.

**Ne oldu:** İlk hâlinde defter S3'teydi. Uyarı gecikmesini ölçünce en kötü 29,9 sn çıktı, yükte 37,5 sn. Yani sınırı aşıyorduk. Spark'ın kendi raporuna bakınca her 10 sn'lik turda defteri S3'e yazmanın ~10 sn sürdüğünü gördük. Defteri bilgisayarın diskine alınca en kötü gecikme 13,4 sn'ye düştü (saniyede 8000 olayda 16,1 sn).

**Bedeli:** Defter tek bilgisayarın diskinde. Disk giderse iş baştan başlar. Raporda yazıyor.

### 4.3 Ham veriyi Delta'da tuttuk

**Deney:** 3 milyon satırlık tablonun üstüne yazarken programı zorla öldürdük (`kill -9`).
- Düz Parquet: eski dosyalar silinmiş, yenileri yarım kalmış. Tablo **okunamıyor**.
- Delta: eski sürüm eksiksiz okunuyor (2.992.895 satır). Delta'nın kayıt defteri yeni sürümü hiç onaylamadığı için yarım dosyaları görmüyor.

### 4.4 Diğerleri (soru gelirse)

- **Kafka konusu 8 bölüm:** Bilgisayarda 8 çekirdek var; Spark her bölümü bir çekirdekle okuyor. Hacim sorun değil: Kafka bu makinede saniyede 18.000'den fazla olay aldı, kartın tepesi 4.000.
- **Boyut tablosu ile broadcast join:** Sensör bilgisi (model, kurulum yılı) 200 satırlık küçük bir tablo. Her işçiye kopyalanıyor, 3 milyon satırlık tablo karıştırılmıyor. 8 koşunun ortancası 9,02 sn'ye karşı 11,05 sn.
- **zstd sıkıştırma:** Aynı veri zstd ile 35,5 MB, varsayılan snappy ile 44,5 MB. Ham JSON 430,7 MB. 3 yıl saklayacağımız için boyut önemli.
- **TimescaleDB:** Bütün sorgularımız "şu sensörün şu zaman aralığı" biçiminde. TimescaleDB tabloyu güne göre bölüyor, sorgu sadece ilgili günü okuyor. Ölçülen p99 en fazla 11,3 ms (sınır 50 ms).
- **Upsert:** Veritabanına "varsa güncelle, yoksa ekle" diye yazıyoruz. Aynı şey iki kez yazılsa sonuç değişmiyor. Çökmede bu çok işe yarıyor (aşağıda).

## 5. Bozma deneyleri (hocanın en çok puan verdiği kısım: 20 puan)

Hoca "sistem nerede kırılır, gerçekten deneyin" diyor. Denediklerimiz:

| Ne yaptık | Ne oldu | Nasıl düzeldi |
|---|---|---|
| Kafka'yı 90 sn kapattık | Üretici bekledi, veri kaybetmedi. Ama akış işi 60 sn sonra öldü ve Kafka gelince kendiliğinden geri dönmedi. Panoda 200 sensörün hepsi "sustu" göründü | Aynı defterle (checkpoint) yeniden başlatınca kaçırdığı 24.577 olayı yakaladı, kayıp 0 |
| Veritabanını 60 sn kapattık | Akış işi öldü, pano hata gösterdi | Yeniden başlatınca kayıp 0, tekrar 0 |
| İşi, veritabanına yazdıktan hemen sonra öldürdük | Aynı parti iki kez yazıldı | Upsert sayesinde tablo yine doğru |
| Checkpoint defterini kaybettik | "Baştan oku" ayarıyla kayıp 0; "sadece yenileri oku" ayarıyla %21,9 kayıp | Defter kalıcı diskte tutulmalı |
| Saniyede 8.000 olay gönderdik | 1,23 milyon olayın hepsi sayıldı. İlk zorlanan şey kapasite değil, uyarı süresi oldu | Checkpoint'i yerel diske almak |
| Toplu iş ile akış işini aynı anda çalıştırdık | Bellek yetti ama akış partileri 36,5 sn'ye uzadı | İkisini aynı anda çalıştırmamak |
| Kafka'yı `docker compose down` ile kapatıp açtık | Bütün Kafka verisi silinmişti. Kitin ayarında bir eksik vardı | Ayarı düzelttik (`KAFKA_LOG_DIRS`) |

## 6. Verimizde ne bulduk?

1. **Titreşim anomalileri saate bağlı:** Yedi bölgenin hepsinde en çok anomali akşam 21.00'de.
2. **TX-200 modeli sorunlu:** Anomali oranı %1,17, diğer dört model %0,30 civarında. Yaklaşık 4 kat.
3. **Veri kaybı sensör numarasıyla ilgili:** Numarası 13'ün katı olan 15 sensör olaylarının yaklaşık %29'unu, 23'ün katı olan 8 sensör %7–10'unu kaybediyor. Modelle ilgisi yok.

## 7. Klasörler

| Klasör | İçinde ne var |
|---|---|
| `is/` | Asıl işler: `toplu_is.py` (toplu hat), `akis_iskelet.py` (akış hattı), `servis_yukle.py` (veritabanına yükleme) |
| `analiz/` | Deney betikleri ve ölçüm araçları. Her deneyin bir betiği var |
| `kanit/` | Deneylerin çıktıları, ekran görüntüleri ve **`olcumler.csv`** (bütün ölçümler) |
| `rapor/` | Rapor bölümleri ve PDF |
| `sunum/` | Sunum PDF'i ve konuşma notları |
| `servis/`, `grafana/` | Veritabanı şeması ve pano ayarları |

## 8. Kendi bilgisayarında çalıştırmak

README'deki "Bizim hattımızı sıfırdan çalıştırmak" bölümünü sırayla izle. Dikkat: 6. adımı (Kafka konusunu 8 bölümle açmak) atlarsan akış işi hata verir. Hoca README ile sıfırdan kurulumu "kodu en az yazan üyenin" denemesini istiyor; bunu Zeynep ya da Behiç son hafta yapacak.

## 9. Sunumda kim ne anlatıyor

| Dakika | Slayt | Konu | Kim |
|---|---|---|---|
| 0–2 | 1–2 | Mimari şema, kartın sınırları ve ölçtüğümüz değerler | Tunahan |
| 2–3,5 | 3 | Karar 1a: 2 dk watermark + update modu | Tunahan |
| 3,5–5 | 4–5 | Karar 1b: checkpoint yerel diske; Karar 2: Delta | Zeynep |
| 5–7 | 6 | Canlı Grafana gösterimi | Zeynep |
| 7–9 | 7–8 | Kafka kesintisi deneyi ve kurtarma | Behiç |
| 9–10 | 9 | Bulgu: veri kaybı sensör numarasına bağlı | Behiç |
| 10–12 | 10 | Sorular (hoca kişiye soruyor) | Hepimiz |

Sorulardan sonra 3 yedek slayt var (garanti tablosu, neden 8 bölüm, sorgu gecikmeleri). Sunulmuyor, soru gelirse açılıyor.

Konuşma notları ve olası soruların cevapları: `sunum/konusma-notlari.md`. Herkes en azından kendi bölümünün sayılarını ve "neden" cevabını bilmeli.

## 10. Kalan işler

- YZ kullanım beyanı ve katkı beyanı (raporun 12. ve 13. bölümü): en sona bıraktık, birlikte yazacağız.
- Teslim günü: depo adı `BSM461_Proje_1A_G221210024` olacak, `teslim` etiketi atılacak, commit kimliği kapağa yazılacak.
- Son hafta: Zeynep ya da Behiç README ile sıfırdan kurulumu kendi bilgisayarında deneyecek.
