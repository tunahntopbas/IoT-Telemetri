# 5. Depolama (Z2, Z4, Z7)

Bütün sayılar `kanit/olcumler.csv` dosyasında (satır adları parantez içinde). Ölçüm ortamı: Apple M1, 8 GB RAM, Docker'a 4.786 GiB, Spark 3.5.3 `local[*]`, SeaweedFS 4.48. Veri: `--grup G221210024 --sablon iot --toplu 3000000 --gun 14` ile üretilen 3.000.000 satır (14 gün, 14 JSONL dosyası). Süre ölçümlerinde her durum önce bir kez ısınma için çalıştırıldı, sonra 3 kez ölçülüp ortalaması alındı.

## 5.1 Biçim: JSON mu Parquet mi? (Z2)

### Ne yaptık?

Ham veri S3'e üreticinin yazdığı gibi satır satır JSON (JSONL) olarak geliyor. Kitin toplu işini (`is/toplu_is.py`) değiştirmeden bir kez çalıştırdık (`kanit/iskelet-ilk-kosu.txt`). Bu iş veriyi tekrarlardan ayıklayıp tarih bölümlü Parquet olarak yazdı. Sonra `analiz/olcum_depolama.py` ile aynı veriyi farklı biçimlerde yazıp boyutlarını ve aynı sorgunun süresini ölçtük.

| Biçim | Toplam boyut | JSON'a göre küçülme | Kaynak |
|---|---|---|---|
| JSON (ham, 14 dosya) | 430.691.739 bayt (430,7 MB) | 1,00 | `z2_json_toplam_boyut` |
| Parquet, sıkıştırmasız | 59.621.594 bayt (59,6 MB) | 7,22 kat | `z2_codec_uncompressed_boyut` |
| Parquet + snappy (Spark varsayılanı) | 44.484.298 bayt (44,5 MB) | 9,68 kat | `z2_codec_snappy_boyut` |
| Parquet + gzip | 36.602.779 bayt (36,6 MB) | 11,77 kat | `z2_codec_gzip_boyut` |
| Parquet + zstd | 35.525.314 bayt (35,5 MB) | 12,12 kat | `z2_codec_zstd_boyut` |

Not: Parquet dosyaları tekrar eden 5.905 satır atıldıktan sonra yazıldı (satırların %0,197'si). Bu yüzden oranlar binde 2 kadar iyimser.

Sıkıştırmasız Parquet bile JSON'dan 7,22 kat küçük. Bunun nedeni sütun adlarının her satırda tekrar yazılmaması ve sayıların metin yerine ikili tutulması. Sıkıştırma bunun üstüne ek kazanç sağlıyor.

Aynı sorgu (Akdeniz bölgesi, Türkiye saatine göre ortalama sıcaklık):

| Okunan biçim | Ortalama süre (3 koşu) | Koşular | Kaynak |
|---|---|---|---|
| JSON (şema elle verildi) | 1,459 s | 1,448 / 1,293 / 1,637 | `z2_sorgu_suresi_json` |
| Parquet + snappy (iskeletin yazdığı) | 1,379 s | 1,510 / 1,360 / 1,268 | `z2_sorgu_suresi_parquet_snappy` |
| Parquet + snappy | 1,075 s | 1,385 / 1,054 / 0,784 | `z2_codec_snappy_okuma_suresi` |
| Parquet + zstd | 1,151 s | 1,085 / 1,258 / 1,110 | `z2_codec_zstd_okuma_suresi` |
| Parquet + gzip | 0,677 s | 0,799 / 0,581 / 0,650 | `z2_codec_gzip_okuma_suresi` |
| Parquet, sıkıştırmasız | 0,741 s | 0,808 / 0,778 / 0,639 | `z2_codec_uncompressed_okuma_suresi` |

### Ne çıktı, neden böyle karar verdik?

- Boyutta fark büyük ve tutarlı. Ölçek hesabımızda (bölüm 4) 1095 günlük ham JSON 26,98 TB çıkmıştı. Aynı veri zstd'li Parquet'te 26,98 TB ÷ 12,12 ≈ 2,23 TB eder (hesap, ölçülen oranla).
- Sürede fark küçük ve koşudan koşuya değişiyor. JSON ile Parquet arasındaki fark (1,459 s'ye karşı 1,379 s) tek bir koşunun dalgalanmasından küçük. Bunun nedeni verinin küçük olması: 430 MB'lık JSON tek makinede birkaç saniyede okunuyor ve sürenin büyük kısmı Spark'ın iş başlatma maliyeti. Codec'ler arasındaki sıra da güvenilir değil: ilk ölçülen snappy'nin ilk koşusu 1,385 s, son koşusu 0,784 s. Bu yüzden "gzip daha hızlı okunuyor" gibi bir sonuç çıkarmıyoruz.
- Karar: Toplu çıktıyı Parquet tabanlı Delta olarak tutuyoruz. Sıkıştırmada depolamayı belirleyen boyut olduğu için zstd daha uygun (snappy'ye göre %20 daha küçük: 35,5 MB'a karşı 44,5 MB). Şu an Delta tablosu Spark varsayılanı olan snappy ile yazılıyor. zstd'ye geçiş `spark.sql.parquet.compression.codec=zstd` ayarıyla yapılacak. Okuma süresi farkını daha büyük veriyle ve daha çok tekrarla yeniden ölçmeden hız üzerine bir iddiada bulunmayacağız.

### Projection ve predicate pushdown

`kanit/explain-0-json-parquet.txt` dosyasında aynı sorgunun iki planı var:

| | JSON | Parquet |
|---|---|---|
| `PushedFilters` | `[IsNotNull(bolge), EqualTo(bolge,Akdeniz)]` | `[IsNotNull(bolge), EqualTo(bolge,Akdeniz)]` |
| `ReadSchema` | `olay_zamani, bolge, sicaklik` | `bolge, olay_zamani, sicaklik` |
| `Batched` | false | true |

Planlar aynı görünüyor ama arkadaki iş farklı. JSON'da her satırın bütün metni diskten okunup ayrıştırılmak zorunda. Filtre ve sütun seçimi ancak ayrıştırmadan sonra uygulanabiliyor. Parquet'te yalnızca `ReadSchema`'daki üç sütunun verisi okunuyor (6 sütundan 3'ü). Satır grubu istatistikleri (en küçük ve en büyük değer) `bolge = 'Akdeniz'` koşulunu hiç sağlamayan blokları atlamaya izin veriyor. `Batched: true` da sütunların vektörel (toplu) okunduğunu gösteriyor. Bu farkın süreye yansıması bizim veri boyutumuzda ölçülemeyecek kadar küçük kaldı.

## 5.2 Nesne deposu ve dizin düzeni (Z4)

### Dizin düzeni

Ham veri `s3://ham/iot/tarih=YYYY-MM-DD/parca-*.jsonl`, temiz tablo `s3://tablolar/iot/tarih=YYYY-MM-DD/` düzeninde. Neden tarih?
- Sorularımız gün aralığıyla çalışıyor ve yeni veri her gün geliyor. Bir günü yeniden işlemek tek bir klasörü değiştiriyor. `is/toplu_is.py --tarih 2026-11-16` ile yalnızca o gün yeniden yazıldı (bkz. 5.3).
- Spark klasör adından `tarih` sütununu kendiliğinden çıkarıyor. Bu sütuna konan filtre klasör düzeyinde budanıyor: `--tarih` koşusunun planında `PartitionFilters: [isnotnull(tarih), (tarih = 2026-11-16)]` görünüyor (`kanit/explain-1.txt`) ve yalnızca o günün dosyası okunuyor.
- `sensor_id`'ye göre klasörlemek 200 sensör × 14 gün = 2.800 klasör ederdi. Sensör başına günde yaklaşık 1.000 olay olduğu için her dosya çok küçük kalırdı.

### Dosya sayısı ve boyutu

| Durum | Dosya sayısı | Ortalama dosya boyutu | `count()` süresi (3 koşu ort.) | Kaynak |
|---|---|---|---|---|
| Kitin iskeleti (`partitionBy("tarih")`, 8 yazıcı) | 112 | 404.524 bayt (0,40 MB) | 0,46 s | `z4_iskelet_*`, `z4_count_suresi_iskelet` |
| Birleştirilmiş (`repartition("tarih")` + `partitionBy("tarih")`) | 14 | 2.888.834 bayt (2,89 MB) | 0,22 s | `z4_birlesik_*`, `z4_count_suresi_birlesik` |

İskelet her günü 8 parçaya bölüyor, çünkü Spark 8 çekirdekle 8 görev açıyor ve her görev her güne ayrı bir dosya yazıyor. Sonuçta 0,4 MB'lık 112 dosya çıkıyor. Bunlar Parquet için çok küçük: her dosyanın kendi başlığı, alt bilgisi ve S3'te ayrı bir istek maliyeti var. Tarihe göre yeniden bölümleyince her gün tek dosya oldu, dosya sayısı 8 kat azaldı ve aynı veriyi saymak 0,46 s'den 0,22 s'ye indi. Gerçek ölçekte bir gün zstd ile yaklaşık 2 GB tutacağı için (24,64 GB ÷ 12,12) gün içinde 128–512 MB'lık birkaç dosya hedeflenmeli.

### HDFS yerine nesne deposunun bedeli

- Listeleme pahalı: S3'te "klasör" yok, yalnızca ön eki aynı olan anahtarlar var. Bir klasörü okumak için önce bütün anahtarlar sayfa sayfa (her istekte en fazla 1.000) listeleniyor. Dosya sayısı arttıkça okuma başlamadan önce geçen süre de artıyor. Yukarıdaki 112 ve 14 dosya farkının bir kısmı buradan geliyor.
- Atomik yeniden adlandırma (rename) yok: HDFS'te bir işin çıktısını önce geçici klasöre yazıp en sonda tek bir rename ile yayınlamak atomik. S3'te rename her dosyayı kopyalayıp silmek demek, yani ne atomik ne ucuz. Spark'ın varsayılan yazma yöntemi bu rename'e dayanıyor. İş yarıda kalırsa klasörde yarım çıktı kalabiliyor. Bunu 5.3'teki deneyde gördük.

## 5.3 Tablo biçimi: Delta Lake (Z7)

### İkinci yazma ve zaman yolculuğu

Temiz olay tablosu `s3a://tablolar/iot` Delta olarak yazılıyor. `DESCRIBE HISTORY` çıktısı ve her sürümün `versionAsOf` ile okunması `kanit/delta-history.txt` dosyasında:

| Sürüm | Ne yapıldı | Satır | Sütun | 16.11 günü |
|---|---|---|---|---|
| 0 | Kitin iskeleti, değiştirilmeden (yalnızca tekrar ayıklama) | 2.994.095 | 7 | 213.843 |
| 1 | Bizim toplu işimiz: tekrar ayıklama, sensor_id'siz kayıtları karantinaya ayırma, boyut tablosuyla join | 2.992.895 | 12 | 213.750 |
| 2 | Yalnızca 16.11 yeniden işlendi (`--tarih 2026-11-16`, `replaceWhere`) | 2.992.895 | 12 | 213.750 |
| 3 | Bütün tablo yeniden yazıldı (karantina tablosundaki düzeltmeden sonra) | 2.992.895 | 12 | 213.750 |
| 4 | 16.11 yine yeniden işlendi | 2.992.895 | 12 | 213.750 |

- Sürüm 0 ile 1 arasındaki 1.200 satırlık fark, `sensor_id`'si olmayan ve karantina tablosuna taşınan kayıtlar. 16.11 günündeki 93 satırlık fark da aynı nedenden.
- Sürüm 2 ve 4'te aynı gün iki kez yeniden işlendi. Satır sayısı değişmedi, `operationMetrics` her seferinde o günün 8 dosyasının kaldırılıp yerine yenilerinin yazıldığını gösteriyor. Yani bir günü yeniden işlemek veriyi katlamıyor (idempotent).
- Bu deneyde bir hata bulup düzelttik: karantina tablosu ilk sürümde `--tarih` modunda sona ekleniyordu (`append`). Aynı günü iki kez işlemek o günün karantina kayıtlarını çoğaltırdı. Şimdi karantina da tarih bölümlü ve aynı `replaceWhere` kuralıyla yazılıyor. Son durumda karantinada 1.200 kayıt var; bu sayı tekrar ayıklandıktan sonraki `sensor_id`'siz kayıt sayısıyla aynı.

### Atomik yazma deneyi

`analiz/atomik_deney.py` ile aynı 2.992.895 satırı (`surum=1` sütunuyla) hem bir Delta tablosuna hem çıplak bir Parquet klasörüne 64 dosya halinde yazdık. Sonra ikisine de `surum=2` ile üzerine yazma başlattık ve yazma sürerken `spark-submit` sürecini `kill -9` ile öldürdük. Kesme anını, araclar kapsayıcısından S3'ü yarım saniyede bir listeleyerek seçtik. Bütün çıktı `kanit/atomik-yazma-deneyi.txt` dosyasında.

| | Kesme anında S3'te | Kesmeden sonra okuma | Kaynak |
|---|---|---|---|
| Delta | 64 eski + 21 yeni `.parquet` dosyası | 2.992.895 satır, hepsi `surum=1`. `DESCRIBE HISTORY`'de yalnızca sürüm 0 var. Klasörde 89 veri dosyası var, 25'i hiçbir sürüme ait değil. | `z7_delta_kesme_sonrasi_satir` |
| Parquet | Eski 64 dosya silinmişti; 15 dosya `_temporary/` altında | Klasörde 20 dosya, hepsi `_temporary/` altında; `_SUCCESS` yok. Okuma hatası: `UNABLE_TO_INFER_SCHEMA`. Eski veri de yeni veri de yok. | `z7_parquet_kesme_sonrasi_veri_dosyasi` |

Not: Parquet denemesini iki kez yaptık. İlk denemede makine o sırada duraklamış, yazma hiç ilerlememişti. Eski veri olduğu gibi kaldığı için o deneme geçersiz sayıldı.

### Tablo biçimi olmasaydı hangi hatayı yaşardık?

Çıplak Parquet'te `overwrite`, yazmaya başlamadan önce eski klasörü siliyor. Yeni dosyalar `_temporary/` altına yazılıyor ve iş bitince yerine taşınıyor. İş arada çökerse eski veri gitmiş, yeni veri yayınlanmamış oluyor. Tabloyu okuyan her şey (pano, servis deposuna yükleme işi) ya hata alıyor ya da boş sonuç görüyor. Veri ancak iş baştan çalıştırılırsa geri geliyor.

Delta'da yeni dosyalar yazılsa da tablonun hangi dosyalardan oluştuğunu `_delta_log` içindeki işlem kaydı belirliyor. Yeni sürümün kaydı ancak bütün dosyalar yazıldıktan sonra tek bir dosya olarak ekleniyor. Kayıt yazılmadan çöken bir işin dosyaları "sahipsiz" kalıyor ve okuyucular bunları görmüyor. Bedeli, deneyde görülen 25 sahipsiz dosya. Bunlar boşuna yer kaplıyor ve `VACUUM` ile temizlenmeleri gerekiyor.
