# 8. Akış hattı (Z8)

Kod: `is/akis_iskelet.py` (kitin iskeleti bizim iki sorumuza göre dolduruldu). Çalıştırma:
`docker compose --profile servis-ts up -d`, ardından `docker compose exec spark spark-submit is/akis_iskelet.py`.
Bütün sayılar `kanit/olcumler.csv` ve `kanit/akis-*.txt` dosyalarında. Ölçüm ortamı: Apple M1, Docker'a 4,786 GiB, Spark 3.5.3 `local[*]` (8 çekirdek).

## 8.1 Konu tasarımı

| Karar | Değer | Gerekçe |
|---|---|---|
| Konu | `sensorler` | Kitin varsayılanı. |
| Bölüm sayısı | 8 | Aşağıda. |
| Anahtar | `sensor_id` | Bir sensörün olayları hep aynı bölüme gidiyor. |
| Kopya | 1 (kit) / 3 (üretimde) | Bölüm 6.4. |
| Saklama | 7 gün (`retention.ms=604800000`) | Akış işi çökse bile bir hafta içinde kaldığı yerden devam edebilir. Uzun saklama S3'te. |

Bölüm sayısının hesabı:
- Hacim küçük. Kartın tepe yükü 4.000 olay/s × 142,58 bayt = 570 KB/s (bölüm 4). Üretici deneyinde tek düğümlü Kafka'mız 5,4 sn'lik koşuda 97.762 olay aldı, yani en az 18.000 olay/s. Bu da tepe yükün 4,5 katı (`kanit/akis-uretici-ayarlari.txt`). Bölüm sayısını hacim belirlemiyor.
- Bölüm sayısını tüketici paralelliği belirliyor. Spark her Kafka bölümünü bir görevle okuyor ve makinemizde 8 çekirdek var. 3 bölümde (kitin varsayılanı) çekirdeklerin 5'i okuma sırasında boş kalırdı. 8'den fazlası bu makinede paralellik kazandırmaz.
- Anahtar sayısı yeterli. 200 sensör 8 bölüme bölündüğünde bölüm başına 25 sensör düşüyor. Ölçülen dağılım dengeli: 8 bölümlü deneme konusunda en dolu bölüm ÷ ortalama = 1,063 (bölüm 6.3).
- Gelecek: 10x'te (tepe 40.000 olay/s, 5,7 MB/s) hacim hâlâ küçük; 8 bölüm, 8 çekirdekli bir tüketiciye yetiyor. 100x'te (tepe 400.000 olay/s, 57 MB/s) tek makine yetmez ve tüketici sayısı artmalı. Bölüm sayısı da tüketici sayısının katı olacak şekilde (ör. 32 ya da 64) yükseltilmeli. Kafka bölüm sayısını sonradan artırmaya izin veriyor, ama artınca anahtarların bölümlere dağılımı değişiyor. Geçiş sırasında bir sensörün eski bölümdeki son olayıyla yeni bölümdeki ilk olayı arasında sıra garantisi kalmıyor. Bu yüzden büyüme bekleniyorsa bölüm sayısı baştan biraz geniş tutulmalı.
- Uygulama: Üretici konuyu ilk mesajda kendiliğinden 3 bölümle açmıştı. Konu silinip 8 bölümle yeniden oluşturuldu (`kanit/akis-konu.txt`).

Sıra garantisi: Kafka sırayı yalnızca bölüm içinde garanti ediyor. Anahtar `sensor_id` olduğu için tek bir sensörün olayları üretildiği sırayla okunuyor. Farklı sensörler arasında sıra yok ve sorularımız bunu gerektirmiyor. Asıl sıra sorunu Kafka'dan değil, olayların %5'inin 2 dakikaya kadar geç gelmesinden kaynaklanıyor. Bu yüzden pencereleri varış sırasına göre değil, olay zamanına (`olay_zamani`) göre kuruyoruz.

## 8.2 Üretici ayarları: `acks=all` ve idempotent üretici

- `acks=all`: Lider, mesajı bütün güncel kopyalar (ISR) yazdıktan sonra onaylıyor. Kopyası olmayan bir bölümde lider düşerse onaylanmış veri kaybolmuyor. Bedeli şu: her yazmada en yavaş kopyayı beklemek.
- İdempotent üretici: Her mesaja üretici kimliği ve sıra numarası ekleniyor. Ağ hatasında yeniden gönderilen mesajı broker ikinci kez yazmıyor; bölüm içi sıra da korunuyor. Bu ayar yalnızca üreticinin kendi yeniden denemesinden doğan tekrarı önlüyor. Uygulamanın aynı olayı iki kez göndermesini (bizim verideki "tekrar" kusuru) önlemiyor.

Ölçüm (`kanit/akis-uretici-ayarlari.txt`): `--hiz 20000 --sure 5`, her ayar 3 koşu, ayrı ve 8 bölümlü konular.

| Ayar | Koşu süreleri (sn) | Gönderilen olay (her koşu) | Kafka'daki mesaj (3 koşu) |
|---|---|---|---|
| `acks=all`, idempotent açık | 5,51 / 5,42 / 5,40 | 97.762 | 293.871 |
| `acks=1`, idempotent kapalı | 5,47 / 5,37 / 5,38 | 97.762 | 293.871 |
| `acks=1`, `--idempotent-kapali` | 5,46 / 5,35 / 5,40 | 97.762 | 293.871 |
| `acks=0` | 5,46 / 5,39 / 5,39 | 97.762 | 293.871 |

Sonuç: Tek düğümlü, tek kopyalı ve ağsız bu kurulumda ayarlar arasında ölçülebilir fark yok. `acks=all`'ın beklediği başka kopya yok. Ağ hatası da olmadığı için yeniden deneme yaşanmıyor. 3 × 97.762 = 293.286 ile 293.871 arasındaki 585 mesaj üreticinin bilerek gönderdiği tekrarlar (%0,2), dört ayarda da aynı sayıda. Bu deney, idempotent üreticinin uygulama düzeyindeki tekrarı önlemediğini gösteriyor. `acks=all`'ın gecikme bedeli ancak 3 kopyalı ve ağ üzerinden çalışan bir kümede ölçülebilir; bunu kendi ortamımızda ölçemedik. Kararımız `acks=all` ve idempotent üretici. Kartın kayıp sınırı %1. Kopya gelmeden onay vermek (`acks=1` ya da `0`), lider düştüğünde onaylanmış mesajı kaybettirebilir.

## 8.3 Akış işi: pencere, watermark, tekrar

```
Kafka (sensorler, 8 bölüm)
  → JSON ayrıştır, sensor_id ve olay_zamani boş olanları ele
  → withWatermark(olay_zamani, 2 dakika)
  → dropDuplicates(bütün alanlar)                 tekrar eden olay bir kez sayılır
  → groupBy(1 dakikalık sabit pencere, sensor_id)  olay, anomali (titreşim > 1,0), en yüksek titreşim,
                                                    ortalama sıcaklık, son olay zamanı, son varış zamanı
  → update modu, 10 sn'de bir tetik
  → foreachBatch: PostgreSQL/TimescaleDB'ye upsert (sensor_id, pencere_baslangic)
```

İki soru bu dakikalık özetten servis deposundaki iki görünümle cevaplanıyor (`servis/sema.sql`):
- Akış 1 (`anomali_son10dk`): Son 10 dakikanın pencerelerinde anomalisi olan sensörler. 10 dakikalık kayan pencereyi Spark'ta değil sorguda kuruyoruz. Spark'ta 1 dakika kaydırmalı 10 dakikalık pencere her olayı 10 pencerede tutardı, yani 10 kat durum. Dakikalık özeti toplamak aynı cevabı veriyor.
- Akış 2 (`susan_sensorler`): 60 saniyedir hiç olayı Kafka'ya ulaşmamış sensörler. Burada olay zamanı değil varış zamanı (`son_varis`, Kafka zaman damgası) kullanılıyor. Olay zamanı kullansaydık, geç gelen bir olay sensörü "susmuş" gibi gösterebilirdi. Varış zamanı ise sensörün şu an konuşup konuşmadığını söylüyor. Hiç olay göndermeyen sensör de görünsün diye görünüm boyut tablosundan (`sensorler`, 200 sensör) başlıyor. Spark'ta "olmayan olay" için çıktı üretmek mümkün değil; olay gelmediği sürece tetiklenecek bir şey yok. Bu yüzden bu soru depoda cevaplanıyor.

### Pencere türü ve boyutu

1 dakikalık sabit (tumbling) pencere. Daha kısa pencere (ör. 10 sn), sensör başına saniyede 0,25 olay gelen deney hızımızda (`--hiz 50`) çoğu pencereyi boş ya da tek olaylı bırakırdı. 1 dakika hem iki sorunun ihtiyacına yetiyor hem de panonun "dakika başına" grafiğiyle aynı birimde.

### Watermark: kartın üç koşulu çelişiyor

| Kart koşulu | Watermark'a etkisi |
|---|---|
| Uyarı en geç 30 sn'de çıkmalı | Pencere sonucu beklemeden yayımlanmalı → kısa watermark ya da update modu |
| Olayların %5'i en fazla 2 dk geç geliyor | Geç olayın sayılması için pencere en az 2 dk açık kalmalı → watermark ≥ 2 dk |
| Sayıma girmeyen olay en fazla %1 | 30 sn'lik watermark %5'in büyük kısmını atar → sınır aşılır (ölçüm aşağıda) |

Seçimimiz: 2 dakikalık watermark ve update modu birlikte. Update modu, pencere kapanmayı beklemeden her tetikte o ana kadarki sayıyı yazıyor. Yani anomali ilk göründüğü tetikte veritabanına gidiyor. Geç gelen olay da pencere 2 dk boyunca açık kaldığı için sonradan sayıma ekleniyor ve aynı satır upsert ile güncelleniyor. Bunun bedeli iki tane:
- Bir pencerenin sayısı 2 dakika boyunca kesin değil. Pano son dakikaları önce eksik gösterip sonra düzeltiyor. "Kesinleşmiş sayı" isteyen bir okuyucu (ör. S3'e append ile yazılan çıktı) sonucu ancak 2 dk + tetik süresi sonra görüyor.
- Durum (state) 2 dakikalık pencereyi ve tekrar ayıklama anahtarlarını tutuyor. Yani watermark ne kadar uzunsa bellek de o kadar çok.

Ödün verdiğimiz şey kesinleşme süresi: uyarı hızlı, sayının kesinleşmesi yavaş. Kayıp sınırından ödün vermiyoruz.

### 2 dk watermark ile 30 sn uyarı nasıl bir arada?

Watermark, sonucun **ne zaman yazıldığını** belirlemiyor. Yalnızca bir pencerenin durumunun ne zaman silineceğini ve hangi olayın "çok geç" sayılıp atılacağını belirliyor. Update modunda Spark her tetikte, o yığında değişen bütün pencereleri o ana kadarki sayılarla yazıyor. Pencerenin kapanmasını (watermark'ın geçmesini) beklemiyor. Bu yüzden 1,0'ı aşan bir titreşim, Kafka'ya vardığı tetikte işleniyor. Aynı yığının sonunda `sensor_dakika` satırı `anomali_sayisi > 0` olarak veritabanında. Pano görünümü (`anomali_son10dk`) bu satırı bir sonraki yenilemede gösteriyor. 2 dakika ise yalnızca o pencereye geç gelen olayların hâlâ eklenebildiği süre.

Append modunu seçseydik durum tersine dönerdi. Pencere ancak watermark geçince (pencere sonu + 2 dk) yazılırdı. Uyarı en erken 3 dk sonra çıkardı. S3'e yazılan "kesinleşmiş" çıktı bu yüzden append, uyarının kaynağı olan veritabanı ise update.

### Uyarı gecikmesi ölçümü

Ölçtüğümüz süre: olayın Kafka'ya varışından, veritabanında okunabilir olduğu ana (yığının commit'i) kadar. `batch_gunlugu` her yığın için commit zamanını (`yazilma`) ve yığına giren en geç varışı (`max_varis`) tutuyor. N. yığına giren olaylar, bir önceki yığının `max_varis`'inden sonra Kafka'ya varmış. Bu yüzden:

- en kısa gecikme = yazilma(N) − max_varis(N)
- en uzun gecikme = yazilma(N) − max_varis(N−1). Bu, yığındaki en erken olay için üst sınır; uyarı açısından bakılması gereken bu.

Hesap `analiz/tepe_analiz.py` ile yapıldı, ham çıktılar `kanit/kirilma-deneyi-tepe.txt`, `kanit/akis-tetik5.txt` ve `kanit/akis-checkpoint-yerel.txt` içinde. Pano 10 sn'de bir yenilendiği için ekranda görünmeye en fazla 10 sn daha eklenebilir.

| Checkpoint | Tetik | Hız (olay/sn) | Yığın süresi ort / en uzun | Gecikme alt (ortanca) | Gecikme üst (ortanca) | Gecikme üst (en kötü) |
|---|---|---|---|---|---|---|
| S3 (SeaweedFS) | 10 sn | 50 | 12,3 / 16,1 sn | 13,9 sn | 24,9 sn | **29,9 sn** |
| S3 | 5 sn | 50 | 12,6 / 14,8 sn | 13,5 sn | 25,8 sn | 29,3 sn |
| S3 | 10 sn | 2000 | 15,7 / 21,0 sn | 15,9 sn | 30,5 sn | **37,5 sn** |
| S3 | 10 sn | 8000 | 14,1 / 16,2 sn | 14,7 sn | 28,7 sn | 31,5 sn |
| Yerel disk | 10 sn | 50 | 2,0 / 4,4 sn | 2,0 sn | 12,1 sn | **13,4 sn** |
| Yerel disk | 10 sn | 8000 | 4,2 / 5,7 sn | 4,6 sn | 14,7 sn | 16,1 sn |

İlk ölçümde (checkpoint S3'te) sınır tam sınırdaydı: saniyede 50 olayda bile en kötü gecikme 29,9 sn, yükte 37,5 sn. Tetiği 5 sn'ye indirmek bir şey değiştirmedi, çünkü yığın süresi 10 sn'nin üstündeydi. Spark'ın ilerleme raporundaki durum deposu süreleri nedeni gösterdi. Saniyede 500 olaylık bir yığında güncelleme toplam ~0,3 sn, ama durumun checkpoint'e yazılması (`commitTimeMs`) bölümler toplamında 9–24 sn sürüyordu. Her yığında iki durumlu operatör × 8 bölüm, S3'e küçük dosyalar yazıyor. SeaweedFS'te her yazma ayrı bir istek. Checkpoint'i makinenin diskine (`--checkpoint file:///kit/veri/checkpoint`, kapsayıcıya bağlı klasör) alınca `commitTimeMs` 0,4–0,5 sn'ye indi. Yığın süresi 2 sn'ye, en kötü uyarı gecikmesi 13,4 sn'ye düştü. Saniyede 8000 olayda 16,1 sn.

**Karar:** Akış işinin checkpoint'i yerel diskte (`is/akis_iskelet.py` varsayılanı). Bedeli: checkpoint tek makinenin diskine bağlı. Disk giderse iş baştan başlar (Kafka'da 7 gün saklama var, yeniden işlenebilir; upsert sayesinde sonuç bozulmaz). Gerçek bir kümede bunun karşılığı, sürücünün kalıcı diski ya da HDFS olurdu. Yerel checkpoint ile çökme deneyi tekrarlandı (bölüm 8.5).

## 8.4 Geç veri deneyi

`analiz/gec_veri_deneyi.sh`: üretici `--hiz 50 --sure 300`. Akış işi aynı konuyu iki ayrı sorguyla, 2 dk ve 30 sn watermark ile işledi. Gerçek değer, konudaki bütün mesajların toplu okunup aynı kuralla ayıklanmasıyla hesaplandı (`analiz/akis_dogrula.py`).

| Geç oranı | Üreticinin "gönderilen"i | Kafka'daki mesaj | Tekrar | sensor_id ya da zaman boş | Tekrarsız geçerli olay | 5 sn'den geç varan | 2 dk watermark: sayıma girmeyen | 30 sn watermark: sayıma girmeyen |
|---|---|---|---|---|---|---|---|---|
| %5 (kart) | 14.673 | 14.702 | 29 | 9 | 14.664 | 763 | 0 (%0) | 606 (%4,13) |
| %30 | 14.634 | 14.660 | 26 | 5 | 14.629 | 4.395 | 0 (%0) | 3.374 (%23,06) |
| %50 | 14.533 | 14.562 | 29 | 9 | 14.524 | 7.223 | 0 (%0) | 5.618 (%38,68) |

(Kaynak: `kanit/akis-gec-kart-*.txt`, `akis-gec-30-*.txt`, `akis-gec-50-*.txt`.)

Sayıların okunuşu:
- Üreticinin yazdığı "gönderilen" sayısı tekrarları içermiyor. Kafka'daki mesaj sayısı ise içeriyor (14.702 − 14.673 = 29 tekrar). Kafka'daki mesajları doğrudan saysaydık olay sayısını 29 fazla bulurduk. Akış işi tekrarları ayıkladığı için tablodaki toplam, Kafka'daki tekrarsız geçerli olay sayısıyla karşılaştırılıyor.
- 2 dakikalık watermark üç oranda da hiçbir olayı kaçırmadı. Üretici geç olayları en fazla 2 dakika geciktiriyor; watermark da en büyük olay zamanından 2 dakika geride, üstüne tetik süresi kadar ek pay var.
- 30 saniyelik watermark, 30 saniyeden geç gelen olayların hemen hepsini attı. Spark'ın ilerleme raporundaki "watermark nedeniyle atılan satır" sayısı bununla uyumlu: kart 608, %30'da 3.380, %50'de 5.632. Aradaki birkaç satırlık fark, tekrar ayıklamadan önce atılan tekrar kopyaları. Kartın oranında bile kayıp %4,13; bu, %1 sınırının 4 katı.
- Geç oranı arttıkça 2 dk watermark'ın sonucu değişmedi. Değişseydi bu, gecikmenin süresinden değil miktarından etkilendiğimiz anlamına gelirdi. Gecikme kartın sınırını (2 dk) aşarsa ne olduğu bölüm 11'de.

## 8.5 Yeniden başlatma deneyi ve teslim garantisi

Deney 1 ve 2 ile geç veri deneyi, checkpoint henüz S3'teyken yapıldı. Checkpoint yerel diske alındıktan sonra deney 2 tekrarlandı (deney 3).

### Deney 1: iş yığınlar arasında öldürülüyor (`analiz/yeniden_baslatma_deneyi.sh`)

Üretici `--hiz 50 --sure 360 --gec-orani 0` ile çalışırken akış işi 20:18:26'da `kill -9` ile öldürüldü. Aynı checkpoint ile 60 sn sonra yeniden başlatıldı. İş hem veritabanına (update) hem S3'e (append, Parquet) yazıyordu. Sonuç `kanit/akis-yeniden-baslatma.txt` içinde:

| | Değer |
|---|---|
| Kafka'daki tekrarsız geçerli olay | 17.513 |
| Tablodaki olay toplamı | 17.513 (kayıp 0) |
| Pencere bazında karşılaştırma | 1.393 pencerenin hepsi doğru, eksik 0, fazla 0 |
| `batch_gunlugu`: iki kez yazılan yığın | 0 (31 yığın, 31 farklı batch_id) |
| S3 append çıktısı | 796 kesinleşmiş pencere, iki kez yazılan 0, eksik/fazla 0 |

Kesinti 60 sn sürdü ama hiçbir olay kaybolmadı. İş, checkpoint'teki son ofsetten devam etti; arada Kafka'ya gelen olaylar ilk yığında okundu. Watermark olay zamanıyla ilerlediği ve iş dururken ilerlemediği için bu olaylar "geç" sayılıp atılmadı. Bu deneyde hiçbir yığın tekrar işlenmedi, çünkü öldürme iki yığının arasına denk geldi.

### Deney 2: yığın veritabanına yazıldıktan sonra, ofset kaydedilmeden çökme (`analiz/cokme_deneyi.sh`)

En kötü an, yığının veritabanına yazılıp Spark'ın bunu checkpoint'e kaydedemediği an. Bunu rastlantıya bırakmamak için akış işine `--cokme-batch 8` seçeneği eklendi: 8. yığın veritabanında commit edildikten hemen sonra süreç `os._exit(1)` ile kapanıyor. Aynı checkpoint ile yeniden başlatınca Spark 8. yığını yeniden işledi (`kanit/akis-cokme-deneyi.txt`):

| | Değer |
|---|---|
| `batch_gunlugu`: yığın 8 | iki kez yazıldı (194 satır, 194 satır; 20:27:06 ve 20:27:34) |
| Kafka'daki tekrarsız geçerli olay | 11.674 |
| Tablodaki olay toplamı | 11.674 (kayıp 0, fazla 0) |
| Pencere bazında karşılaştırma | 995 pencerenin hepsi doğru |

### Deney 3: aynı çökme, checkpoint yerel diskte (`AD=cokme-yerel EK="--checkpoint file:///kit/veri/checkpoint" analiz/cokme_deneyi.sh`)

Checkpoint'in yerini değiştirmek garantiyi değiştirmemeli. Deney 2 aynen tekrarlandı (`kanit/akis-cokme-yerel-deneyi.txt`): yığın 8 yine iki kez yazıldı (178 ve 178 satır). Kafka'daki 11.665 tekrarsız olayın hepsi tabloda, 992 pencerenin hepsi doğru.

### Teslim garantisi

- **Kafka'dan okuma:** En az bir kez. Spark ofseti yığın bittikten sonra kaydediyor. Çökme olursa yarım kalan yığın baştan işleniyor (deney 2'de yığın 8).
- **Veritabanına yazma:** Upsert, `(sensor_id, pencere_baslangic)` anahtarıyla yapılıyor. Değer, pencerenin o ana kadarki toplam sayısı (artış değil). Aynı yığın ikinci kez yazılınca aynı satırlar aynı değerlerle yeniden yazılıyor. Sonuç tam bir kez yazılmış gibi (deney 2: yığın iki kez yazıldı, tablo yine doğru).
- **S3'e yazma (append):** Spark'ın dosya yazıcısı hangi yığının yazıldığını `_spark_metadata` günlüğünde tutuyor. Okuyan taraf yalnızca günlükteki dosyaları görüyor, bu yüzden tam bir kez.
- **Kartın "sayıma girmeyen ≤ %1" koşulu:** 2 dk watermark ile geç veri deneyinde 0, yeniden başlatmada 0, çökmede 0.

Bu garanti, iş yeniden başlatıldığı sürece geçerli. Kafka 60 sn'den uzun kapalı kalırsa akış işi ölüyor ve kendiliğinden geri gelmiyor (bölüm 11). Yeniden başlatılınca yine kayıpsız toparlanıyor.
