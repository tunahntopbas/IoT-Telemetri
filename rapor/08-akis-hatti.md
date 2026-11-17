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

### Uyarı süresi ölçümü

Her mikro yığının süresi Spark'ın ilerleme raporundan kaydedildi (`analiz/cikti/akis-*.jsonl`). Bu kayıtlar `--ilerleme` ile tutuluyor ve depoya girmiyor. Tetik 10 sn. Bir olay en kötü durumda bir tetik bekliyor, sonra yığının işlenmesini bekliyor. Deneylerde (tek Spark uygulaması, aynı anda iki sorgu, `--hiz 50`):

| Deney | Sorgu | Yığın | Yığın süresi ortanca | En uzun |
|---|---|---|---|---|
| kart (%5 geç) | 2 dk watermark | 30 | 10,1 sn | 13,2 sn |
| kart (%5 geç) | 30 sn watermark | 31 | 8,7 sn | 14,3 sn |
| %30 geç | 2 dk | 30 | 10,1 sn | 17,3 sn |
| %50 geç | 2 dk | 30 | 10,0 sn | 15,4 sn |

Bir olayın veritabanına ulaşması en kötü durumda yaklaşık 10 sn (tetik) + 17,3 sn (en uzun yığın) = 27,3 sn sürüyor. Bu kartın 30 sn'lik sınırının altında, ama pay çok az. Dikkat çekici olan şu: yığın süresi tetik aralığına (10 sn) eşit. Saniyede yalnızca 50 olayda bile süre veriden değil, her yığının sabit maliyetinden geliyor: durum deposunun yazılması, iki sorgunun aynı 8 çekirdeği paylaşması, veritabanı bağlantısı. Yük arttığında ne olduğu bölüm 11'deki tepe yük deneyinde.

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

(Deney sonuçları aşağıda.)
