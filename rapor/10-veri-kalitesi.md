# 10. Veri kalitesi (Z10)

## Kusur örnekleri

Kitin üreticisi veriye bilerek dört kusur ekliyor. Aşağıdaki örneklerin hepsi kendi grup kodumuzla (`G221210024`, şablon `iot`) 17.11.2026'da üretilen veriden alındı. Mesajlar değiştirilmeden alıntılandı. Sayılar `kanit/olcumler.csv` dosyasında.

Kullanılan veri:
- Akış: `olcum-bayt` konusuna `--hiz 100 --sure 20` ile gönderilen 1930 olay. Kafka'da 1934 mesaj oldu.
- Toplu: `--toplu 200000 --gun 2` ile S3'e yazılan 200.000 satır (`s3://ham/iot/tarih=2026-11-15/`, `tarih=2026-11-16/`).

### 1. Çarpık anahtar

Bizim şablonumuzda sensör düzeyinde çarpıklık zayıf. 200.000 satırda sensör başına ortalama 999,6 olay var; en çok gönderen `sensor-139` 1100 olayla ortalamanın 1,10 katı. En az gönderen `sensor-026` ise 664 olayla ortalamanın 0,66 katı. Yani sensörlerin bir kısmı az gönderiyor, ama tek bir sensör yükü domine etmiyor. Kitin README'si "birkaç anahtar olayların çoğunu üretir" diyor. Bu durum diğer şablonlarda (log, satış) belirgin; bizim verimizde sensör seçimi neredeyse eşit dağılıyor.

Çarpıklık bölüm düzeyinde ortaya çıkıyor. Yalnızca 200 farklı anahtar olduğu için anahtarlar bölümlere eşit dağılmıyor:
- Kafka'da 3 bölümlü `olcum-bayt` konusuna gelen 1934 mesaj bölümlere 743 / 637 / 554 dağıldı (en dolu / ortalama = 1,15).
- Aynı 200.000 satırı DuckDB'nin `hash(sensor_id) % 8` ifadesiyle 8 bölüme ayırınca en dolu bölümde 32.783, en boşta 17.972 satır çıktı (en dolu / ortalama = 1,31). DuckDB'nin hash fonksiyonu Kafka'nınkiyle (murmur2) aynı değil. Z3'te Kafka ve Spark'ın kendi bölümleyicisiyle yeniden ölçülecek.

En dolu bölüme düşen bir mesaj (Kafka bölüm 2):

```
sensor-049 | {"olay_zamani": "2026-11-17T17:42:42.249Z", "sensor_id": "sensor-049", "bolge": "Marmara", "sicaklik": 22.13, "nem": 54.0, "titresim": 0.261}
```

### 2. Geç olay

Akışta 1934 mesajın 93'ünde Kafka'ya yazılma zamanı (`CreateTime`) olay zamanından 5 saniyeden fazla sonraydı (%4,81; kartta %5). En büyük gecikme 118,7 saniye (kartta en fazla 2 dk). Örnek: aşağıdaki olay Kafka'ya `olay_zamani`'ndan 117,6 saniye sonra yazıldı.

```
{"olay_zamani": "2026-11-17T17:40:45.619Z", "sensor_id": "sensor-156", "bolge": "Akdeniz", "sicaklik": 30.21, "nem": 46.9, "titresim": 0.154}
```

Kısa okumada (`sensorler` konusu, 20 mesaj) da aynı durum görüldü: `2026-11-17T17:17:22` civarı mesajların arasında `olay_zamani` `17:15:45.001Z` ve `17:15:39.175Z` olan iki olay geldi.

### 3. Tekrar eden olay

Akışta üretici 1930 olay gönderdiğini yazdı, Kafka'da 1934 mesaj vardı. Aradaki 4 mesaj aynı olayın ikinci kopyası. Toplu veride 200.000 satırın 427'si birebir tekrar (%0,21). Üretici de "içinde 427 tekrar" yazdı, DuckDB ile `count(*) - count(DISTINCT satır)` aynı sonucu verdi. Örnek (toplu veride iki kez geçiyor):

```
{"olay_zamani": "2026-11-16T19:00:13.119Z", "sensor_id": "sensor-080", "bolge": "İç Anadolu", "sicaklik": 14.88, "nem": 65.7, "titresim": 0.166}
```

Akıştan bir örnek:

```
{"olay_zamani": "2026-11-17T17:42:46.280Z", "sensor_id": "sensor-069", "bolge": "Güneydoğu", "sicaklik": 33.47, "nem": 44.2, "titresim": 0.173}
```

### 4. Kirli kayıt

Toplu veride 182 kayıtta bir alan hiç yok, 195 kayıtta alan var ama değeri `null`. Toplam 377 kayıt, %0,19 (varsayılan oran %0,2).

| Alan | Eksik (alan yok) | Boş (`null`) |
|---|---|---|
| `sensor_id` | 39 | 41 |
| `bolge` | 29 | 38 |
| `sicaklik` | 40 | 42 |
| `nem` | 32 | 46 |
| `titresim` | 42 | 28 |

Eksik alan örneği (`sicaklik` yok):

```
{"olay_zamani": "2026-11-15T08:44:06.697Z", "sensor_id": "sensor-129", "bolge": "İç Anadolu", "nem": 57.7, "titresim": 0.292}
```

Boş değer örnekleri:

```
{"olay_zamani": "2026-11-15T08:53:17.228Z", "sensor_id": "sensor-139", "bolge": "Güneydoğu", "sicaklik": null, "nem": 43.9, "titresim": 0.232}
{"olay_zamani": "2026-11-15T10:36:34.032Z", "sensor_id": null, "bolge": "Güneydoğu", "sicaklik": 36.09, "nem": 48.7, "titresim": 0.226}
```

Akışta 1934 mesajda 1 kirli kayıt çıktı (`sicaklik: null`).

Not: Kusur sayılmayan ama ayırt edilmesi gereken bir durum daha var: gerçek anomaliler. Toplu veride titreşimi 1,0'ın üstünde 782 kayıt var (normal ortalama 0,21). Örnek:

```
{"olay_zamani": "2026-11-15T23:32:18.318Z", "sensor_id": "sensor-167", "bolge": "Güneydoğu", "sicaklik": 58.35, "nem": 46.4, "titresim": 3.123}
```

Bunlar temizlik sırasında atılmamalı, çünkü Akış 1 sorusunun aradığı şey tam olarak bu.

## Kirli veri tablosu ve kararlar

(Toplu iş yazıldığında doldurulacak: kaç kayıt, hangi kusur, at / düzelt / ayrı tablo kararı ve gerekçesi. İlk düşüncemiz: `sensor_id` olmayan kayıt hiçbir soruya bağlanamadığı için ayrı bir "karantina" tablosuna yazılacak. `bolge` eksikse `sensorler.csv`'den doldurulabilir. Ölçüm alanı eksikse kayıt o ölçümün hesabına girmeyecek ama sayımda tutulacak. Tekrarlar tam satır eşleşmesiyle ayıklanacak.)

## KVKK notu

Olaylardaki alanlar `olay_zamani`, `sensor_id`, `bolge`, `sicaklik`, `nem` ve `titresim`. Boyut tablosundaki alanlar `model` ve `kurulum_yili`. Bunların hiçbiri ad, kimlik numarası, telefon ya da e-posta gibi bir kişiyi doğrudan tanımlayan bilgi değil. Ölçülen şey makine ve ortam değerleri.

Yine de dolaylı tanımlama riski var:
- Bir sensör bir eve, küçük bir işletmeye ya da tek kişinin çalıştığı bir yere kuruluysa, `sensor_id` o adresle eşleştirilebilir. Bu durumda zaman damgalı sıcaklık, nem ve titreşim verisi kişinin davranışını ele verebilir (evde olup olmadığı, makinenin ne zaman çalıştığı). O noktada veri kişisel veri sayılır.
- Bizim veride konum yalnızca 7 büyük bölge düzeyinde. Bu tek başına bir kişiyi göstermez. Ama sensör–adres eşlemesi başka bir tabloda tutuluyorsa ikisi birleştirildiğinde risk doğar.

Yaptıklarımız ve yapacaklarımız:
- Projede sensör–adres ya da sensör–müşteri eşlemesi tutmuyoruz. `sensorler.csv`'de yalnızca bölge, model ve kurulum yılı var.
- Panoda ve raporda bölge düzeyinden daha ayrıntılı konum göstermeyeceğiz.
- Gerçek bir sistemde sensör kimliği ile adres eşlemesi ayrı, erişimi kısıtlı bir tabloda tutulmalı. Analiz tarafına yalnızca takma ad (`sensor_id`) gelmeli. Ham veri 1095 gün saklanacağı için saklama süresinin amacı (bakım ve arıza analizi) açıkça yazılmalı, süre dolunca veri silinmeli.
- Kitte güvenlik kapalı (şifre ve TLS yok). Bu yalnızca yerel geliştirme için kabul edilebilir. Gerçek sistemde aktarım şifreli olmalı ve erişim yetkilendirilmeli.
