# Sunum konuşma notları (12 dk)

Bölüşüm: Veli Tunahan slayt 1–4, Zeynep slayt 5–6 (canlı gösterim dahil), Behiç slayt 7–9, sorularda hepimiz. Süre kesin: 10. dakikada sunum kesilir. Sorular kişiye yöneltilebilir, o yüzden herkes her kararı ve sayının hangi `kanit/` dosyasından geldiğini bilmeli.

## 1. Omurga: iki hat, tek servis deposu

Konuşan: Veli Tunahan (0–1 dk). Soldan sağa üst satırı anlat: sensör üretici olayları Kafka’ya yazıyor, sensor_id anahtar olduğu için bir sensörün olayları hep aynı bölümde ve sırası korunuyor. Spark akış işi 1 dakikalık pencerelerde sayıyor, sonucu TimescaleDB’ye upsert ediyor, Grafana sadece TimescaleDB’yi okuyor. Alt satır: 14 günlük ham JSON S3’te gün klasörlerinde duruyor, Spark toplu iş temizleyip sensör tablosuyla birleştiriyor ve Delta tablosuna yazıyor; servis_yukle.py sonucu aynı TimescaleDB’ye taşıyor. Alttaki şeritte okların garantisi var: üretici acks=all ve idempotent, Spark Kafka’dan en az bir kez okuyor ama upsert yüzünden aynı yığın iki kez yazılsa da sonuç değişmiyor. Son cümle: bizde her şey tek kopya, üretimde Kafka 3 kopya olmalı. Ayrıntılı garanti tablosu raporda Tablo 3.2.

## 2. Kart: 2000 olay/s, 30 sn’de uyarı, 3 yıl saklama

Konuşan: Veli Tunahan (1–2 dk). Kartı tek nefeste oku: saniyede 2000 olay, tepede 4000, uyarı 30 saniyede, olayların yüzde 5’i 2 dakikaya kadar geç geliyor, kayıp en fazla yüzde 1, 3 yıl saklama, sorgu p99 50 ms. Olay boyutunu kendimiz ölçtük: Kafka’dan 1934 mesaj okuyup ortalamasını aldık, 142,58 bayt. Buradan günde 24,64 GB, 3 yılda 27 TB çıkıyor. Bizim disk 245 GB, yani ham veri 10 günde dolar. zstd’li Parquet ile 2,23 TB’a iniyor ama yine sığmıyor. Sonuç: hız sorun değil, saklama dağıtık depo istiyor. 10 ve 100 kat hesabı raporda bölüm 4’te.

## 3. Watermark 2 dk, ama uyarı yine 30 sn içinde

Konuşan: Veli Tunahan (2–3,5 dk). Önce çelişkiyi söyle: 30 saniyede uyarı isteniyor ama olaylar 2 dakikaya kadar geç geliyor. Normalde pencere watermark geçince kapanır, yani 2 dakika beklemek gerekir. Biz update modunu seçtik: Spark her 10 saniyede o ana kadarki sayıyı yazıyor, geç olay gelince TimescaleDB’de aynı satırı upsert ile güncelliyor. Yani watermark uyarıyı geciktirmiyor, sadece pencerenin ne kadar süre açık kalacağını belirliyor. Tabloyu göster: aynı veriyle iki watermark denedik. Kart oranında 30 saniye yüzde 4,13 kaybediyor, 2 dakika hiç kaybetmiyor. Geç oranı yüzde 30 ve 50’ye çıkarınca fark büyüyor. Son satırı saklama: gecikme 5 dakikaya uzarsa 2 dakika da yüzde 3 kaybediyor; kartın sınırı 2 dakika olduğu için 2 dakikayı seçtik.

## 4. Uyarı 30 sn’yi aşıyordu: checkpoint yerel diske

Konuşan: Veli Tunahan (3,5–4,5 dk). Bu slayt neden ölçmenin önemli olduğunu gösteriyor. İlk kurulumda checkpoint S3’teydi. Uyarı gecikmesini ölçünce 50 olay/s’de bile en kötü 29,9 saniye çıktı, 2000 olay/s’de 37,5: sınırı aşıyor. İlk akla gelen tetiği 10 saniyeden 5’e indirmekti, denedik, 29,3 çıktı, işe yaramadı. Spark’ın ilerleme raporunda her yığının yaklaşık 10 saniyesinin durumun S3’e yazılmasına gittiğini gördük. Checkpoint’i yerel diske alınca en kötü 13,4, 8000 olay/s’de bile 16,1 saniye. Bedelini söyle: disk giderse checkpoint gider; üretimde kopyalı disk gerekir. Checkpoint kaybolursa ne olduğunu Behiç kırılma kısmında anlatacak.

## 5. Delta, çünkü yarıda kesilen yazma tabloyu bozmamalı

Konuşan: Zeynep (4,5–5 dk). Soru şu: toplu iş tabloyu yeniden yazarken çökerse ne olur? Aynı deneyi iki biçimde yaptık. Çıplak Parquet’te Spark önce eski dosyaları siliyor, yenileri geçici klasörde yarım kalıyor; sonuçta tablo okunamıyor, şema bile çıkarılamıyor. Delta’da yeni dosyalar yazılsa da işlem günlüğüne yeni sürüm eklenmediği için okuyucu hâlâ eski sürümü görüyor: 2.992.895 satırın hepsi orada. Arta kalan 25 dosya sahipsiz, VACUUM ile temizlenir. Bedelini de söyle: Delta yazması 41,4 saniye, Parquet 16,6. Toplu iş zaten dakikalar sürüyor, bu fark bize sorun değil. Ek bilgi sorulursa: Delta tablosunun içi zstd’li Parquet, JSON’dan 12,12 kat küçük; versionAsOf ile eski sürümü okuduk.

## 6. Canlı: akış hattı veri işliyor

Konuşan: Zeynep (5–7 dk). Sunumdan 3 dakika önce hazırla: docker compose --profile servis-ts up -d, akış işini başlat, üreticiyi --hiz 50 ile başlat. Tarayıcıda Grafana panosu açık dursun. Göster: Akış 1 tablosu 10 saniyede bir değişiyor; dakika başına olay grafiği ilerliyor; aşağıda toplu hattın sonuçları aynı panoda. Cümle: Grafana sadece TimescaleDB’yi okuyor, iki hattın sonucu orada birleşiyor. Bir şey aksarsa uğraşma, bu slayttaki görüntüyle anlat ve geç.

## 7. Kafka’yı 90 saniye durdurduk

Konuşan: Behiç (7–8 dk). Deneyi anlat: akış çalışırken Kafka kapsayıcısını 90 saniye durdurduk. Üretici kayıp vermedi: acks=all olduğu için onay gelmeyen olayları bellekte tuttu, Kafka dönünce gönderdi; 29.304 olayın hepsi Kafka’da. Asıl kırılan akış işi: 60 saniye sonra TimeoutException ile öldü ve Kafka geri gelince kendi başına dönmedi. Panoda bunun belirtisi ilginç: 200 sensörün hepsi aynı anda susan görünüyor. Bunu görünce sensörlere değil hatta bakmak gerekiyor. O anda olayların yüzde 83,7’si henüz sayılmamıştı. Bir sonraki slaytta nasıl kurtardığımız.

## 8. Aynı checkpoint ile kayıp 0, checkpoint gidince %21,9

Konuşan: Behiç (8–9 dk). Kurtarma: akış işini aynı checkpoint ile başlattık. Spark kaldığı ofsetten okudu, 24.577 olayı iki yığında yakaladı, kayıp sıfır. Veritabanını kapattığımızda da aynı: iş öldü ama ofset ilerlemediği için yeniden başlatınca kayıp ve tekrar sıfır. Kırmızı sayı önemli: checkpoint kaybolur da biri işi “son ofsetten” yani latest ile başlatırsa 2.289 mesaj atlanıyor, kayıp yüzde 21,9. Earliest ile başlatınca baştan okuyor ama upsert sayesinde sonuç bozulmuyor. Tablodaki üç satır raporun kırılma tablosundan; toplamda 12 kırılma deneyi yaptık. Teslim garantisi sorulursa: Kafka’dan en az bir kez okuyoruz, upsert ile sonuç tam bir kez gibi; bir yığını bilerek iki kez yazdırdık, tablo Kafka ile birebir aynı kaldı.

## 9. Anomali modele, veri kaybı sensör numarasına bağlı

Konuşan: Behiç (9–10 dk). Toplu 2 sorusu iki şeyi soruyordu: hangi model daha çok anomali veriyor ve hangi sensörler veri kaybediyor. Anomali modelle ilgili: TX-200’de yüzde 1,17, diğer dört modelde yüzde 0,30 civarı, yani 4 kat. Kayıp ise modelle ilgili değil: yüzde 20’nin üstünde kaybeden 15 sensörün hepsi 13’ün katı, yüzde 7–10 kaybeden 8 sensörün hepsi 23’ün katı ve bunlar beş modele de dağılmış. Beklenen olay sayısı 200 sensörün 14 gündeki ortancası, 15.338; rastgele dalgalanma bunun karekökü, yüzde 0,81. O yüzden yüzde 1–5 arası kaybı gerçek kayıp saymadık. Bakım ekibine söyleyeceğimiz: anomali için TX-200’e, kayıp için bu 23 sensörün bağlantısına bakın.

## 10. Sorular

Sorular herkese sorulabilir, her üye her kararı bilmeli. Kısa cevaplar:

## Olası sorular ve kısa cevaplar

**Neden 2 dk watermark?** Kart %5 olayın 2 dk’ya kadar geç geldiğini söylüyor; 30 sn watermark kart oranında %4,13 kaybetti, 2 dk hiç kaybetmedi. Uyarı update modu sayesinde gecikmiyor.

**Neden 8 bölüm?** Spark’ta 8 çekirdek, her bölüm bir görev. Hacim sınır değil, tepe 570 KB/s. 200 sensör 8 bölüme dengeli dağılıyor (en dolu / ortalama 1,06). 100 katta 32–64 bölüm; sonradan artırmak anahtar eşlemesini değiştirir.

**Neden TimescaleDB?** Bütün sorgular zaman aralığı okuması; tablo güne göre parçalanıyor, upsert ve SQL join hazır. Ölçülen p99 en fazla 11,3 ms, sınır 50 ms.

**Neden broadcast join?** Boyut tablosu 200 satır, 6,4 KB. Olay tablosu shuffle edilmiyor; explain-1.txt’te BroadcastHashJoin. Üç koşu ortalaması 8,1 sn, sort-merge 10,8 sn.

**Neden zstd?** Snappy’den %20 küçük (35,5 MB’a 44,5 MB). Okuma hızında anlamlı fark ölçemedik, hız iddiamız yok.

**Tam bir kez mi?** Kafka’dan en az bir kez okuyoruz; (sensör, pencere) upsert yüzünden aynı yığın iki kez yazılınca sonuç değişmiyor. Yığın 8’i bilerek iki kez yazdırdık, tablo Kafka ile birebir aynı.

**Bölüm anahtarı ve sıcak nokta?** bolge ile en dolu / ortalama 3,44, sensor_id+saat ile 1,04.

**CAP/PACELC?** Kafka (acks=all, min.insync.replicas=2) ve eşzamanlı yedekli PostgreSQL için PC/EC: bölünmede tutarlılık, normalde gecikme yerine tutarlılık. Uyarının doğru olması birkaç saniyelik yazma beklemesinden önemli. Veritabanı kapanınca pano boş değil hata gösterdi.
