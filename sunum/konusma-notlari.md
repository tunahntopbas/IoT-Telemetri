# Sunum konuşma notları (12 dk)

Önerilen bölüşüm: Veli Tunahan slayt 1–3, Zeynep slayt 4–5 (canlı gösterim dahil), Behiç slayt 6–8, sorularda hepimiz. Herkes kendi bölümündeki sayıların hangi deneyden geldiğini bilmeli (`kanit/` dosya adları aşağıda).

## Slayt notları

**1. Omurga (0–2 dk, Veli Tunahan).** Şemayı soldan sağa anlat: üstte akış hattı (sensör → Kafka → Spark Streaming → TimescaleDB), altta toplu hat (S3 → Spark + Delta → TimescaleDB). İkisi aynı servis deposunda birleşiyor, Grafana yalnızca orayı okuyor. Kartı tek cümleyle söyle: 30 sn uyarı, %5 olay 2 dk geç, kayıp en fazla %1, p99 50 ms.

**2. Watermark (2–3,5 dk, Veli Tunahan).** Çelişkiyi söyle: 30 sn'de uyarı ister, ama olayların bir kısmı 2 dk geç geliyor. Çözüm update modu: pencere kapanmadan her 10 sn'de o ana kadarki sayı yazılıyor, geç olay sonradan aynı satırı güncelliyor. Tablodaki ilk satırı göster: kart oranında 30 sn watermark %4,13 kaybediyor, 2 dk hiç kaybetmiyor. Son satır dürüstlük için: geç olay 5 dk'ya kadar uzarsa 2 dk da yetmiyor (%3,0).

**3. Checkpoint (3,5–4,5 dk, Veli Tunahan).** İlk ölçümde en kötü gecikme 29,9 sn'ydi, yükte 37,5 sn: sınırı aşıyordu. Tetiği 5 sn'ye indirmek bir şey değiştirmedi. Spark'ın ilerleme raporu nedeni gösterdi: durumun S3'e yazılması. Checkpoint yerel diske alınca 13,4 sn. Bedeli tek makinenin diski.

**4. Delta (4,5–5 dk, Zeynep).** Aynı tablonun üstüne yazarken süreci öldürdük. Parquet'te eski dosyalar silinmiş, yenileri yarım: tablo okunamıyor. Delta'da işlem günlüğü yeni sürümü hiç onaylamadığı için eski sürüm eksiksiz okunuyor.

**5. Canlı gösterim (5–7 dk, Zeynep).** Önceden: `docker compose --profile servis-ts up -d`, akış işi ve üretici (`--hiz 50`) sunumdan 3 dk önce başlatılmış olsun. Göster: Akış 1 tablosu, zaman serisi her 10 sn'de ilerliyor, Toplu 1 ısı haritası. Bir şey aksarsa slayttaki ekran görüntüsüne geç; uğraşma.

**6. Kafka kesintisi (7–8 dk, Behiç).** Kafka'yı 90 sn durdurduk. Üretici beklemiş, olay kaybetmemiş. Ama akış işi 60 sn sonra ölmüş ve Kafka gelince geri dönmemiş. Panoda 200 sensörün hepsi "sustu": bu sensör değil hat arızası.

**7. Kurtarma (8–9 dk, Behiç).** Aynı checkpoint ile başlatınca 24.577 olay iki yığında okundu, kayıp 0. Veritabanı kapanınca da aynı. Çökmede yığın iki kez yazılıyor ama upsert sonucu bozmuyor. Kırmızı satır: checkpoint kaybolur da iş "son ofsetten" başlatılırsa %21,9 kayıp.

**8. Bulgu (9–10 dk, Behiç).** Veri kaybı modelle değil sensör numarasıyla ilgili: 13'ün katları %29 civarı, 23'ün katları %7–10 kaybediyor. Anomali ise modelle ilgili: TX-200 diğerlerinin 4 katı. Bakım için anlamı: kayıp için o sensörlerin bağlantısına, anomali için TX-200'e bakılmalı.

**9. Sorular (10–12 dk).**

## Olası sorular ve cevaplar

**Neden 2 dk watermark?** Kart olayların %5'inin 2 dk'ya kadar geç geldiğini söylüyor; kayıp sınırı %1. 30 sn watermark kart oranında %4,13 kaybetti, 2 dk hiç kaybetmedi (`kanit/akis-gec-kart-dogrulama.txt`). Uyarı süresini korumak için update modu kullandık; watermark uyarıyı geciktirmiyor. Bedeli: bir pencerenin sayısı 2 dk boyunca kesinleşmiyor ve durum belleği büyüyor.

**Neden 8 bölüm?** Bölüm sayısını tüketici paralelliği belirliyor: Spark'ta 8 çekirdek var, her bölüm bir görev. Hacim sınır değil: tepe 570 KB/s, Kafka bu makinede saniyede 18.000'den fazla olay kaldırdı. 200 sensör anahtarı 8 bölüme dengeli dağılıyor (en dolu / ortalama = 1,06). 100 kat büyümede 32–64 bölüm gerekir; bölüm sonradan artırılırsa anahtar → bölüm eşlemesi değişir, bunu baştan planlamak gerekir.

**Neden broadcast join?** Boyut tablosu 200 satır. Broadcast ile her göreve kopyalanıyor, 3 milyon satırlık olay tablosu shuffle edilmiyor. Fiziksel planda `BroadcastHashJoin` görünüyor (`kanit/explain-1.txt`).

**Neden TimescaleDB?** Bütün sorgular zaman aralığı okuması: bir sensörün son 10 dakikası ya da son N dakikanın bütün sensörleri. TimescaleDB tabloyu güne göre parçalıyor, sorgu yalnızca ilgili parçayı okuyor. Upsert hazır, SQL ile boyut tablosuna join var, Grafana kaynağı hazır. Ölçülen p99 en fazla 11,3 ms, sınır 50 ms. MongoDB'de gruplama sorguları aggregation pipeline'a dönüşürdü; Cassandra'da "bütün sensörler" sorguları bütün bölümleri tarardı.

**Neden zstd?** Aynı veride zstd'li Parquet 35,5 MB, snappy 44,5 MB: %20 daha küçük. 1095 günlük saklamada depolamayı boyut belirliyor. Okuma hızında anlamlı bir fark ölçemedik, o yüzden hız iddiasında bulunmuyoruz.

**Kafka çökünce ne oldu?** Üretici olayları bellekte tuttu, kayıp olmadı. Akış işi 60 sn sonra `TimeoutException` ile öldü ve kendiliğinden dönmedi; o sırada olayların %83,7'si sayılmamıştı. Aynı checkpoint ile başlatınca her şey yakalandı, kayıp 0. Gerçek sistemde iş gözetmen altında çalışmalı ve Kafka 3 kopyalı olmalı.

**Tam bir kez mi, en az bir kez mi?** Kafka'dan okuma en az bir kez: çökmede yarım kalan yığın baştan işleniyor. Veritabanına yazma (sensör, pencere) anahtarıyla upsert ve değer o pencerenin toplamı. Aynı yığın iki kez yazılınca sonuç değişmiyor. Bunu deneyle gösterdik: yığın 8 iki kez yazıldı, tablo Kafka ile birebir aynı kaldı.

**Ölçümler tekrarlanabilir mi?** Her deney `analiz/` altında bir betik; çıktısı `kanit/` altında, sayılar `kanit/olcumler.csv`'de. Tekrar sayısı CSV'nin `tekrar` sütununda; çoğu deney bir kez yapıldı, bunu raporda belirttik.
