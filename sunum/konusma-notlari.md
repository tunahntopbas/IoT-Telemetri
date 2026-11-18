# Sunum konuşma notları (10 dk + 2 dk soru)

Bölüşüm: Veli Tunahan slayt 1–3, Zeynep slayt 4–6 (canlı gösterim dahil), Behiç slayt 7–9, sorularda hepimiz. Hoca soruları kişiye soruyor; herkes kendi slaytlarındaki sayıların hangi deneyden geldiğini bilmeli (her slaytın sol altında kanıt dosyası yazıyor).

Kural: slayttaki metni okumuyoruz. Başlık slaytın iddiası; onu söyleyip kanıtı gösteriyoruz.

## Slayt notları

**1. Omurga (0–1 dk, Veli Tunahan).** Şemayı soldan sağa anlat: üstte akış hattı (sensör → Kafka → Spark Streaming → TimescaleDB), altta toplu hat (S3 → Spark + Delta → TimescaleDB). İki hat aynı servis deposunda birleşiyor, Grafana yalnızca orayı okuyor. Kutularda ürün ve sürüm var; hepsi tek makinede.

**2. Kart ve ölçülenler (1–2 dk, Veli Tunahan).** Kartın dört sınırını ve bizde ölçüleni tek tek söyle: uyarı 30 sn sınırına karşı en fazla 16,1 sn; kayıp %1 sınırına karşı 0; p99 50 ms sınırına karşı 11,3 ms; tepe yükün 2 katında bile her olay sayıldı. "Bu sonuçlara iki kararla geldik" diyerek geç.

**3. Watermark (2–3,5 dk, Veli Tunahan).** Çelişkiyi söyle: 30 sn'de uyarı ister, ama olayların %5'i 2 dk geç geliyor. Çözüm update modu: pencere kapanmadan her 10 sn'de o ana kadarki sayı yazılıyor, geç olay sonradan aynı satırı güncelliyor. Tablodaki ilk satırı göster: kart oranında 30 sn watermark %4,13 kaybediyor, 2 dk hiç kaybetmiyor. Son satır dürüstlük için: geç gelme 5 dk'ya uzarsa 2 dk da yetmiyor (%3,0).

**4. Checkpoint (3,5–4,5 dk, Zeynep).** İlk ölçümde en kötü gecikme 29,9 sn'ydi, 2.000 olay/s'de 37,5 sn: sınır aşılıyordu. Tetiği 5 sn'ye indirmek bir şey değiştirmedi. Spark'ın ilerleme raporu nedeni gösterdi: durumun S3'e yazılması her yığında 9–24 sn. Checkpoint yerel diske alınca 13,4 sn, 8.000 olay/s'de 16,1 sn. Bedeli tek makinenin diski.

**5. Delta (4,5–5 dk, Zeynep).** Aynı tablonun üstüne yazarken süreci `kill -9` ile öldürdük. Parquet'te eski dosyalar silinmiş, yenileri yarım: tablo okunamıyor. Delta'da işlem günlüğü yeni sürümü hiç onaylamadığı için eski sürüm eksiksiz okunuyor. Bedeli yazma süresi: 41,4 sn'ye karşı 16,6 sn.

**6. Canlı gösterim (5–7 dk, Zeynep).** Önceden: Docker açık, `docker compose --profile servis-ts up -d`, akış işi ve üretici (`--hiz 50`) sunumdan 3 dk önce başlatılmış, Grafana tarayıcıda açık. Sırayla göster: zaman serisi her 10 sn'de ilerliyor, Akış 1 tablosu, Akış 2 boş (susan sensör yok), Toplu 1 ısı haritası. Bir şey aksarsa slayttaki ekran görüntüsüne geç; uğraşma.

**7. Kafka kesintisi (7–8 dk, Behiç).** Ne yaptık, ne gördük, neye bakınca, gerçek sistemde: dört kutuyu sırayla söyle. Asıl nokta: üretici olay kaybetmedi, ama akış işi 60 sn sonra öldü ve Kafka gelince geri dönmedi. Panoda 200 sensörün hepsi aynı anda "sustu": bu sensör değil hat arızası.

**8. Kurtarma (8–9 dk, Behiç).** Aynı checkpoint ile başlatınca kaçırılan 24.577 olay okundu, kayıp 0. Veritabanı kapanınca da aynı. Çökmede yığın iki kez yazılıyor ama upsert sonucu bozmuyor. Sarı satırı vurgula: checkpoint kaybolur da iş "son ofsetten" başlarsa %21,9 kayıp. Son cümle teslim garantisi.

**9. Bulgu (9–10 dk, Behiç).** Grafikte soldaki yüksek çubuklar: numarası 13'ün katı olan 15 sensör %29 civarı, 23'ün katı olan 8 sensör %7–10 kaybediyor. Renkler karışık, yani model değil. Bakım ekibi için anlamı: önce bu sensörlerin bağlantısına bakılmalı. (Soru gelirse ikinci bulgu: TX-200'ün anomali oranı %1,17, diğer modellerin yaklaşık 4 katı.)

**10. Sorular (10–12 dk).** Soru yedek slaytlardan birine denk gelirse o slaytı aç: Y1 garanti tablosu, Y2 neden 8 bölüm, Y3 sorgu gecikmeleri.

## Olası sorular ve cevaplar

**Neden 2 dk watermark?** Kart olayların %5'inin 2 dk'ya kadar geç geldiğini söylüyor; kayıp sınırı %1. 30 sn watermark kart oranında %4,13 kaybetti, 2 dk hiç kaybetmedi (`kanit/akis-gec-kart-dogrulama.txt`). Uyarı süresini korumak için update modu kullandık; watermark uyarıyı geciktirmiyor. Bedeli: bir pencerenin sayısı 2 dk boyunca kesinleşmiyor ve durum belleği büyüyor.

**Neden 8 bölüm?** Bölüm sayısını tüketici paralelliği belirliyor: Spark'ta 8 çekirdek var, her bölüm bir görev. Hacim sınır değil: tepe 570 KB/s, Kafka bu makinede saniyede 18.000'den fazla olay kaldırdı. 200 sensör anahtarı 8 bölüme dengeli dağılıyor (en dolu / ortalama = 1,06). 100 kat büyümede 32–64 bölüm gerekir; bölüm sonradan artırılırsa anahtar → bölüm eşlemesi değişir, bunu baştan planlamak gerekir.

**Neden broadcast join?** Boyut tablosu 200 satır. Broadcast ile her göreve kopyalanıyor, 3 milyon satırlık olay tablosu shuffle edilmiyor. Fiziksel planda `BroadcastHashJoin` görünüyor (`kanit/explain-1.txt`).

**Neden TimescaleDB?** Bütün sorgular zaman aralığı okuması: bir sensörün son 10 dakikası ya da son N dakikanın bütün sensörleri. TimescaleDB tabloyu güne göre parçalıyor, sorgu yalnızca ilgili parçayı okuyor. Upsert hazır, SQL ile boyut tablosuna join var, Grafana kaynağı hazır. Ölçülen p99 en fazla 11,3 ms, sınır 50 ms. MongoDB'de gruplama sorguları aggregation pipeline'a dönüşürdü; Cassandra'da "bütün sensörler" sorguları bütün bölümleri tarardı.

**Neden zstd?** Aynı veride zstd'li Parquet 35,5 MB, snappy 44,5 MB: %20 daha küçük. 1095 günlük saklamada depolamayı boyut belirliyor. Okuma hızında anlamlı bir fark ölçemedik, o yüzden hız iddiasında bulunmuyoruz.

**Kafka çökünce ne oldu?** Üretici olayları bellekte tuttu, kayıp olmadı. Akış işi 60 sn sonra `TimeoutException` ile öldü ve kendiliğinden dönmedi; o sırada olayların %83,7'si sayılmamıştı. Aynı checkpoint ile başlatınca her şey yakalandı, kayıp 0. Gerçek sistemde iş gözetmen altında çalışmalı ve Kafka 3 kopyalı olmalı.

**Tam bir kez mi, en az bir kez mi?** Kafka'dan okuma en az bir kez: çökmede yarım kalan yığın baştan işleniyor. Veritabanına yazma (sensör, pencere) anahtarıyla upsert ve değer o pencerenin toplamı. Aynı yığın iki kez yazılınca sonuç değişmiyor. Bunu deneyle gösterdik: yığın 8 iki kez yazıldı, tablo Kafka ile birebir aynı kaldı.

**Ölçümler tekrarlanabilir mi?** Her deney `analiz/` altında bir betik; çıktısı `kanit/` altında, sayılar `kanit/olcumler.csv`'de. Tekrar sayısı CSV'nin `tekrar` sütununda; çoğu deney bir kez yapıldı, bunu raporda belirttik.
