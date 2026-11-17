# BSM 461 Dönem Projesi — Hocanın İsterleri
Son teslim: 11 Aralık 2026 Cuma 23.59 (geç teslim yok). Sunum 13–14. hafta, 10 dk + 2 dk soru. Grup G221210024 (1A), şablon 3 IoT. Servis deposu: PostgreSQL+TimescaleDB, pano: Grafana (karar verildi).
## Genel kurallar
- En az 2 akış + 2 toplu soru (aynı türden ikisi tekrar olmamalı; eşikler senaryo kartından)
- Bulgular kendi verimizden sorgu + ölçümle. ÖLÇÜM KURALI: her "daha hızlı/iyi" iddiasının yanında ölçülmüş sayı + nasıl ölçüldüğü (veri boyutu, makine, kaç tekrar). Ölçülmemiş sayı = bileşen sıfır.
- Her rapor bölümü: "Ne yaptınız?" ve "Neden böyle yaptınız?"
## Zorunlu bileşenler (puan)
- Z0 (8): Doldurulmuş omurga şeması (her kutuda ürün+sürüm) + her ok için garanti tablosu (kaç kopya / sıra garantisi / ağ bölünürse ne olur) + kitin tek makine sadeleştirmeleri ve üretimde ne olmalı. Sunum bu şemayla başlar.
- Z1 (3): olay/s × bayt × 86400 × saklama günü; Parquet'te ne olur; tepe yük (ortalama mı tepe mi, neden); tek makine mi dağıtık mı; hangi bileşen ilk tıkanır; 10x ve 100x. Her sayının birimi ve kaynağı.
- Z2 (5): JSON vs Parquet toplam boyut + aynı sorgunun iki biçimde süresi; snappy/zstd/gzip boyut + okuma süresi; projection/predicate pushdown (explain'deki PushedFilters, ReadSchema).
- Z3 (7): bölüm anahtarı adayları, her biri için en dolu bölüm ÷ ortalama (8 bölüm); sıcak nokta varsa çözüm (anahtar değişikliği/tuzlama) önce/sonra; çoğaltma (kitte kaç, üretimde kaç); ağ bölünmesinde C mi A mı + PACELC; idempotentlik: tekrar eden olay sayısı, sonuç bozuluyor mu.
- Z4 (3): S3 dizin düzeni (tarih=…/) gerekçesi; dosya sayısı ve ortalama boyut, küçükse birleştirme; HDFS yerine nesne deposunun bedeli (listeleme maliyeti, atomik rename yok → yarım çıktı tehlikesi).
- Z5 (10): toplu iş: ham → temizleme → boyut tablosu (s3://ham/boyut/iot/) ile join → toplulaştırma → yazma; map/shuffle/reduce yapısı; explain(mode="formatted") → kanit/explain-1.txt, her Exchange neden orada, hangi filtre aşağı itildi, join stratejisi; Spark UI (localhost:4040) ekran görüntüsü → kanit/spark-ui-*.png; bir performans kararı (shuffle.partitions, broadcast, cache, tuzlama) aynı veriyle ≥3 koşu, önce/sonra ortalama; kirli veri kararı (at/düzelt/ayrı tablo, neden); 2 toplu soruya en az 2 grafik + birer cümle bulgu.
- Z6 (4): erişim deseni tablosu (hangi sorgu, ne sıklıkla, p99 sınırı kartta 50 ms) → seçim gerekçesi; veri modeli ve anahtar tasarımı; shard/bölüm anahtarı ve monoton artan anahtar riski.
- Z7 (5): toplu çıktı Delta; ikinci yazma (yeni gün ya da düzeltme); DESCRIBE HISTORY + versionAsOf; atomik yazma deneyi: büyük veriyle yazma sırasında Ctrl+C, tablo bozulmadı; aynısı çıplak Parquet ile ne oluyor.
- Z8 (10): konu tasarımı (bölüm sayısı karttaki hızdan hesaplanır; üretici 3 bölümle açar, sil-yeniden oluştur), anahtar, sıra garantisi nerede geçerli; üretici ayarları acks=all + idempotent ve bedeli (isteğe bağlı --acks 1 / --idempotent-kapali ile ölç); akış işi (is/akis_iskelet.py TODO'ları): pencere türü/boyutu, watermark süresi — uyarı süresi, geç veri, kayıp sınırı çelişkisi ve hangisinden ödün verildiği; GEÇ VERİ DENEYİ: karttaki oranla ve --gec-orani 0.3 ile, sayıma girmeyen olay sayısı (üretici gönderdiği sayıyı yazar, çıktıyla karşılaştır; tekrar olaylar sayıyı şişirir, açıkla); YENİDEN BAŞLATMA DENEYİ: --cikti s3, durdur, başlat, kayıp/tekrar var mı; teslim garantisi (en az bir / tam bir kez, checkpoint + atomik/idempotent yazma).
- Z9 (3): Şablon 3 → pano (Grafana): ne gösteriyor, ne sıklıkla güncelleniyor, verisi nereden.
- Z10 (7): KVKK notu (kişiyi tanımlayabilecek alan var mı, ne yapılacak); kirli veri tablosu (kaç kayıt, hangi kusur, ne yapıldı); 2 grafik + bulgu cümleleri.
- Z11 (20!): en az 3 kırılma noktası tablosu: kırılma noktası | tetikleyen | belirti / neye bakınca görürsünüz | ne yaparsınız. En az biri gerçekten denenmiş (docker compose stop kafka, stop s3, --hiz tepe yük, --gec-orani 0.5), kanıt kanit/kirilma-deneyi.*. Her deneyde: ne gördünüz, neye bakınca, gerçek sistemde ne yapardınız. "Hiç kırılmaz" = 0 puan.
- Z12 + düzen (5): YZ kullanım beyanı (hangi araç, hangi adım, ne kadar), rapor düzeni, README ile sıfırdan çalışabilirlik.
- Sunum (10): 0–2 dk doldurulmuş şema, 2–5 iki karar + ölçümleri, 5–7 canlı gösterim/ekran kaydı, 7–9 kırılma deneyi, 9–10 bir bulgu, 10–12 sorular. Her üye bir bölüm anlatır, sorular kişiye.
## Puan kaybettirenler
Ölçülmemiş sayı → bileşen 0 · uydurma kaynak → −10 · README ile ayağa kalkmıyor → Z5 ve Z8 yarı puan · kopya → proje 0 · geç teslim kabul yok.
## Teslim
Rapor PDF ≤25 sayfa (ekler hariç, ≥11 punto, şekil/tablo numaralı, her sayının birimi) → SABIS: BSM461_Proje_1A_G221210024.pdf · Sunum PDF ..._sunum.pdf (ilk slayt doldurulmuş şema) · Katkı beyanı (raporun son sayfası, üç üye) · Açık depo BSM461_Proje_1A_G221210024 (şimdilik IoT-Telemetri) · git tag teslim + commit hash kapakta · README ile sıfırdan çalıştırma denemesi. Ham veri depoya girmez.
## Depo düzeni
README.md, docker-compose.yml, is/, analiz/, uretici/ (değiştirilirse), kanit/{ortam.png, senaryo.txt, spark-ui-*.png, explain-*.txt, olcumler.csv (ne, kaç kez, sonuç), kirilma-deneyi.*}
## Rapor yapısı (bu sırayla)
1 Kapak (grup kodu, şube, üyeler, şablon, senaryo kartı, depo bağlantısı, teslim commit) · 2 Özet (4 soru, ne kurduk, 3 bulgu) · 3 Mimari Z0 (2 s) · 4 Ölçek Z1 (1 s) · 5 Depolama Z2+Z4+Z7 (3 s) · 6 Bölümleme Z3 (2 s) · 7 Toplu hat Z5 (3 s) · 8 Akış hattı Z8 (3 s) · 9 Servis + pano Z6+Z9 (2 s) · 10 Veri kalitesi Z10 (2 s) · 11 Nerede kırılır Z11 (≥2 s) · 12 YZ beyanı · 13 Katkı beyanı · Ekler (ortam kanıtı, veri sözlüğü, uzun ölçümler)
## Takvim
1) 17–22 Kas: kurulum, senaryo, veri sözlüğü, 4 soru, KVKK, Z1, Z0 taslak · 2) 23–29 Kas: Z2, Z4, Z3, Z5, Z7, grafikler · 3) 30 Kas–6 Ara: Z8 deneyleri, Z6, Z9, Z11 deneyleri · 4) 7–11 Ara: Z0 son hâl, rapor, sıfırdan test, teslim, sunum
