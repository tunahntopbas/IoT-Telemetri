# 2. Özet

## Sorularımız

Senaryo kartımız (`kanit/senaryo.txt`): saniyede ortalama 2.000 ölçüm, en yoğun saatte bunun 2 katı. Akış sorularının cevabı en geç 30 saniyede çıkmalı. Olayların %5'i en fazla 2 dakika geç geliyor ve sayıma girmeyen olaylar en fazla %1 olabilir. Ham veri 1095 gün saklanıyor, servis sorgusunda p99 en fazla 50 ms.

### Akış 1: Son 10 dakikada hangi sensör normal aralığın dışına çıktı?

Her sensör için olay zamanına göre pencerelerde sıcaklık ve titreşim izlenir, eşiği aşan sensör uyarı olarak yazılır.

- Ne yaptınız / neden: Eşik olarak ilk örnekte ölçtüğümüz dağılımı kullanmayı düşünüyoruz. 200.000 kayıtta titreşimin %99'u 0,324'ün altında, 1,0 üstü 782 kayıt var. Sıcaklıkta %99 değeri 36,37 °C, 45 °C üstü 500 kayıt var. Sıcaklık bölgeye göre değiştiği için sıcaklık eşiği bölge bazında olacak. Kesin eşikler toplu veriyle (Adım 6) belirlenecek.
- Kartla ilişkisi: Uyarı en geç 30 sn'de çıkmalı, yani tetikleme aralığı ve pencerenin kapanması 30 sn'yi geçmemeli. Öte yandan olayların %5'i 2 dk'ya kadar geç geliyor. Pencereyi 2 dk bekletirsek uyarı süresini aşarız. Bu yüzden uyarı için "update" çıktısıyla ilk eşik aşımında hemen yazmayı, geç gelen olayları da 2 dk'lık watermark içinde sonradan sayıma eklemeyi planlıyoruz. Bu çelişkiyi ve hangi koşuldan ödün verdiğimizi Z8'de ölçerek göstereceğiz.

### Akış 2: Hangi sensör sustu?

Belirli bir süre boyunca hiç olay göndermeyen sensörleri bulmak.

- Neden: Toplu örnekte sensör başına ortalama 999,6 olay varken bazı sensörler belirgin şekilde az gönderiyor (en az: `sensor-026`, 664 olay). En az gönderen 8 sensörün hepsi 13'ün katı (26, 52, 65, 91, 143, 156, 169, 195). Bunlar düzenli olarak mesaj kaybeden bir grup gibi görünüyor. Üretici kodunda ayrıca bazı sensörlerin her saat birkaç dakika tamamen sustuğu görülüyor; bunu kendi verimizde zaman serisiyle Adım 6'da göstereceğiz.
- Kartla ilişkisi: "Sustu" kararı için bekleme süresi geç veri sınırından (2 dk) uzun olmalı. Yoksa yalnızca geç kalan bir sensörü susmuş sayarız. Bekleme süresinin uyarı süresiyle (30 sn) nasıl uzlaşacağı Z8'de tartışılacak. Sensör listesi `sensorler.csv`'den alınacak; hiç olay gelmeyen sensör de görünsün diye boyut tablosuyla karşılaştırma yapılacak.

### Toplu 1: Bölgelere göre günün saatine bağlı sıcaklık ve titreşim deseni

Her bölge için Türkiye saatine (UTC+3) göre saatlik ortalama, %95 ve %99 sıcaklık ve titreşim değerleri.

- Neden: Veride gündüz sıcak, gece serin bir döngü ve bölgeler arasında taban sıcaklık farkı var. Üretici kodunda ayrıca ardışık 10 sensörlük bir grubun günün belirli bir saatinde sık anomali ürettiği görülüyor; bunun bizim verimizde hangi saat ve hangi sensörler olduğu bu soruyla bulunacak. Bu desen Akış 1'in eşiklerini (bölge ve saat bazlı) belirlemek için de kullanılacak.

### Toplu 2: Sensör başına veri kaybı ve anomali oranı, model ve kurulum yılına göre

Olaylar `sensorler.csv` ile join edilir. Her sensör için beklenen olay sayısına göre eksik olay oranı ve anomali oranı hesaplanır, sonra model ve kurulum yılına göre toplanır.

- Neden: Boyut tablosunda 5 model var (TX-200 24 sensörle en az olanı). Hangi modelin daha çok veri kaybettiğini ve daha çok anomali ürettiğini görmek bakım kararına doğrudan bağlanıyor.
- Kartla ilişkisi: Kayıp oranını kartın %1 sınırıyla karşılaştıracağız. Tekrar eden olaylar (toplu örnekte 427 satır, %0,21) temizlenmezse kayıp oranı olduğundan düşük görünür. Bu yüzden önce tekrarları ayıklayacağız.

## Ne kurduk

(Proje sonunda doldurulacak.)

## Üç bulgu

(Toplu ve akış sonuçlarından sonra doldurulacak; her bulgu bir sorgu ve bir ölçümle.)
