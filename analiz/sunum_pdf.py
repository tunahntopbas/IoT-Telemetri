# -*- coding: utf-8 -*-
"""Sunum (10 dk + 2 dk soru) slaytlarını HTML olarak yazar, Chrome ile 16:9 PDF'e çevirir.

Çalıştırma: python3 analiz/sunum_pdf.py   ->   sunum/BSM461_Proje_1A_G221210024_sunum.pdf
Slaytlardaki her sayı kanit/olcumler.csv içinde. Kontrol: python3 analiz/sayi_kontrol.py --sunum
Her slaytın başlığı o slaytın tek cümlelik iddiası; altındaki tablo/şekil kanıtı, sol altta kanıt dosyası.
"Sorular"dan sonraki Y slaytları sunulmuyor, yalnızca soru gelirse açılıyor.
"""
import base64, os, shutil, subprocess

KOK = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CIKTI = os.path.join(KOK, "sunum")
AD = "BSM461_Proje_1A_G221210024_sunum"


def b64(yol):
    with open(os.path.join(KOK, yol), "rb") as f:
        return "data:image/png;base64," + base64.b64encode(f.read()).decode()


def resim(yol, yukseklik):
    return f'<img src="{b64(yol)}" style="max-height:{yukseklik}; max-width:100%">'


def sag_yari(yol, gen=17.5, w=1950, h=720, ust=50):
    """Grafiğin sağ yarısını (üstteki ortak başlık olmadan) gösterir."""
    tam_y = 2 * gen * h / w
    kalan = tam_y * (h - ust) / h
    return (f'<div style="width:{gen}cm;height:{kalan:.2f}cm;overflow:hidden">'
            f'<img src="{b64(yol)}" style="width:{2 * gen}cm;max-width:none;margin:-{tam_y - kalan:.2f}cm 0 0 -{gen}cm"></div>')


def tablo(bas, satirlar, vurgu=(), sinif=""):
    h = f"<table class='{sinif}'><tr>" + "".join(f"<th>{x}</th>" for x in bas) + "</tr>"
    for i, r in enumerate(satirlar):
        h += f"<tr{' class=vurgu' if i in vurgu else ''}>" + "".join(f"<td>{x}</td>" for x in r) + "</tr>"
    return h + "</table>"


def kutu(baslik, metin):
    return f"<div class='kutu'><div class='kb'>{baslik}</div><p>{metin}</p></div>"


T, Z, B = "Veli Tunahan", "Zeynep", "Behiç"

# (başlık = iddia, dakika · konuşan, içerik, kanıt dosyası)
SLAYTLAR = [
    ("Omurga: iki hat, tek servis deposu", f"0–1 dk · {T}",
     resim("rapor/sekiller/mimari.png", "14.8cm"),
     "BSM461 · Grup G221210024 · Şube 1A · Şablon 3 IoT · rapor bölüm 3"),

    ("Kartın dört sınırı ve ölçtüğümüz değerler", f"1–2 dk · {T}",
     "<p>Senaryo kartı: 200 sensör, saniyede 2.000 olay (yoğun saatte 2 katı), olayların %5'i 2 dk'ya kadar geç geliyor.</p>" +
     tablo(["Kartın istediği", "Sınır", "Bizde ölçülen", "Nasıl ölçüldü"],
           [["Uyarı süresi", "≤ 30 sn", "<b>en fazla 16,1 sn</b> (8.000 olay/s)",
             "yığının veritabanına yazılma anı − olayın Kafka'ya varışı"],
            ["Sayıma girmeyen olay", "≤ %1", "<b>0</b> (kartın geç veri oranında)",
             "akış tablosu Kafka'daki olayların tamamıyla karşılaştırıldı"],
            ["Sorgu gecikmesi p99", "≤ 50 ms", "<b>en fazla 11,3 ms</b> (6 sorgu)", "pgbench, her sorgu 1000 kez"],
            ["Tepe yük", "4.000 olay/s", "<b>8.000 olay/s'de 1.233.403 olayın hepsi sayıldı</b>", "üretici --hiz 8000"]]) +
     "<p class='alt'>3 yıllık saklama: 26,98 TB ham JSON, zstd'li Parquet ile 2,23 TB. Tek makinenin 245 GB diskine sığmıyor; ham veri nesne deposunda.</p>",
     "kanit/olcumler.csv · rapor bölüm 4, 8, 9"),

    ("Karar 1a: 2 dk watermark, uyarıyı geciktirmeden geç olayları da sayıyor", f"2–3,5 dk · {T}",
     "<div class='iki'><div>" +
     kutu("Çelişki", "Uyarı 30 sn içinde çıkmalı, ama olayların %5'i 2 dk geç geliyor. Pencereyi 2 dk bekletirsek uyarı geç kalır.") +
     kutu("Çözüm: update modu", "Spark pencere kapanmadan her 10 sn'de o ana kadarki sayıyı yazıyor. Geç olay gelince aynı satır güncelleniyor. "
          "Watermark yalnızca <b>ne kadar geç olayın kabul edileceğini</b> belirliyor.") +
     kutu("Bedeli", "Pencere 2 dk boyunca kesinleşmiyor; Spark o süre boyunca pencereyi bellekte tutuyor.") +
     "</div><div>" +
     tablo(["Geç olay oranı", "2 dk watermark: sayılmayan", "30 sn watermark: sayılmayan"],
           [["%5 (kart)", "<b>0</b>", "606 olay (%4,13)"],
            ["%30", "<b>0</b>", "3.374 olay (%23,06)"],
            ["%50", "<b>0</b>", "5.618 olay (%38,68)"],
            ["%5, ama 5 dk'ya kadar geç", "437 olay (%3,0)", "684 olay (%4,7)"]], vurgu=[0]) +
     "<p class='alt'>Her satır ayrı koşu. Son satır: geç gelme 2 dk'yı aşarsa 2 dk watermark da yetmiyor.</p>"
     "</div></div>",
     "analiz/gec_veri_deneyi.sh · kanit/akis-gec-*-dogrulama.txt"),

    ("Karar 1b: checkpoint S3'teyken uyarı 30 sn'yi aştı, yerel diske aldık", f"3,5–4,5 dk · {Z}",
     tablo(["Checkpoint", "Gelen hız", "Yığın süresi (ort. / en uzun)", "En kötü uyarı gecikmesi"],
           [["S3 (SeaweedFS)", "50 olay/s", "12,3 / 16,1 sn", "29,9 sn"],
            ["S3, tetik 5 sn", "50 olay/s", "12,6 / 14,8 sn", "29,3 sn"],
            ["S3", "2.000 olay/s", "15,7 / 21,0 sn", "<b>37,5 sn</b> (sınır aşıldı)"],
            ["Yerel disk", "50 olay/s", "2,0 / 4,4 sn", "<b>13,4 sn</b>"],
            ["Yerel disk", "8.000 olay/s", "4,2 / 5,7 sn", "<b>16,1 sn</b>"]], vurgu=[3, 4]) +
     "<div class='iki'>" +
     kutu("Nedeni nasıl bulduk", "Tetiği 5 sn'ye indirmek işe yaramadı. Spark'ın ilerleme raporunda durumun checkpoint'e yazılması "
          "(<code>commitTimeMs</code>) her yığında 9–24 sn sürüyordu. Yerel diskte 0,4–0,5 sn.") +
     kutu("Bedeli", "Checkpoint artık tek makinenin diskinde. Disk giderse iş Kafka'yı baştan okumak zorunda. Üretimde kalıcı, kopyalı disk ya da HDFS gerekir.") +
     "</div>",
     "analiz/tepe_yuk_deneyi.sh · kanit/akis-checkpoint-yerel.txt"),

    ("Karar 2: yazma yarıda kesilince çıplak Parquet okunamıyor, Delta eski sürümü veriyor", f"4,5–5 dk · {Z}",
     "<p>2.992.895 satırlık tablonun üstüne yeni sürüm yazılırken Spark süreci <code>kill -9</code> ile öldürüldü, sonra tablo okundu:</p>" +
     tablo(["Biçim", "Kesmeden sonra okuma", "Diskte ne kaldı"],
           [["Çıplak Parquet", "<b>okunamıyor</b>: UNABLE_TO_INFER_SCHEMA", "eski 64 dosya silinmiş, 20 yarım dosya"],
            ["Delta Lake", "<b>eski sürüm eksiksiz</b>: 2.992.895 satır", "25 sahipsiz dosya; işlem günlüğü onları görmüyor"]],
           vurgu=[1]) +
     "<div class='iki'>" +
     kutu("Neden", "S3'te atomik dosya adı değiştirme yok. Parquet eskiyi silip yeniyi yazıyor; arada kesilirse ikisi de yok. "
          "Delta'da yeni sürüm ancak işlem günlüğüne tek bir dosya yazılınca görünür oluyor.") +
     kutu("Bedeli", "Aynı veriyi Delta 41,4 sn'de, çıplak Parquet 16,6 sn'de yazdı (tek koşu). Toplu hat günde bir kez çalıştığı için kabul ettik.") +
     "</div>",
     "analiz/atomik_deney.py · kanit/atomik-yazma-deneyi.txt"),

    ("Canlı gösterim: Kafka'ya düşen olay en geç ~26 sn sonra panoda", f"5–7 dk · {Z}",
     "<div class='iki' style='grid-template-columns: 1fr 2.3fr'><div>"
     "<p><b>Göstereceklerimiz</b></p><ol>"
     "<li>Dakika başına olay grafiği her 10 sn'de ilerliyor.</li>"
     "<li>Akış 1: son 10 dk'da titreşimi 1,0'ı aşan sensörler.</li>"
     "<li>Akış 2: 60 sn'dir susan sensör yok, tablo boş.</li>"
     "<li>Toplu 1: 7 bölge × 24 saat sıcaklık. Toplu 2: modele göre anomali.</li></ol>"
     "<p class='alt'>Olay en geç 16,1 sn'de veritabanında, pano 10 sn'de bir yenileniyor. "
     "Canlı gösterim aksarsa sağdaki ekran görüntüsü yedek.</p></div><div>" +
     resim("kanit/grafana-pano.png", "13cm") + "</div></div>",
     "http://localhost:3000 · kanit/grafana-pano.png"),

    ("Kırılma deneyi: Kafka 90 sn kapalı kaldı, akış işi öldü ve kendiliğinden dönmedi", f"7–8 dk · {B}",
     "<div class='iki' style='grid-template-columns: 1.1fr 1fr'><div>" +
     resim("kanit/kirilma-deneyi-kafka-grafana.png", "13.6cm") + "</div><div>" +
     kutu("Ne yaptık", "<code>docker compose stop kafka</code>, 90 sn sonra <code>start</code>. Üretici ve akış işi çalışıyordu.") +
     kutu("Ne gördük", "Üretici olayları bellekte tuttu, Kafka gelince gönderdi: 29.304 olayın hepsi Kafka'da. "
          "Akış işi 60 sn sonra <code>TimeoutException</code> ile öldü; olayların %83,7'si sayılmadan kaldı.") +
     kutu("Neye bakınca", "Panoda 200 sensörün hepsi aynı anda \"sustu\", zaman serisi kesildi. Tek sensör değil, hattın arızası.") +
     kutu("Gerçek sistemde", "Kafka 3 kopya (min.insync.replicas=2), akış işi gözetmen altında, iş ölünce uyarı.") +
     "</div></div>",
     "analiz/kirilma_kafka_kurtarma.sh · kanit/kirilma-deneyi-kafka*.txt"),

    ("Aynı checkpoint ile yeniden başlatınca kayıp 0; checkpoint kaybolursa %21,9 kayıp", f"8–9 dk · {B}",
     tablo(["Bozduğumuz şey", "Sonuç (tablo, Kafka'daki olayların tamamıyla karşılaştırıldı)"],
           [["Kafka kesintisinden sonra aynı checkpoint ile başlatma", "kaçırılan 24.577 olay 2 yığında okundu · kayıp 0"],
            ["TimescaleDB 60 sn kapalı", "iş öldü, pano hata verdi · yeniden başlatınca kayıp 0, tekrar 0"],
            ["Veritabanına yazdıktan sonra, checkpoint'e işlemeden çökme", "aynı yığın 2 kez yazıldı · upsert sayesinde tablo birebir doğru"],
            ["Checkpoint silindi, iş <code>earliest</code> ile başladı", "Kafka'yı baştan okudu · kayıp 0"],
            ["Checkpoint silindi, iş <code>latest</code> ile başladı", "<b>2.289 mesaj atlandı · kayıp %21,9</b>"],
            ["Toplu iş ile akış işi aynı anda", "bellek yetti (OOM 0) · akış yığını ort. 3,0 sn'den 13,8 sn'ye çıktı"]],
           vurgu=[4]) +
     "<p><b>Teslim garantisi:</b> Kafka'dan en az bir kez okuma + (sensör, pencere) anahtarlı upsert = tabloda tam bir kez sonuç.</p>",
     "kanit/kirilma-deneyi-*.txt · kanit/akis-cokme-deneyi.txt"),

    ("Bulgu: veri kaybı sensör modelinden değil, sensör numarasından geliyor", f"9–10 dk · {B}",
     "<div class='iki' style='grid-template-columns: 1.3fr 1fr'><div>" + sag_yari("analiz/cikti/toplu2.png") + "</div><div>" +
     kutu("Ne bulduk", "Numarası 13'ün katı olan 15 sensör olaylarının %28,7–%30,6'sını, 23'ün katı olan 8 sensör %7,3–%10,1'ini kaybediyor. "
          "Grafikte renkler (modeller) karışık: kayıp modele bağlı değil.") +
     kutu("Nasıl ölçtük", "Toplu 2: her sensörün 14 günde gelen olay sayısı sensörlerin ortancasıyla karşılaştırıldı; tekrarlar önce ayıklandı.") +
     kutu("Bakım ekibi için", "Sorun cihazda değil, bu sensörlerin bağlantısında olabilir: önce onların ağına bakılmalı.") +
     "</div></div>",
     "is/toplu_is.py · rapor bölüm 7"),

    ("Sorular", "10–12 dk · hepimiz",
     "<div class='sorular'><p>github.com/tunahntopbas/IoT-Telemetri</p>"
     "<p class='alt'>Her ölçüm: kanit/olcumler.csv · deney betikleri: analiz/ · rapor: rapor/BSM461_Proje_1A_G221210024.pdf</p></div>",
     ""),

    # ---- Yedek slaytlar: yalnızca soru gelirse ----
    ("Yedek: her ok için kopya sayısı ve bağlantı koparsa ne olduğu", "soru gelirse",
     tablo(["Ok", "Kopya (bizde → üretimde)", "Bağlantı koparsa (ölçülen)"],
           [["Üretici → Kafka", "1 → 3, acks=all", "üretici bekledi, 29.304 olayın hepsi Kafka'ya ulaştı"],
            ["Kafka → Spark akış", "1 → 3", "60 sn sonra iş öldü; aynı checkpoint ile kayıp 0"],
            ["Spark → checkpoint", "yerel disk → kopyalı disk / HDFS", "kaybolursa earliest: kayıp 0, latest: %21,9"],
            ["Spark → TimescaleDB", "1 → 2 (eşzamanlı yedek)", "iş öldü; yeniden başlatınca kayıp 0, tekrar 0"],
            ["Spark toplu → Delta (S3)", "1 → 3", "kill -9: Delta eski sürümü veriyor, Parquet okunamıyor"],
            ["TimescaleDB → Grafana", "1 → 1", "pano boş değil, hata gösterdi"]], sinif="kucuk"),
     "rapor bölüm 3.2"),

    ("Yedek: neden 8 Kafka bölümü", "soru gelirse",
     "<ul><li>Bölüm sayısını hacim değil paralellik belirliyor: Kafka bu makinede 5,4 sn'de 97.762 olay aldı, en az 18.000 olay/s. Kartın tepesi 4.000.</li>"
     "<li>Spark Kafka'yı bölüm başına bir görevle okuyor; makinede 8 çekirdek var.</li>"
     "<li>Anahtar <code>sensor_id</code>: bir sensörün olayları hep aynı bölümde, kendi aralarında sıralı.</li>"
     "<li>200 anahtar 8 bölüme dağılınca en dolu bölüm / ortalama = 1,063.</li>"
     "<li>10x'te tepe 40.000 olay/s. Tek makinede ölçtüğümüz en yüksek akış işleme hızı 15.752 olay/s: küme ve daha çok bölüm gerekir.</li></ul>",
     "kanit/akis-uretici-ayarlari.txt · rapor bölüm 6.3, 8.1"),

    ("Yedek: servis sorgularının gecikmesi (pgbench, her sorgu 1000 kez)", "soru gelirse",
     tablo(["Sorgu", "p50", "p99", "en kötü"],
           [["q1 Sensörün son 10 dakikası", "0,25 ms", "1,30 ms", "20,9 ms"],
            ["q2 Sensörün geçmişteki bir 10 dakikası", "2,03 ms", "8,29 ms", "18,7 ms"],
            ["q3 Son 10 dk anomali (Akış 1)", "0,41 ms", "0,75 ms", "8,9 ms"],
            ["q4 Susan sensörler (Akış 2)", "1,55 ms", "11,34 ms", "37,5 ms"],
            ["q5 Bölgenin saatlik özeti (Toplu 1)", "0,17 ms", "2,61 ms", "27,0 ms"],
            ["q6 Dakika başına olay, son 1 saat", "0,61 ms", "9,45 ms", "27,6 ms"]], vurgu=[3]) +
     "<p class='alt'>Ölçüm sırasında tabloda 14 günlük geçmiş vardı (2,1 milyon satır) ve akış işi aynı tabloya yazıyordu.</p>",
     "analiz/p99_olc.sh · kanit/servis-p99.txt"),
]

CSS = """
@page { size: 33.867cm 19.05cm; margin: 0; }
* { box-sizing: border-box; }
body { margin: 0; font-family: Arial, Helvetica, sans-serif; color: #1c1c1c; }
.slayt { width: 33.867cm; height: 19.05cm; padding: 1.0cm 1.4cm 1.1cm; position: relative;
         page-break-after: always; overflow: hidden; }
.slayt.yedek { background: #f6f6f3; }
h1 { font-size: 25pt; line-height: 1.2; margin: 0 5.5cm 0.45cm 0; color: #10233f; }
.kim { position: absolute; top: 1.15cm; right: 1.4cm; font-size: 12pt; color: #666; text-align: right; }
p, li { font-size: 18pt; line-height: 1.32; margin: 0.12cm 0; } ul, ol { margin: 0.15cm 0; padding-left: 0.8cm; }
li { margin-bottom: 0.2cm; }
p.alt { font-size: 14pt; color: #555; }
table { border-collapse: collapse; margin: 0.25cm 0; width: 100%; font-size: 17pt; }
table.kucuk { font-size: 16pt; }
th, td { border-bottom: 1px solid #c9c9c9; padding: 0.17cm 0.3cm; text-align: left; vertical-align: top; }
th { border-bottom: 2px solid #10233f; color: #10233f; }
tr.vurgu td { background: #fdf0d2; }
code { font-family: Menlo, Consolas, monospace; font-size: 0.85em; background: #eee; padding: 0 3px; }
img { display: block; margin: 0 auto; }
.iki { display: grid; grid-template-columns: 1fr 1fr; gap: 0.7cm; align-items: start; }
.kutu { border-left: 4px solid #c8871a; padding: 0.05cm 0 0.05cm 0.35cm; margin: 0.25cm 0; }
.kutu .kb { font-size: 12.5pt; font-weight: bold; color: #8a5a0a; text-transform: uppercase; letter-spacing: 0.04em; }
.kutu p { font-size: 16.5pt; margin: 0.05cm 0 0; }
.sorular { margin-top: 4.5cm; text-align: center; } .sorular p { font-size: 22pt; }
.alt-bilgi { position: absolute; bottom: 0.45cm; left: 1.4cm; right: 1.4cm; font-size: 10.5pt; color: #888;
             display: flex; justify-content: space-between; border-top: 1px solid #ddd; padding-top: 0.15cm; }
"""


def main():
    os.makedirs(CIKTI, exist_ok=True)
    ana = next(i for i, s in enumerate(SLAYTLAR) if s[0] == "Sorular") + 1
    govde = ""
    for n, (baslik, kim, icerik, kanit) in enumerate(SLAYTLAR, 1):
        yedek = n > ana
        no = f"Y{n - ana}" if yedek else f"{n}/{ana}"
        govde += (f'<div class="slayt{" yedek" if yedek else ""}"><h1>{baslik}</h1><div class="kim">{kim}</div>{icerik}'
                  f'<div class="alt-bilgi"><span>{kanit}</span><span>{no}</span></div></div>')
    h = os.path.join(CIKTI, AD + ".html")
    open(h, "w", encoding="utf-8").write(f'<!doctype html><html lang="tr"><head><meta charset="utf-8"><title>{AD}</title>'
                                         f'<style>{CSS}</style></head><body>{govde}</body></html>')
    chrome = shutil.which("google-chrome") or "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
    subprocess.run([chrome, "--headless", "--disable-gpu", "--no-pdf-header-footer",
                    f"--print-to-pdf={os.path.join(CIKTI, AD + '.pdf')}", "file://" + h], check=True, capture_output=True)
    print("yazıldı: sunum/" + AD + ".pdf")


if __name__ == "__main__":
    main()
