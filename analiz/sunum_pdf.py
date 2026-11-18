# -*- coding: utf-8 -*-
"""Sunum (12 dk) slaytlarını HTML olarak yazar, Chrome ile 16:9 PDF'e çevirir.
Çalıştırma: python3 analiz/sunum_pdf.py   →   sunum/BSM461_Proje_1A_G221210024_sunum.pdf
Slaytlardaki her sayı kanit/olcumler.csv ya da kanit/*.txt içinde."""
import base64, os, shutil, subprocess

KOK = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CIKTI = os.path.join(KOK, "sunum")
AD = "BSM461_Proje_1A_G221210024_sunum"


def resim(yol, yukseklik="12.2cm"):
    with open(os.path.join(KOK, yol), "rb") as f:
        v = base64.b64encode(f.read()).decode()
    return f'<img src="data:image/png;base64,{v}" style="max-height:{yukseklik}; max-width:100%">'


def tablo(bas, satirlar, vurgu=None):
    h = "<table><tr>" + "".join(f"<th>{x}</th>" for x in bas) + "</tr>"
    for i, r in enumerate(satirlar):
        sinif = ' class="vurgu"' if vurgu is not None and i in vurgu else ""
        h += f"<tr{sinif}>" + "".join(f"<td>{x}</td>" for x in r) + "</tr>"
    return h + "</table>"


# (başlık, süre/konuşmacı, içerik)
SLAYTLAR = [
    ("Omurga: iki hat, bir servis deposu", "0–2 dk · Veli Tunahan",
     resim("rapor/sekiller/mimari.png", "11.4cm") +
     "<p class='alt'>Grup G221210024 · Şube 1A · Şablon 3 IoT · 200 sensör, kart: 2.000 ölçüm/sn, uyarı ≤ 30 sn, "
     "%5'i 2 dk'ya kadar geç, kayıp ≤ %1, p99 ≤ 50 ms</p>"),

    ("Karar 1: 2 dk watermark + update modu", "2–3,5 dk · Veli Tunahan",
     "<p>Kart çelişiyor: uyarı 30 sn'de çıkmalı ama olaylar 2 dk geç gelebiliyor. "
     "Update modunda watermark <b>yazma zamanını değil</b>, geç olayın kabul süresini belirliyor.</p>" +
     tablo(["Geç olay oranı", "Kafka'daki tekrarsız olay", "2 dk watermark: sayılmayan", "30 sn watermark: sayılmayan"],
           [["kart (%5)", "14.664", "<b>0</b>", "606 (%4,13)"],
            ["%30", "14.629", "<b>0</b>", "3.374 (%23,06)"],
            ["%50", "14.524", "<b>0</b>", "5.618 (%38,68)"],
            ["%5, ama 5 dk'ya kadar geç", "14.593", "437 (%3,0)", "684 (%4,7)"]], vurgu=[0]) +
     "<p class='alt'>Kaynak: analiz/gec_veri_deneyi.sh, analiz/akis_dogrula.py (tablo Kafka'nın tamamıyla karşılaştırıldı)</p>"),

    ("Karar 1 (devam): checkpoint yerel diske", "3,5–4,5 dk · Veli Tunahan",
     "<p>Olayın Kafka'ya varışından veritabanında görünmesine kadar geçen süre (en kötü durum):</p>" +
     tablo(["Checkpoint", "Hız", "Yığın süresi ort.", "En kötü uyarı gecikmesi"],
           [["S3", "50 olay/sn", "12,3 sn", "29,9 sn"],
            ["S3, tetik 5 sn", "50 olay/sn", "12,6 sn", "29,3 sn"],
            ["S3", "2000 olay/sn", "15,7 sn", "<b>37,5 sn</b>"],
            ["Yerel disk", "50 olay/sn", "2,0 sn", "<b>13,4 sn</b>"],
            ["Yerel disk", "8000 olay/sn", "4,2 sn", "<b>16,1 sn</b>"]], vurgu=[3, 4]) +
     "<p>Neden: durum deposunun her yığında S3'e yazılması (<code>commitTimeMs</code> 9–24 sn). "
     "Bedeli: checkpoint tek makinenin diskine bağlı.</p>"),

    ("Karar 2: ham veri Delta'da (atomik yazma)", "4,5–5 dk · Zeynep",
     "<p>2.992.895 satırlık tablonun üstüne yazarken süreç <code>kill -9</code> ile öldürüldü:</p>" +
     tablo(["", "Kesmeden sonra okuma", "Geride kalan"],
           [["Çıplak Parquet", "<b>okunamıyor</b>: UNABLE_TO_INFER_SCHEMA; eski 64 dosya silinmiş", "20 yarım dosya _temporary altında"],
            ["Delta Lake", "<b>eski sürüm eksiksiz</b>: 2.992.895 satır", "25 sahipsiz dosya (VACUUM siler)"]]) +
     "<p>Ayrıca: boyut tablosuyla broadcast join, Parquet + zstd (snappy'den %20 küçük: 35,5 MB'a karşı 44,5 MB).</p>"),

    ("Canlı gösterim: Grafana panosu", "5–7 dk · Zeynep",
     resim("kanit/grafana-pano.png", "11.6cm") +
     "<p class='alt'>http://localhost:3000 · 10 sn'de bir yenileme · servis sorgularında p99 en fazla 11,3 ms (sınır 50 ms). "
     "Canlı gösterim aksarsa bu ekran görüntüsü yedek.</p>"),

    ("Kırılma deneyi: Kafka 90 sn kapalı", "7–8 dk · Behiç",
     resim("kanit/kirilma-deneyi-kafka-grafana.png", "8.3cm") +
     "<ul><li>Üretici olayları bellekte tuttu, Kafka gelince gönderdi: 29.304 olayın hepsi Kafka'da.</li>"
     "<li>Akış işi 60 sn sonra <code>TimeoutException</code> ile öldü, <b>kendiliğinden dönmedi</b>: "
     "o anda olayların %83,7'si sayılmamıştı, panoda 200 sensörün hepsi susmuş.</li></ul>"),

    ("Kurtarma: aynı checkpoint ile yeniden başlatma", "8–9 dk · Behiç",
     tablo(["Durum", "Sonuç (Kafka'nın tamamıyla karşılaştırma)"],
           [["Kafka kesintisi sonrası yeniden başlatma", "24.577 olay 2 yığında okundu · kayıp 0 · 2.262 pencere doğru"],
            ["TimescaleDB 60 sn kapalı", "iş öldü · yeniden başlatınca kayıp 0, tekrar 0 · pano hata gösterdi"],
            ["DB'ye yazdıktan sonra, ofset kaydedilmeden çökme", "aynı yığın 2 kez yazıldı · upsert sayesinde tablo birebir doğru"],
            ["Checkpoint kayboldu, <code>earliest</code>", "baştan okudu · kayıp 0"],
            ["Checkpoint kayboldu, <code>latest</code>", "<b>2.289 mesaj atlandı · kayıp %21,9</b>"]], vurgu=[4]) +
     "<p>Teslim: Kafka'dan en az bir kez + (sensör, pencere) anahtarlı upsert = sonuç tam bir kez.</p>"),

    ("Bulgu: kayıp sensör numarasında, anomali modelde", "9–10 dk · Behiç",
     resim("analiz/cikti/toplu2.png", "9cm") +
     "<ul><li>13'ün katı 15 sensör olaylarının %28,7–%30,6'sını, 23'ün katı 8 sensör %7,3–%10,1'ini kaybediyor; "
     "modelle ilgisi yok.</li><li>TX-200'ün anomali oranı %1,17: diğer dört modelin (%0,295–%0,302) yaklaşık 4 katı.</li></ul>"),

    ("Sorular", "10–12 dk · hepimiz",
     "<div class='sorular'><p>Depo: github.com/tunahntopbas/IoT-Telemetri</p>"
     "<p>Her ölçüm: kanit/olcumler.csv · Deney betikleri: analiz/ · Rapor: rapor/</p></div>"),
]

CSS = """
@page { size: 33.867cm 19.05cm; margin: 0; }
body { margin: 0; font-family: Arial, Helvetica, sans-serif; color: #1d2430; }
.slayt { width: 33.867cm; height: 19.05cm; box-sizing: border-box; padding: 1.1cm 1.5cm 0.8cm; position: relative;
         page-break-after: always; overflow: hidden; }
h1 { font-size: 26pt; margin: 0 0 0.35cm; color: #12355b; }
.kim { position: absolute; top: 1.25cm; right: 1.5cm; font-size: 12pt; color: #777; }
p, li { font-size: 17pt; line-height: 1.35; margin: 0.15cm 0; } ul { margin: 0.2cm 0; }
p.alt { font-size: 12.5pt; color: #555; }
table { border-collapse: collapse; margin: 0.3cm 0; width: 100%; font-size: 15.5pt; }
th, td { border: 1px solid #b9c2cf; padding: 0.15cm 0.3cm; text-align: left; } th { background: #e8eef6; }
tr.vurgu td { background: #fff4d6; }
code { font-size: 14pt; background: #f1f1f1; padding: 0 3px; }
img { display: block; margin: 0 auto; }
.sorular { margin-top: 4cm; text-align: center; } .sorular p { font-size: 20pt; }
.no { position: absolute; bottom: 0.5cm; right: 1.5cm; font-size: 11pt; color: #999; }
"""


def main():
    os.makedirs(CIKTI, exist_ok=True)
    govde = "".join(f'<div class="slayt"><h1>{b}</h1><div class="kim">{k}</div>{i}<div class="no">{n}/{len(SLAYTLAR)}</div></div>'
                    for n, (b, k, i) in enumerate(SLAYTLAR, 1))
    h = os.path.join(CIKTI, AD + ".html")
    open(h, "w", encoding="utf-8").write(f'<!doctype html><html lang="tr"><head><meta charset="utf-8"><style>{CSS}</style></head><body>{govde}</body></html>')
    chrome = shutil.which("google-chrome") or "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
    subprocess.run([chrome, "--headless", "--disable-gpu", "--no-pdf-header-footer",
                    f"--print-to-pdf={os.path.join(CIKTI, AD + '.pdf')}", "file://" + h], check=True, capture_output=True)
    print("yazıldı: sunum/" + AD + ".pdf")


if __name__ == "__main__":
    main()
