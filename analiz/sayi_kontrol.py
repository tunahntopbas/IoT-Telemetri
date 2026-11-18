# -*- coding: utf-8 -*-
"""Raporda geçen her sayının kanit/olcumler.csv'de olup olmadığını kontrol eder.

Çalıştırma: python3 analiz/sayi_kontrol.py [--ayrinti] [--sunum]
  --sunum: rapor yerine sunum slaytlarının metnini (sunum/*_sunum.html) kontrol eder.
Kurallar:
  - Rapor metni rapor/02..11 bölümleri (kapak, beyanlar ve ekler hariç). Kod blokları, satır içi kod, dosya adları,
    sürüm numaraları, bölüm/şekil/tablo numaraları ve kanıt dosyası adları sayılmaz.
  - Türkçe yazım çözülür: 1.233.403 → 1233403, 13,4 → 13.4, %4,13 → 4.13.
  - Bir sayı CSV'deki herhangi bir sayıya eşitse (yazıldığı basamağa yuvarlanmış hâli dahil) "bulundu" sayılır.
  - Ölçüm olmayan sayılar (kartın koşulları, ayarlar, tanımlar) TANIM listesinde; her biri gerekçesiyle.
"""
import csv, html, re, sys, glob, os

KOK = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Ölçüm değil, tanım ya da ayar olan sayılar (kart, kod, kurulum). Bunlar olcumler.csv'de aranmaz.
TANIM = {
    "2000", "2", "30", "5", "1", "1095", "50", "99", "200",          # senaryo kartı: hız, tepe katsayısı, uyarı, geç oran, kayıp, saklama, p99, sensör
    "8", "3", "7", "10", "60", "120", "90", "1.0", "0.5", "1.5",     # ayarlar: bölüm, kopya, saklama günü, tetik, eşikler, deney süreleri
    "14", "3000000", "4000", "8000", "6", "24", "15", "21", "16",    # toplu veri boyutu ve günü, deney hızları, saat değerleri
    "13", "23", "100", "0", "4", "12", "20", "40", "32", "64", "4.786", "4.8",
    "2020", "1000", "180",   # 2020: yıl (kaynakçadaki S3 tutarlılık duyurusu)
     "300", "240", "360", "400", "2.0", "128", "512", "11", "17", "26", "28", "25", "0.25",
}


def sayilar(metin):
    metin = re.sub(r"```.*?```", " ", metin, flags=re.S)
    metin = re.sub(r"`[^`]*`", " ", metin)
    metin = re.sub(r"!\[[^\]]*\]\([^)]*\)|\]\([^)]*\)", " ", metin)
    metin = re.sub(r"\b(?:bölüm|Bölüm|Şekil|Tablo|Ek|Z|Adım|deney|Deney)\s*\d+(?:\.\d+)*", " ", metin)
    metin = re.sub(r"\b\d+\.\d+\.\d+\b", " ", metin)                       # sürüm numaraları
    metin = re.sub(r"\b\d{1,2}\.\d{2}\.\d{4}\b|\b\d{4}-\d{2}-\d{2}\b", " ", metin)   # tarihler
    metin = re.sub(r"\b\d{1,2}[:.]\d{2}(?::\d{2})?(?=\s*['’]?(?:de|da|te|ta|'|\)|,|\s))", " ", metin)  # saat
    metin = re.sub(r"\b[A-Za-zÇĞİÖŞÜçğıöşü_-]*\d+[A-Za-zÇĞİÖŞÜçğıöşü_-]+\d*\b", " ", metin)   # sensor-013, q1, TX-200
    for m in re.finditer(r"(?<![\w.,])\d{1,3}(?:\.\d{3})+(?:,\d+)?(?![\w])|(?<![\w.,])\d+(?:,\d+)?(?![\w])", metin):
        ham = m.group(0)
        yield ham, ham.replace(".", "").replace(",", "."), metin[max(0, m.start() - 50):m.end() + 30].replace("\n", " ")


def csv_sayilari():
    kume = set()
    for r in csv.DictReader(open(os.path.join(KOK, "kanit", "olcumler.csv"), encoding="utf-8")):
        for alan in (r["deger"], r["not"], r["yontem"]):
            for x in re.findall(r"(?<![\d.])-?\d+(?:\.\d+)?|(?<=\d-)\d+(?:\.\d+)?", (alan or "").replace(",", "")):
                kume.add(float(x))
    return kume


def bulundu(deger, ondalik, kume):
    if any(abs(x - deger) < 1e-9 for x in kume):
        return True
    for x in kume:                                   # yuvarlanmış yazım: 13,4 ↔ 13.42
        if round(x, ondalik) == round(deger, ondalik):
            return True
        for k in (100, 1e-3, 1e-6, 1e-9, 1e-12, 1 / 1024 ** 3, 1 / 1024 ** 2, 1e3):   # yüzde, KB/MB/GB/TB, GiB/MiB
            if round(x * k, ondalik) == round(deger, ondalik):
                return True
    return False


def main():
    kume = csv_sayilari()
    if "--sunum" in sys.argv:
        dosyalar = glob.glob(os.path.join(KOK, "sunum", "*_sunum.html"))
    else:
        dosyalar = sorted(f for f in glob.glob(os.path.join(KOK, "rapor", "*.md"))
                          if re.match(r"(0[2-9]|1[01])-", os.path.basename(f)))
    toplam, eksik = 0, []
    for d in dosyalar:
        metin = open(d, encoding="utf-8").read()
        if d.endswith(".html"):   # yalnızca görünen metin: stil, gömülü resim ve etiketler atılır
            metin = re.sub(r"<style>.*?</style>|<img[^>]*>", " ", metin, flags=re.S)
            metin = re.sub(r"<code>.*?</code>", " ", metin)
            metin = html.unescape(re.sub(r"<[^>]+>", " ", metin))
        for ham, sayi, baglam in sayilar(metin):
            if sayi.rstrip("0").rstrip(".") in TANIM or sayi in TANIM:
                continue
            toplam += 1
            ondalik = len(sayi.split(".")[1]) if "." in sayi else 0
            if not bulundu(float(sayi), ondalik, kume):
                eksik.append((os.path.basename(d), ham, baglam))
    print(f"kontrol edilen sayı: {toplam}, olcumler.csv'de bulunamayan: {len(eksik)}")
    if "--ayrinti" in sys.argv or eksik:
        for d, ham, b in eksik:
            print(f"  {d}: {ham:>12}   …{b}…")
    sys.exit(1 if eksik else 0)


if __name__ == "__main__":
    main()
