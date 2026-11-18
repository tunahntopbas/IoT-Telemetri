# -*- coding: utf-8 -*-
"""Rapor bölümlerini (rapor/*.md) yönergedeki sırayla tek HTML belgede birleştirir, Chrome ile PDF'e çevirir.

Çalıştırma (bilgisayarda, Google Chrome kurulu olmalı):
  python3 analiz/rapor_pdf.py
Çıktılar:
  rapor/BSM461_Proje_1A_G221210024.html
  rapor/BSM461_Proje_1A_G221210024.pdf

Dış kütüphane gerektirmesin diye raporlarda kullandığımız kadar Markdown'ı (başlık, paragraf, liste, tablo, kod,
resim, kalın, eğik, satır içi kod, bağlantı) kendimiz HTML'e çeviriyoruz.
"""
import base64, csv, html, os, re, shutil, subprocess, sys

KOK = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAPOR = os.path.join(KOK, "rapor")
AD = "BSM461_Proje_1A_G221210024"
SIRA = ["01-kapak.md", "02-ozet.md", "03-mimari.md", "04-olcek.md", "05-depolama.md", "06-bolumleme.md",
        "07-toplu-hat.md", "08-akis-hatti.md", "09-servis-pano.md", "10-veri-kalitesi.md", "11-kirilma.md",
        "12-yz-beyani.md", "13-katki-beyani.md", "13a-kaynakca.md", "14-ekler.md"]


def satir_ici(t):
    t = html.escape(t, quote=False)
    kodlar = []
    t = re.sub(r"`([^`]+)`", lambda m: kodlar.append(m.group(1)) or f"\0{len(kodlar) - 1}\0", t)
    t = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", t)
    t = re.sub(r"(?<![\w*])\*(?!\s)(.+?)(?<!\s)\*(?![\w*])", r"<em>\1</em>", t)
    t = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r'<a href="\2">\1</a>', t)
    t = re.sub(r"(?<![\"=>])(https?://[^\s<)]+)", r'<a href="\1">\1</a>', t)
    return re.sub(r"\0(\d+)\0", lambda m: f"<code>{kodlar[int(m.group(1))]}</code>", t)


def resim(yol, dosya_klasoru, alt):
    tam = os.path.normpath(os.path.join(dosya_klasoru, yol))
    tur = "png" if tam.endswith(".png") else "jpeg"
    with open(tam, "rb") as f:
        veri = base64.b64encode(f.read()).decode()
    return f'<figure><img src="data:image/{tur};base64,{veri}" alt="{html.escape(alt)}"></figure>'


def tablo(satirlar):
    hucre = lambda s: [h.strip() for h in s.strip().strip("|").split("|")]
    bas, govde = hucre(satirlar[0]), [hucre(s) for s in satirlar[2:]]
    h = "<table><thead><tr>" + "".join(f"<th>{satir_ici(x)}</th>" for x in bas) + "</tr></thead><tbody>"
    for r in govde:
        h += "<tr>" + "".join(f"<td>{satir_ici(x)}</td>" for x in r) + "</tr>"
    return h + "</tbody></table>"


def md_html(metin, klasor, numarala=True):
    """numarala: başlığı olmayan tablo ve şekillere bölüm numarasıyla "Tablo N.k" / "Şekil N.k" ekler."""
    out, satirlar, i = [], metin.split("\n"), 0
    bolum, son_baslik, sayac = "", "", {"Tablo": 0, "Şekil": 0}

    def etiket(tur):
        sayac[tur] += 1
        onceki = out[-1] if out else ""
        if not numarala or onceki.startswith(f"<p><strong>{tur} "):
            return None
        return f'<p class="etiket"><strong>{tur} {bolum}.{sayac[tur]}.</strong> {satir_ici(son_baslik)}</p>'
    while i < len(satirlar):
        s = satirlar[i]
        if s.startswith("```"):
            j = i + 1
            while j < len(satirlar) and not satirlar[j].startswith("```"):
                j += 1
            out.append("<pre><code>" + html.escape("\n".join(satirlar[i + 1:j])) + "</code></pre>")
            i = j + 1
        elif m := re.match(r"^(#{1,6})\s+(.*)", s):
            n = len(m.group(1))
            if n == 1 and (b := re.match(r"^(\d+)\.", m.group(2))):
                bolum = b.group(1)
            son_baslik = re.sub(r"^[\d.]+\s*", "", m.group(2))
            out.append(f"<h{n}>{satir_ici(m.group(2))}</h{n}>")
            i += 1
        elif m := re.match(r"^!\[([^\]]*)\]\(([^)]+)\)\s*$", s):
            out.append(resim(m.group(2), klasor, m.group(1)))
            # şeklin başlığı: hemen sonraki satırlarda "**Şekil" yoksa ekle
            sonraki = next((x for x in satirlar[i + 1:] if x.strip()), "")
            if not sonraki.startswith("**Şekil"):
                sayac["Şekil"] += 1
                if numarala:
                    out.append(f'<p class="etiket"><strong>Şekil {bolum}.{sayac["Şekil"]}.</strong> {html.escape(m.group(1))}</p>')
            else:
                sayac["Şekil"] += 1
            i += 1
        elif s.startswith("|") and i + 1 < len(satirlar) and re.match(r"^\|[\s:|-]+\|?\s*$", satirlar[i + 1]):
            j = i
            while j < len(satirlar) and satirlar[j].startswith("|"):
                j += 1
            e = etiket("Tablo")
            if e:
                out.append(e)
            out.append(tablo(satirlar[i:j]))
            i = j
        elif re.match(r"^\s*([-*]|\d+\.)\s+", s):
            sirali = bool(re.match(r"^\s*\d+\.", s))
            ogeler = []
            while i < len(satirlar) and (re.match(r"^\s*([-*]|\d+\.)\s+", satirlar[i])
                                         or (satirlar[i].startswith("  ") and satirlar[i].strip() and ogeler)):
                if re.match(r"^\s*([-*]|\d+\.)\s+", satirlar[i]):
                    ogeler.append(re.sub(r"^\s*([-*]|\d+\.)\s+", "", satirlar[i]))
                else:
                    ogeler[-1] += " " + satirlar[i].strip()
                i += 1
            e = "ol" if sirali else "ul"
            out.append(f"<{e}>" + "".join(f"<li>{satir_ici(x)}</li>" for x in ogeler) + f"</{e}>")
        elif s.startswith(">"):
            j = i
            while j < len(satirlar) and satirlar[j].startswith(">"):
                j += 1
            out.append("<blockquote>" + satir_ici(" ".join(x.lstrip("> ") for x in satirlar[i:j])) + "</blockquote>")
            i = j
        elif s.strip() in ("---", "<div class=\"sayfa\"></div>"):
            out.append('<div class="sayfa"></div>')
            i += 1
        elif not s.strip():
            i += 1
        else:
            j = i
            while j < len(satirlar) and satirlar[j].strip() and not re.match(r"^(#|```|\||!\[|\s*[-*]\s|\s*\d+\.\s|>)", satirlar[j]):
                j += 1
            j = max(j, i + 1)
            out.append("<p>" + satir_ici(" ".join(x.strip() for x in satirlar[i:j])) + "</p>")
            i = j
    return "\n".join(out)


CSS = """
@page { size: A4; margin: 2.2cm 2cm 2.2cm 2cm;
        @bottom-center { content: counter(page); font-size: 10pt; } }
body { font-family: "Times New Roman", Times, serif; font-size: 11pt; line-height: 1.3; color: #111; }
h1 { font-size: 17pt; margin: 0 0 10pt; page-break-before: always; }
section.kapak h1 { page-break-before: avoid; }
h2 { font-size: 14pt; margin: 16pt 0 6pt; } h3 { font-size: 12.5pt; margin: 12pt 0 4pt; }
h1, h2, h3 { page-break-after: avoid; font-family: Arial, Helvetica, sans-serif; }
p { margin: 0 0 7pt; text-align: justify; }
table { border-collapse: collapse; width: 100%; margin: 4pt 0 10pt; font-size: 11pt; page-break-inside: auto; }
section.ayni h1 { page-break-before: avoid; margin-top: 24pt; }
th, td { border: 0.6pt solid #888; padding: 2.5pt 4pt; vertical-align: top; text-align: left; }
th { background: #eee; } tr { page-break-inside: avoid; }
code { font-family: Menlo, Consolas, monospace; font-size: 10pt; background: #f3f3f3; padding: 0 2pt;
       word-break: break-all; }
pre { background: #f6f6f6; border: 0.5pt solid #ccc; padding: 5pt; font-size: 9.5pt; white-space: pre-wrap;
      page-break-inside: avoid; } pre code { background: none; font-size: 9.5pt; }
p.etiket { margin: 8pt 0 3pt; text-align: left; }
figure { margin: 6pt 0 2pt; text-align: center; page-break-inside: avoid; }
img { max-width: 100%; max-height: 17cm; }
ul, ol { margin: 0 0 7pt 0; padding-left: 18pt; } li { margin-bottom: 2pt; }
.sayfa { page-break-after: always; }
"""


def olcum_tablosu():
    """Ek C: kanit/olcumler.csv'nin tamamı, Markdown tablosu olarak."""
    satirlar = ["**Tablo C.1.** Ölçümler (`kanit/olcumler.csv`).", "",
                "| # | Ölçüm | Değer | Birim | Yöntem | Tekrar | Not |", "|---|---|---|---|---|---|---|"]
    temiz = lambda x: (x or "").replace("|", "/").replace("\n", " ")
    for i, r in enumerate(csv.DictReader(open(os.path.join(KOK, "kanit", "olcumler.csv"), encoding="utf-8")), 1):
        satirlar.append(f"| {i} | `{temiz(r['olcum'])}` | {temiz(r['deger'])} | {temiz(r['birim'])} | "
                        f"{temiz(r['yontem'])} | {temiz(r['tekrar'])} | {temiz(r['not'])} |")
    return "\n".join(satirlar)


def main():
    govde = []
    for ad in SIRA:
        yol = os.path.join(RAPOR, ad)
        if not os.path.exists(yol):
            sys.exit(f"eksik bölüm: rapor/{ad}")
        sinif = ' class="kapak"' if ad.startswith("01") else ' class="ayni"' if ad.startswith("13") else ""
        metin = open(yol, encoding="utf-8").read().replace("<!-- OLCUMLER -->", olcum_tablosu())
        govde.append(f"<section{sinif}>" + md_html(metin, RAPOR, numarala=ad[:2] not in ("01", "14")) + "</section>")
    belge = (f'<!doctype html><html lang="tr"><head><meta charset="utf-8"><title>{AD}</title>'
             f"<style>{CSS}</style></head><body>" + "\n".join(govde) + "</body></html>")
    h = os.path.join(RAPOR, AD + ".html")
    open(h, "w", encoding="utf-8").write(belge)
    chrome = shutil.which("google-chrome") or "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
    pdf = os.path.join(RAPOR, AD + ".pdf")
    subprocess.run([chrome, "--headless", "--disable-gpu", "--no-pdf-header-footer", f"--print-to-pdf={pdf}",
                    "file://" + h], check=True, capture_output=True)
    print("yazıldı:", os.path.relpath(pdf, KOK))


if __name__ == "__main__":
    main()
