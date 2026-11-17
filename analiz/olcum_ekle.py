# -*- coding: utf-8 -*-
"""kanit/*.txt dosyalarındaki 'OLCUM|...' satırlarını kanit/olcumler.csv'ye ekler (aynı ölçüm adı varsa eklemez).
Çalıştırma: python3 analiz/olcum_ekle.py"""
import csv, glob

CSV = "kanit/olcumler.csv"
var = {r["olcum"] for r in csv.DictReader(open(CSV, encoding="utf-8"))}
yeni = []
for dosya in sorted(glob.glob("kanit/*.txt")):
    for satir in open(dosya, encoding="utf-8"):
        if satir.startswith("OLCUM|"):
            alan = satir.rstrip("\n").split("|")[1:]
            if len(alan) == 7 and alan[1] not in var:
                var.add(alan[1]); yeni.append(alan)
with open(CSV, "a", newline="", encoding="utf-8") as f:
    csv.writer(f).writerows(yeni)
print(f"{len(yeni)} ölçüm eklendi")
