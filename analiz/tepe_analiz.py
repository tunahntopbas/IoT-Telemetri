# -*- coding: utf-8 -*-
"""Tepe yük deneyinin çıktısını aşama aşama özetler (analiz/tepe_yuk_deneyi.sh çağırır).

Uyarı gecikmesi = olayın Kafka'ya vardığı an ile veritabanında görünür olduğu an (yığının commit'i) arası.
batch_gunlugu her yığın için commit zamanını (yazilma) ve yığındaki en geç varışı (max_varis) tutuyor.
N. yığına giren olaylar (N-1). yığının max_varis'inden sonra, N. yığının max_varis'ine kadar Kafka'ya varmış:
  en kısa gecikme  = yazilma(N) - max_varis(N)
  en uzun gecikme  = yazilma(N) - max_varis(N-1)     (yığına giren en erken olay için üst sınır)
"""
import csv, json, re, sys
from datetime import datetime, timedelta, timezone
from statistics import median

C = "analiz/cikti/"
AD = sys.argv[1] if len(sys.argv) > 1 else "tepe"
TETIK = sys.argv[2] if len(sys.argv) > 2 else "10 seconds"


def zaman(s):
    """ISO zamanı ya da PostgreSQL çıktısını ('2026-11-17 20:55:19.44+00') datetime'a çevirir."""
    m = re.match(r"(\d{4}-\d\d-\d\d)[T ](\d\d:\d\d:\d\d)(?:\.(\d+))?(Z|[+-]\d\d(?::?\d\d)?)?$", s.strip())
    gun, saat, kesir, bolge = m.groups()
    t = datetime.strptime(f"{gun} {saat}", "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    if kesir:
        t = t.replace(microsecond=int(kesir[:6].ljust(6, "0")))
    if bolge and bolge != "Z":
        isaret = 1 if bolge[0] == "+" else -1
        sa, dk = int(bolge[1:3]), int(bolge[-2:]) if len(bolge) > 3 else 0
        t -= isaret * timedelta(hours=sa, minutes=dk)
    return t


asamalar = [(int(r["hiz"]), zaman(r["bas"]), zaman(r["son"])) for r in csv.DictReader(open(C + f"{AD}-asamalar.csv"))]
yiginlar = [json.loads(l) for l in open(C + f"akis-{AD}.jsonl")]
gunluk = {}
for r in csv.DictReader(open(C + f"{AD}-batch-gunlugu.csv")):
    gunluk[int(r["batch_id"])] = r            # aynı yığın iki kez yazıldıysa sonuncusu
onceki_varis = None
gecikme = {}
for b in sorted(gunluk):
    r = gunluk[b]
    if not r["max_varis"]:
        continue
    y, v = zaman(r["yazilma"]), zaman(r["max_varis"])
    if onceki_varis is not None:
        gecikme[b] = (v, (y - v).total_seconds(), (y - onceki_varis).total_seconds())
    onceki_varis = v


def yuzdelik(x, p):
    x = sorted(x)
    return x[min(len(x) - 1, int(round(p / 100 * (len(x) - 1))))]


print(f"{'hız':>5} {'yığın':>5} {'girdi/yığın ort':>15} {'en büyük yığın':>14} {'süre ort':>8} {'süre max':>8} "
      f"{'işlem hızı ort':>14} {'gecikme alt ort':>15} {'gecikme üst ort':>15} {'gecikme üst max':>15}")
for hiz, bas, son in asamalar:
    ys = [y for y in yiginlar if bas <= zaman(y["zaman"]) <= son and y["girdi"] > 0]
    gs = [g for b, g in gecikme.items() if bas <= g[0] <= son]
    if not ys or not gs:
        print(f"{hiz:>5}: veri yok"); continue
    sure = [y["sure_ms"]["triggerExecution"] / 1000 for y in ys]
    islem = [y["islem_hizi"] for y in ys if y["islem_hizi"]]
    alt = [g[1] for g in gs]; ust = [g[2] for g in gs]
    print(f"{hiz:>5} {len(ys):>5} {sum(y['girdi'] for y in ys)/len(ys):>15,.0f} {max(y['girdi'] for y in ys):>14,} "
          f"{sum(sure)/len(sure):>7.1f}s {max(sure):>7.1f}s {sum(islem)/len(islem):>13,.0f}/s "
          f"{median(alt):>14.1f}s {median(ust):>14.1f}s {max(ust):>14.1f}s")
    t = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    yontem = f"tepe_yuk_deneyi.sh, uretici --hiz {hiz}, tetik {TETIK}"
    print(f"OLCUM|{t}|z11_{AD}_{hiz}_yigin_suresi_max|{max(sure):.1f}|saniye|{yontem}; Spark ilerleme triggerExecution|{len(ys)}|ortalama {sum(sure)/len(sure):.1f} s")
    print(f"OLCUM|{t}|z11_{AD}_{hiz}_islem_hizi_ort|{sum(islem)/len(islem):.0f}|olay/saniye|{yontem}; Spark processedRowsPerSecond|{len(ys)}|")
    print(f"OLCUM|{t}|z8_uyari_gecikmesi_{AD}_{hiz}_ust_ortanca|{median(ust):.1f}|saniye|{yontem}; batch_gunlugu yazilma(N)-max_varis(N-1)|{len(gs)}|max {max(ust):.1f} s, p95 {yuzdelik(ust,95):.1f} s")
