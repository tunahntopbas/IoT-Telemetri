# -*- coding: utf-8 -*-
"""Toplu soruların grafikleri. Girdi: is/toplu_is.py'nin yazdığı analiz/cikti/toplu1.csv ve toplu2.csv.

  docker compose exec araclar python analiz/grafikler.py
"""
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

CIKTI = "/kit/analiz/cikti"
t1 = pd.read_csv(f"{CIKTI}/toplu1.csv")
t2 = pd.read_csv(f"{CIKTI}/toplu2.csv")

# ── Toplu 1 ────────────────────────────────────────────────────────────────
fig, (a1, a2) = plt.subplots(1, 2, figsize=(13, 4.8))
for bolge, g in t1.groupby("bolge"):
    a1.plot(g["saat_tr"], g["ort_sicaklik"], marker="o", ms=3, label=bolge)
    a2.plot(g["saat_tr"], g["anomali"], marker="o", ms=3, label=bolge)
a1.set(title="Ortalama sıcaklık (14 gün)", xlabel="Saat (Türkiye)", ylabel="°C", xticks=range(0, 24, 3))
a2.set(title=f"Titreşim anomalisi sayısı (titreşim > 1,0)", xlabel="Saat (Türkiye)", ylabel="kayıt", xticks=range(0, 24, 3))
a1.grid(alpha=.3); a2.grid(alpha=.3)
a2.legend(fontsize=8, ncol=2)
fig.suptitle("Şekil: Bölge ve günün saatine göre sıcaklık ve anomali (grup G221210024, 2.992.895 olay)")
fig.tight_layout()
fig.savefig(f"{CIKTI}/toplu1.png", dpi=150)

# ── Toplu 2 ────────────────────────────────────────────────────────────────
model = (t2.groupby("model").agg(sensor=("sensor_id", "count"), gelen=("gelen", "sum"), anomali=("anomali", "sum"),
                                 ort_kayip=("kayip_orani", "mean"), kayipli=("kayip_orani", lambda s: (s > 0.01).sum()))
         .assign(anomali_orani=lambda d: d["anomali"] / d["gelen"]).sort_values("anomali_orani", ascending=False))
fig, (b1, b2) = plt.subplots(1, 2, figsize=(13, 4.8))
b1.bar(model.index, model["anomali_orani"] * 100, color="#c0504d")
b1.set(title="Anomali oranı, sensör modeline göre", ylabel="% (anomali / gelen olay)")
for i, (m, r) in enumerate(model.iterrows()):
    b1.text(i, r["anomali_orani"] * 100, f"{r['anomali_orani'] * 100:.2f}\n({int(r['sensor'])} sensör)", ha="center", va="bottom", fontsize=8)
s = t2.sort_values("kayip_orani", ascending=False).reset_index(drop=True)
renk = {m: c for m, c in zip(sorted(t2["model"].unique()), plt.cm.tab10.colors)}
b2.bar(range(len(s)), s["kayip_orani"] * 100, color=[renk[m] for m in s["model"]], width=1.0)
b2.axhline(1, color="k", ls="--", lw=1)
b2.text(len(s) * 0.55, 1.6, "kart: kayıp sınırı %1", fontsize=8)
b2.set(title="Sensör başına veri kaybı (büyükten küçüğe)", xlabel="sensör sırası", ylabel="% (1 − gelen / ortanca)")
b2.legend(handles=[plt.Rectangle((0, 0), 1, 1, color=c) for c in renk.values()], labels=list(renk), fontsize=8)
b1.grid(axis="y", alpha=.3); b2.grid(axis="y", alpha=.3)
fig.suptitle("Şekil: Sensör modeline göre anomali ve sensör başına veri kaybı (200 sensör, 14 gün)")
fig.tight_layout()
fig.savefig(f"{CIKTI}/toplu2.png", dpi=150)

# ── Bulgu cümleleri için sayılar ───────────────────────────────────────────
g = t1.groupby("saat_tr")["ort_sicaklik"].mean()
print(f"Toplu 1: bölgeler ortalaması en sıcak saat {g.idxmax()} ({g.max():.2f} °C), en serin {g.idxmin()} ({g.min():.2f} °C)")
b = t1.groupby("bolge").apply(lambda d: (d["olay"] * d["ort_sicaklik"]).sum() / d["olay"].sum()).sort_values()
print("Toplu 1: bölge ortalama sıcaklık " + ", ".join(f"{k} {v:.2f}" for k, v in b.items()))
an = t1.groupby("saat_tr")["anomali"].sum()
print(f"Toplu 1: saat başına anomali, en çok saat {an.idxmax()} ({an.max()}), ortanca saat {an.median():.0f}, toplam {an.sum()}")
top = t1.sort_values("anomali", ascending=False).head(3)
print("Toplu 1: en çok anomali bölge×saat: " + "; ".join(f"{r.bolge} {r.saat_tr}:00 {r.anomali}" for r in top.itertuples()))
print(model.to_string())
print(f"Toplu 2: kaybı %1'i aşan sensör {int((t2['kayip_orani'] > 0.01).sum())} / {len(t2)}, en büyük kayıp %{t2['kayip_orani'].max() * 100:.1f}")
print("Toplu 2: kaybı %20'yi aşanlar: " + ", ".join(t2[t2["kayip_orani"] > 0.2]["sensor_id"]))
