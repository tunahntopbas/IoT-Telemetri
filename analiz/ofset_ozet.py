# -*- coding: utf-8 -*-
"""kirilma_checkpoint.sh için: checkpoint'siz yeniden başlatmada işin Kafka'nın neresinden başladığını yazar.
python3 analiz/ofset_ozet.py <ilerleme.jsonl> <earliest|latest>"""
import json, sys

y = [json.loads(l) for l in open(sys.argv[1])]
i = [k for k, p in enumerate(y) if p["batch"] == 0][-1]          # ikinci işin ilk yığını
toplam = lambda d: sum(list(d.values())[0].values()) if d else 0
ilk_son = toplam(y[i - 1]["kaynak"][0]["son"])
yeni = y[i]["kaynak"][0]
yeni_bas = 0 if sys.argv[2] == "earliest" else toplam(yeni["son"])
satir = (f"  ilk iş Kafka'da {ilk_son:,} ofsete kadar işlemişti; checkpoint'siz iş {yeni_bas:,} ofsetinden başladı "
         f"(ilk yığın {y[i]['girdi']:,} olay)")
if sys.argv[2] == "latest":
    satir += f": arada kalan {yeni_bas - ilk_son:,} mesaj hiç okunmadı"
print(satir)
