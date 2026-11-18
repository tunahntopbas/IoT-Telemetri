#!/usr/bin/env bash
# Kırılma deneyi: aynı anda iki Spark işi. Önce toplu iş tek başına, sonra akış işi çalışırken toplu iş.
# docker stats 2 sn'de bir örneklenir (tepe bellek), toplu işin süresi ve çıkış kodu, akış yığınlarının süresi,
# kapsayıcının OOM ile öldürülüp öldürülmediği kaydedilir. Docker'a ayrılan bellek 4,786 GiB.
set -eu
cd "$(dirname "$0")/.."
KONU=akis-bellek; TABLO=deney_bellek; ILER=analiz/cikti/akis-bellek.jsonl; KAYIT=kanit/kirilma-deneyi-bellek-${TEKRAR:-1}.txt
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic $KONU --partitions 8 --replication-factor 1 2>&1 | grep -v JAVA_TOOL || true
rm -f $ILER
zaman() { date -u +%H:%M:%S; }
olc() {  # $1 = dosya; durdurulana kadar 2 sn'de bir bellek örneği
  while true; do docker stats --no-stream --format '{{.Name}},{{.MemUsage}}' | grep bsm461 | sed "s/^/$(date -u +%H:%M:%S),/" >> $1; sleep 2; done; }
tepe() {  # $1 = dosya: kapsayıcı başına en yüksek bellek ve toplam
  python3 - "$1" <<'PY'
import sys, collections
birim = {"KiB": 1/1024**2, "MiB": 1/1024, "GiB": 1}
tepe, zaman_top = collections.defaultdict(float), collections.defaultdict(float)
for l in open(sys.argv[1]):
    z, ad, kul = l.strip().split(",", 2)
    d = kul.split("/")[0].strip()
    for b, k in birim.items():
        if d.endswith(b): g = float(d[:-len(b)]) * k
    tepe[ad] = max(tepe[ad], g); zaman_top[z] += g
for ad, g in sorted(tepe.items()): print(f"    {ad}: en yüksek {g:.2f} GiB")
print(f"    bütün kapsayıcılar toplamı, en yüksek an: {max(zaman_top.values()):.2f} GiB / 4,786 GiB")
PY
}
toplu() {  # $1 = günlük dosyası; süre ve çıkış kodu
  local bas=$(date +%s); set +e
  docker compose exec -T spark spark-submit is/toplu_is.py --explain /tmp/explain-bellek.txt > $1 2>&1; local kod=$?; set -e
  echo "  toplu iş: çıkış kodu $kod, duvar saati $(( $(date +%s) - bas )) sn, işin kendi sayacı: $(grep 'toplam süre' $1 | tail -1)"
  grep -E "OutOfMemory|Killed|exit code 137|Container killed" $1 | head -3 | sed 's/^/    /' || true; }
{
echo "KIRILMA DENEYİ: aynı anda iki Spark işi (toplu iş + akış işi)"
echo "== 1) toplu iş tek başına =="
rm -f analiz/cikti/bellek-1.csv; olc analiz/cikti/bellek-1.csv & O=$!
echo "$(zaman) başladı"; toplu analiz/cikti/toplu-tek.log
kill $O; wait $O 2>/dev/null || true; tepe analiz/cikti/bellek-1.csv
echo "== 2) akış işi (--hiz 50) çalışırken toplu iş =="
rm -f analiz/cikti/bellek-2.csv; olc analiz/cikti/bellek-2.csv & O=$!
docker compose exec -T spark spark-submit is/akis_iskelet.py --konu $KONU --tablo $TABLO --ilerleme /kit/$ILER \
  > analiz/cikti/akis-bellek.log 2>&1 &
until [ -s $ILER ]; do sleep 2; done
docker compose exec -T araclar python araclar/uretici.py --grup G221210024 --sablon iot --konu $KONU \
  --hiz 50 --sure 360 --gec-orani 0 > analiz/cikti/akis-bellek-uretici.txt 2>&1 &
sleep 60
TB=$(date -u +%Y-%m-%dT%H:%M:%S); echo "$(zaman) toplu iş başladı (akış işi açık)"
toplu analiz/cikti/toplu-birlikte.log
TS=$(date -u +%Y-%m-%dT%H:%M:%S); echo "$(zaman) toplu iş bitti"
echo "  akış işi hâlâ çalışıyor mu? $(docker exec bsm461-spark pgrep -f "akis_iskelet.py --konu $KONU" >/dev/null && echo evet || echo HAYIR)"
until grep -q "toplam" analiz/cikti/akis-bellek-uretici.txt; do sleep 3; done
sleep 30; docker exec bsm461-spark pkill -f "akis_iskelet.py --konu $KONU" || true
kill $O; wait $O 2>/dev/null || true; tepe analiz/cikti/bellek-2.csv
echo "  spark kapsayıcısı OOM ile öldürüldü mü? $(docker inspect -f '{{.State.OOMKilled}}' bsm461-spark), yeniden başlama sayısı $(docker inspect -f '{{.RestartCount}}' bsm461-spark)"
python3 - $ILER $TB $TS <<'PY'
import json, sys
from datetime import datetime
y = [json.loads(l) for l in open(sys.argv[1]) if json.loads(l)["girdi"] > 0]
tb, ts = (datetime.fromisoformat(x) for x in sys.argv[2:4])
z = lambda p: datetime.fromisoformat(p["zaman"][:19])
for ad, sec in [("toplu işten önce", [p for p in y if z(p) < tb]), ("toplu iş sürerken", [p for p in y if tb <= z(p) <= ts]),
                ("toplu işten sonra", [p for p in y if z(p) > ts])]:
    s = [p["sure_ms"]["triggerExecution"] / 1000 for p in sec]
    if s: print(f"  akış yığın süresi {ad}: {len(s)} yığın, ortalama {sum(s)/len(s):.1f} sn, en uzun {max(s):.1f} sn")
PY
sleep 5
echo "== Kafka ile karşılaştırma =="
docker compose exec -T spark spark-submit analiz/akis_dogrula.py --konu $KONU --tablo $TABLO --deney z11_bellek 2>&1 \
  | grep -E "Kafka|\[|OLCUM|Error|Exception"
} 2>&1 | tee $KAYIT
