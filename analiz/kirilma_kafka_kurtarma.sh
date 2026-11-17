#!/usr/bin/env bash
# kirilma_kafka.sh sonrası: Kafka kesintisinde ölen akış işini AYNI checkpoint ile yeniden başlatır,
# kaçırdığı olayları yakalayıp yakalamadığını Kafka ile karşılaştırarak kontrol eder.
set -eu
cd "$(dirname "$0")/.."
ILER=analiz/cikti/akis-kurtarma.jsonl; KAYIT=kanit/kirilma-deneyi-kafka-kurtarma.txt
rm -f $ILER
zaman() { date -u +%H:%M:%S; }
{
echo "KURTARMA: Kafka kesintisinde ölen akış işi aynı checkpoint ile yeniden başlatılıyor"
echo "$(zaman) iş başlatıldı"
docker compose exec -T spark spark-submit is/akis_iskelet.py --ilerleme /kit/$ILER > analiz/cikti/akis-kurtarma.log 2>&1 &
until [ -s $ILER ]; do sleep 2; done
echo "$(zaman) ilk yığın bitti:"
head -1 $ILER | python3 -c "import json,sys; p=json.loads(sys.stdin.read()); print(f\"  batch {p['batch']}, girdi {p['girdi']:,} olay, süre {p['sure_ms']['triggerExecution']/1000:.1f} sn\")"
sleep 60
docker exec bsm461-spark pkill -f akis_iskelet || true
echo "$(zaman) iş durduruldu; yığınlar:"
python3 -c "import json; [print(f\"  batch {p['batch']}: girdi {p['girdi']:,}, süre {p['sure_ms']['triggerExecution']/1000:.1f} sn\") for p in map(json.loads, open('$ILER'))]"
echo "  susan sensör sayısı (üretici bittiği için hepsi susmuş olmalı): $(docker exec bsm461-timescaledb psql -U iot -d telemetri -tAc "SELECT count(*) FROM susan_sensorler")"
sleep 5
echo "== Kafka ile karşılaştırma =="
docker compose exec -T spark spark-submit analiz/akis_dogrula.py --konu sensorler --tablo sensor_dakika --pencere \
  --deney z11_kafka_kurtarma 2>&1 | grep -E "Kafka|\[|pencere|OLCUM|Error|Exception"
} 2>&1 | tee $KAYIT
