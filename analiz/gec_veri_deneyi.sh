#!/usr/bin/env bash
# Geç veri deneyi: bir konuya --hiz 50 ile 5 dakika olay gönderilir; akış işi aynı konuyu iki watermark ile
# (2 dk ve 30 sn) işler; sonunda Kafka'daki gerçek veriyle karşılaştırılır.
#   analiz/gec_veri_deneyi.sh <ad> <gec-orani> [gec-en-fazla-dk]   ör. analiz/gec_veri_deneyi.sh gec-30 0.3
set -eu
cd "$(dirname "$0")/.."
AD=$1; ORAN=$2; ENFAZLA=${3:+--gec-en-fazla-dk $3}; KONU="akis-$AD"; TABLO="deney_${AD//-/_}"; ILER="analiz/cikti/akis-$AD.jsonl"
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic "$KONU" --partitions 8 --replication-factor 1 2>&1 | grep -v JAVA_TOOL || true
rm -f "$ILER"
docker compose exec -T spark spark-submit is/akis_iskelet.py --konu "$KONU" --tablo "$TABLO" \
  --watermark "2 minutes" --watermark "30 seconds" --ilerleme "/kit/$ILER" > "analiz/cikti/akis-$AD.log" 2>&1 &
until [ -s "$ILER" ]; do sleep 2; done
docker compose exec -T araclar python araclar/uretici.py --grup G221210024 --sablon iot --konu "$KONU" \
  --hiz 50 --sure 300 --gec-orani "$ORAN" $ENFAZLA 2>&1 | grep -E "→|toplam" | tee "kanit/akis-$AD-uretici.txt"
until [ "$(tail -4 "$ILER" | grep -c '"girdi": 0')" = 4 ]; do sleep 3; done
docker exec bsm461-spark pkill -f akis_iskelet || true
sleep 5
docker compose exec -T spark spark-submit analiz/akis_dogrula.py --konu "$KONU" --tablo "${TABLO}_wm120" --tablo "${TABLO}_wm30" \
  --deney "z8_$AD" 2>&1 | grep -E "Kafka|\[|OLCUM|Error|Exception" | tee "kanit/akis-$AD-dogrulama.txt"
