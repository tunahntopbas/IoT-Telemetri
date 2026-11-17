#!/usr/bin/env bash
# Tepe yük deneyi ve uyarı gecikmesi ölçümü.
# Ayrı bir konuya (akis-tepe) sırayla 50, 2000, 4000, 8000 olay/sn gönderilir; akış işi hiç durmaz.
# Her aşamada: üreticinin gerçekten ulaştığı hız, yığın süresi, Kafka'da işlenmeyi bekleyen mesaj (gecikme/lag)
# ve olayın Kafka'ya varışından veritabanında görünmesine kadar geçen süre (analiz/tepe_analiz.py).
# Değiştirilebilir: AD=tetik5 HIZLAR="50 4000" TETIK="5 seconds" EK="--checkpoint file:///kit/veri/checkpoint" analiz/tepe_yuk_deneyi.sh
set -eu
cd "$(dirname "$0")/.."
AD=${AD:-tepe}; HIZLAR=${HIZLAR:-"50 2000 4000 8000"}; TETIK=${TETIK:-"10 seconds"}
KONU=akis-$AD; TABLO=deney_$AD; ILER=analiz/cikti/akis-$AD.jsonl; ASAMA=analiz/cikti/$AD-asamalar.csv
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic $KONU --partitions 8 --replication-factor 1 2>&1 | grep -v JAVA_TOOL || true
rm -f $ILER; echo "hiz,bas,son" > $ASAMA
docker compose exec -T spark spark-submit is/akis_iskelet.py --konu $KONU --tablo $TABLO --ilerleme /kit/$ILER --tetik "$TETIK" ${EK:-} \
  > analiz/cikti/akis-$AD.log 2>&1 &
until [ -s $ILER ]; do sleep 2; done
for HIZ in $HIZLAR; do
  SURE=$([ $HIZ = 50 ] && echo 180 || echo 90)
  BAS=$(date -u +%Y-%m-%dT%H:%M:%S)
  echo "== --hiz $HIZ --sure $SURE ($BAS UTC) =="
  docker compose exec -T araclar python araclar/uretici.py --grup G221210024 --sablon iot --konu $KONU \
    --hiz $HIZ --sure $SURE 2>&1 | grep -E "toplam" || true
  echo "$HIZ,$BAS,$(date -u +%Y-%m-%dT%H:%M:%S)" >> $ASAMA
done
# Son aşamanın birikmiş mesajları işlenene kadar bekle (girdi 0 olan iki yığın), en fazla 5 dk
for i in $(seq 1 100); do [ "$(tail -2 $ILER | grep -c '"girdi": 0')" = 2 ] && break; sleep 3; done
docker exec bsm461-spark pkill -f akis_iskelet || true
docker exec bsm461-timescaledb psql -U iot -d telemetri -c \
  "COPY (SELECT batch_id, satir, max_varis, yazilma FROM batch_gunlugu WHERE sorgu='$TABLO-db' ORDER BY batch_id, yazilma) TO STDOUT CSV HEADER" \
  > analiz/cikti/$AD-batch-gunlugu.csv
python3 analiz/tepe_analiz.py $AD "$TETIK"
sleep 5
echo "== Kafka ile karşılaştırma =="
docker compose exec -T spark spark-submit analiz/akis_dogrula.py --konu $KONU --tablo $TABLO --deney z11_$AD 2>&1 \
  | grep -E "Kafka|\[|OLCUM|Error|Exception"
