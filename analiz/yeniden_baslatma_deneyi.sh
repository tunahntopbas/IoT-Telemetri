#!/usr/bin/env bash
# Yeniden başlatma deneyi: akış işi (veritabanı + S3 çıktısı) çalışırken kill -9 ile öldürülür, 60 sn sonra
# aynı checkpoint ile yeniden başlatılır. Üretici bu sırada hiç durmaz. Sonunda Kafka'daki gerçek veriyle
# pencere pencere karşılaştırılır. Geç olay kapalı (--gec-orani 0): fark çıkarsa nedeni yeniden başlatmadır.
#   analiz/yeniden_baslatma_deneyi.sh
set -eu
cd "$(dirname "$0")/.."
KONU=akis-yeniden; TABLO=deney_yeniden; ILER=analiz/cikti/akis-yeniden.jsonl; KAYIT=kanit/akis-yeniden-baslatma.txt
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic $KONU --partitions 8 --replication-factor 1 2>&1 | grep -v JAVA_TOOL || true
rm -f $ILER
is_baslat() {
  docker compose exec -T spark spark-submit is/akis_iskelet.py --konu $KONU --tablo $TABLO --cikti db --cikti s3 \
    --ilerleme /kit/$ILER >> analiz/cikti/akis-yeniden.log 2>&1 &
}
zaman() { date -u +%H:%M:%S; }
{
echo "YENİDEN BAŞLATMA DENEYİ — konu $KONU, tablo $TABLO, s3a://islenmis/akis/$TABLO/"
is_baslat; until [ -s $ILER ]; do sleep 2; done
echo "$(zaman) akış işi çalışıyor; üretici başlıyor (--hiz 50 --sure 360 --gec-orani 0)"
docker compose exec -T araclar python araclar/uretici.py --grup G221210024 --sablon iot --konu $KONU \
  --hiz 50 --sure 360 --gec-orani 0 > analiz/cikti/akis-yeniden-uretici.txt 2>&1 &
sleep 120
echo "$(zaman) son tamamlanan yığınlar: $(tail -2 $ILER | python3 -c 'import sys,json;print([(j["sorgu"],j["batch"]) for j in map(json.loads,sys.stdin)])')"
docker exec bsm461-spark pkill -9 -f akis_iskelet || true
echo "$(zaman) akış işi kill -9 ile öldürüldü"
sleep 60
echo "$(zaman) akış işi aynı checkpoint ile yeniden başlatılıyor"
is_baslat
until grep -q "toplam" analiz/cikti/akis-yeniden-uretici.txt; do sleep 3; done
echo "$(zaman) üretici bitti: $(grep -E 'toplam' analiz/cikti/akis-yeniden-uretici.txt)"
until [ "$(tail -4 $ILER | grep -c '"girdi": 0')" = 4 ]; do sleep 3; done
docker exec bsm461-spark pkill -f akis_iskelet || true
echo "$(zaman) akış işi boşta, durduruldu"
sleep 5
echo; echo "== batch_gunlugu: aynı batch_id birden fazla yazıldı mı? =="
docker exec bsm461-timescaledb psql -U iot -d telemetri -c \
  "SELECT batch_id, count(*) AS yazilma_sayisi, min(yazilma), max(yazilma), min(satir) AS satir FROM batch_gunlugu WHERE sorgu = '$TABLO-db' GROUP BY batch_id HAVING count(*) > 1 ORDER BY batch_id"
docker exec bsm461-timescaledb psql -U iot -d telemetri -c \
  "SELECT count(*) AS yigin, count(DISTINCT batch_id) AS farkli_batch, max(batch_id) FROM batch_gunlugu WHERE sorgu = '$TABLO-db'"
echo "== Kafka ile karşılaştırma =="
docker compose exec -T spark spark-submit analiz/akis_dogrula.py --konu $KONU --tablo $TABLO --pencere \
  --s3 s3a://islenmis/akis/$TABLO/ --deney z8_yeniden 2>&1 | grep -E "Kafka|\[|pencere|OLCUM|Error|Exception"
} 2>&1 | tee $KAYIT
