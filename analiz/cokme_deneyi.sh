#!/usr/bin/env bash
# Çökme deneyi: akış işi bir yığını veritabanına yazıp onayladıktan hemen sonra, Spark o yığının bittiğini
# checkpoint'e yazamadan ölür (--cokme-batch). Yeniden başlatılan iş aynı yığını yeniden işler.
# Beklenen: batch_gunlugu'nda o yığın iki kez görünür (en az bir kez teslim), tablo ise doğru kalır (upsert).
# Yerel checkpoint ile: AD=cokme-yerel EK="--checkpoint file:///kit/veri/checkpoint" analiz/cokme_deneyi.sh
set -eu
cd "$(dirname "$0")/.."
AD=${AD:-cokme}; KONU=akis-$AD; TABLO=deney_${AD//-/_}; ILER=analiz/cikti/akis-$AD.jsonl; KAYIT=kanit/akis-$AD-deneyi.txt; N=8
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic $KONU --partitions 8 --replication-factor 1 2>&1 | grep -v JAVA_TOOL || true
rm -f $ILER
zaman() { date -u +%H:%M:%S; }
{
echo "ÇÖKME DENEYİ — konu $KONU, tablo $TABLO, yığın $N yazıldıktan hemen sonra süreç öldürülüyor"
docker compose exec -T spark spark-submit is/akis_iskelet.py --konu $KONU --tablo $TABLO --cokme-batch $N ${EK:-} \
  --ilerleme /kit/$ILER > analiz/cikti/akis-$AD-1.log 2>&1 &
IS=$!
until [ -s $ILER ]; do sleep 2; done
echo "$(zaman) üretici başlıyor (--hiz 50 --sure 240 --gec-orani 0)"
docker compose exec -T araclar python araclar/uretici.py --grup G221210024 --sablon iot --konu $KONU \
  --hiz 50 --sure 240 --gec-orani 0 > analiz/cikti/akis-$AD-uretici.txt 2>&1 &
wait $IS || true
echo "$(zaman) ilk iş bitti: $(grep -h 'DENEY:' analiz/cikti/akis-$AD-1.log)"
echo "$(zaman) aynı checkpoint ile yeniden başlatılıyor"
docker compose exec -T spark spark-submit is/akis_iskelet.py --konu $KONU --tablo $TABLO ${EK:-} \
  --ilerleme /kit/$ILER > analiz/cikti/akis-$AD-2.log 2>&1 &
until grep -q "toplam" analiz/cikti/akis-$AD-uretici.txt; do sleep 3; done
echo "$(zaman) üretici bitti: $(grep toplam analiz/cikti/akis-$AD-uretici.txt)"
sleep 40   # tek sorgulu işte boş yığın gelmiyor: son verinin işlenmesi için bekleniyor
docker exec bsm461-spark pkill -f akis_iskelet || true
sleep 5
echo; echo "== batch_gunlugu: birden fazla yazılan yığınlar =="
docker exec bsm461-timescaledb psql -U iot -d telemetri -c \
  "SELECT batch_id, count(*) AS yazilma_sayisi, array_agg(satir) AS satirlar, min(yazilma), max(yazilma) FROM batch_gunlugu WHERE sorgu = '$TABLO-db' GROUP BY batch_id HAVING count(*) > 1"
echo "== Kafka ile karşılaştırma =="
docker compose exec -T spark spark-submit analiz/akis_dogrula.py --konu $KONU --tablo $TABLO --pencere \
  --deney z8_${AD//-/_} 2>&1 | grep -E "Kafka|\[|pencere|OLCUM|Error|Exception"
} 2>&1 | tee $KAYIT
