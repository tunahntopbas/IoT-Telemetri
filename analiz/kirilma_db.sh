#!/usr/bin/env bash
# Kırılma deneyi: akış işi çalışırken servis deposu (TimescaleDB) 60 sn durdurulur.
# Beklenen: foreachBatch içindeki JDBC bağlantısı kurulamaz, yığın hata verir, iş ölür; Kafka ofseti ilerlemediği için
# veri kaybolmaz. Veritabanı geri gelince iş aynı checkpoint ile yeniden başlatılır ve Kafka ile karşılaştırılır.
set -eu
cd "$(dirname "$0")/.."
KONU=akis-dbkes; TABLO=deney_dbkes; ILER=analiz/cikti/akis-dbkes.jsonl; KAYIT=kanit/kirilma-deneyi-db.txt
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic $KONU --partitions 8 --replication-factor 1 2>&1 | grep -v JAVA_TOOL || true
rm -f $ILER
zaman() { date -u +%H:%M:%S; }
calisiyor() { docker exec bsm461-spark pgrep -f "akis_iskelet.py --konu $KONU" >/dev/null && echo evet || echo HAYIR; }
{
echo "KIRILMA DENEYİ: servis deposu (TimescaleDB) durdurulursa — konu $KONU, tablo $TABLO"
docker compose exec -T spark spark-submit is/akis_iskelet.py --konu $KONU --tablo $TABLO --ilerleme /kit/$ILER \
  > analiz/cikti/akis-dbkes-1.log 2>&1 &
until [ -s $ILER ]; do sleep 2; done
echo "$(zaman) akış işi çalışıyor; üretici başlıyor (--hiz 50 --sure 300 --gec-orani 0)"
docker compose exec -T araclar python araclar/uretici.py --grup G221210024 --sablon iot --konu $KONU \
  --hiz 50 --sure 300 --gec-orani 0 > analiz/cikti/akis-dbkes-uretici.txt 2>&1 &
sleep 60
echo "$(zaman) TimescaleDB durduruluyor (son yığın: $(tail -1 $ILER | cut -c1-90))"
docker compose stop timescaledb 2>&1 | tail -1
for s in 15 30 45 60; do sleep 15; echo "  +$s sn: akış işi çalışıyor mu? $(calisiyor)"; done
echo "  akış işi günlüğündeki ilk hata satırları:"
grep -E "Exception|ERROR" analiz/cikti/akis-dbkes-1.log | grep -v WARN | head -3 | cut -c1-240 | sed 's/^/    /'
echo "$(zaman) TimescaleDB başlatılıyor"
docker compose start timescaledb 2>&1 | tail -1
until [ "$(docker inspect -f '{{.State.Health.Status}}' bsm461-timescaledb)" = healthy ]; do sleep 2; done
echo "$(zaman) TimescaleDB sağlıklı; akış işi çalışıyor mu? $(calisiyor)"
if [ "$(calisiyor)" = HAYIR ]; then
  echo "$(zaman) akış işi aynı checkpoint ile yeniden başlatılıyor"
  docker compose exec -T spark spark-submit is/akis_iskelet.py --konu $KONU --tablo $TABLO --ilerleme /kit/$ILER \
    > analiz/cikti/akis-dbkes-2.log 2>&1 &
fi
until grep -q "toplam" analiz/cikti/akis-dbkes-uretici.txt; do sleep 3; done
echo "$(zaman) üretici bitti: $(grep toplam analiz/cikti/akis-dbkes-uretici.txt)"
sleep 40
docker exec bsm461-spark pkill -f "akis_iskelet.py --konu $KONU" || true
sleep 5
echo "== batch_gunlugu: birden fazla yazılan yığınlar =="
docker exec bsm461-timescaledb psql -U iot -d telemetri -c \
  "SELECT batch_id, count(*) AS yazilma_sayisi, array_agg(satir) AS satirlar FROM batch_gunlugu WHERE sorgu = '$TABLO-db' GROUP BY batch_id HAVING count(*) > 1"
echo "== Kafka ile karşılaştırma =="
docker compose exec -T spark spark-submit analiz/akis_dogrula.py --konu $KONU --tablo $TABLO --pencere \
  --deney z11_db_durdu 2>&1 | grep -E "Kafka|\[|pencere|OLCUM|Error|Exception"
} 2>&1 | tee $KAYIT
