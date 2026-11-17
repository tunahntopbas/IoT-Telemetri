#!/usr/bin/env bash
# Kırılma deneyi: akış hattı çalışırken Kafka 90 saniye durdurulur, sonra yeniden başlatılır.
# Üretici ve akış işi hiç durdurulmaz. Sonunda Kafka'daki veri ile veritabanındaki sayım karşılaştırılır.
# Aynı koşu sırasında (Kafka durmadan önce) servis deposu sorgularının gecikmesi de ölçülür.
set -eu
cd "$(dirname "$0")/.."
ILER=analiz/cikti/akis-ana.jsonl; KAYIT=kanit/kirilma-deneyi-kafka.txt
rm -f $ILER
zaman() { date -u +%H:%M:%S; }
{
echo "KIRILMA DENEYİ: Kafka durdurulursa — konu sensorler, tablo sensor_dakika"
docker compose exec -T spark spark-submit is/akis_iskelet.py --ilerleme /kit/$ILER > analiz/cikti/akis-ana.log 2>&1 &
until [ -s $ILER ]; do sleep 2; done
echo "$(zaman) akış işi çalışıyor; üretici başlıyor (--hiz 50 --sure 600)"
docker compose exec -T araclar python araclar/uretici.py --grup G221210024 --sablon iot --hiz 50 --sure 600 \
  > analiz/cikti/uretici-ana.txt 2>&1 &
sleep 100
echo "$(zaman) servis deposu gecikme ölçümü başlıyor (yazma sürerken)"
analiz/p99_olc.sh 1000 | tee kanit/servis-p99.txt
echo "$(zaman) Kafka durduruluyor"
docker compose stop kafka 2>&1 | tail -1
sleep 90
echo "$(zaman) 90 sn sonra durum:"
echo "  üretici günlüğünün son satırları:"; tail -4 analiz/cikti/uretici-ana.txt | sed 's/^/    /'
echo "  akış işi günlüğündeki hata/uyarı satırları (ilk 5):"
grep -E "ERROR|WARN|Exception" analiz/cikti/akis-ana.log | grep -vE "spill|RowBasedKeyValueBatch" | head -5 | cut -c1-220 | sed 's/^/    /'
echo "  son yığın: $(tail -1 $ILER | cut -c1-160)"
echo "  veritabanındaki en son varış: $(docker exec bsm461-timescaledb psql -U iot -d telemetri -tAc "SELECT max(son_varis) FROM sensor_dakika")"
echo "  susan sensör sayısı (pano): $(docker exec bsm461-timescaledb psql -U iot -d telemetri -tAc "SELECT count(*) FROM susan_sensorler")"
echo "$(zaman) Kafka yeniden başlatılıyor"
docker compose start kafka 2>&1 | tail -1
until [ "$(docker inspect -f '{{.State.Health.Status}}' bsm461-kafka)" = healthy ]; do sleep 2; done
echo "$(zaman) Kafka sağlıklı"
sleep 60
echo "$(zaman) 60 sn sonra: akış işi çalışıyor mu? $(docker exec bsm461-spark pgrep -f akis_iskelet >/dev/null && echo evet || echo HAYIR)"
echo "  son yığın: $(tail -1 $ILER | cut -c1-160)"
echo "  susan sensör sayısı: $(docker exec bsm461-timescaledb psql -U iot -d telemetri -tAc "SELECT count(*) FROM susan_sensorler")"
until grep -q "toplam" analiz/cikti/uretici-ana.txt; do sleep 3; done
echo "$(zaman) üretici bitti: $(grep toplam analiz/cikti/uretici-ana.txt)"
echo "  üretici günlüğündeki hata satırları:"; grep -vE "olay gönderildi|→|toplam" analiz/cikti/uretici-ana.txt | head -8 | cut -c1-220 | sed 's/^/    /'
if docker exec bsm461-spark pgrep -f akis_iskelet >/dev/null; then
  sleep 40   # tek sorgulu işte boş yığın gelmiyor: son verinin işlenmesi için bekleniyor
  docker exec bsm461-spark pkill -f akis_iskelet || true
fi
echo "  akış işi çıkış nedeni (varsa):"; grep -E "Exception|Error" analiz/cikti/akis-ana.log | grep -vE "WARN" | head -3 | cut -c1-260 | sed 's/^/    /'
sleep 5
echo "== Kafka ile karşılaştırma =="
docker compose exec -T spark spark-submit analiz/akis_dogrula.py --konu sensorler --tablo sensor_dakika --pencere \
  --deney z11_kafka_durdu 2>&1 | grep -E "Kafka|\[|pencere|OLCUM|Error|Exception"
} 2>&1 | tee $KAYIT
