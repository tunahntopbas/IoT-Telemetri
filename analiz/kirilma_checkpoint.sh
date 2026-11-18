#!/usr/bin/env bash
# Kırılma deneyi: checkpoint kaybolursa. Akış işi durdurulur, checkpoint klasörü silinmez, adı değiştirilerek
# yedeklenir (kayıp taklidi), iş checkpoint'siz yeniden başlatılır. Sonunda checkpoint geri konur.
#   analiz/kirilma_checkpoint.sh earliest   # konunun başından okur
#   analiz/kirilma_checkpoint.sh latest     # yalnızca yeni gelenleri okur
set -eu
cd "$(dirname "$0")/.."
MOD=$1; KONU=akis-ckpt-$MOD; TABLO=deney_ckpt_$MOD; ILER=analiz/cikti/akis-ckpt-$MOD.jsonl
KAYIT=kanit/kirilma-deneyi-checkpoint-$MOD.txt; CK=veri/checkpoint/$TABLO-db
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic $KONU --partitions 8 --replication-factor 1 2>&1 | grep -v JAVA_TOOL || true
rm -f $ILER
zaman() { date -u +%H:%M:%S; }
{
echo "KIRILMA DENEYİ: checkpoint kaybı, yeniden başlatmada --baslangic $MOD — konu $KONU, tablo $TABLO"
docker compose exec -T spark spark-submit is/akis_iskelet.py --konu $KONU --tablo $TABLO --ilerleme /kit/$ILER \
  > analiz/cikti/akis-ckpt-$MOD-1.log 2>&1 &
until [ -s $ILER ]; do sleep 2; done
echo "$(zaman) akış işi çalışıyor; üretici başlıyor (--hiz 50 --sure 240 --gec-orani 0)"
docker compose exec -T araclar python araclar/uretici.py --grup G221210024 --sablon iot --konu $KONU \
  --hiz 50 --sure 240 --gec-orani 0 > analiz/cikti/akis-ckpt-$MOD-uretici.txt 2>&1 &
sleep 90
docker exec bsm461-spark pkill -f "akis_iskelet.py --konu $KONU" || true
echo "$(zaman) akış işi durduruldu; son yığın: $(tail -1 $ILER | cut -c1-80)"
echo "  tablodaki olay toplamı: $(docker exec bsm461-timescaledb psql -U iot -d telemetri -tAc "SELECT sum(olay_sayisi) FROM $TABLO")"
mv $CK $CK.yedek
echo "$(zaman) checkpoint klasörü yedeğe alındı ($CK → $CK.yedek), 30 sn bekleniyor"
sleep 30
echo "$(zaman) iş checkpoint'siz başlatılıyor: --baslangic $MOD"
docker compose exec -T spark spark-submit is/akis_iskelet.py --konu $KONU --tablo $TABLO --baslangic $MOD \
  --ilerleme /kit/$ILER > analiz/cikti/akis-ckpt-$MOD-2.log 2>&1 &
until grep -q "toplam" analiz/cikti/akis-ckpt-$MOD-uretici.txt; do sleep 3; done
echo "$(zaman) üretici bitti: $(grep toplam analiz/cikti/akis-ckpt-$MOD-uretici.txt)"
sleep 40
docker exec bsm461-spark pkill -f "akis_iskelet.py --konu $KONU" || true
python3 analiz/ofset_ozet.py $ILER $MOD
sleep 5
echo "== Kafka ile karşılaştırma =="
docker compose exec -T spark spark-submit analiz/akis_dogrula.py --konu $KONU --tablo $TABLO --pencere \
  --deney z11_checkpoint_$MOD 2>&1 | grep -E "Kafka|\[|pencere|OLCUM|Error|Exception"
rm -rf $CK; mv $CK.yedek $CK
echo "$(zaman) özgün checkpoint geri kondu ($CK)"
} 2>&1 | tee $KAYIT
