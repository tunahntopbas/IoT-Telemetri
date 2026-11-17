-- Sensör X'in geçmişteki rastgele bir 10 dakikası (14 günlük veri, 2,1 milyon satır)
\set s random(1, 200)
\set d random(0, 20000)
SELECT pencere_baslangic, olay_sayisi, anomali_sayisi, max_titresim, ort_sicaklik
FROM sensor_dakika
WHERE sensor_id = 'sensor-' || lpad(:s::text, 3, '0')
  AND pencere_baslangic >= TIMESTAMPTZ '2026-11-03 00:00+00' + :d * INTERVAL '1 minute'
  AND pencere_baslangic <  TIMESTAMPTZ '2026-11-03 00:10+00' + :d * INTERVAL '1 minute'
ORDER BY pencere_baslangic;
