-- Sensör X'in son 10 dakikadaki dakikalık ölçümleri (akıştan gelen güncel veri)
\set s random(1, 200)
SELECT pencere_baslangic, olay_sayisi, anomali_sayisi, max_titresim, ort_sicaklik
FROM sensor_dakika
WHERE sensor_id = 'sensor-' || lpad(:s::text, 3, '0') AND pencere_baslangic > now() - INTERVAL '10 minutes'
ORDER BY pencere_baslangic;
