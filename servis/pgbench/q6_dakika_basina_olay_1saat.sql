-- Panodaki "dakika başına olay" grafiği: son 1 saat
SELECT time_bucket('1 minute', pencere_baslangic) AS t, sum(olay_sayisi), sum(anomali_sayisi)
FROM sensor_dakika WHERE pencere_baslangic > now() - INTERVAL '1 hour'
GROUP BY 1 ORDER BY 1;
