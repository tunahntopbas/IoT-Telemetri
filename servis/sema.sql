-- Servis deposu şeması (PostgreSQL 16 + TimescaleDB). Kapsayıcı ilk kez başlarken kendiliğinden çalışır.
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Boyut tablosu: s3://ham/boyut/iot/sensorler.csv (is/servis_yukle.py doldurur)
CREATE TABLE IF NOT EXISTS sensorler (
    sensor_id     text PRIMARY KEY,
    bolge         text NOT NULL,
    model         text NOT NULL,
    kurulum_yili  int  NOT NULL
);

-- Akış sonucu: sensör başına 1 dakikalık pencere. Akış işi (is/akis_iskelet.py) her tetikte
-- güncellenen pencereleri ON CONFLICT ... DO UPDATE ile yazar: aynı pencere iki kez yazılsa da sonuç aynı.
CREATE TABLE IF NOT EXISTS sensor_dakika (
    pencere_baslangic timestamptz NOT NULL,
    sensor_id         text        NOT NULL,
    bolge             text,
    olay_sayisi       int         NOT NULL,
    anomali_sayisi    int         NOT NULL,
    max_titresim      double precision,
    ort_sicaklik      double precision,
    son_olay          timestamptz,           -- penceredeki en geç olay zamanı
    son_varis         timestamptz,           -- penceredeki olayların Kafka'ya en son ulaştığı an
    yazilma           timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (sensor_id, pencere_baslangic)
);
SELECT create_hypertable('sensor_dakika', by_range('pencere_baslangic', INTERVAL '1 day'), if_not_exists => TRUE);

-- Her mikro yığın için bir satır. Birincil anahtar YOK: iş çöküp aynı yığını yeniden işlerse
-- burada aynı batch_id iki kez görünür (en az bir kez teslimin kanıtı).
CREATE TABLE IF NOT EXISTS batch_gunlugu (
    sorgu       text        NOT NULL,
    batch_id    bigint      NOT NULL,
    satir       int         NOT NULL,
    max_varis   timestamptz,
    yazilma     timestamptz NOT NULL DEFAULT now()
);

-- Toplu sonuçlar (Delta'dan is/servis_yukle.py ile)
CREATE TABLE IF NOT EXISTS toplu1 (
    bolge text, saat_tr int, olay bigint, ort_sicaklik double precision, ort_titresim double precision,
    p95_titresim double precision, anomali bigint, PRIMARY KEY (bolge, saat_tr)
);
CREATE TABLE IF NOT EXISTS toplu2 (
    sensor_id text PRIMARY KEY, gelen bigint, anomali bigint, model text, kurulum_yili int,
    beklenen bigint, kayip_orani double precision, anomali_orani double precision
);

-- Akış 1: son 10 dakikada titreşim eşiğini (1,0) aşan sensörler
CREATE OR REPLACE VIEW anomali_son10dk AS
SELECT sensor_id, max(bolge) AS bolge, sum(anomali_sayisi) AS anomali, max(max_titresim) AS max_titresim,
       max(son_varis) AS son_varis
FROM sensor_dakika
WHERE pencere_baslangic > now() - INTERVAL '10 minutes'
GROUP BY sensor_id
HAVING sum(anomali_sayisi) > 0;

-- Akış 2: 60 saniyedir hiç olayı Kafka'ya ulaşmayan sensörler. Varış zamanı kullanılıyor, olay zamanı değil:
-- geç gelen bir olay sensörü "susmuş" göstermesin. Hiç olay gelmeyen sensör de görünsün diye boyut tablosundan başlanıyor.
CREATE OR REPLACE VIEW susan_sensorler AS
SELECT s.sensor_id, s.bolge, s.model, son.son_varis, now() - son.son_varis AS sessizlik
FROM sensorler s
LEFT JOIN LATERAL (
    SELECT max(d.son_varis) AS son_varis FROM sensor_dakika d
    WHERE d.sensor_id = s.sensor_id AND d.pencere_baslangic > now() - INTERVAL '30 minutes'
) son ON TRUE
WHERE son.son_varis IS NULL OR son.son_varis < now() - INTERVAL '60 seconds';
