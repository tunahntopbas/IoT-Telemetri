# -*- coding: utf-8 -*-
"""Spark işlerinden servis deposuna (PostgreSQL + TimescaleDB) yazmak için ortak parçalar.
Bağlantı Spark'a eklenen PostgreSQL JDBC sürücüsüyle (spark-defaults.conf) JVM üzerinden kuruluyor."""
URL = "jdbc:postgresql://timescaledb:5432/telemetri"
KULLANICI, SIFRE = "iot", "iot-gizli"
JDBC = {"url": URL, "user": KULLANICI, "password": SIFRE, "driver": "org.postgresql.Driver"}


def baglanti(spark):
    # Sürücü Spark'ın paket sınıf yükleyicisinde; DriverManager onu göremediği için doğrudan sürücü nesnesi kullanılıyor.
    jvm = spark._jvm
    ozellik = jvm.java.util.Properties()
    ozellik.setProperty("user", KULLANICI)
    ozellik.setProperty("password", SIFRE)
    return jvm.org.postgresql.Driver().connect(URL, ozellik)


def calistir(spark, *komutlar):
    c = baglanti(spark)
    try:
        st = c.createStatement()
        for k in komutlar:
            st.execute(k)
    finally:
        c.close()
