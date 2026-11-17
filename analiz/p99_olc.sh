#!/usr/bin/env bash
# Servis deposu sorgularının gecikmesi: her sorgu pgbench ile 1000 kez (tek istemci) çalıştırılır,
# işlem başına gecikme günlüğünden p50 / p95 / p99 / en çok hesaplanır.
#   analiz/p99_olc.sh [tekrar]
set -eu
cd "$(dirname "$0")/.."
N=${1:-1000}
docker exec bsm461-timescaledb sh -c 'rm -rf /tmp/pgb && mkdir -p /tmp/pgb'
docker cp servis/pgbench/. bsm461-timescaledb:/tmp/pgb/
for f in servis/pgbench/*.sql; do
  ad=$(basename "$f" .sql)
  docker exec -w /tmp/pgb bsm461-timescaledb sh -c \
    "rm -f gunluk.* && pgbench -n -c 1 -t $N -f $ad.sql -l --log-prefix=gunluk -U iot -h 127.0.0.1 telemetri > /dev/null 2>&1 && cat gunluk.*" \
  | awk -v ad="$ad" '{print $3/1000}' | sort -n | awk -v ad="$ad" '{a[NR]=$1} END{
      n=NR;
      printf "%s n=%d p50=%.2f p95=%.2f p99=%.2f max=%.2f ms\n", ad, n, a[int(n*0.50)], a[int(n*0.95)], a[int(n*0.99)], a[n]}'
done
