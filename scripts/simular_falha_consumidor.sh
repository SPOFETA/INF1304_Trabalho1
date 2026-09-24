#!/bin/sh
# Simula a queda do consumer-1 e mostra o rebalanceamento no consumer-2.
# Esperado: o consumer-2 registra "[REBALANCE] Partições atribuídas" com
# todas as partições, inclusive as que eram do consumer-1.

GRUPO=${GRUPO:-grupo-sensores}

echo "Derrubando o consumer-1..."
docker compose stop consumer-1
sleep 10

docker compose logs --tail 20 consumer-2 | grep REBALANCE
docker compose exec -T kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka-1:9092 --describe --group "$GRUPO"

echo "Restaurar: make restart-consumer"
