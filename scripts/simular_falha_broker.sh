#!/bin/sh
# Simula a queda de um broker Kafka (kafka-2) e mostra o estado do tópico depois.
# Esperado: as partições que tinham o kafka-2 como líder passam a ter outro líder,
# e produtores/consumidores continuam funcionando.

TOPICO=${TOPICO:-dados-sensores}

echo "Derrubando o broker kafka-2..."
docker compose stop kafka-2

echo "Aguardando o cluster reeleger os líderes..."
sleep 10

docker compose exec -T kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 --describe --topic "$TOPICO"

echo "Acompanhe: docker compose logs -f sensor-produtor consumer-1 consumer-2"
echo "Restaurar:  make restart-broker"
