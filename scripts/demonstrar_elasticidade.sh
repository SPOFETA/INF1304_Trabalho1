#!/bin/sh
# Demonstra a elasticidade: aumenta o número de sensores e adiciona um terceiro
# consumidor com o sistema rodando. O Kafka redistribui as partições sozinho.
#
# Uso: ./scripts/demonstrar_elasticidade.sh [quantidade_de_sensores]

QUANTIDADE=${1:-4}
GRUPO=${GRUPO:-grupo-sensores}

echo "Escalando os sensores para $QUANTIDADE instâncias..."
docker compose up -d --no-deps --scale sensor-produtor="$QUANTIDADE" sensor-produtor

echo "Adicionando o consumer-3 ao grupo de consumo..."
docker compose up -d --no-deps consumer-3
sleep 15

docker compose ps sensor-produtor
docker compose exec -T kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka-1:9092 --describe --group "$GRUPO"
