#!/bin/sh
# Executa todos os testes em sequência (estado inicial, queda de broker,
# queda de consumidor e elasticidade) e salva as evidências em logs/evidencias/.
# Cada arquivo traz o estado do tópico/grupo e os logs dos serviços no período.

TOPICO=${TOPICO:-dados-sensores}
GRUPO=${GRUPO:-grupo-sensores}
ESPERA=${ESPERA:-20}   # segundos observando o sistema em cada etapa
DIR=logs/evidencias
mkdir -p "$DIR"

agora() { date -u +%Y-%m-%dT%H:%M:%SZ; }

# Anexa ao arquivo $1 a descrição do tópico (líderes/réplicas) e do grupo (quem lê cada partição).
estado() {
  {
    echo "===== Tópico $TOPICO ($(agora)) ====="
    docker compose exec -T kafka-1 /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server kafka-1:9092 --describe --topic "$TOPICO"
    echo "===== Grupo $GRUPO ====="
    docker compose exec -T kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
      --bootstrap-server kafka-1:9092 --describe --group "$GRUPO"
  } >> "$1" 2>&1
}

# Anexa ao arquivo $1 os logs dos serviços informados, a partir do instante $2.
logs_desde() {
  arquivo=$1; desde=$2; shift 2
  echo "===== Logs de $* desde $desde =====" >> "$arquivo"
  docker compose logs --no-color --timestamps --since "$desde" "$@" >> "$arquivo" 2>&1
}

echo "[1/4] Subindo o sistema..."
INICIO=$(agora)
docker compose up -d --build
sleep "$ESPERA"
ARQ="$DIR/01_estado_inicial.txt"; : > "$ARQ"
estado "$ARQ"
logs_desde "$ARQ" "$INICIO" init-kafka consumer-1 consumer-2

echo "[2/4] Teste: queda do broker kafka-2..."
ARQ="$DIR/02_falha_broker.txt"; : > "$ARQ"
T=$(agora)
docker compose stop kafka-2
sleep "$ESPERA"
estado "$ARQ"
logs_desde "$ARQ" "$T" sensor-produtor consumer-1 consumer-2
docker compose start kafka-2
sleep "$ESPERA"

echo "[3/4] Teste: queda do consumer-1 (rebalanceamento)..."
ARQ="$DIR/03_falha_consumidor.txt"; : > "$ARQ"
T=$(agora)
docker compose stop consumer-1
sleep "$ESPERA"
estado "$ARQ"
echo "--- consumer-1 volta ao grupo ---" >> "$ARQ"
docker compose start consumer-1
sleep "$ESPERA"
estado "$ARQ"
logs_desde "$ARQ" "$T" consumer-1 consumer-2

echo "[4/4] Elasticidade: 4 sensores e um terceiro consumidor..."
ARQ="$DIR/04_elasticidade.txt"; : > "$ARQ"
T=$(agora)
docker compose up -d --no-deps --scale sensor-produtor=4 sensor-produtor
docker compose up -d --no-deps consumer-3
sleep "$ESPERA"
docker compose --profile extra ps >> "$ARQ" 2>&1
estado "$ARQ"
logs_desde "$ARQ" "$T" consumer-1 consumer-2 consumer-3

echo "Pronto. Evidências em $DIR/:"
ls -1 "$DIR"
