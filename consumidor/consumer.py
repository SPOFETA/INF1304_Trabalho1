from confluent_kafka import Consumer
import json
import os
import signal
import socket
import sys


def obter_config():
    """Lê as configurações do consumidor a partir das variáveis de ambiente."""
    servidores = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    grupo = os.getenv("KAFKA_GROUP_ID")

    if not servidores:
        raise ValueError("KAFKA_BOOTSTRAP_SERVERS não está definido.")
    if not grupo:
        raise ValueError("KAFKA_GROUP_ID não está definido.")

    return {
        "servidores": servidores,
        "grupo": grupo,
        "topico": os.getenv("KAFKA_TOPICO", "dados-sensores"),
        "limite_temperatura": float(os.getenv("LIMITE_TEMPERATURA", "45.0")),
        "diretorio_log": os.getenv("DIRETORIO_LOG", "/app/logs"),
    }


def particoes_atribuidas(consumer, partitions):
    """Informa quais partições foram atribuídas ao consumidor após um rebalanceamento."""
    print(f"[REBALANCE] Partições atribuídas: {[p.partition for p in partitions]}")


def particoes_revogadas(consumer, partitions):
    """Informa quais partições foram retiradas do consumidor durante um rebalanceamento."""
    print(f"[REBALANCE] Partições revogadas: {[p.partition for p in partitions]}")


def registrar(arquivo_log, linha):
    """Exibe a linha no console e a grava no arquivo de log, para análise posterior."""
    print(linha)
    arquivo_log.write(linha + "\n")
    arquivo_log.flush()


def processar_mensagem(msg, arquivo_log, limite_temperatura):
    """Registra a leitura do sensor e gera um alerta se a temperatura passar do limite."""
    dados = json.loads(msg.value().decode("utf-8"))

    registrar(
        arquivo_log,
        f"Sensor: {dados['sensor_id']} | "
        f"Temperatura: {dados['temperatura']} | "
        f"Vibração: {dados['vibracao']} | "
        f"Partição: {msg.partition()}",
    )

    if dados["temperatura"] > limite_temperatura:
        registrar(
            arquivo_log,
            f"[ALERTA] Sensor {dados['sensor_id']} com temperatura "
            f"{dados['temperatura']} acima do limite de {limite_temperatura}",
        )


def encerrar(signum, frame):
    """Converte o SIGTERM do 'docker stop' em saída normal.

    Assim o consumidor fecha a conexão e sai do grupo na hora, disparando o
    rebalanceamento imediatamente (sem isso o Kafka esperaria o timeout de sessão).
    """
    sys.exit(0)


def iniciar_consumidor():
    """Inicia o consumidor e processa continuamente as mensagens recebidas."""
    config = obter_config()
    signal.signal(signal.SIGTERM, encerrar)

    consumer_id = socket.gethostname()
    os.makedirs(config["diretorio_log"], exist_ok=True)
    # Um arquivo por consumidor: evita dois containers escrevendo no mesmo arquivo
    caminho_log = os.path.join(config["diretorio_log"], f"{consumer_id}.log")

    consumidor = Consumer({
        "bootstrap.servers": config["servidores"],
        "group.id": config["grupo"],
        "client.id": consumer_id,  # identifica o consumidor no kafka-consumer-groups.sh
        "auto.offset.reset": "earliest",
    })
    consumidor.subscribe(
        [config["topico"]],
        on_assign=particoes_atribuidas,
        on_revoke=particoes_revogadas,
    )

    print(f"Consumidor {consumer_id} iniciado. Log em {caminho_log}")

    with open(caminho_log, "a", encoding="utf-8") as arquivo_log:
        try:
            while True:
                msg = consumidor.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    print(f"Erro ao consumir mensagem: {msg.error()}")
                    continue
                processar_mensagem(msg, arquivo_log, config["limite_temperatura"])
        except KeyboardInterrupt:
            pass
        finally:
            consumidor.close()  # sai do grupo de consumo, liberando as partições


if __name__ == "__main__":
    iniciar_consumidor()
