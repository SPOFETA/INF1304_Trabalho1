from confluent_kafka import Consumer
import json
import os
import socket


def obter_config():
    """Lê as configurações do consumidor a partir das variáveis de ambiente."""
    servidores = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    grupo = os.getenv("KAFKA_GROUP_ID")

    if not servidores:
        raise ValueError("KAFKA_BOOTSTRAP_SERVERS não está definido.")

    if not grupo:
        raise ValueError("KAFKA_GROUP_ID não está definido.")

    return servidores, grupo


def particoes_atribuidas(consumer, partitions):
    """Informa quais partições foram atribuídas ao consumidor após um rebalanceamento."""
    ids = [particao.partition for particao in partitions]
    print(f"[REBALANCE] Partições atribuídas: {ids}")


def particoes_revogadas(consumer, partitions):
    """Informa quais partições foram retiradas do consumidor durante um rebalanceamento."""
    ids = [particao.partition for particao in partitions]
    print(f"[REBALANCE] Partições revogadas: {ids}")


def processar_mensagem(msg):
    """Converte e exibe os dados recebidos do sensor."""
    dados = json.loads(msg.value().decode("utf-8"))

    print(
        f"Sensor: {dados['sensor_id']} | "
        f"Temperatura: {dados['temperatura']} | "
        f"Vibração: {dados['vibracao']} | "
        f"Partição: {msg.partition()}"
    )


def iniciar_consumidor():
    """Inicia o consumidor e processa continuamente as mensagens recebidas."""
    servidores, grupo = obter_config()

    consumer_id = socket.gethostname()

    consumidor = Consumer({
        "bootstrap.servers": servidores,
        "group.id": grupo,
        "auto.offset.reset": "earliest"
    })

    consumidor.subscribe(
        ["dados-sensores"],
        on_assign=particoes_atribuidas,
        on_revoke=particoes_revogadas
    )

    print(f"Consumidor {consumer_id} iniciado.")

    try:
        while True:
            msg = consumidor.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                print(f"Erro ao consumir mensagem: {msg.error()}")
                continue

            processar_mensagem(msg)

    except KeyboardInterrupt:
        pass

    finally:
        consumidor.close()


if __name__ == "__main__":
    iniciar_consumidor()