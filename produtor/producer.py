from confluent_kafka import Producer
import json
import os
import random
import signal
import socket
import sys
import time


def obter_config():
    """Lê as configurações do produtor a partir das variáveis de ambiente."""
    servidores = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    if not servidores:
        raise ValueError("KAFKA_BOOTSTRAP_SERVERS não está definido.")

    topico = os.getenv("KAFKA_TOPICO", "dados-sensores")
    intervalo = float(os.getenv("INTERVALO_ENVIO", "2"))
    return servidores, topico, intervalo


def gerar_dados(sensor_id):
    """Gera um dicionário simulando a leitura de temperatura e vibração."""
    return {
        "sensor_id": sensor_id,
        "temperatura": round(random.uniform(-5, 60), 2),
        "vibracao": abs(round(random.normalvariate(3, 2), 2)),
    }


def relatar_entrega(err, msg):
    """Callback executado pelo Kafka para confirmar o sucesso ou falha da entrega."""
    if err is not None:
        print(f"Falha ao entregar mensagem: {err}")
    else:
        print(f"Entregue -> partição {msg.partition()}")


def encerrar(signum, frame):
    """Converte o SIGTERM do 'docker stop' em saída normal, para o finally rodar."""
    sys.exit(0)


def iniciar_sensor():
    """Inicia o ciclo de vida do produtor, enviando leituras continuamente."""
    servidores, topico, intervalo = obter_config()
    signal.signal(signal.SIGTERM, encerrar)

    # O hostname do container diferencia os sensores quando o serviço é escalado
    sensor_id = f"{os.getenv('SENSOR_NOME', 'desconhecido')}-{socket.gethostname()}"

    produtor = Producer({"bootstrap.servers": servidores})
    print(f"Sensor {sensor_id} iniciado, enviando para o tópico '{topico}'.")

    try:
        while True:
            # Sem chave: o Kafka distribui as mensagens entre as partições,
            # o que permite ver a carga sendo dividida entre os consumidores.
            produtor.produce(
                topico,
                value=json.dumps(gerar_dados(sensor_id)).encode("utf-8"),
                callback=relatar_entrega,
            )
            produtor.poll(0)
            time.sleep(intervalo)
    except KeyboardInterrupt:
        pass
    finally:
        # Tenta enviar as mensagens pendentes antes de sair. O limite de tempo evita
        # travar o encerramento se nenhum broker estiver acessível.
        produtor.flush(5)


if __name__ == "__main__":
    iniciar_sensor()
