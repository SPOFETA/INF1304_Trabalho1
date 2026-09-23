from confluent_kafka import Producer
import json
import random
import os
import time


def obter_config():
    """Lê as configurações de ambiente, falhando rapidamente se não existirem."""
    servidores = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
    if not servidores:
        raise ValueError('KAFKA_BOOTSTRAP_SERVERS não está definido.')
    return servidores
    
def gerar_dados(sensor_id):
    """Gera um dicionário simulando a leitura de temperatura e vibração."""
    temperatura: float = round(random.uniform(-5,60),2)
    vibracao: float = abs(round(random.normalvariate(3,2),2))

    dados = {
        "sensor_id": sensor_id,
        "temperatura": temperatura,
        "vibracao": vibracao
    }

    return dados

def dict_para_json(sensor_id):
    """Converte o dicionário de dados gerado para o formato JSON."""
    dados = gerar_dados(sensor_id)
    return json.dumps(dados)

def iniciar_sensores():
    """Inicia o ciclo de vida do produtor, enviando leituras continuamente."""
    servidores = obter_config()
    sensor_id = os.getenv('SENSOR_ID', 'desconhecido')
    produtor = Producer({'bootstrap.servers': servidores}) # bootstrap: Fornece os hosts iniciais que servem como ponto de partida para que um cliente Kafka descubra o conjunto completo de servidores ativos
    try:
        while True:
            produtor.produce('dados-sensores', key=sensor_id.encode('utf-8'), value=dict_para_json(sensor_id).encode('utf-8'), callback=relatar_entrega)
            produtor.poll(0)
            time.sleep(2)
    except KeyboardInterrupt:
        pass
    finally:
        produtor.flush()

def relatar_entrega(err, msg):
    """Callback executado pelo Kafka para confirmar o sucesso ou falha da entrega."""
    if err is not None:
        print(f'Falha ao entregar mensagem do sensor {msg.key()}: {err}')
    else:
        print(f'Entrege: sensor {msg.key()} -> particao {msg.partition()}')

if __name__ == "__main__": iniciar_sensores()