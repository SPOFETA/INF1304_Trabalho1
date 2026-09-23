from confluent_kafka import Producer
import json
import random
import os
import time


def obter_config():
    servidores = os.getenv('KAFKA_BOOTSTRAP_SERVERS', None)
    return servidores
    
def gerar_dados():
    temperatura: float = random.uniform(-5,60).__round__(2)
    vibracao: float = abs(random.normalvariate(3,2).__round__(2))
    
    dados = {
        "temperatura": temperatura,
        "vibracao": vibracao
    }

    return dados

def dict_para_json():
    dados = gerar_dados()
    dados_json = json.dumps(dados)
    return dados_json

def iniciar_sensores():
    servidores = obter_config()
    produtor = Producer({'bootstrap.servers': servidores}) # bootstrap: Fornece os hosts iniciais que servem como ponto de partida para que um cliente Kafka descubra o conjunto completo de servidores ativos

    while True:
        produtor.produce('dados-sensores', dict_para_json().encode('utf-8'))
        time.sleep(2)


