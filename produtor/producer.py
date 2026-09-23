from confluent_kafka import Producer
import json
import random
import os
import time


# bootstrap: Fornece os hosts iniciais que servem como ponto de partida
#  para que um cliente Kafka descubra o conjunto completo de servidores ativos
servidores = os.getenv('KAFKA_BOOTSTRAP_SERVERS', None)
producer = Producer({'bootstrap.servers': servidores})

def gerar_dados():
    temperatura: float = random.uniform(-5,60).__round__(2)
    vibracao: float = abs(random.normalvariate(3,2).__round__(2))
    
    dados = {
        "temperatura": temperatura,
        "vibracao": vibracao
    }

    return dados
    