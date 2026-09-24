# Relatório — Sistema de Monitoramento de Sensores em uma Fábrica Inteligente

**Disciplina:** INF1304 — Distribuição e Concorrência (2026/1)
**Tema:** Balanceamento de Carga, Elasticidade e Failover com Kafka em Clusters Docker
**Integrantes:** _(preencher)_

---

## 1. Introdução

O sistema simula o monitoramento de uma fábrica inteligente: sensores publicam
leituras de temperatura e vibração em um tópico Kafka e um grupo de consumidores
processa essas leituras em paralelo, gerando alertas quando a temperatura passa
de um limite. O sistema continua funcionando quando um broker ou um consumidor
cai, e aceita novos sensores e consumidores sem ser reiniciado.

## 2. Arquitetura

```
   sensor-produtor (N réplicas)
            │  JSON: {sensor_id, temperatura, vibracao}
            ▼
  ┌──────────────── tópico dados-sensores ────────────────┐
  │   3 partições, fator de replicação 2                  │
  │   kafka-1  ◄──►  kafka-2  ◄──►  kafka-3               │
  │   (cada nó é broker + controller, modo KRaft)         │
  └───────────────────────────────────────────────────────┘
            │  grupo de consumo: grupo-sensores
            ▼
  consumer-1   consumer-2   (consumer-3, na demonstração)
            │
            ▼
  logs/consumidores/<consumidor>.log  (leituras + alertas)
```

| Serviço | Papel |
|---|---|
| `kafka-1`, `kafka-2`, `kafka-3` | Cluster Kafka em modo KRaft (sem Zookeeper). Cada nó é broker e controller. |
| `init-kafka` | Container temporário: espera o cluster responder, cria o tópico `dados-sensores` e termina. |
| `sensor-produtor` | Sensor simulado (Python). Envia uma leitura a cada `INTERVALO_ENVIO` segundos. Sobe com 2 réplicas e pode ser escalado. |
| `consumer-1`, `consumer-2` | Processadores (Python) no mesmo grupo de consumo; o Kafka divide as partições entre eles. |
| `consumer-3` | Consumidor extra (profile `extra`), usado para demonstrar elasticidade. |

### Decisões de projeto

- **3 nós Kafka em vez de 2.** Em modo KRaft, os controllers precisam de
  maioria para eleger líderes de partição. Com 2 nós a maioria é 2, então a
  queda de qualquer um paralisaria o cluster. Com 3 nós, a queda de 1 ainda deixa
  2 ativos (maioria), e o teste de falha de broker funciona.
- **3 partições, replicação 2.** Cada partição tem cópia em dois brokers, então
  a queda de um broker não perde dados nem deixa partições sem líder. Com 3
  partições é possível ver a divisão entre 2 e depois 3 consumidores.
- **Criação do tópico controlada.** A auto-criação de tópicos está desligada e
  produtores/consumidores só sobem depois que o `init-kafka` termina com
  sucesso (`depends_on: condition: service_completed_successfully`). Sem isso,
  o produtor poderia criar o tópico antes, com 1 partição e sem replicação.
- **Mensagens sem chave.** O produtor não define chave, então o Kafka distribui
  as leituras entre as partições e a carga se divide entre os consumidores.
  A contrapartida é não garantir ordem por sensor, o que não é necessário para
  a detecção por limite de temperatura.
- **Encerramento limpo.** Produtor e consumidor tratam o `SIGTERM` enviado pelo
  `docker stop`. O consumidor chama `close()` e sai do grupo na hora, então o
  rebalanceamento é imediato em vez de esperar o timeout de sessão (~45 s).
- **Persistência simples.** Cada consumidor grava leituras e alertas em um
  arquivo próprio em `logs/consumidores/`, montado como volume no host. Um
  arquivo por consumidor evita dois processos escrevendo no mesmo arquivo.

## 3. Configuração

Nenhum valor de configuração está fixo no código: tudo vem de variáveis de
ambiente definidas no `docker-compose.yml`.

| Variável | Usada por | Descrição | Valor no compose |
|---|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | todos | Endereços dos brokers | `kafka-1:9092,kafka-2:9092,kafka-3:9092` |
| `KAFKA_TOPICO` | todos | Nome do tópico | `dados-sensores` |
| `KAFKA_PARTICOES` / `KAFKA_REPLICACAO` | init-kafka | Partições e replicação do tópico | `3` / `2` |
| `SENSOR_NOME` | produtor | Prefixo do id do sensor (completado com o hostname) | `sensor-fabrica` |
| `INTERVALO_ENVIO` | produtor | Segundos entre leituras | `2` |
| `KAFKA_GROUP_ID` | consumidores | Grupo de consumo compartilhado | `grupo-sensores` |
| `LIMITE_TEMPERATURA` | consumidores | Temperatura acima da qual há alerta | `45.0` |
| `DIRETORIO_LOG` | consumidores | Onde gravar o log (padrão no código) | `/app/logs` |

## 4. Instalação

Pré-requisitos: Docker com o plugin Compose, `make` (opcional) e ~4 GB de RAM livres.

```bash
cd INF1304_Trabalho1-main
make up          # = docker compose up -d --build
make ps          # confere se tudo subiu (init-kafka deve aparecer como "exited (0)")
```

Se uma versão anterior do projeto já foi executada na máquina, rode `make clean`
antes, para apagar os volumes antigos do Kafka.

## 5. Operação

| Comando | Efeito |
|---|---|
| `make up` / `make down` | Sobe / para o sistema |
| `make logs` | Acompanha os logs de todos os serviços |
| `docker compose logs -f consumer-1 consumer-2` | Só os consumidores (leituras e `[ALERTA]`) |
| `make demo` | Executa todos os testes e salva as evidências |
| `make kill-broker` / `make restart-broker` | Derruba / restaura o `kafka-2` |
| `make kill-consumer` / `make restart-consumer` | Derruba / restaura o `consumer-1` |
| `make elasticidade N=4` | Escala os sensores para N e adiciona o `consumer-3` |
| `make clean` | Remove containers, volumes e imagens do projeto |

Exemplo de saída de um consumidor:

```
[REBALANCE] Partições atribuídas: [0, 1]
Sensor: sensor-fabrica-3f2a1c9e0b7d | Temperatura: 52.31 | Vibração: 2.87 | Partição: 1
[ALERTA] Sensor sensor-fabrica-3f2a1c9e0b7d com temperatura 52.31 acima do limite de 45.0
```

## 6. Testes de falha e elasticidade

O comando `make demo` (script `scripts/executar_demo.sh`) executa os testes
abaixo em sequência. Para cada etapa ele grava em `logs/evidencias/` a descrição
do tópico (líder e réplicas de cada partição), a descrição do grupo de consumo
(qual consumidor lê cada partição) e os logs dos serviços no período.

### 6.1 Estado inicial — `01_estado_inicial.txt`

- **O que verificar:** o tópico tem 3 partições, cada uma com 2 réplicas
  (`ReplicationFactor: 2`) distribuídas entre os brokers; no grupo, as 3
  partições estão divididas entre `consumer-1` e `consumer-2`.

### 6.2 Queda de broker — `02_falha_broker.txt`

- **Ação:** `docker compose stop kafka-2`.
- **Esperado:** as partições que tinham o broker 2 como líder passam a ter
  outro líder, e o broker 2 some da coluna `Isr`. Os logs do período mostram os
  sensores recebendo `Entregue -> partição ...` e os consumidores processando
  leituras normalmente.

### 6.3 Queda de consumidor — `03_falha_consumidor.txt`

- **Ação:** `docker compose stop consumer-1` e, depois, `docker compose start consumer-1`.
- **Esperado:** o `consumer-2` registra `[REBALANCE] Partições revogadas` e em
  seguida `[REBALANCE] Partições atribuídas: [0, 1, 2]`, assumindo as partições
  do `consumer-1`. A descrição do grupo mostra só o `consumer-2`. Quando o
  `consumer-1` volta, há um novo rebalanceamento e as partições são
  divididas outra vez.

### 6.4 Elasticidade — `04_elasticidade.txt`

- **Ação:** escala `sensor-produtor` para 4 réplicas e sobe o `consumer-3`.
- **Esperado:** aparecem 4 containers de sensor, com leituras de 4 `sensor_id`
  diferentes. O grupo de consumo passa a ter 3 membros, cada um com uma
  partição, sem que nenhum serviço tenha sido reiniciado.

## 7. O que funcionou e o que não funcionou

**Funcionou:** _(conferir com as evidências de `make demo` antes da entrega)_

- Cluster Kafka com 3 brokers, tópico com 3 partições e replicação 2
- Sensores como produtores em containers distintos, escaláveis
- Consumidores no mesmo grupo com divisão automática de partições
- Continuidade do serviço com um broker a menos
- Rebalanceamento quando um consumidor sai ou entra no grupo
- Detecção de temperatura acima do limite e gravação de leituras/alertas em arquivo

**Limitações conhecidas:**

- Os dados processados são gravados em arquivo de texto, não em banco de dados.
- Se um consumidor for encerrado à força (`docker kill`), o rebalanceamento só
  acontece após o timeout de sessão do Kafka (~45 s); com `docker stop` é imediato.
- O sistema tolera a queda de **um** broker. Com dois brokers fora, o quorum de
  controllers é perdido.
- Mais consumidores que partições (mais de 3) não aumentam o paralelismo: os
  consumidores extras ficam ociosos.
- Os scripts de teste consultam o estado do cluster pelo `kafka-1`, então ele
  não deve ser o broker derrubado nos testes.

## 8. Conclusão

A replicação das partições, o quorum de 3 controllers e o grupo de consumo
do Kafka garantem os três requisitos pedidos: tolerância a falhas (queda de
broker ou de consumidor não interrompe o fluxo), balanceamento de carga
(partições divididas automaticamente entre os consumidores) e elasticidade
(novos sensores e consumidores entram com o sistema em execução).
