# Relatório — Sistema de Monitoramento de Sensores em uma Fábrica Inteligente

**Disciplina:** INF1304 — Distribuição e Concorrência (2026/1)
**Tema:** Balanceamento de Carga, Elasticidade e Failover com Kafka em Clusters Docker
**Integrantes:**

| Nome | Matrícula |
|---|---|
| Miguel Mendes | 2111705 |
| Simão Oliveira | 2620261 |
| _(nome)_ | _(matrícula)_ |

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
| `kafka-1`, `kafka-2`, `kafka-3` | Cluster Kafka 4.2.0 em modo KRaft (sem Zookeeper). Cada nó é broker e controller. |
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

**No GitHub Codespaces**, rode antes `sudo iptables-legacy -I FORWARD 1 -j ACCEPT`
(o motivo está na seção 7). Em Docker Desktop ou Linux comum, não é necessário.

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

Exemplo de saída de um consumidor (trecho real de `01_estado_inicial.txt`):

```
[REBALANCE] Partições atribuídas: [0, 1]
Sensor: sensor-fabrica-5fc173b93ca4 | Temperatura: 50.08 | Vibração: 7.24 | Partição: 0
[ALERTA] Sensor sensor-fabrica-5fc173b93ca4 com temperatura 50.08 acima do limite de 45.0
```

## 6. Testes de falha e elasticidade

O comando `make demo` (script `scripts/executar_demo.sh`) executa os testes
abaixo em sequência. Para cada etapa ele grava em `logs/evidencias/` a descrição
do tópico (líder e réplicas de cada partição), a descrição do grupo de consumo
(qual consumidor lê cada partição) e os logs dos serviços no período.

### 6.1 Estado inicial — `01_estado_inicial.txt`

- **Esperado:** tópico com 3 partições e 2 réplicas cada, distribuídas entre os
  brokers; partições divididas entre `consumer-1` e `consumer-2`.
- **Resultado:** conforme o esperado. Cada broker lidera uma partição e cada
  partição tem réplica em outro broker:

  ```
  Partition: 0    Leader: 2    Replicas: 2,3    Isr: 2,3
  Partition: 1    Leader: 3    Replicas: 3,1    Isr: 3,1
  Partition: 2    Leader: 1    Replicas: 1,2    Isr: 1,2
  ```

  O Kafka dividiu as partições entre os consumidores
  (`consumer-1 → [0, 1]`, `consumer-2 → [2]`), e os alertas aparecem para
  leituras acima de 45,0, por exemplo:
  `[ALERTA] Sensor sensor-fabrica-f757762476a7 com temperatura 57.38 acima do limite de 45.0`.

### 6.2 Queda de broker — `02_falha_broker.txt`

- **Ação:** `docker compose stop kafka-2`.
- **Esperado:** as partições lideradas pelo broker 2 ganham outro líder, e o
  sistema segue produzindo e consumindo.
- **Resultado:** conforme o esperado. A partição 0 passou do líder 2 para o
  líder 3, e o broker 2 saiu da lista de réplicas sincronizadas:

  ```
  Partition: 0    Leader: 3    Replicas: 2,3    Isr: 3
  Partition: 2    Leader: 1    Replicas: 1,2    Isr: 1
  ```

  O produtor registrou a desconexão do `kafka-2` e continuou entregando uma
  leitura a cada 2 s, sem falhas de entrega (`Entregue -> partição ...`). Os
  consumidores continuaram processando normalmente.

### 6.3 Queda de consumidor — `03_falha_consumidor.txt`

- **Ação:** `docker compose stop consumer-1` e, depois, `docker compose start consumer-1`.
- **Esperado:** o `consumer-2` assume as partições do `consumer-1`; quando o
  `consumer-1` volta, a carga é dividida de novo.
- **Resultado:** conforme o esperado. O rebalanceamento levou cerca de 0,15 s:

  ```
  15:17:02.820  consumer-1  [REBALANCE] Partições revogadas: [0, 1]
  15:17:02.913  consumer-2  [REBALANCE] Partições revogadas: [2]
  15:17:02.977  consumer-2  [REBALANCE] Partições atribuídas: [0, 1, 2]
  ```

  Com o `consumer-1` fora, a descrição do grupo mostra as três partições com o
  `consumer-2`. Depois que ele volta, a divisão se refaz (`consumer-1 → [0, 1]`,
  `consumer-2 → [2]`). Nesse momento o `kafka-2`, religado no teste anterior,
  já aparece de novo no ISR (`Isr: 2,3`). A liderança da partição 0 continuou
  com o broker 3, porque o Kafka só devolve a liderança ao líder preferido
  periodicamente (a cada 5 minutos, por padrão).

### 6.4 Elasticidade — `04_elasticidade.txt`

- **Ação:** escala `sensor-produtor` para 4 réplicas e sobe o `consumer-3`,
  com o sistema em execução.
- **Esperado:** 4 sensores ativos e o grupo de consumo com 3 membros.
- **Resultado:** conforme o esperado. Os logs passam a mostrar leituras de 4
  sensores diferentes (`f757762476a7`, `5fc173b93ca4`, `9a34c5295be6` e
  `21c37624e4a6`). Cerca de 1 s após subir, o `consumer-3` recebeu uma partição,
  e o grupo ficou com uma partição por consumidor:

  ```
  consumer-1 → [0]    consumer-2 → [1]    consumer-3 → [2]
  ```

  Nenhum serviço precisou ser reiniciado.

## 7. O que funcionou e o que não funcionou

**Funcionou** (comprovado pelas evidências em `logs/evidencias/`):

- Cluster Kafka com 3 brokers, tópico com 3 partições e replicação 2
- Sensores como produtores em containers distintos, escaláveis
- Consumidores no mesmo grupo com divisão automática de partições
- Continuidade do serviço com um broker a menos
- Rebalanceamento quando um consumidor sai ou entra no grupo
- Detecção de temperatura acima do limite e gravação de leituras/alertas em arquivo

**Problemas encontrados durante o desenvolvimento:**

- Com a imagem `apache/kafka:3.9.0` os três brokers terminavam logo ao iniciar,
  com o erro `advertised.listeners cannot use the nonroutable meta-address 0.0.0.0`.
  A versão 3.9.0 passou a exigir um endereço anunciado para o listener do
  controller e não aceitava o `CONTROLLER://0.0.0.0:9093` herdado de
  `KAFKA_LISTENERS`. O código-fonte do Kafka mostra que isso foi corrigido a
  partir da 3.9.1 (o `0.0.0.0` passou a ser trocado pelo hostname do nó). A
  solução foi fixar a imagem em `apache/kafka:4.2.0`, a mesma apontada pela tag
  `latest` usada no material da disciplina.

- Com a versão corrigida, os brokers passaram a iniciar, mas desligavam após
  ~60 s com `unable to register with the controller quorum`. Os logs mostravam
  cada nó votando em si mesmo sem nunca receber resposta dos outros
  (`UNRECORDED`) e várias linhas `Disconnecting from node N due to socket
  connection setup timeout`: as conexões na porta 9093 nunca se completavam.
  Para separar o problema do Kafka, subimos dois containers simples (um
  servidor HTTP e um cliente) na mesma rede, e a conexão também dava timeout.
  A causa estava no ambiente: no GitHub Codespaces o Docker roda dentro de
  outro container, e a tabela antiga de firewall (`iptables-legacy`) tinha a
  política `FORWARD DROP`, que descartava todo tráfego entre containers. O
  Docker do projeto escreve suas regras de liberação na tabela nova (`nft`),
  mas o kernel aplica as duas. A solução foi liberar o encaminhamento com
  `sudo iptables-legacy -I FORWARD 1 -j ACCEPT`. Nenhuma mudança no projeto
  foi necessária para isso.

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
