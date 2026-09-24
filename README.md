# Sistema de Monitoramento de Sensores em uma Fábrica Inteligente

Trabalho 1 de INF1304 — Balanceamento de Carga, Elasticidade e Failover com Kafka.

O relatório completo (arquitetura, testes de falha, elasticidade e resultados)
está em [`RELATORIO.md`](./RELATORIO.md).

## Requisitos

- Docker com o plugin Docker Compose (`docker compose version`)
- `make` (opcional: todos os alvos são atalhos para comandos `docker compose`)
- ~4 GB de RAM livres

## Uso rápido

```bash
make up      # sobe tudo: 3 brokers, tópico, 2 sensores, 2 consumidores
make logs    # acompanha os logs (Ctrl+C para sair)
make demo    # roda todos os testes e salva as evidências em logs/evidencias/
make down    # para tudo
```

> Se você já rodou uma versão anterior deste projeto, execute `make clean`
> antes do primeiro `make up`: os volumes antigos guardam um cluster com outra
> configuração e o tópico com o número errado de partições.

## Rodando no GitHub Codespaces

No Codespace, o Docker roda dentro de outro container e herda uma regra de
firewall (`iptables-legacy`, `FORWARD policy DROP`) que bloqueia a comunicação
entre containers. Os brokers sobem, mas não conseguem conversar entre si.
Antes do `make up` ou `make demo`, libere o tráfego com:

```bash
sudo iptables-legacy -I FORWARD 1 -j ACCEPT
```

A regra vale até o Codespace ser reiniciado. Em Docker Desktop ou Linux
comum, esse passo não é necessário.

## Testes individuais

| Comando | O que faz |
|---|---|
| `make kill-broker` / `make restart-broker` | Derruba / restaura o broker `kafka-2` |
| `make kill-consumer` / `make restart-consumer` | Derruba / restaura o `consumer-1` |
| `make elasticidade N=4` | Escala os sensores para N e adiciona o `consumer-3` |

Os dados processados por cada consumidor ficam em `logs/consumidores/`.
