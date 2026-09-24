# Atalhos para operar o sistema. Cada alvo só chama docker compose ou um script.
# --profile extra inclui o consumer-3 (usado na demonstração de elasticidade).

.PHONY: up down build logs ps demo kill-broker restart-broker kill-consumer restart-consumer elasticidade clean

up: ## Sobe o sistema: 3 brokers, criação do tópico, 2 sensores e 2 consumidores
	docker compose up -d --build

down: ## Para e remove os containers
	docker compose --profile extra down

build: ## Reconstrói as imagens do produtor e do consumidor
	docker compose build

logs: ## Acompanha os logs de todos os serviços
	docker compose --profile extra logs -f

ps: ## Lista os containers
	docker compose --profile extra ps

demo: ## Roda todos os testes em sequência e salva as evidências em logs/evidencias/
	sh ./scripts/executar_demo.sh

kill-broker: ## Simula a queda do broker kafka-2
	sh ./scripts/simular_falha_broker.sh

restart-broker: ## Restaura o broker kafka-2
	docker compose start kafka-2

kill-consumer: ## Simula a queda do consumer-1
	sh ./scripts/simular_falha_consumidor.sh

restart-consumer: ## Restaura o consumer-1
	docker compose start consumer-1

elasticidade: ## Escala os sensores e adiciona o consumer-3 (uso: make elasticidade N=4)
	sh ./scripts/demonstrar_elasticidade.sh $(N)

clean: ## Remove containers, volumes (dados do Kafka) e imagens do projeto
	docker compose --profile extra down -v --rmi local
