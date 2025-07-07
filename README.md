# AS - CE (João Felipe Vilas Boas)

Este projeto foi desenvolvido como parte do Trabalho da disciplina de Computação Escalável da EMAP/FGV (Avaliação Substitutiva). O objetivo é realizar um experimento prático para comparar o desempenho e a corretude de três modelos de paralelismo distintos: Multiprocessamento local, Message Broker com workers e Apache Spark. 

## Objetivo

O sistema simula um cenário real de coleta e processamento de dados meteorológicos da rede "ClimaData". Ele gera um grande volume de dados sintéticos e os processa utilizando três arquiteturas paralelas diferentes para calcular um conjunto de métricas.  Os resultados de desempenho (tempo de execução) e corretude (precisão na detecção de anomalias) são exibidos em um dashboard interativo. 


## Funcionalidades

* **Gerador de Dados Sintéticos:** Cria um conjunto de dados meteorológicos customizável (volume e percentual de anomalias) para simular múltiplas estações. 
* **Três Soluções de Processamento Paralelo:**
    1.  **Multiprocessamento Local:** Utiliza o módulo `multiprocessing` do Python para processamento em memória compartilhada, explorando os múltiplos núcleos de uma única máquina. 
    2.  **Message Broker:** Implementa uma arquitetura com workers distribuídos que consomem tarefas de uma fila gerenciada pelo RabbitMQ. 
    3.  **Apache Spark:** Emprega o motor de processamento de dados distribuído Apache Spark para executar as agregações e cálculos. 
* **Dashboard Interativo:** Uma interface web construída com Streamlit  que permite ao usuário configurar e iniciar os experimentos, visualizando os resultados de desempenho e corretude em tempo real. 
* **Análise de Desempenho e Corretude:** O sistema não apenas mede o tempo de execução de cada abordagem, mas também valida a precisão da detecção de anomalias comparando os resultados com a lista de anomalias originalmente geradas. 


## Arquitetura e Tecnologias

* **Linguagem (Versão) Utilizada:** Python 3.11
* **Orquestração de Containers:** Docker e Docker Compose
* **Dashboard:** Streamlit
* **Message Broker:** RabbitMQ
* **Processamento Distribuído:** Apache Spark
* **Dependências Principais:** `pyspark`, `pika`, `faker`


## Como Executar o Projeto

Certifique-se de ter o **Docker Desktop** instalado e em execução na sua máquina. O processo de execução é totalmente containerizado para garantir a reprodutibilidade.

#### 1. Clonar o Repositório

Clone este projeto para a sua máquina local.

```bash
git clone https://github.com/Vilasz/AS-CE.git
```

#### 2. Verificar o Arquivo de Dependências

Garanta que o arquivo `requirements.txt` na raiz do projeto contém todas as bibliotecas necessárias. Exemplo:
```txt
streamlit
pandas
pika
pyspark
faker
```

#### 3. Construir a Imagem Docker (Build)

No terminal, na pasta raiz do projeto, execute o comando de build. Este passo lê o `Dockerfile`, instala o Java e todas as dependências Python, criando a imagem customizada para o dashboard.

```bash
docker-compose build
```
*(Este comando só precisa ser executado na primeira vez ou se você alterar o `Dockerfile` ou o `requirements.txt`)*

#### 4. Iniciar a Aplicação

Execute o comando abaixo para iniciar todos os serviços (RabbitMQ, Spark Master, Spark Worker e o Dashboard) em background.

```bash
docker-compose up
```

#### 5. Acessar o Dashboard

Abra seu navegador e acesse a seguinte URL:
**[http://localhost:8501](http://localhost:8501)**

#### 6. Executar o Experimento

Na interface do Streamlit:
1.  Configure os **Parâmetros do Experimento** na barra lateral (Número de Eventos, Percentual de Anomalias, Graus de Paralelismo).
2.  Clique no botão **" Iniciar Experimento"**.
3.  Acompanhe os resultados sendo preenchidos em tempo real nas abas "Desempenho" e "Corretude das Anomalias".

#### 7. Parar a Aplicação

Quando terminar, para parar e remover todos os containers, execute no terminal:
```bash
docker-compose down
```

## Monitoramento dos Serviços

Enquanto a aplicação estiver rodando, você pode monitorar os serviços auxiliares:

* **Logs em Tempo Real:** Para ver os logs de todos os containers, use o comando:
    ```bash
    docker-compose logs -f
    ```
* **RabbitMQ Management UI:** Acesse **[http://localhost:15672](http://localhost:15672)** para visualizar as filas e o tráfego de mensagens (login/senha: `guest`/`guest`).
* **Spark Master UI:** Acesse **[http://localhost:9090](http://localhost:9090)** para ver o status do cluster Spark e os jobs em execução.


## Estrutura do Projeto

```
.
├── core/                   # Módulos centrais (lógica de métricas)
├── dashboard/              # Código da aplicação Streamlit
├── data_generator/         # Script para geração de dados sintéticos
├── solution_message_broker/ # Implementação com RabbitMQ
├── solution_multiprocessing/ # Implementação com Multiprocessing
├── solution_spark/         # Implementação com Apache Spark
├── data/                   # Pasta para os dados gerados (CSV, JSON)
├── Dockerfile              # Define a imagem do container do dashboard
├── docker-compose.yml      # Orquestra todos os serviços
└── requirements.txt        # Dependências Python do projeto
```


## Análise dos Resultados

A análise detalhada dos resultados obtidos, incluindo a discussão sobre os trade-offs de desempenho, escalabilidade e complexidade de cada abordagem, deve ser documentada em um relatório separado, conforme solicitado pela disciplina.
